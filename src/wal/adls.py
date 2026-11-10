"""
Put sealed WAL files on Azure Data Lake Storage Gen2 (ADLS), atomically.

``AdlsPut`` is a drop-in ``put(local, rel)`` for ``Uploader`` (same contract
as ``uploader.put_file``), for a collector running outside Databricks::

    put = AdlsPut("abfss://landing@myaccount.dfs.core.windows.net/polymarket")
    put(Path("data/wal/2026-11-10/000003.jsonl.gz"),
        "ws/2026-11-10/host-ab12cd34-2026-11-10-000003.jsonl.gz")
    # -> abfss://landing@myaccount.dfs.core.windows.net/polymarket/ws/2026-11-10/...

Why staging + rename: an ADLS upload is create -> append -> flush, and the
file is visible (empty or partial) before the flush. A reader such as Auto
Loader listing ``ws/`` at that moment would record a partial file as done
and never read it again. So the file is uploaded to ``{prefix}/_staging/``
first, its size checked, then renamed into place. With the hierarchical
namespace (required: ADLS Gen2, HNS on) a rename is atomic.

Re-uploading the same ``rel`` overwrites the target: for a given source the
same name always carries the same bytes, so this is safe and lets the
uploader retry blindly.

Auth: ``DefaultAzureCredential`` (``az login`` locally; service principal
env vars or managed identity elsewhere). The identity needs "Storage Blob
Data Contributor" on the container.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient

from src.wal.uploader import STAGING_DIR, check_rel, staging_name

logger = logging.getLogger(__name__)

# Upload in pieces of this size. The SDK reads one piece into memory at a time;
# its default is 100 MB, too much next to a 1 GiB segment (~190 MB gzipped) on
# a 1 GiB VM. 8 MB keeps memory flat; same-region throughput barely changes.
UPLOAD_CHUNK_BYTES = 8 * 1024 * 1024

# abfss://<file system>@<host>[/<prefix>]
_ABFSS_RE = re.compile(r"^abfss://(?P<fs>[^@/]+)@(?P<host>[^/]+)(?:/(?P<prefix>.*))?$")


@dataclass(frozen=True)
class AdlsLocation:
    """
    Where files go, parsed from an ``abfss://`` URL.

    例子:
        AdlsLocation.parse("abfss://landing@acct.dfs.core.windows.net/polymarket/")
        -> AdlsLocation(account_url="https://acct.dfs.core.windows.net",
                        file_system="landing", prefix="polymarket")
    """

    account_url: str    # https://<account>.dfs.core.windows.net
    file_system: str    # container
    prefix: str         # folder inside the container, no leading/trailing "/", may be ""

    @classmethod
    def parse(cls, url: str) -> "AdlsLocation":
        m = _ABFSS_RE.match(url)
        if m is None:
            raise ValueError(
                f"not an ADLS URL: {url!r} "
                "(expected abfss://<container>@<account>.dfs.core.windows.net[/<folder>])"
            )
        prefix = (m.group("prefix") or "").strip("/")
        if prefix:
            check_rel(prefix)
        return cls(account_url=f"https://{m.group('host')}", file_system=m.group("fs"), prefix=prefix)

    def path(self, rel: str) -> str:
        """Path inside the container for ``rel``: ``{prefix}/{rel}``."""
        return f"{self.prefix}/{rel}" if self.prefix else rel

    def url(self, rel: str = "") -> str:
        host = self.account_url.removeprefix("https://")
        return f"abfss://{self.file_system}@{host}/{self.path(rel)}".rstrip("/")


class AdlsPut:
    """
    ``put(local, rel)`` that uploads ``local`` to ``{url}/{rel}`` on ADLS.

    Raises on failure and leaves no staging file behind; the caller (the
    ``Uploader``) decides whether to retry. Sync: ``Uploader`` runs it in a
    worker thread, one upload at a time.

    file_system_client: inject a client (tests); otherwise one is built from
                        the URL and ``credential`` (default: DefaultAzureCredential).
    """

    def __init__(
        self,
        url: str,
        *,
        credential: Any = None,
        file_system_client: FileSystemClient | None = None,
    ) -> None:
        self.location = AdlsLocation.parse(url)
        self._owns_client = file_system_client is None
        if file_system_client is None:
            if credential is None:
                from azure.identity import DefaultAzureCredential
                credential = DefaultAzureCredential()
            service = DataLakeServiceClient(self.location.account_url, credential=credential)
            file_system_client = service.get_file_system_client(self.location.file_system)
        self._fs = file_system_client
        # Directories known to exist, so each is checked once per process.
        self._known_dirs: set[str] = set()

    def __repr__(self) -> str:
        return f"AdlsPut({self.location.url()!r})"

    def __call__(self, local: Path, rel: str) -> None:
        check_rel(rel)
        local = Path(local)
        size = local.stat().st_size
        final_path = self.location.path(rel)
        tmp = self._fs.get_file_client(self.location.path(f"{STAGING_DIR}/{staging_name(rel)}"))
        try:
            with open(local, "rb") as fh:
                tmp.upload_data(fh, length=size, overwrite=True,
                                chunk_size=UPLOAD_CHUNK_BYTES, max_concurrency=1)
            remote_size = tmp.get_file_properties().size
            if remote_size != size:
                raise OSError(f"size mismatch uploading {local}: local {size} B, remote {remote_size} B")
            self._ensure_dir(str(PurePosixPath(final_path).parent))
            self._rename(tmp, final_path)
        except BaseException:
            self._delete_quietly(tmp)
            raise

    def close(self) -> None:
        """Close the HTTP connections (only if this object created the client)."""
        if self._owns_client:
            self._fs.close()

    # --- helpers ---------------------------------------------------------

    def _ensure_dir(self, path: str) -> None:
        # The rename target's parent must exist; a file upload creates its own
        # parents, a rename may not.
        if path in ("", ".") or path in self._known_dirs:
            return
        d = self._fs.get_directory_client(path)
        if not d.exists():
            try:
                d.create_directory()
            except ResourceExistsError:
                pass   # created concurrently: fine
        self._known_dirs.add(path)

    def _rename(self, tmp, final_path: str) -> None:
        new_name = f"{self.location.file_system}/{final_path}"
        try:
            tmp.rename_file(new_name)
        except ResourceExistsError:
            # ADLS overwrites the target by default; if this account refuses,
            # drop the old copy (same bytes, see module docstring) and retry once.
            self._fs.get_file_client(final_path).delete_file()
            tmp.rename_file(new_name)

    @staticmethod
    def _delete_quietly(file_client) -> None:
        try:
            file_client.delete_file()
        except ResourceNotFoundError:
            pass       # never created, or already renamed away
        except Exception:
            # Don't mask the original error; a leftover tmp is overwritten by the retry.
            logger.warning("adls: could not delete staging file %s", file_client.path_name, exc_info=True)
