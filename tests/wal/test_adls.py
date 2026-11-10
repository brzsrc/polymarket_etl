"""
AdlsPut tests against an in-memory fake ADLS file system (no network).

A live test runs only when ADLS_TEST_URL is set, e.g.
    ADLS_TEST_URL=abfss://landing@myacct.dfs.core.windows.net/_test \
        .venv/bin/python -m pytest tests/wal/test_adls.py -q -p no:anyio
(uses DefaultAzureCredential, i.e. `az login`).
"""
from __future__ import annotations

import asyncio
import gzip
import json
import os
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from src.common.utilities import Rec
from src.wal.adls import AdlsLocation, AdlsPut
from src.wal.uploader import Uploader
from src.wal.writer import Walwriter

FS = "landing"
URL = f"abfss://{FS}@acct.dfs.core.windows.net/polymarket"


# --- fake ADLS -------------------------------------------------------------------

class FakeFs:
    """
    In-memory ADLS file system with the calls AdlsPut uses. Mimics HNS:
    a file upload creates its parents, a rename needs the target's parent.
    `fail` maps an operation name to an exception raised once.
    """

    def __init__(self, rename_overwrites: bool = True):
        self.files: dict[str, bytes] = {}
        self.dirs: set[str] = set()
        self.calls: list[tuple] = []
        self.fail: dict[str, Exception] = {}
        self.truncate_upload = False           # simulate a short upload
        self.rename_overwrites = rename_overwrites
        self.closed = False

    def _maybe_fail(self, op):
        if op in self.fail:
            raise self.fail.pop(op)

    def get_file_client(self, path):
        return FakeFile(self, path)

    def get_directory_client(self, path):
        return FakeDir(self, path)

    def close(self):
        self.closed = True

    def add_parents(self, path):
        parts = path.split("/")[:-1]
        for i in range(1, len(parts) + 1):
            self.dirs.add("/".join(parts[:i]))


class FakeFile:
    def __init__(self, fs: FakeFs, path: str):
        self.fs, self.path_name = fs, path

    def upload_data(self, data, length=None, overwrite=False):
        self.fs.calls.append(("upload", self.path_name))
        self.fs._maybe_fail("upload")
        body = data.read()
        assert length == len(body)
        if self.fs.truncate_upload:
            body = body[:-1]
        if not overwrite and self.path_name in self.fs.files:
            raise ResourceExistsError("exists")
        self.fs.add_parents(self.path_name)
        self.fs.files[self.path_name] = body

    def get_file_properties(self):
        self.fs._maybe_fail("props")
        return SimpleNamespace(size=len(self.fs.files[self.path_name]))

    def rename_file(self, new_name):
        self.fs.calls.append(("rename", self.path_name, new_name))
        self.fs._maybe_fail("rename")
        fs_name, _, target = new_name.partition("/")
        assert fs_name == FS
        parent = target.rsplit("/", 1)[0]
        if parent not in self.fs.dirs:
            raise ResourceNotFoundError("parent missing")
        if target in self.fs.files and not self.fs.rename_overwrites:
            raise ResourceExistsError("target exists")
        self.fs.files[target] = self.fs.files.pop(self.path_name)

    def delete_file(self):
        self.fs.calls.append(("delete", self.path_name))
        if self.path_name not in self.fs.files:
            raise ResourceNotFoundError("missing")
        del self.fs.files[self.path_name]


class FakeDir:
    def __init__(self, fs: FakeFs, path: str):
        self.fs, self.path = fs, path

    def exists(self):
        return self.path in self.fs.dirs

    def create_directory(self):
        self.fs.calls.append(("mkdir", self.path))
        self.fs.add_parents(self.path + "/x")


def local_file(tmp_path: Path, data: bytes = b"hello\n") -> Path:
    p = tmp_path / "000003.jsonl.gz"
    p.write_bytes(data)
    return p


REL = "ws/2026-11-10/h-ab12-2026-11-10-000003.jsonl.gz"
FINAL = f"polymarket/{REL}"
STAGING = "polymarket/_staging/ws__2026-11-10__h-ab12-2026-11-10-000003.jsonl.gz.tmp"


# --- URL parsing -------------------------------------------------------------------

@pytest.mark.parametrize("url, fs, prefix", [
    ("abfss://landing@acct.dfs.core.windows.net/polymarket", "landing", "polymarket"),
    ("abfss://landing@acct.dfs.core.windows.net/polymarket/", "landing", "polymarket"),
    ("abfss://landing@acct.dfs.core.windows.net/a/b", "landing", "a/b"),
    ("abfss://landing@acct.dfs.core.windows.net", "landing", ""),
    ("abfss://landing@acct.dfs.core.windows.net/", "landing", ""),
])
def test_parse(url, fs, prefix):
    loc = AdlsLocation.parse(url)
    assert loc.account_url == "https://acct.dfs.core.windows.net"
    assert loc.file_system == fs and loc.prefix == prefix
    assert loc.path("ws/x.gz") == (f"{prefix}/ws/x.gz" if prefix else "ws/x.gz")


@pytest.mark.parametrize("url", [
    "/Volumes/polymarket/raw/landing",
    "abfs://landing@acct.dfs.core.windows.net/x",     # not abfss
    "abfss://acct.dfs.core.windows.net/x",            # no container
    "abfss://landing@acct.dfs.core.windows.net/../x",
])
def test_parse_rejects(url):
    with pytest.raises(ValueError):
        AdlsLocation.parse(url)


def test_repr_shows_destination():
    assert repr(AdlsPut(URL, file_system_client=FakeFs())) == f"AdlsPut('{URL}')"


# --- put -------------------------------------------------------------------------------

def test_put_stages_then_renames(tmp_path):
    fs = FakeFs()
    AdlsPut(URL, file_system_client=fs)(local_file(tmp_path), REL)

    assert fs.files == {FINAL: b"hello\n"}                       # only the final file is left
    assert fs.calls[0] == ("upload", STAGING)                    # uploaded under _staging first
    assert ("mkdir", "polymarket/ws/2026-11-10") in fs.calls     # rename target's parent created
    assert fs.calls[-1] == ("rename", STAGING, f"{FS}/{FINAL}")


def test_parent_dir_checked_once_per_process(tmp_path):
    fs = FakeFs()
    put = AdlsPut(URL, file_system_client=fs)
    put(local_file(tmp_path), REL)
    put(local_file(tmp_path), REL.replace("000003", "000004"))
    assert sum(1 for c in fs.calls if c[0] == "mkdir") == 1


def test_reupload_same_name_overwrites(tmp_path):
    fs = FakeFs()
    put = AdlsPut(URL, file_system_client=fs)
    put(local_file(tmp_path), REL)
    put(local_file(tmp_path), REL)
    assert fs.files == {FINAL: b"hello\n"}


def test_rename_refused_on_existing_target_deletes_then_renames(tmp_path):
    fs = FakeFs(rename_overwrites=False)
    put = AdlsPut(URL, file_system_client=fs)
    put(local_file(tmp_path), REL)
    put(local_file(tmp_path, b"hello\n"), REL)
    assert fs.files == {FINAL: b"hello\n"}
    assert ("delete", FINAL) in fs.calls


def test_size_mismatch_raises_and_cleans_up(tmp_path):
    fs = FakeFs()
    fs.truncate_upload = True
    with pytest.raises(OSError, match="size mismatch"):
        AdlsPut(URL, file_system_client=fs)(local_file(tmp_path), REL)
    assert fs.files == {}                                        # staging deleted, nothing published


@pytest.mark.parametrize("op", ["upload", "props", "rename"])
def test_failure_raises_and_leaves_nothing(tmp_path, op):
    fs = FakeFs()
    fs.fail[op] = ConnectionError(f"{op} down")
    with pytest.raises(ConnectionError):
        AdlsPut(URL, file_system_client=fs)(local_file(tmp_path), REL)
    assert fs.files == {}


def test_bad_rel_rejected_before_any_call(tmp_path):
    fs = FakeFs()
    with pytest.raises(ValueError):
        AdlsPut(URL, file_system_client=fs)(local_file(tmp_path), "../escape.gz")
    assert fs.calls == []


def test_close_only_closes_own_client():
    fs = FakeFs()
    AdlsPut(URL, file_system_client=fs).close()
    assert fs.closed is False


# --- with the real Uploader + Walwriter ----------------------------------------------------

def test_uploader_with_adls_put_lands_every_segment(tmp_path):
    fs = FakeFs()
    data = tmp_path / "data"
    t0 = 1762560000_000000000    # 2025-11-08 00:00:00 UTC

    async def go():
        put = AdlsPut(URL, file_system_client=fs)
        async with Uploader(data, put, host_id="h", scan_interval_sec=0.01) as up:
            async with Walwriter(data, segment_max_bytes=300, check_interval_sec=0.01) as w:
                for i in range(20):
                    await w.write(Rec(ts_recv_ns=t0 + i, asset_ids=("111",), raw=json.dumps({"i": i})))
                    await asyncio.sleep(0.002)
                await w.write_market_snapshot(t0, [{"market_id": "m1", "raw": {"id": "m1"}}])
                await w.write_event(t0, {"conn_id": 0, "event": "connected", "extra": {}})
                await w.write_metrics(t0, {"msgs_total": 20})
        return up.stats()

    st = asyncio.run(go())
    ws = sorted(p for p in fs.files if p.startswith("polymarket/ws/"))
    got = [json.loads(json.loads(l)["raw"])["i"] for p in ws for l in gzip.decompress(fs.files[p]).splitlines()]
    assert len(ws) > 1 and got == list(range(20))                # rotated, nothing lost, in order
    assert any(p.startswith("polymarket/markets/2025-11-08/h-") for p in fs.files)
    assert any(p.startswith("polymarket/events/2025-11-08/h-") for p in fs.files)
    assert any(p.startswith("polymarket/metrics/2025-11-08/h-") for p in fs.files)
    assert not any("/_staging/" in p for p in fs.files)
    assert st["pending"] == 0 and st["failures"] == 0


# --- real SDK, fake HTTP ---------------------------------------------------------------------
# The fake file system above checks our logic; this checks that we drive the
# real azure SDK correctly (method names, rename format, response parsing) by
# swapping only its HTTP transport.

from urllib.parse import parse_qs, urlparse

from azure.core.credentials import AzureNamedKeyCredential
from azure.core.pipeline.transport import HttpResponse, HttpTransport
from azure.storage.filedatalake import DataLakeServiceClient


class _Resp(HttpResponse):
    def __init__(self, req, status, headers=None):
        super().__init__(req, None)
        self.status_code, self.reason, self.content_type = status, "OK", None
        self.headers = {"x-ms-request-id": "r", "ETag": '"e"', "Content-Length": "0",
                        "Last-Modified": "Mon, 10 Nov 2026 00:00:00 GMT", **(headers or {})}

    def body(self):
        return b""


class FakeDfsEndpoint(HttpTransport):
    """Minimal ADLS Gen2 REST endpoint: create / append / flush / HEAD / mkdir / rename / delete."""

    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.dirs: set[str] = set()
        self.requests: list[tuple[str, str, dict, dict]] = []

    def __enter__(self): return self
    def __exit__(self, *a): pass
    def open(self): pass
    def close(self): pass

    def send(self, req, **kw):
        u = urlparse(req.url)
        path = u.path.lstrip("/").split("/", 1)[1]          # drop the file system
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        h = {k.lower(): v for k, v in req.headers.items()}
        self.requests.append((req.method, path, q, h))
        if req.method == "PUT" and "x-ms-rename-source" in h:
            src = h["x-ms-rename-source"].lstrip("/").split("/", 1)[1]
            self.files[path] = self.files.pop(src)
            return _Resp(req, 201)
        if req.method == "PUT" and q.get("resource") == "file":
            self.files[path] = b""
            return _Resp(req, 201)
        if req.method == "PUT" and q.get("resource") == "directory":
            self.dirs.add(path)
            return _Resp(req, 201)
        if req.method == "PATCH" and q.get("action") == "append":
            self.files[path] += req.data if isinstance(req.data, bytes) else req.data.read()
            return _Resp(req, 202)
        if req.method == "PATCH" and q.get("action") == "flush":
            return _Resp(req, 200)
        if req.method == "HEAD":
            if path in self.files:
                return _Resp(req, 200, {"Content-Length": str(len(self.files[path])), "x-ms-resource-type": "file"})
            if path in self.dirs:
                return _Resp(req, 200, {"x-ms-resource-type": "directory"})
            return _Resp(req, 404, {"x-ms-error-code": "PathNotFound"})
        if req.method == "DELETE":
            self.files.pop(path, None)
            return _Resp(req, 200)
        return _Resp(req, 400)


def test_real_sdk_request_sequence(tmp_path):
    http = FakeDfsEndpoint()
    svc = DataLakeServiceClient("https://acct.dfs.core.windows.net",
                                credential=AzureNamedKeyCredential("acct", "a2V5"), transport=http)
    put = AdlsPut(URL, file_system_client=svc.get_file_system_client(FS))

    put(local_file(tmp_path, b"hello world\n"), REL)

    assert http.files == {FINAL: b"hello world\n"}
    steps = [(m, p, q.get("resource") or q.get("action")) for m, p, q, _h in http.requests]
    assert steps == [
        ("PUT", STAGING, "file"),                       # create under _staging
        ("PATCH", STAGING, "append"),
        ("PATCH", STAGING, "flush"),
        ("HEAD", STAGING, None),                        # size check
        ("HEAD", "polymarket/ws/2026-11-10", None),     # parent exists?
        ("PUT", "polymarket/ws/2026-11-10", "directory"),
        ("PUT", FINAL, None),                           # rename into place
    ]
    rename_headers = http.requests[-1][3]
    assert rename_headers["x-ms-rename-source"].endswith(f"/{FS}/{STAGING}")
    assert "if-none-match" not in rename_headers        # no condition -> ADLS overwrites an existing target


# --- live (opt-in) -------------------------------------------------------------------------

@pytest.mark.skipif("ADLS_TEST_URL" not in os.environ, reason="set ADLS_TEST_URL to run against real ADLS")
def test_live_roundtrip(tmp_path):
    put = AdlsPut(os.environ["ADLS_TEST_URL"])
    rel = f"ws/live-test/{uuid.uuid4().hex}.jsonl.gz"
    try:
        put(local_file(tmp_path, b"live\n"), rel)
        put(local_file(tmp_path, b"live\n"), rel)                # re-upload: must overwrite cleanly
        final = put._fs.get_file_client(put.location.path(rel))
        assert final.download_file().readall() == b"live\n"
        staging = put._fs.get_directory_client(put.location.path("_staging"))
        assert not any(p.name.endswith(".tmp") and "live-test" in p.name
                       for p in put._fs.get_paths(path=staging.path_name)) if staging.exists() else True
    finally:
        put._fs.get_file_client(put.location.path(rel)).delete_file()
        put.close()
