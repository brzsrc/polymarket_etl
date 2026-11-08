# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 原型：在 Databricks 上运行采集
# MAGIC
# MAGIC 运行 `src.collector.run()`：Orchestrator（市场发现 + WS 连接池）→ Walwriter → Uploader。
# MAGIC
# MAGIC ```
# MAGIC 本地临时盘 /tmp/polymarket/                       landing volume（/Volumes/...）
# MAGIC   wal/{date}/NNNNNN.jsonl      ← 正在写
# MAGIC   wal/{date}/NNNNNN.jsonl.gz   ← 封存好 ──上传──▶  ws/{date}/{source_id}-{date}-NNNNNN.jsonl.gz
# MAGIC   markets/{date}/...                     ──上传──▶  markets/{date}/...
# MAGIC   metrics.jsonl, events.jsonl            ──退出时─▶  _runs/{source_id}/
# MAGIC ```
# MAGIC
# MAGIC - WAL 必须写在本地盘：`/Volumes` 不支持追加写。
# MAGIC - 本地盘在 Job 结束后会被回收，所以退出时会把所有文件封存并上传。
# MAGIC   **Job 的超时时间必须比 `duration_sec` 长**（建议多 30 分钟），否则被强制终止时来不及收尾。
# MAGIC - 运行环境：Serverless，environment version 2 或更高（代码需要 Python 3.11+）。
# MAGIC
# MAGIC 跑完后接着运行 `bronze_ingest`，把 `ws/` 导入 `bronze.ws_raw`。

# COMMAND ----------

# MAGIC %pip install -q msgspec "websockets>=13" httpx

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. 参数

# COMMAND ----------

dbutils.widgets.text("landing", "/Volumes/polymarket/raw/landing_dev")
dbutils.widgets.text("duration_sec", "1800")
dbutils.widgets.text("n_markets", "50")
dbutils.widgets.text("segment_max_mb", "50")
dbutils.widgets.text("host_id", "dbx")

LANDING = dbutils.widgets.get("landing").rstrip("/")
DURATION_SEC = float(dbutils.widgets.get("duration_sec"))
N_MARKETS = int(dbutils.widgets.get("n_markets"))
SEGMENT_MAX_BYTES = int(float(dbutils.widgets.get("segment_max_mb")) * 1024 * 1024)
HOST_ID = dbutils.widgets.get("host_id")

print(f"landing     = {LANDING}")
print(f"duration    = {DURATION_SEC:.0f}s")
print(f"n_markets   = {N_MARKETS}")
print(f"segment_max = {SEGMENT_MAX_BYTES / 1024 / 1024:.0f} MiB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 准备：找到仓库根目录、检查 volume、配置日志

# COMMAND ----------

import logging
import os
import sys
import tempfile
from pathlib import Path

# The notebook lives in <repo>/scripts/notebook/ inside a Git folder; walk up
# to the directory that contains src/ so `import src...` works.
repo_root = Path(os.getcwd()).resolve()
while not (repo_root / "src" / "collector.py").exists():
    if repo_root.parent == repo_root:
        raise RuntimeError(f"cannot find the repo root (src/collector.py) above {os.getcwd()}")
    repo_root = repo_root.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))
print(f"repo root   = {repo_root}")

# The volume itself must exist (CREATE VOLUME ...); folders inside it are created automatically.
volume_root = Path(*Path(LANDING).parts[:5])      # /Volumes/<catalog>/<schema>/<volume>
if not volume_root.is_dir():
    raise RuntimeError(f"volume {volume_root} does not exist. Create it first, e.g. CREATE VOLUME ...")

# Databricks has already configured the root logger; force=True replaces it so our logs show.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,
)

DATA_DIR = Path(tempfile.gettempdir()) / "polymarket"   # local disk, gone when the job ends
print(f"data_dir    = {DATA_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 运行
# MAGIC
# MAGIC 会一直运行到 `duration_sec`，然后按顺序退出：停止接收 → 封存所有文件 → 上传剩下的文件。

# COMMAND ----------

from src.collector import run

stats = await run(
    data_dir=DATA_DIR,
    landing=LANDING,
    host_id=HOST_ID,
    n_markets=N_MARKETS,
    duration_sec=DURATION_SEC,
    segment_max_bytes=SEGMENT_MAX_BYTES,
)
stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 检查结果

# COMMAND ----------

wal_stats, up_stats = stats["wal"], stats["uploader"]
print(f"source_id      = {up_stats['source_id']}")
print(f"messages       = {wal_stats['messages']}")
print(f"market records = {wal_stats['market_records']}")
print(f"sealed         = {wal_stats['sealed']}")
print(f"uploaded       = {up_stats['uploaded']}  ({up_stats['uploaded_bytes'] / 1e6:.1f} MB)")
print(f"failures       = {up_stats['failures']}  last_error={up_stats['last_error']}")

# This run's files at the destination
sid = up_stats["source_id"]
for stream in ("ws", "markets"):
    files = sorted(Path(LANDING, stream).glob(f"*/{sid}-*.jsonl.gz"))
    print(f"{stream}: {len(files)} files")
    for f in files[-5:]:
        print(f"   {f.relative_to(LANDING)}  {f.stat().st_size / 1e6:.1f} MB")

staging = Path(LANDING, "_staging")
left_in_staging = list(staging.iterdir()) if staging.exists() else []
print(f"_staging: {len(left_in_staging)} files (should be 0)")

# Fail the job if anything did not make it to the volume.
assert up_stats["pending"] == 0, f"{up_stats['pending']} sealed files were NOT uploaded"
