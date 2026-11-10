# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 0: UC objects + connectivity check (Serverless)
# MAGIC Pass criteria: receive a `book` message over WS, and the rename on the Volume succeeds.

# COMMAND ----------

# MAGIC %pip install websockets requests -q

# COMMAND ----------

# Serverless may restart Python after pip; re-import in each cell after this.
spark.sql("CREATE CATALOG IF NOT EXISTS polymarket")
# If the line above fails with "managed location required", the metastore has no root storage.
# Use instead: CREATE CATALOG IF NOT EXISTS polymarket MANAGED LOCATION 'abfss://<container>@<account>.dfs.core.windows.net/polymarket'
for s in ["raw", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS polymarket.{s}")
spark.sql("CREATE VOLUME IF NOT EXISTS polymarket.raw.landing")
spark.sql("CREATE VOLUME IF NOT EXISTS polymarket.raw.checkpoints")
display(spark.sql("SHOW VOLUMES IN polymarket.raw"))

# COMMAND ----------

# Get a live token_id from Gamma (also checks plain HTTPS egress).
import json, requests
r = requests.get(
    "https://gamma-api.polymarket.com/markets",
    params={"active": "true", "closed": "false", "limit": 5, "order": "volume24hr", "ascending": "false"},
    timeout=15,
)
r.raise_for_status()
m = r.json()[0]
token_ids = m["clobTokenIds"]
token_ids = json.loads(token_ids) if isinstance(token_ids, str) else token_ids
TOKEN = token_ids[0]
print("HTTP egress ok |", m["question"], "|", TOKEN)

# COMMAND ----------

import asyncio, json, websockets

async def t():
    async with websockets.connect("wss://ws-subscriptions-clob.polymarket.com/ws/market") as ws:
        await ws.send(json.dumps({"type": "market", "assets_ids": [TOKEN]}))
        for _ in range(5):
            raw = await asyncio.wait_for(ws.recv(), 10)
            print(raw[:300])
            msg = json.loads(raw)
            msgs = msg if isinstance(msg, list) else [msg]
            if any(x.get("event_type") == "book" for x in msgs):
                print("WS ok: got book")
                return
        raise RuntimeError("connected but no book message")

await t()  # the notebook already has an event loop: await directly, not asyncio.run

# COMMAND ----------

import os
base = "/Volumes/polymarket/raw/landing"
os.makedirs(f"{base}/_staging", exist_ok=True)
os.makedirs(f"{base}/ws/_probe", exist_ok=True)
with open(f"{base}/_staging/x", "wb") as f:
    f.write(b"1")
os.replace(f"{base}/_staging/x", f"{base}/ws/_probe/x")
assert open(f"{base}/ws/_probe/x", "rb").read() == b"1"
print("rename ok")
