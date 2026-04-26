from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any
import httpx

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
DATA_BASE = "https://data-api.polymarket.com"

client = httpx.AsyncClient(
    timeout=15.0,
    http2=True,
    headers={"User-Agent": "polymarket-rag-demo/0.1"},
    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
)


url = f"{GAMMA_BASE}/markets"
params = {
    "limit": 1,
    "active": str(True).lower(),
    "closed": str(False).lower(),
    "order": "volume24hr",
    "ascending": str(False).lower(),
}

async def main():
    """带最多 3 次重试的 GET. 处理 Polymarket 偶发的 5xx."""
    last_err = None
    for attempt in range(3):
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            print(resp)
            print("=====" * 10)
            raw = resp.json()
            print(raw)
            print("-----" * 30)
            await client.aclose()  # 关一下连接
            return
        except (httpx.HTTPError, json.JSONDecodeError) as e:
                last_err = e
                if attempt < 2:
                    await asyncio.sleep(0.5 * (2 ** attempt))
    raise RuntimeError(f"GET {url} failed after 3 retries: {last_err}")

asyncio.run(main())

# import asyncio
#
#
# async def fetch():
#     return 42
#
# result = fetch()
# print(result)  # <coroutine object fetch at 0x...>,不是 42
#
# async def function_x():
#     total = 0
#     for i in range(10):     # 跑 10 秒
#         total += i
#         print(total)
#     return total
#
# async def task_A():
#     print("A: 开始")
#     # await function_x()
#     await asyncio.sleep(2)
#     print("A: 结束")
#
# async def task_B():
#     print("B: 开始")
#     await asyncio.sleep(1)
#     print("B: 结束")
#
# async def main():
#     await asyncio.gather(task_A(), task_B())
#
asyncio.run(main())