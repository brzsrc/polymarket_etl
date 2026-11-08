from __future__ import annotations

import time
from pathlib import Path

import msgspec


DIR_ROOT = Path(__file__).parent.parent.parent


def now_ns() -> int:
    return time.time_ns()


def fmt_ts(ns: int) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ns / 1e9))


class Rec(msgspec.Struct):        # WAL 一行 (ts_recv_iso 不解析)
    ts_recv_ns: int
    asset_ids: tuple[str, ...]
    raw: str                      # 二次编码的 JSON 字符串
