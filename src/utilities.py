import time


def now_ns() -> int:
    return time.time_ns()


def fmt_ts(ns: int) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ns / 1e9))