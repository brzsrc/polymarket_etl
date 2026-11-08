"""WAL -> 有序事件流。对应 hftbacktest 的 data/utils/*.py (convert) + data/validation.py。"""
from __future__ import annotations
import glob, gzip, hashlib, heapq, re, zipfile
from collections import Counter, deque
from pathlib import Path
from typing import Iterator
import msgspec
from .events import TICK, QTY_SCALE, Delta, Level, MarketEvent, Snapshot, Trade, px_of, qty_of
from ..common.utilities import Rec

_dec_rec = msgspec.json.Decoder(Rec)
_dec_raw = msgspec.json.Decoder()  # -> dict

_MARKET = re.compile(r'"market"\s*:\s*"([^"]+)"')


class Dedup:
    """按报文内容去重 (时间窗内)。样本里市场 0x5d99 的 4 条 price_change (ts 922/925/927/928) 在 0.5 ms 后被整段重放,
    原因是 price_change 是市场级报文 (含两个 token), 一个市场的两个 token 若订阅在不同连接上, 两条连接各收一份。
    直接回放会把 0.16 档从 35 打回 30 再回 35 -> 假撤单 -> queue 被错误推进。
    绝对量语义下丢掉真重复也无损 (再 apply 一次是 no-op), 所以宁可多丢。window_ns<=0 表示关闭。"""
    def __init__(self, window_ns: int = 5_000_000_000):
        self.window, self._seen, self._q, self.dropped = window_ns, {}, deque(), 0
    def is_dup(self, rec: Rec) -> bool:
        if self.window <= 0: return False
        raw, local_ts = rec.raw, rec.ts_recv_ns
        k = hashlib.blake2b(raw.encode(), digest_size=8).digest()
        cutoff = local_ts - self.window
        while self._q and self._q[0][0] < cutoff:
            _, old = self._q.popleft(); self._seen.pop(old, None)
        if k in self._seen:
            self.dropped += 1; return True
        self._seen[k] = local_ts; self._q.append((local_ts, k)); return False



def partition(data_dir: Path | str, date: str, out_dir: Path | str, markets: set[str] | None = None) -> dict[
    str, int]:
    """一次性把当天所有 segment 切成 out_dir/{date}/{market}.jsonl (去重、按 ts_recv_ns 有序)。
    你的采集量 (~12k msg/s、~7 MB/s、2 万 asset) 意味着回放一个 asset 不能每次扫全量;
    这一步 = hftbacktest 的 convert() (原始 dump -> 每 asset 一个 npz), 之后 replay(那个文件) 即可。"""
    out = Path(out_dir) / date;
    out.mkdir(parents=True, exist_ok=True)
    fh: dict[str, object] = {};
    counts: Counter = Counter()
    for rec in _records(_files(Path(data_dir), date), None, Dedup()):
        market = _market_of(rec.raw)
        if markets is not None and market not in markets: continue
        f = fh.get(market)
        if f is None:
            if len(fh) >= 256: fh.popitem()[1].close()  # 有限句柄, 追加模式可反复开
            f = fh[market] = open(out / f"{market}.jsonl", "ab")
        f.write(msgspec.json.encode(rec) + b"\n");
        counts[market] += 1
    for f in fh.values(): f.close()
    return dict(counts)

def replay(source: Path | str, date: str | None = None, asset_ids: set[str] | None = None, *,
           local_ts_offset_ns: int = 0, dedup: Dedup | None = None) -> Iterator[MarketEvent]:
    """source = 原始 WAL 根目录 (扫当天全部 segment) 或 partition 产出的单文件。按 (local_ts, 到达序) 有序输出。
    local_ts_offset_ns 来自 wal_stats(): 与 hftbacktest 的 correct_local_timestamp 同义, 用最小延迟平移, 不用中位数
    (减中位数会把一半延迟压成负数)。"""
    src = Path(source)
    if dedup is None: dedup = Dedup(0) if src.is_file() else Dedup()
    for rec in _records(_files(src, date), asset_ids, dedup):
        yield from parse(rec, asset_ids, local_ts_offset_ns)

def wal_stats(source: Path | str, date: str | None = None, asset_ids: set[str] | None = None,
              base_latency_ms: float = 1.0) -> dict:
    """跑引擎前先跑这个 (= hftbacktest validate_data + correct_local_timestamp):
    延迟分布 -> local_ts_offset_ns 与 lmax_ns; 重复率; exch_ts 倒序; 网格对齐; 事件计数。"""
    import numpy as np
    dedup, types, lat, rev, last, mis = Dedup(), Counter(), [], 0, {}, Counter()
    for rec in _records(_files(Path(source), date), asset_ids, dedup):
        m = _dec_raw.decode(rec.raw)
        for ch in m.get("price_changes", []):  # 对齐检查: 若有 3 位小数的 size 或 4 位小数的价, 在这里暴露而不是被 round 吞掉
            if abs(float(ch["price"]) * TICK - round(float(ch["price"]) * TICK)) > 1e-6: mis["px"] += 1
            if abs(float(ch["size"]) * QTY_SCALE - round(float(ch["size"]) * QTY_SCALE)) > 1e-6: mis["qty"] += 1
        for ev in parse(rec, asset_ids):
            types[type(ev).__name__] += 1
            lat.append(ev.local_ts - ev.exch_ts)
            if ev.exch_ts < last.get(ev.asset_id, 0): rev += 1
            last[ev.asset_id] = max(last.get(ev.asset_id, 0), ev.exch_ts)
    lat = np.asarray(lat, dtype=np.int64)
    if lat.size == 0:
        return {"events": 0, "dupes_dropped": dedup.dropped}
    ms = lambda x: round(float(x) / 1e6, 3)
    return {"events": int(lat.size), "by_type": dict(types), "dupes_dropped": dedup.dropped,
            "misaligned": dict(mis), "exch_ts_reversals": rev,
            "feed_latency_ms": {"min": ms(lat.min()), "p50": ms(np.percentile(lat, 50)),
                                "p99": ms(np.percentile(lat, 99)), "max": ms(lat.max())},
            "suggested_local_ts_offset_ns": int(max(0, base_latency_ms * 1e6 - lat.min())),
            "suggested_lmax_ns": int(2 * lat.max())}


def parse(rec: Rec, want: set[str] | None, offset_ns: int = 0) -> list[MarketEvent]:
    """一条 WAL 记录 -> 0..n 个事件。字段名按 2026-04 的真实报文对齐。"""
    m = _dec_raw.decode(rec.raw)
    local_ts, exch_ts = rec.ts_recv_ns + offset_ns, int(m["timestamp"]) * 1_000_000
    et, out = m["event_type"], []
    if et == "price_change":
        # 一条报文 = 同一笔订单在两个互补 token 上的镜像: (A, 0.01, BUY, 27020.01) 与 (B, 0.99, SELL, 27020.01)。
        # 只回放一个 token 时另一半自然被 want 过滤掉; 两个都要时各进各的簿。
        chs = [ch for ch in m["price_changes"] if want is None or ch["asset_id"] in want]
        last = {ch["asset_id"]: i for i, ch in enumerate(chs)}
        for i, ch in enumerate(chs):
            out.append(Delta(asset_id=ch["asset_id"], exch_ts=exch_ts, local_ts=local_ts,
                             side="B" if ch["side"] == "BUY" else "A",
                             px=px_of(ch["price"]), qty=qty_of(ch["size"]),
                             best_bid=px_of(ch["best_bid"]), best_ask=px_of(ch["best_ask"]),
                             eom=(last[ch["asset_id"]] == i)))
    elif et == "book":                              # TODO: 样本里没有 book 报文, 用一条真实的核对字段名
        aid = m["asset_id"]
        if want is None or aid in want:
            out.append(Snapshot(asset_id=aid, exch_ts=exch_ts, local_ts=local_ts,
                                bids=tuple(Level(px_of(l["price"]), qty_of(l["size"])) for l in m["bids"]),
                                asks=tuple(Level(px_of(l["price"]), qty_of(l["size"])) for l in m["asks"])))
    elif et == "last_trade_price":                  # TODO: 同上; 并用数据确认 side 是 taker 方向、互补 token 是否各报一次
        aid = m["asset_id"]
        if want is None or aid in want:
            out.append(Trade(asset_id=aid, exch_ts=exch_ts, local_ts=local_ts,
                             px=px_of(m["price"]), qty=qty_of(m["size"]),
                             aggressor="B" if m["side"] == "BUY" else "A"))
    # tick_size_change: 内部网格固定 0.001, 不处理
    return out

def _iter_lines(path: Path) -> Iterator[bytes]:
    """逐行读一个 WAL 文件 (.jsonl / .jsonl.gz / 旧格式 .zip)。只产出以 \\n 结尾的完整行: 正在写的 segment 末尾可能是半行。"""
    if path.suffix == ".zip":                                       # 旧格式: 一个 gen{N}/ 的全部 shard 打成一个包
        with zipfile.ZipFile(path) as z:
            for name in sorted(z.namelist()):
                with z.open(name) as f:
                    yield from _complete_lines(f)
        return
    try:
        f = gzip.open(path, "rb") if path.suffix == ".gz" else open(path, "rb")
    except FileNotFoundError:
        gz = path.with_name(path.name + ".gz")                      # glob 之后这个 segment 刚被封存删掉: 改读它的 .gz
        if path.suffix != ".jsonl" or not gz.exists(): raise
        f = gzip.open(gz, "rb")
    with f:
        yield from _complete_lines(f)

def _complete_lines(f) -> Iterator[bytes]:
    for line in f:
        if line.endswith(b"\n") and line.strip(): yield line

def _files(source: Path, date: str | None) -> list[str]:
    """当天全部 WAL 文件: 新格式 wal/{date}/NNNNNN.jsonl(.gz), 以及旧格式 gen{N}/*.jsonl 和 archive_*.zip。"""
    if source.is_file(): return [str(source)]                       # 已分区的单文件 (见 partition)
    root = source / "wal" / date
    found: set[str] = set()
    for pat in ("*.jsonl", "*.jsonl.gz", "*.zip"):                  # ** 也匹配零层目录, 新旧两种布局都能找到
        found |= set(glob.glob(str(root / "**" / pat), recursive=True))
    # 封存时 rename 和删除源文件之间, NNNNNN.jsonl 与 NNNNNN.jsonl.gz 会同时存在: 两者内容相同, 只读 .gz
    return sorted(f for f in found if not (f.endswith(".jsonl") and f + ".gz" in found))

def _records(files: list[str], want: set[str] | None, dedup: Dedup) -> Iterator[Rec]:
    """所有文件按 ts_recv_ns 归并 + 去重。WAL 不按 asset 分文件, 回放某个 asset 也必须扫当天全部文件再按 asset 过滤。"""
    want_b = [a.encode() for a in want] if want else None
    def gen(path: str) -> Iterator[Rec]:
        for line in _iter_lines(Path(path)):
            if want_b is not None and not any(a in line for a in want_b):
                continue                          # 不解析就跳过: asset id 是长数字串, 子串匹配足够安全, 比 decode 便宜一个量级
            yield _dec_rec.decode(line)
    for rec in heapq.merge(*(gen(f) for f in files), key=lambda r: r.ts_recv_ns):   # 各文件内部按写入时间有序
        if not dedup.is_dup(rec):
            yield rec


def _market_of(raw: str) -> str:                  # 不为取一个字段付整条 decode 的钱
    m = _MARKET.search(raw)
    return m.group(1) if m else _dec_raw.decode(raw)["market"]

