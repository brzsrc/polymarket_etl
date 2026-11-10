from src.replay_engine.book import Book, NO_ASK, NO_BID
from src.replay_engine.events import Level, Snapshot

def test_update_returns_prev_new_best():
    b = Book()
    assert b.update("B", 500, 100) == (0, 100, NO_BID, 500)
    assert b.update("B", 510, 50) == (0, 50, 500, 510)
    assert b.update("B", 510, 0) == (50, 0, 510, 500)
    assert b.update("A", 520, 10) == (0, 10, NO_ASK, 520)
    assert b.update("A", 520, 0) == (10, 0, 520, NO_ASK)

def test_snapshot_diff_only_changed_levels():
    b = Book(); b.update("B", 500, 100); b.update("B", 499, 200); b.update("A", 510, 300)
    snap = Snapshot(asset_id="A", exch_ts=1, local_ts=1, bids=(Level(500, 100), Level(498, 50)), asks=(Level(510, 300),))
    ch = b.apply(snap)
    assert sorted((s, px, prev, new) for s, px, prev, new, _, _ in ch) == [("B", 498, 0, 50), ("B", 499, 200, 0)]
    assert (b.best_bid, b.best_ask) == (500, 510)