"""W42 — :mod:`flowfile_core.ai.replay_buffer` tests.

Cases (~10):

* ``test_append_and_read_all`` — append 3, read with cursor=None → 3.
* ``test_read_after_skips_acknowledged`` — append 10, read after id ".4" →
  exactly 5 entries (ids 5..9).
* ``test_read_after_skips_session_local`` — cursor on different session
  yields nothing for unrelated session.
* ``test_ring_buffer_caps_at_64`` — append 100; read returns last 64.
* ``test_read_after_with_cursor_beyond_buffer`` — cursor newer than every
  entry → empty iterator.
* ``test_disk_persists_across_buffer_instances`` — second buffer instance
  pointing at the same root reads the prior frames.
* ``test_corrupt_tail_recovery`` — manually corrupt the NDJSON's last
  line; new instance reads only the well-formed frames; subsequent
  appends keep working.
* ``test_drop_removes_disk_and_memory`` — buffer.drop() wipes file +
  in-memory mirror.
* ``test_malformed_cursor_returns_all`` — cursor without a dot logs WARN
  and falls back to returning every cached frame.
* ``test_payload_roundtrip_preserves_bytes`` — UTF-8 + binary payloads
  survive base64 encode-decode.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from flowfile_core.ai import replay_buffer
from flowfile_core.ai.replay_buffer import DEFAULT_CAP, ReplayBuffer


def _payload(i: int) -> bytes:
    return f"event:tool_call\ndata:{i}\n\n".encode()


def test_append_and_read_all(tmp_path: Path) -> None:
    rb = ReplayBuffer(tmp_path / "ai_sessions", cap=8)
    sid = "abc"
    for i in range(3):
        rb.append(flow_id=1, session_id=sid, event_id=f"{sid}.{i}", payload=_payload(i))
    out = list(rb.read_after(flow_id=1, session_id=sid, event_id=None))
    assert [eid for eid, _ in out] == [f"{sid}.0", f"{sid}.1", f"{sid}.2"]
    assert [data for _, data in out] == [_payload(0), _payload(1), _payload(2)]


def test_read_after_skips_acknowledged(tmp_path: Path) -> None:
    rb = ReplayBuffer(tmp_path / "ai_sessions", cap=16)
    sid = "skip"
    for i in range(10):
        rb.append(flow_id=1, session_id=sid, event_id=f"{sid}.{i}", payload=_payload(i))

    out = list(rb.read_after(flow_id=1, session_id=sid, event_id=f"{sid}.4"))
    assert [eid for eid, _ in out] == [f"{sid}.{i}" for i in range(5, 10)]


def test_read_after_skips_session_local(tmp_path: Path) -> None:
    rb = ReplayBuffer(tmp_path / "ai_sessions", cap=8)
    rb.append(flow_id=1, session_id="alpha", event_id="alpha.0", payload=_payload(0))
    rb.append(flow_id=1, session_id="alpha", event_id="alpha.1", payload=_payload(1))

    # No frames stored under "beta".
    assert list(rb.read_after(flow_id=1, session_id="beta", event_id=None)) == []


def test_ring_buffer_caps_at_64(tmp_path: Path) -> None:
    rb = ReplayBuffer(tmp_path / "ai_sessions", cap=DEFAULT_CAP)
    sid = "ring"
    for i in range(100):
        rb.append(flow_id=1, session_id=sid, event_id=f"{sid}.{i}", payload=_payload(i))
    out = list(rb.read_after(flow_id=1, session_id=sid, event_id=None))
    assert len(out) == DEFAULT_CAP
    # Oldest preserved id is the 36th (since 100-64 == 36).
    assert out[0][0] == f"{sid}.36"
    assert out[-1][0] == f"{sid}.99"


def test_read_after_with_cursor_beyond_buffer(tmp_path: Path) -> None:
    rb = ReplayBuffer(tmp_path / "ai_sessions", cap=8)
    sid = "fwd"
    for i in range(3):
        rb.append(flow_id=1, session_id=sid, event_id=f"{sid}.{i}", payload=_payload(i))
    out = list(rb.read_after(flow_id=1, session_id=sid, event_id=f"{sid}.99"))
    assert out == []


def test_disk_persists_across_buffer_instances(tmp_path: Path) -> None:
    root = tmp_path / "ai_sessions"
    sid = "persist"
    a = ReplayBuffer(root, cap=8)
    a.append(flow_id=1, session_id=sid, event_id=f"{sid}.0", payload=_payload(0))
    a.append(flow_id=1, session_id=sid, event_id=f"{sid}.1", payload=_payload(1))

    b = ReplayBuffer(root, cap=8)
    out = list(b.read_after(flow_id=1, session_id=sid, event_id=None))
    assert [eid for eid, _ in out] == [f"{sid}.0", f"{sid}.1"]


def test_corrupt_tail_recovery(tmp_path: Path) -> None:
    root = tmp_path / "ai_sessions"
    sid = "corrupt"
    a = ReplayBuffer(root, cap=16)
    for i in range(3):
        a.append(flow_id=1, session_id=sid, event_id=f"{sid}.{i}", payload=_payload(i))

    path = a._replay_path(1, sid)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write("{this is not valid json\n")
        fh.write("\n")  # blank tail line
        fh.write('{"event_id":"orphan","data":"@@invalid_b64@@"}\n')

    # Cold buffer reads only the three well-formed frames; the corrupt and
    # blank lines are skipped without raising.
    b = ReplayBuffer(root, cap=16)
    out = list(b.read_after(flow_id=1, session_id=sid, event_id=None))
    assert [eid for eid, _ in out] == [f"{sid}.0", f"{sid}.1", f"{sid}.2"]

    # Subsequent appends keep working.
    b.append(flow_id=1, session_id=sid, event_id=f"{sid}.3", payload=_payload(3))
    out2 = list(b.read_after(flow_id=1, session_id=sid, event_id=None))
    assert any(eid == f"{sid}.3" for eid, _ in out2)


def test_drop_removes_disk_and_memory(tmp_path: Path) -> None:
    rb = ReplayBuffer(tmp_path / "ai_sessions", cap=8)
    sid = "evict"
    rb.append(flow_id=1, session_id=sid, event_id=f"{sid}.0", payload=_payload(0))
    path = rb._replay_path(1, sid)
    assert path.exists()

    rb.drop(flow_id=1, session_id=sid)
    assert not path.exists()
    assert list(rb.read_after(flow_id=1, session_id=sid, event_id=None)) == []


def test_malformed_cursor_returns_all(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rb = ReplayBuffer(tmp_path / "ai_sessions", cap=8)
    sid = "bad"
    for i in range(2):
        rb.append(flow_id=1, session_id=sid, event_id=f"{sid}.{i}", payload=_payload(i))

    with caplog.at_level(logging.WARNING, logger=replay_buffer.logger.name):
        out = list(rb.read_after(flow_id=1, session_id=sid, event_id="no-dot-here"))
    assert len(out) == 2
    matches = [r for r in caplog.records if "malformed cursor" in r.getMessage()]
    assert matches, "expected a WARN log on malformed cursor"


def test_payload_roundtrip_preserves_bytes(tmp_path: Path) -> None:
    rb = ReplayBuffer(tmp_path / "ai_sessions", cap=8)
    sid = "raw"
    payloads = [
        b"\x00\x01\x02\xff",  # arbitrary binary
        "event:tool_call\ndata:{\"x\":\"é\"}\n\n".encode(),
        b"",  # empty
    ]
    for i, p in enumerate(payloads):
        rb.append(flow_id=1, session_id=sid, event_id=f"{sid}.{i}", payload=p)
    out = list(rb.read_after(flow_id=1, session_id=sid, event_id=None))
    assert [data for _, data in out] == payloads


def test_disk_rewrite_caps_file_growth(tmp_path: Path) -> None:
    rb = ReplayBuffer(tmp_path / "ai_sessions", cap=4)
    sid = "rewrite"
    # Append 20 entries → file should rewrite once line count exceeds 8.
    for i in range(20):
        rb.append(flow_id=1, session_id=sid, event_id=f"{sid}.{i}", payload=_payload(i))
    path = rb._replay_path(1, sid)
    with open(path, encoding="utf-8") as fh:
        line_count = sum(1 for _ in fh)
    assert line_count <= 8  # cap=4, threshold=2x


def test_invalid_payload_type_raises(tmp_path: Path) -> None:
    rb = ReplayBuffer(tmp_path / "ai_sessions")
    with pytest.raises(TypeError):
        rb.append(flow_id=1, session_id="x", event_id="x.0", payload="not bytes")  # type: ignore[arg-type]


def test_lazy_litellm_contract() -> None:
    import sys

    sys.modules.pop("litellm", None)
    sys.modules.pop("flowfile_core.ai.replay_buffer", None)
    from flowfile_core.ai import replay_buffer as _rb  # noqa: F401

    assert "litellm" not in sys.modules
