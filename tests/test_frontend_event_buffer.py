from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
INDEX_HTML = ROOT / "src" / "react_agent" / "static" / "index.html"
BUFFER_START = "// BEGIN CONTIGUOUS_EVENT_BUFFER"
BUFFER_END = "// END CONTIGUOUS_EVENT_BUFFER"


def _buffer_source() -> str:
    source = INDEX_HTML.read_text(encoding="utf-8")
    _, marked = source.split(BUFFER_START, 1)
    body, _ = marked.split(BUFFER_END, 1)
    return body


def test_durable_event_buffer_delivers_only_a_contiguous_prefix() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for the browser reducer regression test")

    script = f"""
      "use strict";
      const assert = require("node:assert/strict");
      {_buffer_source()}

      const buffer = createContiguousEventBuffer(0);
      const durable = (sequence) => ({{ durable: true, sequence, marker: `#${{sequence}}` }});
      const sequences = (result) => result.ready.map((event) => event.sequence);

      let result = buffer.accept(durable(3));
      assert.deepEqual(sequences(result), []);
      assert.equal(result.cursor, 0);
      assert.deepEqual(result.gap, {{ after: 0, before: 3 }});

      result = buffer.accept(durable(1));
      assert.deepEqual(sequences(result), [1]);
      assert.equal(result.cursor, 1);
      assert.deepEqual(result.gap, {{ after: 1, before: 3 }});

      const live = {{ durable: false, live_sequence: 99, marker: "live" }};
      result = buffer.accept(live);
      assert.deepEqual(result.ready, [live]);
      assert.equal(result.cursor, 1);

      result = buffer.accept(durable(2));
      assert.deepEqual(sequences(result), [2, 3]);
      assert.equal(result.cursor, 3);
      assert.equal(result.gap, null);

      for (const sequence of [2, 3]) {{
        result = buffer.accept(durable(sequence));
        assert.deepEqual(sequences(result), []);
        assert.equal(result.cursor, 3);
        assert.equal(result.duplicate, true);
      }}

      result = buffer.accept(durable(5));
      assert.deepEqual(sequences(result), []);
      assert.deepEqual(result.gap, {{ after: 3, before: 5 }});
      result = buffer.accept(durable(5));
      assert.equal(result.duplicate, true);
      result = buffer.accept(durable(4));
      assert.deepEqual(sequences(result), [4, 5]);
      assert.equal(result.cursor, 5);
      assert.equal(result.gap, null);

      const resumed = createContiguousEventBuffer(7);
      assert.deepEqual(sequences(resumed.accept(durable(9))), []);
      assert.deepEqual(sequences(resumed.accept(durable(8))), [8, 9]);
      assert.equal(resumed.cursor, 9);

      process.stdout.write(JSON.stringify({{
        cursor: buffer.cursor,
        buffered: buffer.bufferedCount,
      }}));
    """
    completed = subprocess.run(
        [node, "-e", script],
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(completed.stdout) == {"cursor": 5, "buffered": 0}


def test_runtime_reconnect_uses_only_the_contiguous_durable_cursor() -> None:
    source = INDEX_HTML.read_text(encoding="utf-8")

    assert 'after_sequence: String(Math.max(0, cursor))' in source
    assert 'headers["Last-Event-ID"] = String(cursor)' in source
    cursor_expression = (
        "const cursor = first && initialAfter !== null ? initialAfter : state.durableCursor"
    )
    assert cursor_expression in source
    assert "if (durableBuffer.bufferedCount > 0)" in source
    assert "snapshotLastSequence > state.durableCursor" in source
    assert "terminalStatus && !hasUnseenDurable" in source
