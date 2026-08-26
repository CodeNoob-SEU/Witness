"""Browser-side regression tests, executed under Node.

The console ships as plain ES modules with no build step, so the two pieces of
front-end logic that can silently corrupt what an operator sees — ordered
delivery of durable events, and what cursor a reconnect resumes from — are
tested directly rather than only through a browser.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
JS_DIR = ROOT / "src" / "react_agent" / "static" / "assets" / "js"
BUFFER_MODULE = JS_DIR / "event-buffer.js"
SSE_MODULE = JS_DIR / "sse.js"


def _node() -> str:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for the browser-side regression tests")
    return node


def _run_module(script: str) -> str:
    completed = subprocess.run(
        [_node(), "--input-type=module", "-e", script],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def test_durable_event_buffer_delivers_only_a_contiguous_prefix() -> None:
    script = f"""
      import {{ createContiguousEventBuffer }} from {json.dumps(BUFFER_MODULE.as_uri())};
      import assert from "node:assert/strict";

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
    assert json.loads(_run_module(script)) == {"cursor": 5, "buffered": 0}


def test_a_reconnect_resumes_from_the_contiguous_cursor_not_the_highest_seen() -> None:
    """The invariant that keeps a dropped connection from losing events.

    The first connection delivers sequences 1 and 3 — a gap — then drops. If the
    reconnect asked for everything after 3, sequence 2 would be lost forever and
    the operator would be shown a history the log never produced. It must ask
    for everything after 1.
    """

    script = f"""
      import assert from "node:assert/strict";

      const frame = (sequence) => {{
        const payload = {{ kind: "model_started", durable_sequence: sequence, data: {{}} }};
        const data = JSON.stringify(payload);
        return `id: ${{sequence}}\\nevent: model_started\\ndata: ${{data}}\\n\\n`;
      }};

      const requests = [];
      let call = 0;
      globalThis.fetch = async (url, options) => {{
        requests.push({{ url: String(url), headers: options?.headers ?? {{}} }});
        call += 1;
        if (call === 1) {{
          // Deliver 1 and 3 (a gap), then drop the connection.
          //
          // The error has to be raised asynchronously: `controller.error()`
          // resets the stream's queue, so erroring inside `start` would discard
          // the two frames instead of delivering them first.
          return {{
            ok: true,
            body: new ReadableStream({{
              start(controller) {{
                const encoder = new TextEncoder();
                controller.enqueue(encoder.encode(frame(1)));
                controller.enqueue(encoder.encode(frame(3)));
                setTimeout(() => controller.error(new Error("connection reset")), 60);
              }},
            }}),
          }};
        }}
        // Second attempt: record it, then end cleanly so the loop stops.
        return {{ ok: true, body: new ReadableStream({{ start: (c) => c.close() }}) }};
      }};

      const {{ followRun }} = await import({json.dumps(SSE_MODULE.as_uri())});

      const delivered = [];
      const handle = followRun("run-1", {{ onEvent: (event) => delivered.push(event.sequence) }});
      await new Promise((resolve) => setTimeout(resolve, 1800));
      handle.close();

      // Only the contiguous prefix reached the UI: 3 is still held back.
      assert.deepEqual(delivered, [1]);
      assert.ok(requests.length >= 2, `expected a reconnect, got ${{requests.length}} request(s)`);

      const reconnect = requests[requests.length - 1];
      assert.ok(
        reconnect.url.includes("after_sequence=1"),
        `reconnect asked for ${{reconnect.url}}`,
      );
      assert.equal(reconnect.headers["Last-Event-ID"], "1");

      process.stdout.write(JSON.stringify({{ delivered, attempts: requests.length }}));
    """
    result = json.loads(_run_module(script))
    assert result["delivered"] == [1]
    assert result["attempts"] >= 2


def test_live_events_without_a_durable_sequence_pass_straight_through() -> None:
    """Model text deltas carry no sequence and must not be held for ordering."""

    script = f"""
      import assert from "node:assert/strict";

      const delta = JSON.stringify({{ kind: "model_text_delta", live_sequence: 4 }});
      const started = JSON.stringify({{ kind: "run_started", durable_sequence: 1 }});
      const body = [
        `event: model_text_delta\\ndata: ${{delta}}\\n\\n`,
        `id: 1\\nevent: run_started\\ndata: ${{started}}\\n\\n`,
      ].join("");

      globalThis.fetch = async () => ({{
        ok: true,
        body: new ReadableStream({{
          start(controller) {{
            controller.enqueue(new TextEncoder().encode(body));
            controller.close();
          }},
        }}),
      }});

      const {{ followRun }} = await import({json.dumps(SSE_MODULE.as_uri())});
      const kinds = [];
      const handle = followRun("run-1", {{ onEvent: (event) => kinds.push(event.kind) }});
      await new Promise((resolve) => setTimeout(resolve, 600));
      handle.close();

      assert.deepEqual(kinds, ["model_text_delta", "run_started"]);
      process.stdout.write(JSON.stringify(kinds));
    """
    assert json.loads(_run_module(script)) == ["model_text_delta", "run_started"]
