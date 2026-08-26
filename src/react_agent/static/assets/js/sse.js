/* Server-sent events reader for `/api/runs/{id}/events`.
 *
 * Two things matter here beyond plain parsing:
 *
 * 1. Durable events (those with a `durable_sequence`) are routed through the
 *    contiguous buffer so the UI never renders sequence N+1 before N.
 * 2. Reconnects resume from the last delivered sequence via `Last-Event-ID`,
 *    so a dropped connection replays the gap instead of losing it.
 */

import { createContiguousEventBuffer } from "./event-buffer.js";

function parseBlock(block) {
  if (!block) return null;
  let eventName = "message";
  let eventId = null;
  const dataLines = [];
  for (const line of block.split(/\r\n|\n|\r/)) {
    if (!line || line.startsWith(":")) continue;
    const colon = line.indexOf(":");
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    if (field === "event") eventName = value || "message";
    else if (field === "id") eventId = value || null;
    else if (field === "data") dataLines.push(value);
  }
  if (!dataLines.length) return null;
  const raw = dataLines.join("\n");
  let payload = raw;
  if (raw !== "[DONE]") {
    try {
      payload = JSON.parse(raw);
    } catch {
      /* Plain text is valid SSE data; hand it through unparsed. */
    }
  } else {
    payload = {};
  }
  return { eventName, eventId, payload, raw };
}

function normalize(frame) {
  const payload = frame.payload;
  if (!payload || typeof payload !== "object") return null;
  const sequence = Number.isSafeInteger(payload.durable_sequence)
    ? payload.durable_sequence
    : Number.isSafeInteger(payload.sequence)
      ? payload.sequence
      : null;
  return {
    ...payload,
    kind: String(payload.kind ?? frame.eventName ?? "").replace(/[.-]/g, "_"),
    sequence,
    // The buffer keys ordering off this flag: live model deltas carry no
    // durable sequence and must pass straight through.
    durable: sequence !== null,
  };
}

/**
 * Follow one run's event stream.
 *
 * Returns a handle with `close()`. `onEvent` receives normalized, in-order
 * events; `onStatus` receives connection transitions so the UI can show them
 * rather than silently appearing stale.
 */
export function followRun(runId, { afterSequence = 0, onEvent, onStatus, onGap } = {}) {
  const controller = new AbortController();
  const buffer = createContiguousEventBuffer(afterSequence);
  let closed = false;
  let attempt = 0;

  const status = (state, detail) => onStatus?.(state, detail);

  async function connect() {
    while (!closed) {
      try {
        status(attempt === 0 ? "connecting" : "reconnecting");
        const response = await fetch(
          `/api/runs/${encodeURIComponent(runId)}/events?after_sequence=${buffer.cursor}&follow=true`,
          {
            signal: controller.signal,
            headers: buffer.cursor ? { "Last-Event-ID": String(buffer.cursor) } : {},
          },
        );
        if (!response.ok) {
          status("error", `HTTP ${response.status}`);
          return;
        }
        if (!response.body) {
          status("error", "no readable stream");
          return;
        }
        attempt = 0;
        status("live");

        const reader = response.body.getReader();
        const decoder = new TextDecoder("utf-8");
        let pending = "";
        try {
          for (;;) {
            const { value, done } = await reader.read();
            if (done) break;
            pending += decoder.decode(value, { stream: true });
            for (;;) {
              const match = /(?:\r\n|\r|\n)(?:\r\n|\r|\n)/.exec(pending);
              if (!match) break;
              const block = pending.slice(0, match.index);
              pending = pending.slice(match.index + match[0].length);
              dispatch(block);
            }
          }
          pending += decoder.decode();
          if (pending.trim()) dispatch(pending);
        } finally {
          reader.releaseLock();
        }

        // A clean end of stream means the run reached a terminal event.
        status("ended");
        return;
      } catch (error) {
        if (closed || error?.name === "AbortError") return;
        attempt += 1;
        if (attempt > 5) {
          status("error", "stream lost");
          return;
        }
        status("reconnecting", `attempt ${attempt}`);
        await new Promise((resolve) => setTimeout(resolve, Math.min(400 * 2 ** attempt, 5000)));
      }
    }
  }

  function dispatch(block) {
    const frame = parseBlock(block);
    if (!frame) return;
    if (frame.eventName === "error") {
      status("error", frame.payload?.message ?? "stream error");
      return;
    }
    const event = normalize(frame);
    if (!event || !event.kind) return;
    const result = buffer.accept(event);
    if (result.gap) onGap?.(result.gap);
    for (const ready of result.ready) onEvent?.(ready);
  }

  connect();

  return {
    close() {
      closed = true;
      controller.abort();
    },
    get cursor() {
      return buffer.cursor;
    },
  };
}
