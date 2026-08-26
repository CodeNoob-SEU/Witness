/* Ordered delivery of a durable event stream.
 *
 * The SSE stream interleaves durable events (which carry a monotonic
 * `sequence`) with live, non-durable ones. A durable event must never be
 * shown before its predecessors: doing so would let the UI display a state the
 * log never passed through. This buffer holds out-of-order arrivals until the
 * prefix is contiguous, and reports the gap while one exists.
 *
 * Extracted verbatim from the original single-file console so it can be
 * imported directly by `tests/test_frontend_event_buffer.py` under Node.
 */

// BEGIN CONTIGUOUS_EVENT_BUFFER
export function createContiguousEventBuffer(initialCursor = 0) {
  let cursor = Number.isSafeInteger(initialCursor) && initialCursor >= 0 ? initialCursor : 0;
  const pending = new Map();

  function currentGap() {
    if (!pending.size) return null;
    const firstPending = Math.min(...pending.keys());
    return firstPending > cursor + 1 ? { after: cursor, before: firstPending } : null;
  }

  function accept(event) {
    if (!event || event.durable !== true) {
      return { ready: event ? [event] : [], cursor, gap: currentGap(), duplicate: false };
    }
    const sequence = event.sequence;
    if (!Number.isSafeInteger(sequence) || sequence < 1) {
      return { ready: [], cursor, gap: currentGap(), duplicate: false, invalid: true };
    }
    if (sequence <= cursor || pending.has(sequence)) {
      return { ready: [], cursor, gap: currentGap(), duplicate: true };
    }

    pending.set(sequence, event);
    const ready = [];
    while (pending.has(cursor + 1)) {
      cursor += 1;
      ready.push(pending.get(cursor));
      pending.delete(cursor);
    }
    return { ready, cursor, gap: currentGap(), duplicate: false };
  }

  return {
    accept,
    get cursor() { return cursor; },
    get gap() { return currentGap(); },
    get bufferedCount() { return pending.size; },
  };
}
// END CONTIGUOUS_EVENT_BUFFER
