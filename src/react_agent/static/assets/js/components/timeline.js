/* The event timeline.
 *
 * Renders the durable log verbatim, with execution boundaries called out. A
 * boundary is the visual point of the whole view: it is where a run stopped
 * being one process's story and became a fact another process could resume.
 */

import { el, emptyState } from "../dom.js";
import { eventDetail, eventGroup, projectExecutions } from "../projection.js";
import { timeOfDay } from "../format.js";

export function renderTimeline(events, { onSelect } = {}) {
  if (!events.length) {
    return emptyState("No events yet", "Dispatch a task and its durable log appears here as it commits.");
  }

  const executions = projectExecutions(events);
  const boundaries = new Map(
    executions.filter((item) => item.resumed).map((item) => [item.firstSequence, item]),
  );

  const rows = [];
  for (const event of events) {
    const boundary = boundaries.get(event.sequence);
    if (boundary) {
      rows.push(
        el("div", { class: "tl-boundary" }, [
          el("span", { text: "⎯⎯ execution boundary" }),
          el("span", {
            class: "mono",
            style: "font-weight:400",
            text: `resumed at #${boundary.firstSequence} · execution ${boundary.executionId.slice(0, 8)}`,
          }),
        ]),
      );
    }
    const group = eventGroup(event.kind);
    rows.push(
      el(
        "div",
        {
          class: "tl-row",
          role: "button",
          tabindex: "-1",
          onClick: () => onSelect?.(event),
        },
        [
          el("span", { class: "tl-seq", text: `#${event.sequence ?? "—"}` }),
          el("span", { class: `tl-kind k-${group}`, text: event.kind }),
          el("span", { class: "tl-seq", text: event.step ? `step ${event.step}` : "" }),
          el("span", { class: "tl-detail truncate" }, [
            eventDetail(event),
            event.timestamp
              ? el("span", { class: "faint", text: `  ${timeOfDay(event.timestamp)}` })
              : null,
          ]),
        ],
      ),
    );
  }

  return el("div", { class: "timeline" }, rows);
}
