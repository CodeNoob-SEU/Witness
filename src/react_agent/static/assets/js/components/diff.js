/* The patch viewer.
 *
 * Selecting a line is how provenance is requested: the click carries the file
 * up to the evidence panel, which then shows the tool call whose checkpoint
 * moved that file's tree. Attribution is per-file, not per-line, and the
 * header says so — a file rewritten by several calls lists them all rather
 * than guessing which write owns a given line.
 */

import { el, badge, emptyState } from "../dom.js";
import { splitPath } from "../format.js";

function fileHeader(file, onSelect) {
  const { dir, name } = splitPath(file.path);
  const tone =
    file.change === "added" ? "ok" : file.change === "deleted" ? "bad" : "";

  return el("div", { class: "diff-file-head" }, [
    el("span", { class: "file-path grow truncate" }, [
      dir ? el("span", { class: "file-dir", text: dir }) : null,
      el("span", { text: name }),
    ]),
    el("span", { class: "file-delta" }, [
      el("span", { class: "delta-add", text: `+${file.additions}` }),
      " ",
      el("span", { class: "delta-del", text: `−${file.deletions}` }),
    ]),
    badge(file.change, tone),
    file.attribution === "shared"
      ? badge("shared attribution", "warn")
      : file.attribution === "unattributed"
        ? badge("unattributed", "warn")
        : null,
    el("button", {
      class: "btn btn-sm btn-ghost",
      text: "Evidence",
      onClick: () => onSelect?.(file, null),
    }),
  ]);
}

function hunkNode(file, hunk, onSelect) {
  const rows = hunk.lines.map((line) => {
    const cls =
      line.kind === "added"
        ? "diff-line diff-added"
        : line.kind === "removed"
          ? "diff-line diff-removed"
          : "diff-line";
    const sign = line.kind === "added" ? "+" : line.kind === "removed" ? "−" : " ";
    const row = el(
      "div",
      {
        class: cls,
        role: "button",
        tabindex: "-1",
        "data-path": file.path,
        onClick: () => onSelect?.(file, line),
      },
      [
        el("span", { class: "diff-num", text: line.old_line ?? "" }),
        el("span", { class: "diff-num", text: line.new_line ?? "" }),
        el("span", { class: "diff-sign", text: sign, "aria-hidden": "true" }),
        el("span", { class: "diff-text", text: line.text }),
      ],
    );
    return row;
  });

  return [
    el("div", {
      class: "diff-hunk-head",
      text: `@@ -${hunk.old_start},${hunk.old_count} +${hunk.new_start},${hunk.new_count} @@${
        hunk.section ? ` ${hunk.section}` : ""
      }`,
    }),
    el("div", { class: "diff-lines" }, rows),
  ];
}

/** Render a whole patch. `onSelect(file, line|null)` drives the evidence panel. */
export function renderPatch(patch, { onSelect, filterPath = null } = {}) {
  const files = filterPath
    ? patch.files.filter((file) => file.path === filterPath)
    : patch.files;

  if (!files.length) {
    return emptyState(
      "No changes yet",
      patch.files.length
        ? "That file is not part of this patch."
        : "This run has not moved the workspace tree. Reads and refusals leave no diff — which is what makes the ones that do meaningful.",
    );
  }

  return el(
    "div",
    { class: "stack gap-6" },
    files.map((file) =>
      el("section", { class: "diff-file" }, [
        fileHeader(file, onSelect),
        file.binary
          ? el("div", { class: "evidence-empty", text: "Binary file — not shown." })
          : file.hunks.flatMap((hunk) => hunkNode(file, hunk, onSelect)),
      ]),
    ),
  );
}

/** Mark one line as selected, clearing any previous selection. */
export function selectLine(root, node) {
  for (const previous of root.querySelectorAll('.diff-line[aria-selected="true"]')) {
    previous.removeAttribute("aria-selected");
  }
  if (node) node.setAttribute("aria-selected", "true");
}
