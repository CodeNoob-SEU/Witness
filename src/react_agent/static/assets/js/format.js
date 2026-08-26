/* Formatting rules.
 *
 * One rule dominates this file: an unknown value is rendered as "unknown", not
 * as zero, blank, or a dash that reads like zero. The cost ledger refuses to
 * round an interrupted attempt's spend down to 0, and the interface it feeds
 * would undo that guarantee if it displayed "$0.00".
 */

export const UNKNOWN = "unknown";

export function money(micros, currency) {
  if (micros === null || micros === undefined) return UNKNOWN;
  const amount = micros / 1_000_000;
  const digits = amount !== 0 && Math.abs(amount) < 0.01 ? 6 : 4;
  return `${amount.toFixed(digits)}${currency ? ` ${currency}` : ""}`;
}

export function count(value) {
  if (value === null || value === undefined) return UNKNOWN;
  return new Intl.NumberFormat("en-US").format(value);
}

export function shortId(value, head = 8) {
  if (!value) return "—";
  const text = String(value);
  return text.length > head + 4 ? text.slice(0, head) : text;
}

export function duration(seconds) {
  if (seconds === null || seconds === undefined) return UNKNOWN;
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${Math.round(seconds % 60)}s`;
}

export function timeOfDay(epochSeconds) {
  if (!epochSeconds) return "";
  const date = new Date(epochSeconds * 1000);
  return date.toLocaleTimeString([], { hour12: false });
}

export function relativeTime(epochSeconds) {
  if (!epochSeconds) return "";
  const delta = Date.now() / 1000 - epochSeconds;
  if (delta < 60) return "just now";
  if (delta < 3600) return `${Math.floor(delta / 60)}m ago`;
  if (delta < 86400) return `${Math.floor(delta / 3600)}h ago`;
  return `${Math.floor(delta / 86400)}d ago`;
}

/** Split a path so the directory can be dimmed and the basename kept legible. */
export function splitPath(path) {
  const index = String(path).lastIndexOf("/");
  if (index < 0) return { dir: "", name: String(path) };
  return { dir: path.slice(0, index + 1), name: path.slice(index + 1) };
}

/** Map a run status onto the console's four-colour state vocabulary. */
export function statusTone(status, terminal) {
  if (status === "completed") return "ok";
  if (status === "aborted" || status === "failed") return "bad";
  if (status === "needs_reconciliation") return "warn";
  return terminal ? "ok" : "run";
}

export function statusLabel(snapshot) {
  if (!snapshot) return "unknown";
  if (snapshot.status && snapshot.status !== "running") return snapshot.status;
  return snapshot.state ?? "running";
}
