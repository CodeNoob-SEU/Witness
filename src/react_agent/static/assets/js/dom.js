/* Small DOM helpers. Kept deliberately thin — this console builds elements
 * directly rather than through a virtual DOM, so the helpers exist only to
 * remove repetition, not to become a framework. */

/** Create an element. `props` may include `class`, `text`, `html`, and attrs. */
export function el(tag, props = {}, children = []) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (value === null || value === undefined || value === false) continue;
    if (key === "class") node.className = value;
    else if (key === "text") node.textContent = value;
    else if (key === "dataset") Object.assign(node.dataset, value);
    else if (key.startsWith("on") && typeof value === "function") {
      node.addEventListener(key.slice(2).toLowerCase(), value);
    } else if (value === true) node.setAttribute(key, "");
    else node.setAttribute(key, String(value));
  }
  // Flattened deeply so a helper may return an array of nodes and be dropped
  // straight into a children list without the caller spreading it.
  for (const child of [children].flat(Infinity)) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

export function clear(node) {
  node.replaceChildren();
  return node;
}

export function mount(node, ...children) {
  node.replaceChildren(...children.flat().filter(Boolean));
  return node;
}

export function qs(selector, scope = document) {
  return scope.querySelector(selector);
}

/** An empty-state block, used everywhere a list can legitimately be empty. */
export function emptyState(title, detail) {
  return el("div", { class: "empty" }, [
    el("div", { class: "empty-inner" }, [
      el("strong", { text: title }),
      detail ? el("p", { text: detail }) : null,
    ]),
  ]);
}

export function badge(text, tone = "") {
  return el("span", { class: `badge${tone ? ` badge-${tone}` : ""}`, text });
}

export function dot(tone = "", pulse = false) {
  return el("span", {
    class: `dot${tone ? ` dot-${tone}` : ""}${pulse ? " dot-pulse" : ""}`,
    "aria-hidden": "true",
  });
}

export function kv(pairs) {
  const list = el("dl", { class: "kv" });
  for (const [term, value] of pairs) {
    if (value === null || value === undefined || value === "") continue;
    list.append(el("dt", { text: term }));
    list.append(
      el("dd", {}, [value instanceof Node ? value : el("span", { class: "mono", text: String(value) })]),
    );
  }
  return list;
}

let toastTimer = null;

export function toast(message, tone = "") {
  const existing = qs(".toast");
  if (existing) existing.remove();
  if (toastTimer) clearTimeout(toastTimer);
  const node = el("div", { class: `toast${tone ? ` toast-${tone}` : ""}`, role: "status", text: message });
  document.body.append(node);
  toastTimer = setTimeout(() => node.remove(), 6000);
}

/** Announce a state change to assistive technology without visual noise. */
export function announce(message) {
  const region = qs("#announcer");
  if (region) region.textContent = message;
}
