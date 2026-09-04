/* HTTP client for the console's read-only projections and run commands. */

class ApiError extends Error {
  constructor(message, status) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: { accept: "application/json", ...(options.headers ?? {}) },
    ...options,
  });
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const body = await response.json();
      if (body && typeof body.detail === "string") detail = body.detail;
    } catch {
      /* A non-JSON error body is still an error; keep the status line. */
    }
    throw new ApiError(detail, response.status);
  }
  if (response.status === 204) return null;
  return response.json();
}

export const api = {
  ApiError,

  console: () => request("/api/console"),
  health: () => request("/api/health"),

  tasks: () => request("/api/tasks"),
  startTask: (taskId) => request(`/api/tasks/${encodeURIComponent(taskId)}/runs`, { method: "POST" }),

  run: (runId) => request(`/api/runs/${encodeURIComponent(runId)}`),
  sessionRuns: (sessionId) => request(`/api/sessions/${encodeURIComponent(sessionId)}/runs`),
  patch: (runId) => request(`/api/runs/${encodeURIComponent(runId)}/patch`),
  integrity: (runId) => request(`/api/runs/${encodeURIComponent(runId)}/integrity`),
  evals: () => request("/api/evals"),

  cancel: (runId) =>
    request(`/api/runs/${encodeURIComponent(runId)}/cancel`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ reason: "user_requested" }),
    }),
  resume: (runId) => request(`/api/runs/${encodeURIComponent(runId)}/resume`, { method: "POST" }),
  fork: (runId, fromSequence) =>
    request(`/api/runs/${encodeURIComponent(runId)}/fork`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(fromSequence ? { from_sequence: fromSequence } : {}),
    }),
};
