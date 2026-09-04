# SWE-bench end-to-end runs on server3 (2026-09-04)

Instance: `pytest-dev__pytest-7490` (SWE-bench Verified, "15 min - 1 hour").
Model: `gpt-5.5` via an OpenAI-compatible relay, Responses API, reasoning effort high.
Runtime: `AgentRuntime` + `PostgresRunJournal` + `GitWorktreeWorkspace`; tests executed
inside the official SWE-bench image with the worktree bind-mounted (`ContainerCommandRunner`).

| Run | Tools | Crashes injected | Result |
| --- | --- | --- | --- |
| `run_13016e96` | harness-defined (pre-module) | SIGKILL during `run_tests`, SIGKILL during model call | 400 events, 3 executions, FAIL_TO_PASS 2/2, PASS_TO_PASS 78/78 |
| `run_16393f75_builtin_tools` | `react_agent.repo_tools` (built-in) | SIGKILL during `run_tests` | 392 events, 2 executions, FAIL_TO_PASS 2/2, PASS_TO_PASS 78/78 |

`harness/swe_harness.py` is the current harness (built-in tools). `logs/provider_http.ndjson`
is the diagnostic capture that exposed the `parsed_arguments` replay bug fixed in
`provider.py`. Run 2's evaluation copy had to be seeded with the gitignored
`src/_pytest/_version.py` (see `evaluation.json` note) because Git worktrees only
materialize tracked files.
