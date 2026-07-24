# Kernel-integration regression investigation (handoff prompt)

Investigate a kernel-integration test regression on `main`.

## Observed
- `poetry run pytest flowfile_core/tests -m kernel` is GREEN at commit c83b039b
  ("Handle missing join keys… #591") and FAILS at 79debb64 ("Auto-escalate CSV
  schema inference on type conflicts", #593). 79debb64 is the only commit in the
  range c83b039b..79debb64.
- Failing run: 15 failed / 62 passed. The FIRST failure is
  test_kernel_integration.py::TestPythonScriptNodeCancellation::
  test_cancel_long_running_kernel_execution — it runs a python_script node that
  sleeps 30s, cancels the graph after ~3s, then calls
  manager.execute(kernel_id, ExecuteRequest(node_id=100, code="x = 1 + 1")),
  which returns HTTP 500 from the kernel container (POST .../execute).
- EVERY later test then fails the same way: all of
  test_kernel_persistence_integration.py (flowfile_ctx.publish_artifact(...) → 500)
  and test_lsp_kernel_integration.py (/execute 500s, and pl./hover completions
  returning empty). They all share ONE kernel (kernel_id='integration-test',
  a single KernelManager instance).
- Reproduction requires Docker (the `kernel_manager` fixture builds the kernel
  image and starts a container).

## Goal
Find the root cause of the 500s and decide, WITH EVIDENCE, whether #593 is
genuinely responsible or the bisect is confounded (e.g. a flaky/ordering issue
that the commit merely perturbs). Then land the smallest change that makes
`-m kernel` reliably green, and prove it with repeated runs.

## Answer these explicitly — do not assume the answer up front
1. Is the failure deterministic on 79debb64? Run `-m kernel` several times; run
   the trigger alone (`-k test_cancel_long_running_kernel_execution`); run a
   persistence test WITHOUT the cancel test running first. Does one wedged kernel
   poison the rest of the session, or does each /execute fail independently?
2. What actually raises the 500? The client only sees "500 Internal Server Error."
   Get the kernel CONTAINER's stderr/traceback for the failing /execute (docker
   logs, or the in-container log file) and quote it verbatim.
3. Map each failing test's runtime code path against #593's diff
   (`git show 79debb64`). Does any failing test execute code #593 changed? (Verify:
   #593's edits are in create_from_path_{csv,json} and
   flow_node.py::_do_execute_remote, and it touches no kernel_runtime/ file or
   Dockerfile — so the kernel image should be byte-identical on both commits.)
4. If #593 is the cause, state the exact mechanism. If it's a pre-existing race
   that #593 only reshuffles timing for, characterize the race.

## Entry points
- Bisect: `git show 79debb64`, `git log --oneline c83b039b..79debb64`.
- Tests + fixture: flowfile_core/tests/flowfile/test_kernel_integration.py,
  test_kernel_persistence_integration.py, test_lsp_kernel_integration.py, and the
  `kernel_manager` fixture (check its scope and why one kernel is shared).
- Cancel/interrupt path: flowfile_core/flowfile_core/kernel/manager.py (execute,
  interrupt/cancel, raise_for_status ~:1584) and the kernel side in
  kernel_runtime/kernel_runtime/main.py (the /execute handler + interrupt/SIGUSR1
  handling and how it readies itself for the next request).
- #593 code: flow_node.py::_do_execute_remote,
  flowfile_core/.../flow_data_engine/create/funcs.py,
  flowfile_worker/.../create/funcs.py.

## Deliverable
A short root-cause writeup answering 1–4 (include the container traceback), plus a
minimal fix — a real kernel/recovery fix if it's a regression, or
test-isolation / interrupt-recovery hardening if it's a shared-kernel race —
verified by running `-m kernel` green multiple times. Do not broaden scope beyond
making the kernel suite reliably green.

## Constraints
- Docker required. On a shared dev box, don't disturb already-running flowfile
  services (core :63578 / worker :63579); prefer a clean/CI-like environment.
