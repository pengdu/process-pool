# Testing

No external framework (no gtest/catch2). Tests are standalone C++ programs with hand-rolled assertions. Four suites run under ctest; two heavy suites are manual:

| Suite | Level | Contents |
|---|---|---|
| `pp_smoke` | e2e smoke | fork workers, dispatch, verify via proof files |
| `pp_unit` | unit (26 cases) | single-process: errno classify, heap, buffer, channel, timer, address, mutex, msgqueue, logger, reactor registry |
| `pp_func` | functional (10 cases) | fork-per-case: scheduling, IPC modes, reload, respawn, async, negative paths |
| `pp_integration` | integration (6 cases) | multi-process: 120-task stress, reload-continues-serving, targeted dispatch, request/reply matching, async burst, worker-id stability |
| `pp_stress` (manual) | stress | 50k-task dispatch throughput + checksum, msgbox reply flood, reload-under-load (zero loss), msgqueue throughput; `pp_stress [N]` scales load |
| `pp_soak` (manual) | soak | long-run mixed workload (dispatch + msgbox + periodic reloads + worker recycling) with RSS leak signal: `pp_soak [seconds] [workers]` |

## Running

```bash
./build.sh --test
# or
ctest --test-dir build --output-on-failure
ctest --test-dir build -R pp_func        # single test binary
./build/tests/pp_func                    # direct run

# heavy manual suites
./build/tests/pp_stress 1               # default load
./build/tests/pp_soak 60                # 1-minute soak; 3600 for hour-long runs
```

`pp_func` has a 60s ctest timeout (tests/CMakeLists.txt).

Coverage (LLVM source-based; needs brew llvm): `tools/coverage.sh`.

## Test files

### tests/smoke.cc (`pp_smoke`)

End-to-end smoke: fork 3 workers, dispatch one payload each via `dispatch_sync()`; workers write proof files to `/tmp/pp_pp_smoke_{id}.txt`; master verifies distinct child PIDs and payloads.

### tests/func.cc (`pp_func`, 9 cases)

Custom fork-per-test harness: each case runs in a freshly forked child to isolate pool state, signal handlers, and `PPWG` globals. Cases registered in a `kCases[]` array of `TestCase` structs; `main()` forks per case. Cross-process assertions use shared-memory counters via `pp_shm_malloc`.

| Case | Verifies |
|---|---|
| `unixsock_scheduling` | `dispatch()` spreads 12 tasks round-robin across 3 workers |
| `msgqueue_ipc` | `dispatch_sync()` over SysV msgqueue to targeted workers |
| `max_request_recycle` | worker exits after N tasks, `wait()` respawns (distinct PIDs) |
| `reload` | graceful reload; callbacks fire exactly once; SIGTERM-ignoring workers get SIGKILL via timeout killer |
| `crash_respawn` | SIGKILL triggers `onWorkerError` + respawn |
| `crash_in_task` | SIGSEGV mid-task reported, worker respawned, pool still serves |
| `exit_nonzero_respawn` | non-zero exit respawned silently; `onWorkerError` must NOT fire |
| `worker_message` | `push_message`/`onWorkerMessage` round-trip via SIGIO |
| `negative_paths` | `PP_IPC_SOCKET` rejected; stream stubs return NOT_SUPPORT; worker-id validation; oversized `push_message` rejected |

## Conventions for new tests

- Assertion macro: `EXPECT(cond)` — increments `test_failures`, prints to stderr (defined in func.cc)
- Test functions named `test_<name>()`, added to the `kCases[]` array
- Fork-per-test to isolate signals and process-pool globals; use `pp_shm_malloc` counters for assertions that cross the fork boundary
- New test binaries: add to `tests/CMakeLists.txt` with `add_executable` + `add_test`, link `pp_process_pool`
