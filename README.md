# procpool

A standalone C++14 master/worker process pool for POSIX systems, extracted
from [swoole-src](https://github.com/swoole/swoole-src).

A master process forks a fixed set of worker processes, dispatches
fixed-size task packets (`pp::EventData`) to them over local IPC, and takes
care of the lifecycle chores that usually get reimplemented badly: worker
reaping and respawn on crash, graceful reload with timeout escalation,
max-request recycling, and worker-to-master messaging.

Everything compiles into one static library, `pp_process_pool`, with **zero
external dependencies** — only the system threads library (plus `rt` on
non-Apple UNIX).

## Features

- **Master/worker pool** — `create()` / `start()` / `dispatch()` /
  `shutdown()` / `wait()` / `destroy()` lifecycle
- **Three IPC transports**, chosen per pool at `create()` time:

  | Mode | Transport | Notes |
  | --- | --- | --- |
  | `PP_IPC_UNIXSOCK` | one unix datagram socketpair per worker | default for most uses |
  | `PP_IPC_MSGQUEUE` | SysV message queue | targeted dispatch by worker id; needs `HAVE_MSGQUEUE` |
  | `PP_IPC_NONE` | shared-memory message box only | no task transport: workers need a custom `main_loop`; `dispatch()` returns `PP_ERR` |

- **Round-robin or targeted dispatch** — `dispatch()` writes without
  blocking; `dispatch_sync()` sends synchronously (blocks until written).
  Replies travel back separately via `push_message()`/`onWorkerMessage`
- **Crash handling** — a worker killed by a signal fires `onWorkerError` and
  is respawned; a non-zero exit is respawned silently
- **Graceful reload** — `reload()` replaces workers one at a time with
  SIGTERM, escalating to SIGKILL after `max_wait_time` seconds
- **Max-request recycling** — workers exit and are respawned after serving
  N tasks (`set_max_request()`), with an optional random grace margin
- **Worker→master messages** — `push_message()` writes to a shared-memory
  ring buffer and signals the master with SIGIO; the master delivers via
  `onWorkerMessage`
- **12 lifecycle callbacks** — plain C function pointers: `onStart`,
  `onShutdown`, `onBeforeReload`, `onAfterReload`, `onTask`, `onWorkerStart`,
  `onMessage`, `onWorkerExit`, `onWorkerStop`, `onWorkerError`,
  `onWorkerMessage`, `onWorkerNotFound`
- **Event-driven (async) workers** — `set_async(true)` gives each worker its own
  reactor (epoll on Linux, kqueue on macOS/BSD); incoming packets arrive via
  `onMessage` instead of the task-protocol loop; the loop runs until SIGTERM

## Quick start

Requires CMake ≥ 3.13 and a C++14 compiler on Linux or macOS/BSD.

```sh
./build.sh            # configure + build
./build.sh --test     # ... then run ctest
./build.sh --clean    # remove build/
```

Or manually:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j$(getconf _NPROCESSORS_ONLN)
ctest --test-dir build --output-on-failure
```

Artifacts:

- `build/libpp_process_pool.a` — the static library
- `build/demo/pp_demo`, `build/demo/pp_task_dispatch` — runnable examples
- `build/tests/pp_smoke`, `build/tests/pp_func` — tests (registered with ctest)

## Usage

```cpp
#include "pp.h"
#include "pp_process_pool.h"

static int on_task(pp::ProcessPool* pool, pp::Worker* worker, pp::EventData* task) {
  // runs inside the forked worker process
  printf("[worker %u] got %u bytes\n", worker->id, task->info.len);
  worker->add_request_count();
  return 0;
}

int main() {
  pp_init();                           // boot the runtime; call this first

  pp::ProcessPool pool {};
  pool.create(4, 0, PP_IPC_UNIXSOCK);  // 4 workers over unix-socket IPC
  pool.onTask = on_task;
  pool.set_protocol(PP_PROTOCOL_TASK);
  pool.start();                        // forks the workers

  pp::EventData task {};
  task.info.len = 5;
  memcpy(task.data, "hello", 5);
  int dst = -1;                        // -1 = let the pool pick the next worker
  pool.dispatch(&task, &dst);          // nonblocking; dispatch_sync() blocks until written

  usleep(100 * 1000);                  // let workers drain the pipe first --
  pool.shutdown();                     // shutdown() SIGTERMs the workers
  pool.wait();                         // reap every child
  pool.destroy();
}
```

Link against `pp_process_pool`; the target exports `include/` and the
generated `build/include` via `target_include_directories`, so a plain
`target_link_libraries(your_app pp_process_pool)` is enough.

See `demo/demo.cc` (round-robin squares over shared memory) and
`demo/task_dispatch.cc` (typed jobs + worker→master replies) for complete
programs.

## Scope

Only `PP_PROTOCOL_TASK` dispatch is supported — one `EventData` per message.
Everything else was **physically removed** from the sources, with no option
to turn it back on:

- the server, coroutine runtime, and async-IO thread pool
- OpenSSL/TLS, the network client/stream/proxy stack, and all application
  protocols (HTTP, WebSocket, Redis, MySQL, …)
- DNS / name resolution of any kind (`Address::assign` only accepts numeric
  IPs and unix-socket paths)
- Windows/Cygwin/MinGW support (POSIX only; the poll() fallback reactor was
  removed too)

`PP_IPC_SOCKET` and the stream/message protocols do not exist in this tree;
`create()` rejects `PP_IPC_SOCKET` with `PP_ERROR_OPERATION_NOT_SUPPORT`.
The resulting static library contains zero OpenSSL, coroutine, server,
network-client, DNS or Windows symbols (verified with `nm -u` / `otool -L`).

## Layout

```
process-pool/
├── CMakeLists.txt        build definition + platform feature probes
├── build.sh              convenience wrapper around cmake/ctest
├── cmake/config.h.in     feature header template (CMake replaces autoconf)
├── include/              public headers: pp.h (umbrella) + pp_*.h
├── src/
│   ├── core/             runtime: init, logging, buffers, errno classify
│   ├── os/               process_pool.cc (core), channel, pipes, msgqueue, signals
│   ├── network/          socket + address primitives used by the pipes
│   ├── reactor/          base + epoll/kqueue backends + timer
│   ├── memory/           shared memory + global memory pool
│   ├── lock/             mutex
│   └── wrapper/          pp_event_* / pp_timer_* C API wrappers
├── demo/                 pp_demo, pp_task_dispatch
└── tests/                smoke.cc + func.cc
```

Platform features (`HAVE_EPOLL`, `HAVE_KQUEUE`, `HAVE_MSGQUEUE`,
`HAVE_SIGNALFD`, …) are probed by CMake and written to
`build/include/config.h`; sources gate on them with `#ifdef`.

## Testing

No external test framework — tests are standalone programs registered with
ctest:

- **`tests/smoke.cc`** verifies the extraction end to end: forks three
  workers, dispatches one payload to each, and asserts on the master side
  that each proof file names a distinct child process and the expected
  payload.
- **`tests/unit.cc`** (26 cases) covers single-process internals; **`tests/integration.cc`** adds 6 multi-process scenarios; **`tests/stress.cc`** (throughput/correctness under load) and **`tests/soak.cc`** (long-run stability with an RSS leak signal, `pp_soak <seconds> [workers]`) are manual suites; `tools/coverage.sh` measures LLVM source coverage.
- **`tests/func.cc`** runs 10 functional cases in a fork-per-test harness
  (each case gets a fresh child to isolate pool state and signal handlers):
  round-robin scheduling, msgqueue IPC, max-request recycling, graceful
  reload with SIGKILL escalation, crash respawn, mid-task SIGSEGV recovery,
  non-zero-exit respawn, worker messages, and negative paths.

```sh
ctest --test-dir build --output-on-failure
```

Note: `shutdown()` sends SIGTERM to the workers. If you tear the pool down
immediately after dispatching, a worker may be killed before it drains the
pipe — sleep briefly before shutting down, as the demos and tests do.

## Code style

- Formatting: `.clang-format` (Google base, 200-column, 2-space) and
  `.cmake-format`
- Static analysis and naming: `.clang-tidy` — namespace `pp`, classes
  `CamelCase`, variables `lower_case`, private members with a trailing `_`,
  macros/enums `PP_*`, C API `pp_*`, globals `PPG` / `PPTG` / `PPWG`

```sh
clang-format -i include/*.h src/*/*.cc demo/*.cc tests/*.cc
cmake-format -i CMakeLists.txt demo/CMakeLists.txt tests/CMakeLists.txt
clang-tidy <file> -p build --extra-arg="-isysroot$(xcrun --show-sdk-path)"  # macOS
```

## Provenance

Extracted from swoole-src at commit
`21f44eeb27b693fa78250b26828c491f8b64d30a` (2026-10-05, version `6.3.0RC1`).
The tree started as a verbatim copy of upstream `src/` and `include/` and
was trimmed in successive passes:

1. whole subsystems the pool never reaches were deleted outright;
2. `#ifdef` branches for removed subsystems (`SW_USE_OPENSSL`, `_WIN32`,
   coroutine, iouring, stream/message protocol) were eliminated with
   `unifdef`, keeping the POSIX/blocking side verbatim;
3. dead declarations, enum values, and global fields were removed by hand;
4. object-level reachability analysis over `nm` output found and deleted
   translation units no entry point can reach.

All identifiers were renamed from the upstream `swoole`/`SW_`/`sw_` prefixes
to `pp`/`PP_`/`pp_`. There are no extraction guard macros left: the tree only
builds in exactly this configuration. The sibling `swoole-src` tree is left
untouched; this library coexists with the original rather than replacing it.

## License

Apache-2.0, inherited from swoole-src. License banners in the headers retain
the original Swoole attribution as required.
