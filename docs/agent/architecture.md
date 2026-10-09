# Architecture

Standalone extraction of swoole-src's process pool (vendored from swoole-src commit `21f44eeb`, v6.3.0RC1). Scope: task protocol only, POSIX only. Server/coroutine/OpenSSL/DNS/stream-client subsystems were physically removed upstream of this tree.

## Layout

```
include/            24 public headers: pp.h (umbrella) + pp_*.h
src/
  core/             runtime: base.cc (pp_init, globals, pp_fork), log, buffer, error, heap, misc
  os/               process_pool.cc (CORE), channel (shared-memory box), pipe/unix_socket, msg_queue, signal, wait, timer
  reactor/          base + epoll/kqueue + timer.cc (Timer rides the reactor loop)
  network/          socket.cc (largest file), address.cc (numeric IP / unix path only, no DNS)
  memory/           global_memory.cc, shared_memory.cc (pp_shm_malloc, mmap-based)
  lock/             mutex.cc
  wrapper/          event.cc, timer.cc — pp_event_* / pp_timer_* C API wrappers
demo/               demo.cc, task_dispatch.cc
tests/              smoke.cc, func.cc
cmake/config.h.in   feature-probe header template
```

## Core abstraction: ProcessPool (src/os/process_pool.cc, include/pp_process_pool.h)

Master forks N workers, dispatches `pp::EventData` (`DataHead info` + `char data[PP_IPC_BUFFER_SIZE]`) to them, reaps and respawns.

Lifecycle: `create()` → `set_protocol()` / `set_type()` / `set_start_id()` / `set_max_request()` → `start()` (forks workers) → `dispatch()` / `dispatch_sync()` → `shutdown()` → `wait()` → `destroy()`.

Workers run `main_loop` (default `run_with_task_protocol`): blocking read of tasks from IPC, calls `onTask`.

## IPC transports (strategy, chosen at create())

| Mode | Mechanism | Notes |
|---|---|---|
| `PP_IPC_UNIXSOCK` | `UnixSocket` (AF_UNIX dgram socketpair) per worker | default/common |
| `PP_IPC_MSGQUEUE` | SysV msgqueue (`MsgQueue`) | gated on `HAVE_MSGQUEUE` |
| `PP_IPC_NONE` | shared-memory message box only | no per-worker pipe |
| `PP_IPC_SOCKET` | removed — `create()` returns `PP_ERR` | |

`Worker::send_pipe_message()` dispatches between transports based on `pool->use_msgqueue` and event-loop availability.

## Worker→master: message box

`create_message_box()` makes a `Channel` (shared-memory ring buffer, `PP_CHAN_LOCK | PP_CHAN_SHM`). Workers call `pool->push_message()` → writes `EventData` + sends `SIGIO` to master. Master's `wait()` loop pops via `read_message` and fires `onWorkerMessage`.

## Reactor (platform abstraction)

`Reactor` delegates to `ReactorImpl` (abstract: `add`/`set`/`del`/`wait`) → `ReactorEpoll` or `ReactorKqueue`, auto-selected. Used only in `ProcessPool::run_async()` (event-driven worker mode); blocking task protocol uses `read_sync()` directly.

## Global state

- `PPG` (`pp::Global`) — process-wide: memory pool, logger, CPU info, signal state (defined in `src/core/base.cc`)
- `PPTG` (`pp::ThreadGlobal`, thread_local) — reactor, timer, error code
- `PPWG` (`pp::WorkerGlobal`) — worker id, pid, running/shutdown flags, max_request (defined in `src/os/process_pool.cc`)

Declared `extern` in `pp.h`. `pp_init()` must be called before any other API.

## Callbacks

`ProcessPool` has 12 plain C function-pointer hooks (not `std::function`): `onStart`, `onShutdown`, `onBeforeReload`, `onAfterReload`, `onTask`, `onWorkerStart`, `onMessage`, `onWorkerExit`, `onWorkerStop`, `onWorkerError`, `onWorkerMessage`, `onWorkerNotFound`.

## Reload (graceful worker replacement)

`pool.reload()` creates a `ReloadTask`, SIGTERMs workers one at a time (`kill_one()`), escalates to SIGKILL after `max_wait_time` via `add_timeout_killer()`. The `wait()` loop respawns each killed worker; when all replaced, `onAfterReload` fires.

## Feature gating

Sources use `#ifdef HAVE_MSGQUEUE / HAVE_EPOLL / HAVE_KQUEUE / HAVE_SIGNALFD ...`, defined by the CMake-generated `build/include/config.h` (included via `pp.h` under `HAVE_CONFIG_H`).

## Entry points for users

- C++: `pp::ProcessPool` — include `"pp.h"` + `"pp_process_pool.h"`
- C API: `pp_init`, `pp_fork`, `pp_waitpid`, `pp_timer_*`, `pp_event_*`, `pp_signal_*`, `pp_malloc`/`pp_shm_malloc`, `pp_get/set_worker_id/pid/type`
