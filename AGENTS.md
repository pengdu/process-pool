# AGENTS.md

This file provides guidance to codeflicker when working with code in this repository.

## WHY: Purpose and Goals

`procpool` is a standalone C++ extraction of swoole-src's master/worker process pool (task protocol only, POSIX only). It provides a `pp::ProcessPool` that forks N workers, dispatches `EventData` packets over IPC (unix socketpair / SysV msgqueue / shared-memory message box), and handles reload, crash respawn, and max-request recycling.

## WHAT: Technical Stack

- Language: C++14 (plus a small C surface), `-Wall`
- Build: CMake >= 3.13 + `build.sh` wrapper; feature probes generate `build/include/config.h`
- Dependencies: pthreads only (plus `rt` on non-Apple UNIX); no external libs, no test framework
- Platforms: Linux (epoll/signalfd) and macOS/BSD (kqueue), gated via `HAVE_*` macros
- Key targets: `pp_process_pool` (static lib), `pp_demo`, `pp_task_dispatch`, `pp_smoke`, `pp_func`

## HOW: Core Development Workflow

```bash
# Configure + build
./build.sh

# Build + run tests (ctest)
./build.sh --test

# Clean
./build.sh --clean
```

## Progressive Disclosure

For detailed information, consult these documents as needed:

- `docs/agent/development_commands.md` - All build, test, and tooling commands
- `docs/agent/architecture.md` - Module structure, IPC modes, reactor, globals, callbacks
- `docs/agent/testing.md` - Test harness, test cases, how to run/add tests
- `docs/agent/conventions.md` - Naming, formatting, clang-tidy rules

**When working on a task, first determine which documentation is relevant, then read only those files.**
