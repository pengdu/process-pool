# Development Commands

## Build

```bash
./build.sh                 # configure (if needed) + build
./build.sh --test          # configure + build + run ctest
./build.sh --clean         # remove build/ directory
```

Manual equivalents:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j$(getconf _NPROCESSORS_ONLN)
ctest --test-dir build --output-on-failure
```

Default build type is `Debug` (forced in CMakeLists.txt if unset).

## Targets

| Target | Kind | Source | Notes |
|---|---|---|---|
| `pp_process_pool` | static library | globbed from `src/*.cc` | core library |
| `pp_demo` | executable | `demo/demo.cc` | 4-worker square-computation demo |
| `pp_task_dispatch` | executable | `demo/task_dispatch.cc` | typed job dispatch demo |
| `pp_smoke` | ctest test | `tests/smoke.cc` | end-to-end smoke test |
| `pp_func` | ctest test | `tests/func.cc` | 9 functional cases, 60s timeout |

Build outputs: `build/libpp_process_pool.a`, `build/demo/pp_demo`, `build/demo/pp_task_dispatch`, `build/tests/pp_smoke`, `build/tests/pp_func`.

## Run a single test

```bash
ctest --test-dir build -R pp_func --output-on-failure
./build/tests/pp_smoke        # run directly
./build/tests/pp_func         # runs all 9 cases, fork-per-test
```

## Generated config header

CMake feature probes (`check_function_exists`, etc.) configure `cmake/config.h.in` into `build/include/config.h`, defining `HAVE_EPOLL`, `HAVE_KQUEUE`, `HAVE_MSGQUEUE`, `HAVE_SIGNALFD`, `HAVE_MUTEX_TIMEDLOCK`, `PP_HOST_ALIGN`, etc. All sources see it via `HAVE_CONFIG_H` (PUBLIC compile definition) and `pp.h`'s `#include "config.h"`.

## Formatting / static analysis

No CI scripts are defined. Config files present:

```bash
clang-format -i include/*.h src/*/*.cc demo/*.cc tests/*.cc   # .clang-format (Google base, 200 col, 2-space)
cmake-format -i CMakeLists.txt demo/CMakeLists.txt tests/CMakeLists.txt
clang-tidy <file> -p build --extra-arg="-isysroot$(xcrun --show-sdk-path)"   # macOS needs the sysroot flag
```

`compile_commands.json` is generated via `cmake -S . -B build -DCMAKE_EXPORT_COMPILE_COMMANDS=ON`.

Note: `.clang-tidy-ignore` contains stale paths from another project.
