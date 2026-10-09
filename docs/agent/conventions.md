# Conventions

## Naming (project-wide rename from swoole)

All identifiers renamed from upstream `swoole`/`SW_`/`sw_` to `pp`/`PP_`/`pp_`:

- Namespace: `pp` (sub-namespaces `pp::network`, `pp::async`)
- C functions: `pp_init()`, `pp_malloc()`, `pp_signal_set()`
- Macros/enums: `PP_OK`, `PP_ERR`, `PP_IPC_UNIXSOCK`, `PP_PROTOCOL_TASK`
- Headers: `pp_*.h`, umbrella `pp.h`
- Globals: `PPG` / `PPTG` / `PPWG`
- Typedefs: `ppDataHead`, `ppSignalHandler`, `ppSocketFd`
- Source files: lowercase_underscore `.cc` (`process_pool.cc`, `msg_queue.cc`)

When porting from upstream swoole, apply this renaming consistently.

## clang-tidy identifier rules (.clang-tidy)

- Classes/structs/functions/namespaces: `CamelCase` (note: methods in practice are `lower_case` like `dispatch`, `create` — follow surrounding code)
- Variables: `lower_case` (`worker_num`, `ipc_mode`)
- Private members: trailing `_` suffix
- Global constants: `k` prefix + CamelCase (`kWorkerNum`)
- Thresholds: 200 statements/function, 7 params, cognitive complexity 87
- Enabled check groups: cppcoreguidelines, google, llvm, performance, modernize, misc, readability

## Formatting (.clang-format)

- Base: Google; column limit 200; 2-space indent, no tabs
- Braces: Mozilla variant (`AfterEnum: true`, `AfterNamespace: false`, `AfterFunction: false`, `BeforeElse: false`)
- Pointer alignment: left (`int* p`)
- Namespace indentation: Inner; short functions never single-line
- Include sorting disabled in practice (`SortIncludes` commented out) — preserve existing include order in each file

## Include order (observed in sources)

1. Project headers (`"pp_process_pool.h"` ...)
2. C standard library (`<cstdio>` ...)
3. POSIX/system headers (`<unistd.h>`, `<sys/wait.h>` ...)
4. C++ standard library (`<string>`, `<memory>` ...)

Files vary; match the file you are editing.

## Other patterns

- Callbacks are plain C function pointers, not `std::function` — keep this when adding hooks
- Platform-specific code goes behind `#ifdef HAVE_*` guards fed by `cmake/config.h.in`; add new probes in CMakeLists.txt and the template together
- Error codes: `ppErrorCode` enum in `pp_error.h` (range 500–9101); use `pp_strerror` / `pp::Exception`
- Scope helpers available in `pp_util.h`: `ON_SCOPE_EXIT`, `ScopeGuard`, `DeferTask`
- `.clang-tidy-ignore` and parts of `build/` contain stale entries from a previous project location — do not treat them as authoritative
