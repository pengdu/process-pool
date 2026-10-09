#!/usr/bin/env bash
# Measure test coverage of the library using LLVM source-based coverage.
#
# Usage: tools/coverage.sh [test-binary...]   (defaults to all suites + demos)
#
# Notes:
# - Workers forked by the pool exit via signals during most tests, so their
#   profiles are not captured; numbers below reflect master/test-process code.
# - src/core/misc.cc still contains the deliberately-kept (currently unused)
#   futex/AtomicWaitRegistry machinery, which caps its coverage.
set -euo pipefail

LLVM_DIR="${LLVM_DIR:-$(brew --prefix llvm@22)}"
COV_DIR="build-cov"

cmake -S . -B "$COV_DIR" \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_COMPILER="$LLVM_DIR/bin/clang" \
  -DCMAKE_CXX_COMPILER="$LLVM_DIR/bin/clang++" \
  -DCMAKE_CXX_FLAGS="-fprofile-instr-generate -fcoverage-mapping" >/dev/null
cmake --build "$COV_DIR" -j"$(getconf _NPROCESSORS_ONLN)" >/dev/null

cd "$COV_DIR"
rm -f ./*.profraw all.profdata
export LLVM_PROFILE_FILE="%p.profraw"

for bin in tests/pp_smoke tests/pp_unit tests/pp_func tests/pp_integration \
           tests/pp_stress demo/pp_demo demo/pp_task_dispatch; do
  [[ -x "$bin" ]] && "$bin" >/dev/null 2>&1 || true
done

# Merge only well-formed profiles: a few forked/killed children can leave
# torn files behind, and one bad file makes llvm-profdata refuse the batch.
GOOD=""
for f in *.profraw; do
  if "$LLVM_DIR/bin/llvm-profdata" merge -sparse "$f" -o /dev/null 2>/dev/null; then
    GOOD="$GOOD $f"
  else
    echo "skipping torn profile: $f" >&2
  fi
done
"$LLVM_DIR/bin/llvm-profdata" merge -sparse $GOOD -o all.profdata

"$LLVM_DIR/bin/llvm-cov" report tests/pp_unit \
  -instr-profile=all.profdata \
  -object tests/pp_func -object tests/pp_integration \
  -object tests/pp_smoke -object demo/pp_demo -object demo/pp_task_dispatch \
  2>/dev/null | awk 'NR==1 || $1 ~ /^src\// || /TOTAL/'
