#!/usr/bin/env bash
#
# Build the standalone process-pool library
#
# Usage:
#   ./build.sh            # configure + build everything
#   ./build.sh --test     # configure + build + run ctest
#   ./build.sh --clean    # remove the build directory
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${HERE}/build"

CMAKE_ARGS=(-S "${HERE}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE=Debug)

clean() {
    echo ">> removing ${BUILD_DIR}"
    rm -rf "${BUILD_DIR}"
}

configure_and_build() {
    echo ">> configuring"
    cmake "${CMAKE_ARGS[@]}"
    echo ">> building"
    cmake --build "${BUILD_DIR}" -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
}

case "${1:-}" in
    --clean)
        clean
        ;;
    --test)
        configure_and_build
        echo ">> running tests"
        ctest --test-dir "${BUILD_DIR}" --output-on-failure
        ;;
    *)
        configure_and_build
        echo
        echo ">> done. artifacts:"
        echo "   ${BUILD_DIR}/libpp_process_pool.a"
        echo "   ${BUILD_DIR}/demo/pp_demo"
        echo "   ${BUILD_DIR}/tests/pp_smoke"
        ;;
esac
