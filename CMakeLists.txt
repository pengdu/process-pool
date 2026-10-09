cmake_minimum_required (VERSION 3.13)
project (procpool LANGUAGES C CXX)

set (CMAKE_CXX_STANDARD 14)
set (CMAKE_CXX_STANDARD_REQUIRED ON)
set (CMAKE_POSITION_INDEPENDENT_CODE ON)

if (NOT CMAKE_BUILD_TYPE)
  set (CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif ()

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")

include (CheckCXXSourceCompiles)
include (CheckFunctionExists)
include (CheckIncludeFile)
include (CheckLibraryExists)
include (CheckSymbolExists)
include (CheckTypeSize)

# ---------------------------------------------------------------------------
# Host platform probe (mirrors config.m4 upstream autoconf host-align probe)
# ---------------------------------------------------------------------------
check_type_size ("long" SIZEOF_LONG)
if (SIZEOF_LONG EQUAL 8)
  set (PP_HOST_ALIGN 8)
else ()
  set (PP_HOST_ALIGN 4)
endif ()

# ---------------------------------------------------------------------------
# Feature probes; results feed cmake/config.h.in
# ---------------------------------------------------------------------------
find_package (Threads REQUIRED)

check_library_exists (pthread pthread_mutex_timedlock "" HAVE_MUTEX_TIMEDLOCK)

check_function_exists (msgget HAVE_MSGQUEUE)
check_function_exists (signalfd HAVE_SIGNALFD)
check_function_exists (epoll_create HAVE_EPOLL)
check_function_exists (kqueue HAVE_KQUEUE)
check_function_exists (accept4 HAVE_ACCEPT4)
check_function_exists (arc4random HAVE_ARC4RANDOM)
check_function_exists (getrandom HAVE_GETRANDOM)
check_function_exists (mkostemp HAVE_MKOSTEMP)
check_function_exists (fdatasync HAVE_FDATASYNC)
check_function_exists (malloc_trim HAVE_MALLOC_TRIM)
check_function_exists (execinfo HAVE_EXECINFO)
check_function_exists (ccrandomgeneratebytes HAVE_CCRANDOMGENERATEBYTES)

# ---------------------------------------------------------------------------
# Generated config.h
# ---------------------------------------------------------------------------
configure_file (
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/config.h
)

# ---------------------------------------------------------------------------
# Target: pp_process_pool
# ---------------------------------------------------------------------------
# src/ contains only the sources the task protocol's dependency closure actually needs. Excluded
# subsystems (server, coroutine runtime, OpenSSL glue, Windows support, DNS/name resolution,
# application protocols, the network client/stream stack, message bus) were physically removed from
# the tree — including every #ifdef branch that referenced them.
file (GLOB_RECURSE PP_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc
      ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c
)

list (LENGTH PP_SOURCES PP_SOURCE_COUNT)
message (STATUS "procpool: compiling ${PP_SOURCE_COUNT} sources")

add_library (pp_process_pool STATIC ${PP_SOURCES})

target_include_directories (
  pp_process_pool PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_definitions (pp_process_pool PUBLIC HAVE_CONFIG_H)

target_link_libraries (pp_process_pool PUBLIC Threads::Threads)

if (UNIX AND NOT APPLE)
  target_link_libraries (pp_process_pool PUBLIC rt)
endif ()

# ---------------------------------------------------------------------------
# Demo + smoke tests
# ---------------------------------------------------------------------------
add_subdirectory (demo)
enable_testing ()
add_subdirectory (tests)
