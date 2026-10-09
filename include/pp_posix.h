
#pragma once

#include <errno.h>
#include <pthread.h>
#include <sys/uio.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <unistd.h>

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

static inline int pp_errno() {
  return errno;
}

static inline void pp_set_errno(int e) {
  errno = e;
}

// Cross-platform socket file descriptor type
// On POSIX: int (same as always)
// On Windows: SOCKET (UINT_PTR, 8 bytes on x64)
// This avoids truncation when SOCKET values exceed int range on 64-bit Windows.
using ppSocketFd = int;
#define PP_BAD_SOCKET ((ppSocketFd) - 1)
