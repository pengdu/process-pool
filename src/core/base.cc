
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/stat.h>

#include <cassert>
#include <cstdarg>

#include "pp.h"

#ifdef __MACH__
#include <sys/syslimits.h>
#endif

#include <chrono>
#include <random>

#include "pp_api.h"
#include "pp_memory.h"
#include "pp_util.h"

using pp::Logger;

pp::Global PPG = {};
thread_local pp::ThreadGlobal PPTG = {};
thread_local char pp_errmsg[PP_ERROR_MSG_SIZE];
std::mutex pp_thread_lock;

static void pp_fatal_error_impl(int code, const char* format, ...);

pp::Logger* pp_logger() {
  return PPG.logger;
}

void* pp_malloc(size_t size) {
  return PPG.std_allocator.malloc(size);
}

void pp_free(void* ptr) {
  return PPG.std_allocator.free(ptr);
}

void* pp_realloc(void* ptr, size_t size) {
  return PPG.std_allocator.realloc(ptr, size);
}

void pp_init() {
  if (PPG.init) {
    return;
  }

  PPG = {};
  pp_memset_zero(pp_errmsg, PP_ERROR_MSG_SIZE);

  PPG.init = 1;
  PPG.std_allocator = {malloc, calloc, realloc, free};
  PPG.fatal_error = pp_fatal_error_impl;
  PPG.pagesize = getpagesize();
  // random seed
  srandom(time(nullptr));

  if (!PPG.logger) {
    PPG.logger = new Logger();
  }

  pp_thread_init(true);

#ifdef PP_DEBUG
  pp_logger()->set_level(0);
  PPG.trace_flags = 0x7fffffff;
#else
  pp_logger()->set_level(PP_LOG_INFO);
#endif

  // init global shared memory
  PPG.memory_pool = new pp::GlobalMemory(PP_GLOBAL_MEMORY_PAGESIZE, true);
#ifndef HAVE_FUTEX
  pp_atomic_wait_init();
#endif
  // init signalfd
#ifdef HAVE_SIGNALFD
  pp_signalfd_init();
  PPG.enable_signalfd = 1;
#endif
}

PP_EXTERN_C_BEGIN

PP_EXTERN_C_END

PP_API int pp_get_log_level() {
  if (pp_logger()) {
    return pp_logger()->get_level();
  } else {
    return PP_LOG_NONE;
  }
}

pid_t pp_fork(int flags) {
  if (flags & PP_FORK_PRECHECK) {
    return 0;
  }

  pid_t pid = 0;
  while (true) {
    pid = fork();
    if (pid < 0) {
      pp_sys_warning("fork() failed");
      sleep(1);
    } else {
      break;
    }
  }

  if (pid == 0) {
    if (flags & PP_FORK_DAEMON) {
      return pid;
    }
    /**
     * [!!!] All timers and event loops must be cleaned up after fork
     */
    if (pp_timer_is_available()) {
      pp_timer_free();
    }
    if (!(flags & PP_FORK_EXEC)) {
      /**
       * Do not release the allocated memory pages.
       * The global memory will be returned to the OS upon process termination.
       */
      PPG.memory_pool = new pp::GlobalMemory(PP_GLOBAL_MEMORY_PAGESIZE, true);
      // reopen log file
      pp_logger()->reopen();
      // reset eventLoop
      if (pp_event_is_available()) {
        pp_event_free();
        pp_trace_log(PP_TRACE_REACTOR, "reactor has been destroyed");
      }
    } else {
      pp_logger()->close();
    }
    pp_signal_clear();
  }

  return pid;
}

bool pp_is_main_thread() {
  return PPTG.main_thread;
}

ppReturnCode pp_classify_errno(int err) {
  switch (err) {
  case 0:
  case EAGAIN:
#if EAGAIN != EWOULDBLOCK
  case EWOULDBLOCK:
#endif
  case ENOBUFS:
    return PP_WAIT;
  case EINTR:
    return PP_CONTINUE;
  case EBADF:
  case ENOENT:
    return PP_INVALID;
  case ECONNRESET:
  case ECONNABORTED:
  case EPIPE:
  case ENOTCONN:
  case ETIMEDOUT:
  case ECONNREFUSED:
  case ENETDOWN:
  case ENETUNREACH:
  case EHOSTDOWN:
  case EHOSTUNREACH:
    return PP_CLOSE;
  default:
    return PP_ERROR;
  }
}

void pp_thread_init(bool main_thread) {
  if (!main_thread) {
    pp_signal_block_all();
  }
  PPTG.main_thread = main_thread;
}

/**
 * Recursive directory creation
 */

#ifndef RAND_MAX
#define RAND_MAX 2147483647
#endif

int pp_system_random(int min, int max) {
  static int dev_random_fd = -1;
  unsigned random_value = 0;
  unsigned range = max - min + 1;

  assert(max > min);

  if (dev_random_fd == -1) {
    dev_random_fd = open("/dev/urandom", O_RDONLY);
    if (dev_random_fd < 0) {
      return pp_rand(min, max);
    }
  }

  auto next_random_byte = (char*)&random_value;
  constexpr int bytes_to_read = sizeof(random_value);

  // rejection sampling to avoid modulo bias
  unsigned limit = UINT_MAX - UINT_MAX % range;
  do {
    if (read(dev_random_fd, next_random_byte, bytes_to_read) < bytes_to_read) {
      pp_sys_warning("read() from /dev/urandom failed");
      return PP_ERR;
    }
  } while (random_value >= limit);

  return min + (random_value % range);
}

/**
 * Maximum common divisor
 */
/**
 * The least common multiple
 */
size_t pp_snprintf(char* buf, size_t size, const char* format, ...) {
  va_list args = nullptr;
  va_start(args, format);
  int retval = vsnprintf(buf, size, format, args);
  va_end(args);

  if (size == 0) {
    return retval;
  } else if (pp_unlikely(retval < 0)) {
    retval = 0;
    buf[0] = '\0';
  } else if (pp_unlikely(retval >= (int)size)) {
    retval = size - 1;
    buf[retval] = '\0';
  }
  return retval;
}

size_t pp_vsnprintf(char* buf, size_t size, const char* format, va_list args) {
  int retval = vsnprintf(buf, size, format, args);
  if (pp_unlikely(retval < 0)) {
    retval = 0;
    buf[0] = '\0';
  } else if (pp_unlikely(retval >= (int)size)) {
    retval = size - 1;
    buf[retval] = '\0';
  }
  return retval;
}

bool pp_wait_for(const std::function<bool(void)>& fn, int timeout_ms) {
  if (timeout_ms < 0) {
    timeout_ms = INT_MAX;
  }
  int sleep_msec = 1;
  while (timeout_ms >= 0) {
    if (fn()) {
      return true;
    }
    usleep(sleep_msec * 1000);
    sleep_msec *= 2;
    // Align the time so that the timeout is consistent with the user settings
    if (timeout_ms > 0 && timeout_ms - sleep_msec < 0) {
      sleep_msec = timeout_ms;
      timeout_ms = 0;
    } else {
      timeout_ms -= sleep_msec;
    }
  }
  return false;
}

int pp_itoa(char* buf, long value) {
  long i = 0;
  long j = 0;

  long sign_mask = value >> (sizeof(long) * 8 - 1);
  unsigned long nn = (value + sign_mask) ^ sign_mask;
  do {
    buf[i++] = nn % 10 + '0';
  } while (nn /= 10);

  buf[i] = '-';
  i += sign_mask & 1;
  buf[i] = '\0';

  int s_len = i;

  for (i = 0, j = s_len - 1; i < j; ++i, --j) {
    char swap = buf[i];
    buf[i] = buf[j];
    buf[j] = swap;
  }
  buf[s_len] = 0;
  return s_len;
}

int pp_rand(int min, int max) {
  static thread_local std::random_device rd;
  static thread_local std::mt19937 gen(rd());
  static thread_local std::uniform_int_distribution<int> dis;
  std::uniform_int_distribution<int>::param_type params(min, max);
  return dis(gen, params);
}

#ifdef HAVE_BOOST_STACKTRACE
#include <boost/stacktrace.hpp>
#include <iostream>
void pp_print_backtrace() {
  std::cout << boost::stacktrace::stacktrace();
}
#elif defined(HAVE_EXECINFO)
#include <execinfo.h>
void pp_print_backtrace() {
  int size = 16;
  void* array[16];
  int stack_num = backtrace(array, size);
  char** stacktrace = backtrace_symbols(array, stack_num);
  int i;

  for (i = 0; i < stack_num; ++i) {
    printf("%s\n", stacktrace[i]);
  }
  free(stacktrace);
}
#else
void pp_print_backtrace() {
}
#endif

static void pp_fatal_error_impl(int code, const char* format, ...) {
  size_t retval = 0;
  va_list args = nullptr;

  retval += pp_snprintf(pp_errmsg, PP_ERROR_MSG_SIZE, "(ERROR %d): ", code);
  va_start(args, format);
  retval += pp_vsnprintf(pp_errmsg + retval, PP_ERROR_MSG_SIZE - retval, format, args);
  va_end(args);
  pp_logger()->put(PP_LOG_ERROR, pp_errmsg, retval);
  pp_exit(1);
}

void pp_exit(int status) {
#ifdef PP_THREAD
  /**
   * If multiple threads call exit simultaneously, it can result in a crash.
   * Implementing locking mechanisms can prevent concurrent calls to exit.
   */
  std::unique_lock<std::mutex> guard(pp_thread_lock);
#endif
  exit(status);
}

namespace pp {
//-------------------------------------------------------------------------------

double microtime() {
  using namespace std::chrono;
  return duration_cast<duration<double>>(system_clock::now().time_since_epoch()).count();
}
}; // namespace pp
