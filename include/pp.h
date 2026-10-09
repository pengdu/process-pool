
#pragma once

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef __cplusplus
#define PP_EXTERN_C_BEGIN extern "C" {
#define PP_EXTERN_C_END }
#else
#define PP_EXTERN_C_BEGIN
#define PP_EXTERN_C_END
#endif

#include "pp_posix.h"

#ifndef _PTHREAD_PSHARED
#define _PTHREAD_PSHARED
#endif

/*--- C standard library ---*/
#include <inttypes.h>

#include <cassert>
#include <cctype>
#include <climits>
#include <cstdarg>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>

using ulong_t = unsigned long;

#if defined(__GNUC__)
#if __GNUC__ >= 3
#define pp_inline inline __attribute__((always_inline))
#else
#define pp_inline inline
#endif
#else
#define pp_inline inline
#endif

#if defined(__GNUC__) && __GNUC__ >= 4
#define PP_API __attribute__((visibility("default")))
#else
#define PP_API
#endif

#if !defined(__GNUC__) || __GNUC__ < 3
#define __builtin_expect(x, expected_value) (x)
#endif

#if defined(__GNUC__)
#define pp_likely(x) __builtin_expect(!!(x), 1)
#define pp_unlikely(x) __builtin_expect(!!(x), 0)
#else
#define pp_likely(x) (x)
#define pp_unlikely(x) (x)
#endif

/*----------------------------------------------------------------------------*/

#include "pp_atomic.h"
#include "pp_config.h"
#include "pp_error.h"
#include "pp_log.h"

#define PP_MAX(A, B) ((A) > (B) ? (A) : (B))
#define PP_MIN(A, B) ((A) < (B) ? (A) : (B))
#define PP_LOOP_N(n) for (decltype(n) i = 0; i < n; i++)
#define PP_LOOP for (;;)

#define PP_NUM_BILLION (1000 * 1000 * 1000)
#define PP_NUM_MILLION (1000 * 1000)

#ifdef PP_DEBUG
#define PP_ASSERT(e) assert(e)
#define PP_ASSERT_1BYTE(v)                                                                                                                                                                             \
  do {                                                                                                                                                                                                 \
    size_t i = 0, n = 0;                                                                                                                                                                               \
    for (; i < sizeof(v); i++) {                                                                                                                                                                       \
      n += ((v >> i) & 1) ? 1 : 0;                                                                                                                                                                     \
    }                                                                                                                                                                                                  \
    assert(n == 1);                                                                                                                                                                                    \
  } while (0)
#else
#define PP_ASSERT(e)
#endif

#ifdef PP_THREAD
#define PP_THREAD_LOCAL thread_local
#else
#define PP_THREAD_LOCAL
#endif

extern std::mutex pp_thread_lock;

/**
 * Naming rules
 * -----------------------------------
 * - pp_* functions and PP_* macros: public C API surface
 * - pp namespace classes: internal C++ implementation details
 */

/*-----------------------------------Memory------------------------------------*/
void* pp_malloc(size_t size);
void pp_free(void* ptr);
void* pp_realloc(void* ptr, size_t size);

// Evaluates to the number of elements in 'array'

#define PP_DEFAULT_ALIGNMENT sizeof(unsigned long)
#define PP_MEM_ALIGNED_SIZE(size) PP_MEM_ALIGNED_SIZE_EX(size, PP_DEFAULT_ALIGNMENT)
#define PP_MEM_ALIGNED_SIZE_EX(size, alignment) (((size) + ((alignment) - 1LL)) & ~((alignment) - 1LL))

/*-------------------------------Declare Struct--------------------------------*/
namespace pp {
class MemoryPool;
class Reactor;
class Timer;
struct TimerNode;
struct Event;
class SocketPair;
class Pipe;
class UnixSocket;
namespace network {
  struct Socket;
  struct Address;
} // namespace network
namespace async {
  class ThreadPool;
}
struct EventData;
struct DataHead;
using ReactorHandler = int (*)(Reactor* reactor, Event* event);
using Callback = std::function<void(void*)>;
using TimerCallback = std::function<void(Timer*, TimerNode*)>;
using SignalHandler = void (*)(int);

int16_t translate_events_to_poll(int events);

static inline long sec2msec(const long sec) {
  return sec * 1000;
}

static inline long sec2msec(const int sec) {
  return sec * 1000;
}

static inline long sec2msec(const double sec) {
  return static_cast<long>(sec * 1000);
}

static inline double msec2sec(const int msec) {
  return static_cast<double>(msec) / 1000.0;
}
} // namespace pp

using ppDataHead = pp::DataHead;

/** always return less than size, zero termination  */
#ifdef __GNUC__
size_t pp_snprintf(char* buf, size_t size, const char* format, ...) __attribute__((format(printf, 3, 4)));
#else
size_t pp_snprintf(char* buf, size_t size, const char* format, ...);
#endif
size_t pp_vsnprintf(char* buf, size_t size, const char* format, va_list args);
bool pp_wait_for(const std::function<bool()>& fn, int timeout_ms);

static inline void pp_memset_zero(void* s, size_t n) {
  memset(s, 0, n);
}

static inline size_t pp_strlcpy(char* dest, const char* src, size_t size) {
  const size_t len = strlen(src);
  if (size != 0) {
    const size_t n = (std::min)(len, size - 1);
    memcpy(dest, src, n);
    dest[n] = '\0';
  }
  return len;
}

/* string equal */

/*--------------------------------Constants------------------------------------*/
enum ppResultCode
{
  PP_OK = 0,
  PP_ERR = -1,
};

enum ppReturnCode
{
  PP_SUCCESS = 0,
  PP_CONTINUE = 1,
  PP_WAIT = 2,
  PP_CLOSE = 3,
  PP_ERROR = 4,
  PP_READY = 5,
  PP_INVALID = 6,
};

enum ppFdType
{
  PP_FD_PIPE,
  PP_FD_SIGNAL,
};

enum ppSocketType
{
  PP_SOCK_TCP = 1,
  PP_SOCK_UDP = 2,
  PP_SOCK_TCP6 = 3,
  PP_SOCK_UDP6 = 4,
  PP_SOCK_UNIX_STREAM = 5, // unix sock stream
  PP_SOCK_UNIX_DGRAM = 6,  // unix sock dgram
  PP_SOCK_RAW = 7,
  PP_SOCK_RAW6 = 8,
};

enum ppTimeoutType
{
  PP_TIMEOUT_CONNECT = 1 << 1,
  PP_TIMEOUT_READ = 1 << 2,
  PP_TIMEOUT_WRITE = 1 << 3,
  PP_TIMEOUT_RDWR = PP_TIMEOUT_READ | PP_TIMEOUT_WRITE,
  PP_TIMEOUT_ALL = PP_TIMEOUT_CONNECT | PP_TIMEOUT_RDWR,
};

enum ppEventType
{
  PP_EVENT_DEFAULT = 1u << 8,
  PP_EVENT_READ = 1u << 9,
  PP_EVENT_WRITE = 1u << 10,
  PP_EVENT_ERROR = 1u << 11,
  PP_EVENT_ONCE = 1u << 12,
};

enum ppForkType
{
  PP_FORK_EXEC = 1 << 1,
  PP_FORK_DAEMON = 1 << 2,
  PP_FORK_PRECHECK = 1 << 3,
};

//------------------------------Base--------------------------------
#ifndef uchar
using uchar = unsigned char;
#endif

/**
 * This function appends a '\0' at the end of the string,
 * so the allocated memory buffer must be len + 1.
 */

//------------------------------Base--------------------------------
enum ppEventDataFlag
{
  PP_EVENT_DATA_CHUNK = 1u << 2,
  PP_EVENT_DATA_BEGIN = 1u << 3,
  PP_EVENT_DATA_END = 1u << 4,
};

extern thread_local char pp_errmsg[PP_ERROR_MSG_SIZE];

enum ppPipeType
{
  PP_PIPE_MASTER = 1,
  PP_PIPE_NONBLOCK = 2,
};

//----------------------Tool Function---------------------

int pp_rand(int min, int max);
int pp_system_random(int min, int max);

void pp_print_backtrace();

void pp_init();
void pp_exit(int status);
pid_t pp_fork(int flags);
pid_t pp_waitpid(pid_t pid, int* stat_loc, int options);
void pp_thread_init(bool main_thread = false);

namespace pp {
using SessionId = long;
using TaskId = long;
using ReactorId = uint8_t;
using WorkerId = uint32_t;
using EventType = ppEventType;
using SocketType = ppSocketType;
using TimeoutType = ppTimeoutType;
using FdType = ppFdType;
using ReturnCode = ppReturnCode;
using ResultCode = ppResultCode;

struct Event {
  ppSocketFd fd;
  int16_t reactor_id;
  FdType type;
  network::Socket* socket;
};

struct DataHead {
  SessionId fd;
  uint32_t len;
  uint8_t type;
  int16_t reactor_id;
};

struct EventData {
  DataHead info;
  char data[PP_IPC_BUFFER_SIZE];

  uint32_t size() const {
    return sizeof(info) + len();
  }

  uint32_t len() const {
    return info.len;
  }
};

struct ThreadGlobal {
  uint16_t id;
  uint8_t type;
  bool main_thread;
  int32_t error;
  Reactor* reactor;
  Timer* timer;
  bool signal_blocking_all;
};

struct Allocator {
  void* (*malloc)(size_t size);
  void* (*calloc)(size_t nmemb, size_t size);
  void* (*realloc)(void* ptr, size_t size);
  void (*free)(void* ptr);
};

struct Global {
  uchar init : 1;
  uchar enable_signalfd : 1;
  /**
   * Under macOS or FreeBSD, kqueue does not support listening for writable events on pipes. When a large amount of
   * data is written to a pipe in process A, and the buffer becomes full, listening for writable events will not work.
   * In process B, even after consuming the data from the pipe, the writable event in process A cannot be triggered.
   * As a result, the functionality of Task and Process Server cannot be supported, making all scenarios relying on
   * pipes for inter-process communication unable to function properly.
   */
  uchar enable_kqueue : 1;

  bool signal_dispatch;

  long trace_flags;

  void (*fatal_error)(int code, const char* str, ...);

  //-----------------------[System]--------------------------
  uint32_t pagesize;
  //-----------------------[Memory]--------------------------
  MemoryPool* memory_pool;
  Allocator std_allocator;
  //------------------[Single Instance]----------------------
  Logger* logger;
  // bug report message
};

double microtime();
void realtime_get(timespec* time);
void realtime_add(timespec* time, int64_t add_msec);
} // namespace pp

/* signal handling (C API) — implementations live in src/os/signal.cc */
#ifdef HAVE_SIGNALFD
void pp_signalfd_init();
#endif
PP_API pp::SignalHandler pp_signal_set(int signo, pp::SignalHandler func, bool safety = false);
PP_API pp::SignalHandler pp_signal_set(int signo, pp::SignalHandler func, int restart, int mask);
PP_API void pp_signal_clear();
PP_API void pp_signal_block_all();
PP_API char* pp_signal_to_str(int sig);
PP_API void pp_signal_callback(int signo);
PP_API void pp_signal_dispatch();

/* worker identity (C API) */
PP_API pp::WorkerId pp_get_worker_id();
PP_API pid_t pp_get_worker_pid();
PP_API int pp_get_worker_type();
PP_API void pp_set_worker_pid(pid_t pid);
PP_API void pp_set_worker_id(pp::WorkerId worker_id);
PP_API void pp_set_worker_type(int type);
PP_API char pp_get_worker_symbol();

extern pp::Global PPG;                     // Local Global Variable
extern thread_local pp::ThreadGlobal PPTG; // Thread Global Variable

static inline void pp_set_last_error(int error) {
  PPTG.error = error;
}

static inline int pp_get_last_error() {
  return PPTG.error;
}

static inline uint32_t pp_pagesize() {
  return PPG.pagesize;
}

PP_API const char* pp_strerror(int code);

/**
 * Classify an errno into a standard return code (core-level policy):
 * - 0, EAGAIN/EWOULDBLOCK -> PP_WAIT (no data yet, retry later)
 * - EINTR                 -> PP_CONTINUE (interrupted, retry immediately)
 * - EBADF, ENOENT         -> PP_INVALID
 * - connection errors     -> PP_CLOSE
 * - ENOBUFS               -> PP_WAIT (kernel buffer full)
 * - anything else         -> PP_ERROR
 */
PP_API ppReturnCode pp_classify_errno(int err);
PP_API bool pp_is_main_thread();
PP_API int pp_get_log_level();

enum ppEventInitFlag
{
  PP_EVENTLOOP_WAIT_EXIT = 1,
};

/**
 * manually_trigger:
 * Once enabled, the timer will no longer be triggered by event polling or the operating system's timer;
 * instead, it will be managed directly at the user space.
 */
PP_API pp::Timer* pp_timer_create(bool manually_trigger = false);
PP_API long pp_timer_after(long ms, const pp::TimerCallback& callback, void* private_data = nullptr);
PP_API long pp_timer_tick(long ms, const pp::TimerCallback& callback, void* private_data = nullptr);
PP_API pp::TimerNode* pp_timer_add(double ms, bool persistent, const pp::TimerCallback& callback, void* private_data = nullptr);
PP_API pp::TimerNode* pp_timer_add(long ms, bool persistent, const pp::TimerCallback& callback, void* private_data = nullptr);
PP_API bool pp_timer_del(pp::TimerNode* tnode);
PP_API void pp_timer_free();
PP_API void pp_timer_select();
PP_API int64_t pp_timer_get_next_msec();
PP_API bool pp_timer_is_available();

PP_API int pp_event_init(int flags);
PP_API int pp_event_add(pp::network::Socket* socket, int events);
PP_API int pp_event_del(pp::network::Socket* socket);
PP_API void pp_event_defer(const pp::Callback& cb, void* private_data);
PP_API ssize_t pp_event_write(pp::network::Socket* socket, const void* data, size_t len);
PP_API int pp_event_wait();
PP_API int pp_event_free();
PP_API void pp_event_set_handler(int fd_type, int event, pp::ReactorHandler handler);
PP_API bool pp_event_isset_handler(int fd_type, int event);
PP_API bool pp_event_is_available();

static pp_inline pp::MemoryPool* pp_mem_pool() {
  return PPG.memory_pool;
}

static pp_inline const pp::Allocator* pp_std_allocator() {
  return &PPG.std_allocator;
}

static pp_inline pp::Reactor* pp_reactor() {
  return PPTG.reactor;
}

static pp_inline pp::Timer* pp_timer() {
  return PPTG.timer;
}
