

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "pp_posix.h"

#define PP_LOG_BUFFER_SIZE (PP_ERROR_MSG_SIZE + 256)
#define PP_LOG_DATE_STRLEN 128
#define PP_LOG_DEFAULT_DATE_FORMAT "%F %T"

enum ppLogLevel
{
  PP_LOG_DEBUG = 0,
  PP_LOG_TRACE,
  PP_LOG_INFO,
  PP_LOG_NOTICE,
  PP_LOG_WARNING,
  PP_LOG_ERROR,
  PP_LOG_NONE,
};

enum ppLogRotationType
{
  PP_LOG_ROTATION_SINGLE = 0,
  PP_LOG_ROTATION_MONTHLY,
  PP_LOG_ROTATION_DAILY,
  PP_LOG_ROTATION_HOURLY,
  PP_LOG_ROTATION_EVERY_MINUTE,
};

namespace pp {
class Logger {
private:
  bool opened_ = false;
  bool display_backtrace_ = false;
  FILE* log_fp_ = stdout;
  int log_level_ = PP_LOG_INFO;
  bool date_with_microseconds_ = false;
  std::string date_format_ = PP_LOG_DEFAULT_DATE_FORMAT;
  std::string log_file_;
  std::string log_real_file_;
  std::mutex lock_;
  int log_rotation_ = PP_LOG_ROTATION_SINGLE;

  void reopen_without_lock();

public:
  /**
   * Choose how log files rotate: PP_LOG_ROTATION_SINGLE (default),
   * MONTHLY, DAILY, HOURLY or EVERY_MINUTE. Takes effect on the next
   * open()/reopen().
   */
  void set_log_rotation(int mode) {
    log_rotation_ = mode;
  }

  bool open(const char* logfile);
  void reopen();
  /**
   * Only the `put` and `reopen` functions are thread-safe,
   * other functions must be used in a single-threaded environment.
   */
  void put(int level, const char* content, size_t length);
  void close();
  void set_level(int lv);
  int get_level() const;
  std::string gen_real_file(const std::string& file) const;
  static std::string get_pretty_name(const std::string& prettyFunction, bool strip = true);
};
} // namespace pp

pp::Logger* pp_logger();
#define __PP_FUNC__ (pp::Logger::get_pretty_name(__PRETTY_FUNCTION__).c_str())

#define pp_warning(str, ...)                                                                                                                                                                           \
  do {                                                                                                                                                                                                 \
    if (PP_LOG_WARNING >= pp_get_log_level()) {                                                                                                                                                        \
      size_t pp_errmsg_len = pp_snprintf(pp_errmsg, PP_ERROR_MSG_SIZE, "%s(): " str, __PP_FUNC__, ##__VA_ARGS__);                                                                                      \
      pp_logger()->put(PP_LOG_WARNING, pp_errmsg, pp_errmsg_len);                                                                                                                                      \
    }                                                                                                                                                                                                  \
  } while (0)

#define pp_sys_warning(str, ...)                                                                                                                                                                       \
  do {                                                                                                                                                                                                 \
    pp_set_last_error(errno);                                                                                                                                                                          \
    if (PP_LOG_WARNING >= pp_get_log_level()) {                                                                                                                                                        \
      size_t pp_errmsg_len = pp_snprintf(pp_errmsg, PP_ERROR_MSG_SIZE, "%s(): " str ", Error: %s[%d]", __PP_FUNC__, ##__VA_ARGS__, pp_strerror(errno), errno);                                         \
      pp_logger()->put(PP_LOG_WARNING, pp_errmsg, pp_errmsg_len);                                                                                                                                      \
    }                                                                                                                                                                                                  \
  } while (0)

#define pp_error(str, ...)                                                                                                                                                                             \
  do {                                                                                                                                                                                                 \
    size_t pp_errmsg_len = pp_snprintf(pp_errmsg, PP_ERROR_MSG_SIZE, "%s(): " str, __PP_FUNC__, ##__VA_ARGS__);                                                                                        \
    pp_logger()->put(PP_LOG_ERROR, pp_errmsg, pp_errmsg_len);                                                                                                                                          \
    pp_exit(1);                                                                                                                                                                                        \
  } while (0)

#define pp_fatal_error(code, str, ...) PPG.fatal_error(code, str, ##__VA_ARGS__)

#define pp_error_log(level, error, str, ...)                                                                                                                                                           \
  do {                                                                                                                                                                                                 \
    pp_set_last_error(error);                                                                                                                                                                          \
    if (level >= pp_get_log_level()) {                                                                                                                                                                 \
      size_t pp_errmsg_len = pp_snprintf(pp_errmsg, PP_ERROR_MSG_SIZE, "%s() (ERRNO %d): " str, __PP_FUNC__, error, ##__VA_ARGS__);                                                                    \
      pp_logger()->put(level, pp_errmsg, pp_errmsg_len);                                                                                                                                               \
    }                                                                                                                                                                                                  \
  } while (0)

enum ppTraceWhat : long long
{
  PP_TRACE_CLIENT = 1u << 2,
  PP_TRACE_BUFFER = 1u << 3,
  PP_TRACE_EVENT = 1u << 5,
  PP_TRACE_WORKER = 1u << 6,
  PP_TRACE_REACTOR = 1u << 8,
  PP_TRACE_CLOSE = 1u << 14,
  PP_TRACE_NORMAL = 1u << 21,
  PP_TRACE_TIMER = 1u << 23,
  PP_TRACE_SOCKET = 1u << 24,
};

#ifdef PP_LOG_TRACE_OPEN
#define pp_trace_log(what, str, ...)                                                                                                                                                                   \
  if (PP_LOG_TRACE >= pp_get_log_level() && (what & PPG.trace_flags)) {                                                                                                                                \
    size_t pp_errmsg_len = pp_snprintf(pp_errmsg, PP_ERROR_MSG_SIZE, "%s(:%d): " str, __PP_FUNC__, __LINE__, ##__VA_ARGS__);                                                                           \
    pp_logger()->put(PP_LOG_TRACE, pp_errmsg, pp_errmsg_len);                                                                                                                                          \
  }
#else
#define pp_trace_log(what, str, ...)
#endif

#define pp_trace(str, ...) pp_trace_log(PP_TRACE_NORMAL, str, ##__VA_ARGS__)
