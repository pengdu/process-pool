

#include <fcntl.h>

#include <chrono> // NOLINT [build/c++11]
#include <cstring>
#include <string>

#include "pp.h"

namespace pp {

std::string Logger::get_pretty_name(const std::string& pretty_function, bool strip) {
  const size_t brackets = pretty_function.find_first_of('(');
  if (brackets == std::string::npos) {
    return pretty_function;
  }

  const size_t begin = pretty_function.substr(0, brackets).rfind(' ') + 1;
  const size_t end = brackets - begin;
  if (!strip) {
    return pretty_function.substr(begin, end);
  }

  auto method_name = pretty_function.substr(begin, end);
  size_t count = 0;
  size_t index = method_name.length();
  while (true) {
    index = method_name.rfind("::", index);
    if (index == std::basic_string<char>::npos) {
      if (count == 1) {
        return method_name.substr(method_name.rfind("::") + 2);
      }
      break;
    }
    count++;
    if (count == 2) {
      return method_name.substr(index + 2);
    }
    index -= 2;
  }

  return method_name;
}

bool Logger::open(const char* log_file) {
  if (opened_) {
    close();
  }

  log_file_ = log_file;

  if (log_rotation_) {
    log_real_file_ = gen_real_file(log_file_);
  } else {
    log_real_file_ = log_file_;
  }

  auto log_fd = ::open(log_real_file_.c_str(), O_APPEND | O_RDWR | O_CREAT, 0666);
  if (log_fd < 0) {
    pp_error_log(PP_LOG_WARNING, PP_ERROR_SYSTEM_CALL_FAIL, "open('%s') failed. Error: %s[%d]", log_real_file_.c_str(), strerror(errno), errno);
    opened_ = false;
    log_file_ = "";
    log_real_file_ = "";

    return false;
  } else {
    opened_ = true;
    log_fp_ = fdopen(log_fd, "a");

    return true;
  }
}

void Logger::close() {
  if (opened_) {
    fclose(log_fp_);
    log_fp_ = stdout;
    log_file_ = "";
    opened_ = false;
  }
}

int Logger::get_level() const {
  return log_level_;
}

void Logger::set_level(int level) {
  if (level < PP_LOG_DEBUG) {
    level = PP_LOG_DEBUG;
  }
  if (level > PP_LOG_NONE) {
    level = PP_LOG_NONE;
  }
  log_level_ = level;
}

void Logger::reopen_without_lock() {
  if (!opened_) {
    return;
  }

  std::string new_log_file(log_file_);
  close();
  open(new_log_file.c_str());
}

void Logger::reopen() {
  std::unique_lock<std::mutex> lock(lock_);
  reopen_without_lock();
}

std::string Logger::gen_real_file(const std::string& file) const {
  char date_str[16];
  auto now_sec = ::time(nullptr);
  const char* fmt = nullptr;

  switch (log_rotation_) {
  case PP_LOG_ROTATION_MONTHLY:
    fmt = "%Y%m";
    break;
  case PP_LOG_ROTATION_HOURLY:
    fmt = "%Y%m%d%H";
    break;
  case PP_LOG_ROTATION_EVERY_MINUTE:
    fmt = "%Y%m%d%H%M";
    break;
  case PP_LOG_ROTATION_DAILY:
  default:
    fmt = "%Y%m%d";
    break;
  }

  size_t l_data_str = std::strftime(date_str, sizeof(date_str), fmt, std::localtime(&now_sec));
  std::string real_file = file + "." + std::string(date_str, l_data_str);

  return real_file;
}

void Logger::put(int level, const char* content, size_t length) {
  const char* level_str = nullptr;
  char date_str[PP_LOG_DATE_STRLEN];
  char log_str[PP_LOG_BUFFER_SIZE];

  if (level < log_level_) {
    return;
  }

  switch (level) {
  case PP_LOG_DEBUG:
    level_str = "DEBUG";
    break;
  case PP_LOG_TRACE:
    level_str = "TRACE";
    break;
  case PP_LOG_NOTICE:
    level_str = "NOTICE";
    break;
  case PP_LOG_WARNING:
    level_str = "WARNING";
    break;
  case PP_LOG_ERROR:
    level_str = "ERROR";
    break;
  case PP_LOG_INFO:
  default:
    level_str = "INFO";
    break;
  }

  auto now = std::chrono::system_clock::now();
  auto now_sec = std::chrono::system_clock::to_time_t(now);
  size_t l_data_str = std::strftime(date_str, sizeof(date_str), date_format_.c_str(), std::localtime(&now_sec));

  if (log_rotation_) {
    std::string tmp = gen_real_file(log_file_);
    /**
     * If the current thread fails to acquire the lock, it will forgo executing the log rotation.
     */
    if (tmp != log_real_file_ && lock_.try_lock()) {
      reopen_without_lock();
      lock_.unlock();
    }
  }

  if (date_with_microseconds_) {
    auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    l_data_str += pp_snprintf(date_str + l_data_str, PP_LOG_DATE_STRLEN - l_data_str, "<.%lld>", (long long)now_us - now_sec * 1000000);
  }

  int worker_id = pp_get_worker_id();
  pid_t worker_pid = pp_get_worker_pid();
  if (worker_pid == 0) {
    worker_pid = getpid();
  }
  char worker_symbol = pp_get_worker_symbol();

  size_t n = pp_snprintf(
    log_str, PP_LOG_BUFFER_SIZE, "[%.*s %c%d.%d]\t%s\t%.*s\n", static_cast<int>(l_data_str), date_str, worker_symbol, worker_pid, worker_id, level_str, static_cast<int>(length), content);

  lock_.lock();
  if (opened_) {
    flockfile(log_fp_);
  }
  fwrite(log_str, n, 1, log_fp_);
  fflush(log_fp_);
  if (opened_) {
    funlockfile(log_fp_);
  }
  lock_.unlock();

  if (display_backtrace_) {
    pp_print_backtrace();
  }
}
} // namespace pp
