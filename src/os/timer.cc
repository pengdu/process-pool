
#include <sys/time.h>

#include <csignal>

#include "pp_signal.h"
#include "pp_timer.h"

namespace pp {
static int SystemTimer_set(Timer* timer, long next_msec);

void Timer::init_with_system_timer() {
  set = SystemTimer_set;
  close = [](Timer* timer) { SystemTimer_set(timer, -1); };
  pp_signal_set(SIGALRM, [](int sig) {});
}

static int SystemTimer_set(Timer* timer, long next_msec) {
  itimerval timer_set {};
  if (next_msec > 0) {
    timer_set.it_interval = {next_msec / 1000, static_cast<int>((next_msec % 1000) * 1000)};
    timer_set.it_value = timer_set.it_interval;
  }
  return setitimer(ITIMER_REAL, &timer_set, nullptr) < 0 ? PP_ERR : PP_OK;
}

void realtime_get(timespec* time) {
  auto now = std::chrono::system_clock::now();
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
  time->tv_sec = ns.count() / PP_NUM_BILLION;
  time->tv_nsec = ns.count() % PP_NUM_BILLION;
}

void realtime_add(timespec* time, const int64_t add_msec) {
  time->tv_sec += add_msec / 1000;
  time->tv_nsec += add_msec % 1000 * PP_NUM_MILLION;
  if (time->tv_nsec >= PP_NUM_BILLION) {
    int secs = time->tv_nsec / PP_NUM_BILLION;
    time->tv_sec += secs;
    time->tv_nsec -= secs * PP_NUM_BILLION;
  }
}
} // namespace pp
