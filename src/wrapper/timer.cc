
#include "pp_timer.h"

using pp::sec2msec;
using pp::Timer;
using pp::TimerCallback;
using pp::TimerNode;

bool pp_timer_is_available() {
  return PPTG.timer != nullptr;
}

TimerNode* pp_timer_add(double timeout, bool persistent, const TimerCallback& callback, void* private_data) {
  if (pp_unlikely(timeout < PP_TIMER_MIN_SEC)) {
    timeout = PP_TIMER_MIN_SEC;
  }
  return pp_timer_add(sec2msec(timeout), persistent, callback, private_data);
}

Timer* pp_timer_create(bool manually_trigger) {
  PPTG.timer = new Timer(manually_trigger);
  return PPTG.timer;
}

PP_API int64_t pp_timer_get_next_msec() {
  if (pp_unlikely(!pp_timer_is_available())) {
    return -1;
  }
  return PPTG.timer->get_next_msec();
}

TimerNode* pp_timer_add(long ms, bool persistent, const TimerCallback& callback, void* private_data) {
  if (pp_unlikely(!pp_timer_is_available())) {
    pp_timer_create(false);
  }
  return PPTG.timer->add(ms, persistent, private_data, callback);
}

bool pp_timer_del(TimerNode* tnode) {
  if (pp_unlikely(!pp_timer_is_available())) {
    pp_warning("timer is not available");
    return false;
  }
  return PPTG.timer->remove(tnode);
}

long pp_timer_after(long ms, const TimerCallback& callback, void* private_data) {
  if (ms <= 0) {
    pp_warning("Timer must be greater than 0");
    return PP_ERR;
  }
  const auto tnode = pp_timer_add(ms, false, callback, private_data);
  if (pp_unlikely(!tnode)) {
    return PP_ERR;
  }
  return tnode->id;
}

long pp_timer_tick(long ms, const TimerCallback& callback, void* private_data) {
  if (pp_unlikely(ms <= 0)) {
    pp_warning("Timer must be greater than 0");
    return PP_ERR;
  }
  const auto tnode = pp_timer_add(ms, true, callback, private_data);
  if (pp_unlikely(!tnode)) {
    return PP_ERR;
  }
  return tnode->id;
}

void pp_timer_free() {
  if (!pp_timer_is_available()) {
    pp_print_backtrace();
    pp_warning("timer is not available");
    return;
  }
  delete PPTG.timer;
  PPTG.timer = nullptr;
}

void pp_timer_select() {
  if (pp_likely(pp_timer_is_available())) {
    PPTG.timer->select();
  }
}
