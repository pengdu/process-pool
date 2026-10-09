

#pragma once

#include <unordered_map>

#include "pp.h"
#include "pp_heap.h"
namespace pp {
class Reactor;
} // namespace pp
#include "pp_util.h"

#define PP_TIMER_MIN_SEC 0.001

namespace pp {
using TimerDestructor = std::function<void(TimerNode*)>;

struct TimerNode {
  enum Type
  {
    TYPE_KERNEL,
  };
  long id;
  Type type;
  int64_t exec_msec;
  int64_t interval;
  uint64_t exec_count;
  uint64_t round;
  bool removed;
  HeapNode* heap_node;
  TimerCallback callback;
  void* data;
  TimerDestructor destructor;
};

class Timer {
  /*--------------signal timer--------------*/
  Reactor* reactor_ = nullptr;
  Heap heap_;
  std::unordered_map<long, TimerNode*> map_;
  uint64_t round_;
  long next_id_;
  long current_id_;
  /*---------------event timer--------------*/
  int64_t base_time_;
  /**
   * The time when the next timer will trigger, in milliseconds.
   * This event will serve as the timeout for the event loop's epoll/poll/kqueue,
   * or be set as the trigger time for the system clock.
   */
  int64_t next_msec_;
  /*----------------------------------------*/
  std::function<int(Timer* timer, long exec_msec)> set;
  std::function<void(Timer* timer)> close;

  void init(bool manually_trigger);
  void init_with_reactor(Reactor* reactor);
  void init_with_system_timer();
  void release_node(TimerNode* tnode);

public:
  explicit Timer(bool manually_trigger);
  ~Timer();

  int64_t get_relative_msec() const {
    return get_absolute_msec() - base_time_;
  }

  int64_t get_next_msec() const {
    return next_msec_;
  }

  static int64_t get_absolute_msec() {
    return time<std::chrono::milliseconds>(true);
  }

  TimerNode* add(long msec, bool persistent, void* data, const TimerCallback& callback);
  bool remove(TimerNode* tnode);
  void reinit(bool manually_trigger = false);
  void select();

  size_t count() const {
    return map_.size();
  }
};

} // namespace pp
