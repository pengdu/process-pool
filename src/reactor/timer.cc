
#include "pp_reactor.h"
#include "pp_timer.h"

namespace pp {
Timer::Timer(bool manually_trigger) : heap_(1024, Heap::MIN_HEAP) {
  current_id_ = -1;
  next_msec_ = -1;
  next_id_ = 1;
  round_ = 0;
  base_time_ = get_absolute_msec();
  init(manually_trigger);
}

void Timer::init(bool manually_trigger) {
  if (manually_trigger) {
    set = [](Timer*, long) -> int { return PP_OK; };
    close = [](Timer*) {};
    return;
  }
  if (PPTG.reactor) {
    init_with_reactor(PPTG.reactor);
  } else {
    init_with_system_timer();
  }
}

void Timer::release_node(TimerNode* tnode) {
  if (tnode->destructor) {
    tnode->destructor(tnode);
  }
  delete tnode;
}

void Timer::init_with_reactor(Reactor* reactor) {
  reactor_ = reactor;
  set = [](Timer* timer, long exec_msec) -> int {
    timer->reactor_->timeout_msec = exec_msec;
    return PP_OK;
  };
  close = [](Timer* timer) { timer->set(timer, -1); };

  reactor->set_end_callback(Reactor::PRIORITY_TIMER, [this](Reactor*) { select(); });

  reactor->set_exit_condition(Reactor::EXIT_CONDITION_TIMER, [this](Reactor* reactor, size_t& event_num) -> bool { return count() == 0; });

  reactor->add_destroy_callback([](void*) {
    if (pp_timer_is_available()) {
      pp_timer_free();
    }
  });
}

void Timer::reinit(bool manually_trigger) {
  close(this);
  init(manually_trigger);
  set(this, next_msec_);
}

Timer::~Timer() {
  if (close) {
    close(this);
  }
  for (const auto& iter : map_) {
    release_node(iter.second);
  }
}

TimerNode* Timer::add(long msec, bool persistent, void* data, const TimerCallback& callback) {
  if (pp_unlikely(msec <= 0)) {
    pp_error_log(PP_LOG_WARNING, PP_ERROR_INVALID_PARAMS, "msec value[%ld] is invalid", msec);
    return nullptr;
  }

  auto* tnode = new TimerNode();
  tnode->id = next_id_++;
  tnode->data = data;
  tnode->type = TimerNode::TYPE_KERNEL;
  tnode->exec_msec = get_relative_msec() + msec;
  tnode->interval = persistent ? msec : 0;
  tnode->removed = false;
  tnode->callback = callback;
  tnode->round = round_;
  tnode->destructor = nullptr;

  if (next_msec_ < 0 || next_msec_ > msec) {
    set(this, msec);
    next_msec_ = msec;
  }

  tnode->heap_node = heap_.push(tnode->exec_msec, tnode);
  if (pp_unlikely(tnode->heap_node == nullptr)) {
    release_node(tnode);
    return nullptr;
  }
  map_.emplace(tnode->id, tnode);
  pp_trace_log(PP_TRACE_TIMER, "id=%ld, exec_msec=%" PRId64 ", msec=%ld, round=%" PRIu64 ", exist=%lu", tnode->id, tnode->exec_msec, msec, tnode->round, count());
  return tnode;
}

bool Timer::remove(TimerNode* tnode) {
  if (pp_unlikely(!tnode || tnode->removed)) {
    return false;
  }
  if (pp_unlikely(current_id_ > 0 && tnode->id == current_id_)) {
    tnode->removed = true;
    pp_trace_log(PP_TRACE_TIMER, "set-remove: id=%ld, exec_msec=%" PRId64 ", round=%" PRIu64 ", exist=%lu", tnode->id, tnode->exec_msec, tnode->round, count());
    return true;
  }
  if (pp_unlikely(!map_.erase(tnode->id))) {
    return false;
  }
  if (tnode->heap_node) {
    heap_.remove(tnode->heap_node);
  }
  pp_trace_log(PP_TRACE_TIMER, "id=%ld, exec_msec=%" PRId64 ", round=%" PRIu64 ", exist=%lu", tnode->id, tnode->exec_msec, tnode->round, count());
  release_node(tnode);
  return true;
}

void Timer::select() {
  const int64_t now_msec = get_relative_msec();
  TimerNode* tnode = nullptr;
  HeapNode* tmp = nullptr;

  pp_trace_log(PP_TRACE_TIMER, "select begin: now_msec=%" PRId64 ", round=%" PRId64, now_msec, round_);

  while ((tmp = heap_.top())) {
    tnode = static_cast<TimerNode*>(tmp->data);
    if (tnode->exec_msec > now_msec || tnode->round == round_) {
      break;
    }

    current_id_ = tnode->id;
    if (!tnode->removed) {
      pp_trace_log(PP_TRACE_TIMER, "execute callback [id=%ld, exec_msec=%" PRId64 ", round=%" PRIu64 ", exist=%lu]", tnode->id, tnode->exec_msec, tnode->round, count() - 1);
      tnode->callback(this, tnode);
    }
    current_id_ = -1;

    // persistent timer
    if (tnode->interval > 0 && !tnode->removed) {
      while (tnode->exec_msec <= now_msec) {
        tnode->exec_msec += tnode->interval;
      }
      tnode->exec_count++;
      heap_.change_priority(tnode->exec_msec, tmp);
      continue;
    }

    heap_.pop();
    map_.erase(tnode->id);
    release_node(tnode);
    tnode = nullptr;
  }

  if (!tnode || !tmp) {
    next_msec_ = -1;
    set(this, -1);
  } else {
    next_msec_ = tnode->exec_msec - now_msec;
    if (next_msec_ <= 0) {
      next_msec_ = 1;
    }
    set(this, next_msec_);
  }
  round_++;
}
}; // namespace pp
