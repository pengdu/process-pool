
#include "pp_api.h"
#include "pp_reactor.h"
#include "pp_signal.h"
#include "pp_socket.h"
#include "pp_timer.h"

namespace pp {
using network::Socket;

#ifdef PP_USE_MALLOC_TRIM
#ifdef __APPLE__
#include <sys/malloc.h>
#else
#include <malloc.h>
#endif
#endif

#ifdef HAVE_EPOLL
ReactorImpl* make_reactor_epoll(Reactor* reactor, int max_events);
#endif

#ifdef HAVE_KQUEUE
ReactorImpl* make_reactor_kqueue(Reactor* reactor, int max_events);
#endif

void ReactorImpl::after_removal_failure(const Socket* socket) const {
  if (!socket->silent_remove) {
    pp_error_log(PP_LOG_WARNING, PP_ERROR_EVENT_REMOVE_FAILED, "failed to delete events[fd=%d#%d, type=%d, events=%d, errno=%d]", socket->fd, reactor_->id, socket->fd_type, socket->events, errno);
    pp_print_backtrace();
  }
}

Reactor::Reactor(int max_event, Type type) {
  if (type == TYPE_AUTO) {
#if defined(HAVE_EPOLL)
    type_ = TYPE_EPOLL;
#elif defined(HAVE_KQUEUE)
    type_ = TYPE_KQUEUE;
#else
#error "no supported event loop backend (epoll or kqueue required)"
#endif
  } else {
    type_ = type;
  }

  switch (type_) {
#ifdef HAVE_EPOLL
  case TYPE_EPOLL:
    impl_ = make_reactor_epoll(this, max_event);
    break;
#endif
#ifdef HAVE_KQUEUE
  case TYPE_KQUEUE:
    impl_ = make_reactor_kqueue(this, max_event);
    break;
#endif
  default:
    pp_fatal_error(PP_ERROR_OPERATION_NOT_SUPPORT, "unsupported reactor type [%d]", type_);
    impl_ = nullptr;
    break;
  }

  ready_ = impl_->ready();
  if (!ready_) {
    return;
  }

  timeout_msec = -1;
  idle_task = {};
  future_task = {};

  write = default_write;
  writev = default_writev;
  close = default_close;

  default_write_handler = default_writable_callback;

  set_end_callback(PRIORITY_DEFER_TASK, [](Reactor* reactor) {
    CallbackManager* cm = reactor->defer_tasks;
    if (cm) {
      reactor->defer_tasks = nullptr;
      cm->execute();
      delete cm;
    }
  });

  set_exit_condition(EXIT_CONDITION_DEFER_TASK, [](Reactor* reactor, size_t& event_num) -> bool { return reactor->defer_tasks == nullptr; });

  set_end_callback(PRIORITY_IDLE_TASK, [](Reactor* reactor) {
    if (reactor->idle_task.callback) {
      reactor->idle_task.callback(reactor->idle_task.data);
    }
  });

  if (pp_is_main_thread()) {
    set_end_callback(PRIORITY_SIGNAL_CALLBACK, [](Reactor*) { pp_signal_dispatch(); });
  }

  set_end_callback(PRIORITY_TRY_EXIT, [](Reactor* reactor) {
    if (reactor->wait_exit && reactor->if_exit()) {
      reactor->running = false;
    }
  });

#ifdef PP_USE_MALLOC_TRIM
  if (pp_is_main_thread()) {
    set_end_callback(PRIORITY_MALLOC_TRIM, [](Reactor* reactor) {
      time_t now = ::time(nullptr);
      if (reactor->last_malloc_trim_time < now - PP_MALLOC_TRIM_INTERVAL) {
        malloc_trim(PP_MALLOC_TRIM_PAD);
        reactor->last_malloc_trim_time = now;
      }
    });
  }
#endif

  set_exit_condition(EXIT_CONDITION_DEFAULT, [](Reactor*, size_t& event_num) -> bool { return event_num == 0; });
}

void Reactor::set_handler(const int fd_type, const int event, const ReactorHandler handler) {
  if (isset_read_event(event)) {
    read_handler[fd_type] = handler;
  } else if (isset_write_event(event)) {
    write_handler[fd_type] = handler;
  } else if (isset_error_event(event)) {
    error_handler[fd_type] = handler;
  } else {
    // Reactor users are expected to pass exactly one event class here.
    // Invalid values are treated as a programmer error and kept under assert.
    assert(0);
  }
}

bool Reactor::isset_handler(const int fd_type, const int event) const {
  if (isset_read_event(event)) {
    return read_handler[fd_type] != nullptr;
  } else if (isset_write_event(event)) {
    return write_handler[fd_type] != nullptr;
  } else if (isset_error_event(event)) {
    return error_handler[fd_type] != nullptr;
  } else {
    return false;
  }
}

bool Reactor::if_exit() {
  size_t event_num = get_event_num();
  for (auto& kv : exit_conditions_) {
    if (!kv.second(this, event_num)) {
      return false;
    }
  }
  return true;
}

int16_t translate_events_to_poll(int events) {
  int16_t poll_events = 0;

  if (events & PP_EVENT_READ) {
    poll_events |= POLLIN;
  }
  if (events & PP_EVENT_WRITE) {
    poll_events |= POLLOUT;
  }

  return poll_events;
}

int Reactor::default_close(Reactor* reactor, Socket* socket) {
  pp_trace_log(PP_TRACE_CLOSE, "fd=%d", socket->fd);
  socket->free();
  return PP_OK;
}

ssize_t Reactor::write_func(const Reactor* reactor, Socket* socket, const size_t len, const std::function<ssize_t()>& send_fn, const std::function<void(Buffer* buffer)>& append_fn) {
  Buffer* buffer = socket->out_buffer;

  if (socket->buffer_size == 0) {
    socket->set_memory_buffer_size(Socket::default_buffer_size);
  }

  if (socket->nonblock == 0) {
    socket->set_fd_option(1, -1);
  }

  if ((uint32_t)len > socket->buffer_size) {
    pp_error_log(PP_LOG_WARNING, PP_ERROR_PACKAGE_LENGTH_TOO_LARGE, "data packet is too large, cannot exceed the socket buffer size");
    return PP_ERR;
  }

  if (Buffer::empty(buffer)) {
    // Nothing queued yet: try a direct send first. Retry on EINTR; a partial
    // send or EAGAIN falls through to queueing the remaining data below.
    while (true) {
      ssize_t retval = send_fn();

      if (retval > 0) {
        if ((ssize_t)len == retval) {
          return retval; // fully flushed to the kernel
        }
        break; // partial send: queue the remainder
      }
      if (errno == EINTR) {
        continue;
      }
      if (socket->catch_write_error(errno) != PP_WAIT) {
        pp_set_last_error(errno);
        return PP_ERR;
      }
      break; // kernel buffer full: queue and wait for writability
    }

    if (!socket->out_buffer) {
      buffer = new Buffer(socket->chunk_size);
      socket->out_buffer = buffer;
    }
    if (!socket->isset_writable_event()) {
      reactor->add_write_event(socket);
    }
  } else if (buffer->length() + len > socket->buffer_size) {
    pp_error_log(PP_LOG_WARNING, PP_ERROR_OUTPUT_BUFFER_OVERFLOW, "socket#%d output buffer overflow: (%u/%u)", socket->get_fd(), buffer->length(), socket->buffer_size);
    return PP_ERR;
  }

  /**
   * Part of the data has been successfully written to the kernel's socket buffer,
   * and at this point, writing to the memory queue is permitted under any circumstances.
   * Ensure that the async write operation either succeeds completely or fails entirely.
   */
  append_fn(buffer);
  return len;
}

ssize_t Reactor::default_write(Reactor* reactor, Socket* socket, const void* buf, size_t n) {
  ssize_t send_bytes = 0;
  auto send_fn = [&send_bytes, socket, buf, n]() -> ssize_t {
    send_bytes = socket->send(buf, n, 0);
    return send_bytes;
  };
  auto append_fn = [&send_bytes, buf, n](Buffer* buffer) {
    ssize_t offset = send_bytes > 0 ? send_bytes : 0;
    buffer->append(static_cast<const char*>(buf) + offset, n - offset);
  };
  return write_func(reactor, socket, n, send_fn, append_fn);
}

ssize_t Reactor::default_writev(Reactor* reactor, Socket* socket, const iovec* iov, size_t iovcnt) {

  ssize_t send_bytes = 0;
  size_t n = 0;
  PP_LOOP_N(iovcnt) {
    n += iov[i].iov_len;
  }
  auto send_fn = [&send_bytes, socket, iov, iovcnt]() -> ssize_t {
    send_bytes = socket->writev(iov, iovcnt);
    return send_bytes;
  };
  auto append_fn = [&send_bytes, iov, iovcnt](Buffer* buffer) {
    ssize_t offset = send_bytes > 0 ? send_bytes : 0;
    buffer->append(iov, iovcnt, offset);
  };
  return write_func(reactor, socket, n, send_fn, append_fn);
}

int Reactor::default_writable_callback(Reactor* reactor, Event* ev) {
  int ret = 0;

  Socket* socket = ev->socket;
  Buffer* buffer = socket->out_buffer;

  while (!Buffer::empty(buffer)) {
    BufferChunk* chunk = buffer->front();
    if (chunk->type == BufferChunk::TYPE_CLOSE) {
      return reactor->close(reactor, ev->socket);
    } else {
      ret = socket->handle_send();
    }

    if (ret < 0) {
      if (socket->close_wait) {
        return reactor->trigger_close_event(ev);
      } else if (socket->send_wait) {
        return PP_OK;
      }
    }
  }

  if (socket->send_timer) {
    pp_timer_del(socket->send_timer);
    socket->send_timer = nullptr;
  }

  // remove EPOLLOUT event
  if (Buffer::empty(buffer)) {
    reactor->remove_write_event(ev->socket);
  }

  return PP_OK;
}

void Reactor::add_destroy_callback(const Callback& cb, void* data) {
  destroy_callbacks.append(cb, data);
}

void Reactor::set_end_callback(const EndCallback id, const std::function<void(Reactor*)>& fn) {
  end_callbacks_[id] = fn;
}

void Reactor::erase_end_callback(const EndCallback id) {
  end_callbacks_.erase(id);
}

/**
 * Returns false, the reactor cannot be exited, the next condition is skipped
 * Returns true, the reactor can exit and will continue to execute the next conditional function
 */
void Reactor::set_exit_condition(const ExitCondition id, const std::function<bool(Reactor*, size_t&)>& fn) {
  exit_conditions_[id] = fn;
}

void Reactor::defer(const Callback& cb, void* data) {
  if (defer_tasks == nullptr) {
    defer_tasks = new CallbackManager;
  }
  defer_tasks->append(cb, data);
}

void Reactor::execute_end_callbacks(bool timed_out) {
  for (auto& kv : end_callbacks_) {
    kv.second(this);
  }
}

void Reactor::execute_begin_callback() const {
  if (future_task.callback) {
    future_task.callback(future_task.data);
  }
}

Reactor::~Reactor() {
  destroyed = true;
  destroy_callbacks.execute();
  delete impl_;
}
} // namespace pp
