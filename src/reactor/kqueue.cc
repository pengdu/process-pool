
#include "pp_reactor.h"
#include "pp_signal.h"
#include "pp_socket.h"

#ifdef HAVE_KQUEUE

#ifdef USE_KQUEUE_IDE_HELPER
#include "helper/kqueue.h"
#else
#include <sys/event.h>
#endif
#ifdef __NetBSD__
#include <sys/param.h>
#endif

namespace pp {
using network::Socket;

class ReactorKqueue : public ReactorImpl {
  int epfd_;
  int event_max_;
  struct kevent* events_ = nullptr;

  bool fetch_event(Event* event, void* udata) {
    event->socket = (Socket*)udata;
    event->fd = event->socket->fd;
    event->type = event->socket->fd_type;
    event->reactor_id = reactor_->id;

    return event->socket->removed == 0;
  }

  void del_once_socket(Socket* socket) {
    if ((socket->events & PP_EVENT_ONCE) && !socket->removed) {
      del(socket);
    }
  }

public:
  ReactorKqueue(Reactor* reactor, int max_events);
  ~ReactorKqueue();
  bool ready() override;
  int add(Socket* socket, int events) override;
  int set(Socket* socket, int events) override;
  int del(Socket* socket) override;
  int wait() override;
};

ReactorImpl* make_reactor_kqueue(Reactor* reactor, int max_events) {
  return new ReactorKqueue(reactor, max_events);
}

ReactorKqueue::ReactorKqueue(Reactor* reactor, int max_events) : ReactorImpl(reactor) {
  epfd_ = kqueue();
  if (epfd_ < 0) {
    pp_sys_warning("kqueue() failed");
    return;
  }

  reactor_->max_event_num = max_events;
  reactor_->native_handle = epfd_;
  event_max_ = max_events;
  events_ = new struct kevent[max_events];
}

bool ReactorKqueue::ready() {
  return epfd_ >= 0;
}

ReactorKqueue::~ReactorKqueue() {
  if (epfd_ >= 0) {
    close(epfd_);
  }
  delete[] events_;
}

int ReactorKqueue::add(Socket* socket, int events) {
  struct kevent changelist[2];
  int n = 0;

  int fd = socket->fd;

#if !defined(__NetBSD__) || (defined(__NetBSD__) && __NetBSD_Version__ >= 1000000000)
  auto sobj = socket;
#else
  auto sobj = reinterpret_cast<intptr_t>(socket);
#endif

  if (Reactor::isset_read_event(events)) {
    int fflags = 0;
#ifdef NOTE_EOF
    fflags = NOTE_EOF;
#endif
    EV_SET(&changelist[n++], fd, EVFILT_READ, EV_ADD, fflags, 0, sobj);
  }

  if (Reactor::isset_write_event(events)) {
    EV_SET(&changelist[n++], fd, EVFILT_WRITE, EV_ADD, 0, 0, sobj);
  }

  if (n > 0 && ::kevent(epfd_, changelist, n, nullptr, 0, nullptr) < 0) {
    pp_error_log(PP_LOG_WARNING, PP_ERROR_EVENT_ADD_FAILED, "add events[fd=%d, reactor_id=%d, type=%d, events=%d] failed", fd, reactor_->id, socket->fd_type, events);
    pp_print_backtrace();
    return PP_ERR;
  }

  reactor_->add_socket(socket, events);
  pp_trace_log(PP_TRACE_EVENT, "[THREAD #%d]epfd=%d, fd=%d, events=%d", PPTG.id, epfd_, fd, socket->events);

  return PP_OK;
}

int ReactorKqueue::set(Socket* socket, int events) {
  struct kevent changelist[2];
  int n = 0;

  int fd = socket->fd;
  int old_events = socket->events;

#if !defined(__NetBSD__) || (defined(__NetBSD__) && __NetBSD_Version__ >= 1000000000)
  auto sobj = socket;
#else
  auto sobj = reinterpret_cast<intptr_t>(socket);
#endif

  reactor_->set_socket(socket, events);

  if (Reactor::isset_read_event(events)) {
    int fflags = 0;
#ifdef NOTE_EOF
    fflags = NOTE_EOF;
#endif
    EV_SET(&changelist[n++], fd, EVFILT_READ, EV_ADD, fflags, 0, sobj);
  } else if (Reactor::isset_read_event(old_events)) {
    EV_SET(&changelist[n++], fd, EVFILT_READ, EV_DELETE, 0, 0, sobj);
  }

  if (Reactor::isset_write_event(events)) {
    EV_SET(&changelist[n++], fd, EVFILT_WRITE, EV_ADD, 0, 0, sobj);
  } else if (Reactor::isset_write_event(old_events)) {
    EV_SET(&changelist[n++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, sobj);
  }

  if (n > 0 && ::kevent(epfd_, changelist, n, nullptr, 0, nullptr) < 0) {
    pp_error_log(PP_LOG_WARNING, PP_ERROR_EVENT_UPDATE_FAILED, "kqueue->set(%d, events=%d) failed", fd, events);
    pp_print_backtrace();
    return PP_ERR;
  }

  pp_trace_log(PP_TRACE_EVENT, "[THREAD #%d]epfd=%d, fd=%d, events=%d", PPTG.id, epfd_, fd, socket->events);

  return PP_OK;
}

int ReactorKqueue::del(Socket* socket) {
  struct kevent changelist[2];
  int n = 0;
  int fd = socket->fd;

#if !defined(__NetBSD__) || (defined(__NetBSD__) && __NetBSD_Version__ >= 1000000000)
  auto sobj = socket;
#else
  auto sobj = reinterpret_cast<intptr_t>(socket);
#endif

  if (socket->removed) {
    pp_error_log(PP_LOG_WARNING, PP_ERROR_EVENT_REMOVE_FAILED, "failed to delete event[%d], has been removed", socket->fd);
    pp_print_backtrace();
    return PP_ERR;
  }

  if (socket->events & PP_EVENT_READ) {
    EV_SET(&changelist[n++], fd, EVFILT_READ, EV_DELETE, 0, 0, sobj);
  }

  if (socket->events & PP_EVENT_WRITE) {
    EV_SET(&changelist[n++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, sobj);
  }

  if (n > 0 && ::kevent(epfd_, changelist, n, nullptr, 0, nullptr) < 0) {
    after_removal_failure(socket);
    if (errno != EBADF && errno != ENOENT) {
      return PP_ERR;
    }
  }

  reactor_->del_socket(socket);
  pp_trace_log(PP_TRACE_EVENT, "[THREAD #%d]epfd=%d, fd=%d", PPTG.id, epfd_, fd);

  return PP_OK;
}

int ReactorKqueue::wait() {
  Event event;
  ReactorHandler handler = nullptr;

  int i = 0;
  int n = 0;
  struct timespec t = {};
  struct timespec* t_ptr = nullptr;

  reactor_->before_wait();

  while (reactor_->running) {
    reactor_->execute_begin_callback();

    const int timeout_msec = reactor_->get_timeout_msec();
    if (timeout_msec > 0) {
      t.tv_sec = timeout_msec / 1000;
      t.tv_nsec = (timeout_msec - t.tv_sec * 1000) * 1000 * 1000;
      t_ptr = &t;
    } else if (timeout_msec == 0 && reactor_->defer_tasks) {
      t.tv_sec = 0;
      t.tv_nsec = 0;
      t_ptr = &t;
    } else {
      t_ptr = nullptr;
    }

    n = ::kevent(epfd_, nullptr, 0, events_, event_max_, t_ptr);
    if (n < 0) {
      if (!reactor_->catch_error()) {
        pp_sys_warning("kevent(epfd=%d) failed ", epfd_);
        return PP_ERR;
      }
      // interrupted: retry, still run end-of-round callbacks
      reactor_->execute_end_callbacks(false);
    } else if (n > 0) {

      pp_trace_log(PP_TRACE_EVENT, "n %d events", n);

      for (i = 0; i < n; i++) {
        struct kevent* kevent = &events_[i];
        void* udata = (void*)kevent->udata;
        if (!udata) {
          continue;
        }
        switch (kevent->filter) {
        case EVFILT_READ:
        case EVFILT_WRITE: {
          if (fetch_event(&event, udata)) {
            handler = reactor_->get_handler(event.type, kevent->filter == EVFILT_READ ? PP_EVENT_READ : PP_EVENT_WRITE);
            if (pp_unlikely(handler(reactor_, &event) < 0)) {
              pp_sys_warning("kqueue event %s socket#%d handler failed", kevent->filter == EVFILT_READ ? "read" : "write", event.fd);
              pp_print_backtrace();
            }
            del_once_socket(event.socket);
          }
          break;
        }
        case EVFILT_SIGNAL: {
          auto* signal_data = (Signal*)udata;
          if (signal_data->activated) {
            if (signal_data->handler) {
              if (pp_likely(signal_data->handler != SIG_IGN)) {
                signal_data->handler(signal_data->signo);
              }
            } else {
              pp_error_log(PP_LOG_WARNING, PP_ERROR_UNREGISTERED_SIGNAL, PP_UNREGISTERED_SIGNAL_FMT, pp_signal_to_str(signal_data->signo));
            }
          }
          break;
        }
        default:
          pp_error_log(PP_LOG_WARNING, PP_ERROR_EVENT_UNKNOWN_DATA, "unknown event filter[%d]", kevent->filter);
          break;
        }
      }
    } else { // n == 0: timed out
      reactor_->execute_end_callbacks(true);
    }

    if (reactor_->once) {
      break;
    }
  }
  return 0;
}
} // namespace pp
#endif
