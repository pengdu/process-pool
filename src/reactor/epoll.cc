

#include "pp_reactor.h"
#include "pp_socket.h"

#ifdef HAVE_EPOLL
#include <sys/epoll.h>
#ifndef EPOLLRDHUP
#error "require linux kernel version 2.6.32 or later"
#endif

#ifndef EPOLLONESHOT
#error "require linux kernel version 2.6.32 or later"
#endif

namespace pp {

using network::Socket;

class ReactorEpoll final : public ReactorImpl {
  int epfd_;
  epoll_event* events_ = nullptr;

public:
  ReactorEpoll(Reactor* reactor, int max_events);
  ~ReactorEpoll() override;
  bool ready() override;
  int add(Socket* socket, int events) override;
  int set(Socket* socket, int events) override;
  int del(Socket* socket) override;
  int wait() override;

  static int get_events(const int fd_type) {
    int events = 0;
    if (Reactor::isset_read_event(fd_type)) {
      events |= EPOLLIN;
    }
    if (Reactor::isset_write_event(fd_type)) {
      events |= EPOLLOUT;
    }
    if (fd_type & PP_EVENT_ONCE) {
      events |= EPOLLONESHOT;
    }
    if (Reactor::isset_error_event(fd_type)) {
      events |= (EPOLLRDHUP | EPOLLHUP | EPOLLERR);
    }
    return events;
  }
};

ReactorImpl* make_reactor_epoll(Reactor* reactor, int max_events) {
  return new ReactorEpoll(reactor, max_events);
}

ReactorEpoll::ReactorEpoll(Reactor* reactor, int max_events) : ReactorImpl(reactor) {
  epfd_ = epoll_create(512);
  if (!ReactorEpoll::ready()) {
    pp_sys_warning("epoll_create() failed");
    return;
  }

  events_ = new epoll_event[max_events];
  reactor_->max_event_num = max_events;
  reactor_->native_handle = epfd_;
}

bool ReactorEpoll::ready() {
  return epfd_ >= 0;
}

ReactorEpoll::~ReactorEpoll() {
  if (epfd_ >= 0) {
    close(epfd_);
  }
  delete[] events_;
}

int ReactorEpoll::add(Socket* socket, int events) {
  epoll_event e;

  e.events = get_events(events);
  e.data.ptr = socket;

  if (epoll_ctl(epfd_, EPOLL_CTL_ADD, socket->fd, &e) < 0) {
    pp_sys_warning("[Reactor#%d] epoll_ctl(epfd=%d, EPOLL_CTL_ADD, fd=%d, fd_type=%d, events=%d) failed", reactor_->id, epfd_, socket->fd, socket->fd_type, events);
    pp_print_backtrace_on_error();
    return PP_ERR;
  }

  reactor_->add_socket(socket, events);
  pp_trace_log(PP_TRACE_EVENT, "add events[fd=%d#%d, type=%d, events=%d]", socket->fd, reactor_->id, socket->fd_type, events);

  return PP_OK;
}

int ReactorEpoll::del(Socket* socket) {
  if (socket->removed) {
    pp_error_log(
      PP_LOG_WARNING, PP_ERROR_EVENT_REMOVE_FAILED, "[Reactor#%d] failed to delete events[fd=%d, fd_type=%d], this socket has already been removed", reactor_->id, socket->fd, socket->fd_type);
    pp_print_backtrace_on_error();
    return PP_ERR;
  }

  if (epoll_ctl(epfd_, EPOLL_CTL_DEL, socket->fd, nullptr) < 0) {
    after_removal_failure(socket);
    /**
     * Before removing it from the epoll event loop, the close operation has be executed,
     * must cleanup related resources with this socket.
     */
    if (errno != EBADF && errno != ENOENT) {
      pp_sys_warning("[Reactor#%d] epoll_ctl(epfd=%d, EPOLL_CTL_DEL, fd=%d, fd_type=%d) failed", reactor_->id, epfd_, socket->fd, socket->fd_type);
      pp_print_backtrace_on_error();
      return PP_ERR;
    }
  }

  pp_trace_log(PP_TRACE_REACTOR, "remove event[reactor_id=%d|fd=%d|type=%d]", reactor_->id, socket->fd, socket->fd_type);
  reactor_->del_socket(socket);

  return PP_OK;
}

int ReactorEpoll::set(Socket* socket, int events) {
  epoll_event e;

  e.events = get_events(events);
  e.data.ptr = socket;

  int ret = epoll_ctl(epfd_, EPOLL_CTL_MOD, socket->fd, &e);
  if (ret < 0) {
    pp_sys_warning("[Reactor#%d] epoll_ctl(epfd=%d, EPOLL_CTL_MOD, fd=%d, fd_type=%d, events=%d) failed", reactor_->id, epfd_, socket->fd, socket->fd_type, events);
    pp_print_backtrace_on_error();
    return PP_ERR;
  }

  pp_trace_log(PP_TRACE_EVENT, "set event[reactor_id=%d, fd=%d, events=%d]", reactor_->id, socket->fd, events);
  reactor_->set_socket(socket, events);

  return PP_OK;
}

int ReactorEpoll::wait() {
  Event event;
  ReactorHandler handler;
  int n, ret;

  reactor_->before_wait();

  while (reactor_->running) {
    reactor_->execute_begin_callback();

    n = epoll_wait(epfd_, events_, reactor_->max_event_num, reactor_->get_timeout_msec());
    if (n < 0) {
      if (!reactor_->catch_error()) {
        pp_sys_warning("[Reactor#%d] epoll_wait(epfd=%d, max_events=%d, timeout=%d) failed", reactor_->id, epfd_, reactor_->max_event_num, reactor_->get_timeout_msec());
        return PP_ERR;
      }
      // interrupted: retry, still run end-of-round callbacks
      reactor_->execute_end_callbacks(false);
    } else if (n > 0) {
      for (int i = 0; i < n; i++) {
        event.reactor_id = reactor_->id;
        event.socket = static_cast<Socket*>(events_[i].data.ptr);
        event.type = event.socket->fd_type;
        event.fd = event.socket->fd;

        if (events_[i].events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
          event.socket->event_hup = 1;
        }
        // read
        if ((events_[i].events & EPOLLIN) && !event.socket->removed) {
          handler = reactor_->get_handler(event.type, PP_EVENT_READ);
          ret = handler(reactor_, &event);
          if (ret < 0) {
            pp_sys_warning("EPOLLIN handle failed [fd=%d, type=%d]", event.fd, event.type);
          }
        }
        // write
        if ((events_[i].events & EPOLLOUT) && !event.socket->removed) {
          handler = reactor_->get_handler(event.type, PP_EVENT_WRITE);
          ret = handler(reactor_, &event);
          if (ret < 0) {
            pp_sys_warning("EPOLLOUT handle failed [fd=%d, type=%d]", event.fd, event.type);
          }
        }
        // error
        if ((events_[i].events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) && !event.socket->removed) {
          // ignore ERR and HUP, because event is already processed at IN and OUT handler.
          if ((events_[i].events & EPOLLIN) || (events_[i].events & EPOLLOUT)) {
            continue;
          }
          handler = reactor_->get_error_handler(event.type);
          ret = handler(reactor_, &event);
          if (ret < 0) {
            pp_sys_warning("EPOLLERR handle failed [fd=%d, type=%d]", event.fd, event.type);
          }
        }
        if (!event.socket->removed && (event.socket->events & PP_EVENT_ONCE)) {
          reactor_->del_socket(event.socket);
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
