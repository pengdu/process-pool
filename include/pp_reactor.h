

#pragma once

#include <list>
#include <map>
#include <unordered_map>

#include "pp_socket.h"

namespace pp {

struct DeferCallback {
  Callback callback;
  void* data;
};

class Reactor;

class ReactorImpl {
protected:
  Reactor* reactor_ {nullptr};

public:
  explicit ReactorImpl(Reactor* reactor) {
    reactor_ = reactor;
  }
  void after_removal_failure(const network::Socket* socket) const;
  virtual ~ReactorImpl() = default;
  virtual bool ready() = 0;
  virtual int add(network::Socket* socket, int events) = 0;
  virtual int set(network::Socket* socket, int events) = 0;
  virtual int del(network::Socket* socket) = 0;
  virtual int wait() = 0;
};

class CallbackManager {
public:
  using TaskList = std::list<std::pair<Callback, void*>>;
  void append(const Callback& fn, void* private_data) {
    list_.emplace_back(fn, private_data);
  }
  void execute() {
    while (!list_.empty()) {
      std::pair<Callback, void*> task = list_.front();
      list_.pop_front();
      task.first(task.second);
    }
  }

protected:
  TaskList list_;
};

class Reactor {
public:
  enum Type
  {
    TYPE_AUTO,
    TYPE_EPOLL,
    TYPE_KQUEUE,
  };

  enum EndCallback
  {
    PRIORITY_TIMER = 0,
    PRIORITY_DEFER_TASK,
    PRIORITY_IDLE_TASK,
    PRIORITY_SIGNAL_CALLBACK,
    PRIORITY_TRY_EXIT,
    PRIORITY_MALLOC_TRIM,
  };

  enum ExitCondition
  {
    EXIT_CONDITION_TIMER = 0,
    EXIT_CONDITION_DEFER_TASK,
    EXIT_CONDITION_SIGNALFD,
    EXIT_CONDITION_DEFAULT = 999,
  };

  Type type_;
  void* ptr = nullptr;
  int native_handle = -1;
  uint32_t max_event_num = 0;

  bool ready_ = false;
  bool running = false;
  bool once = false;
  bool wait_exit = false;
  bool destroyed = false;

  /**
   * reactor->wait timeout (millisecond) or -1
   */
  int32_t timeout_msec = 0;

  uint16_t id = 0;

#ifdef PP_USE_MALLOC_TRIM
  time_t last_malloc_trim_time = 0;
#endif

  ReactorHandler read_handler[PP_MAX_FDTYPE] = {};
  ReactorHandler write_handler[PP_MAX_FDTYPE] = {};
  ReactorHandler error_handler[PP_MAX_FDTYPE] = {};

  ReactorHandler default_write_handler = nullptr;
  ReactorHandler default_error_handler = nullptr;

  int add(network::Socket* socket, int events) const {
    return impl_->add(socket, events);
  }

  int set(network::Socket* socket, int events) const {
    return impl_->set(socket, events);
  }

  int del(network::Socket* socket) const {
    return impl_->del(socket);
  }

  int wait() const {
    return impl_->wait();
  }

  CallbackManager* defer_tasks = nullptr;
  CallbackManager destroy_callbacks;

  DeferCallback idle_task;
  DeferCallback future_task;

  ssize_t (*write)(Reactor* reactor, network::Socket* socket, const void* buf, size_t n) = nullptr;
  ssize_t (*writev)(Reactor* reactor, network::Socket* socket, const iovec* iov, size_t iovcnt) = nullptr;
  int (*close)(Reactor* reactor, network::Socket* socket) = nullptr;

private:
  ReactorImpl* impl_ {nullptr};
  std::map<int, std::function<void(Reactor*)>> end_callbacks_;
  std::map<int, std::function<bool(Reactor*, size_t&)>> exit_conditions_;
  std::unordered_map<int, network::Socket*> sockets_;

public:
  explicit Reactor(int max_event = PP_REACTOR_MAXEVENTS, Type type = TYPE_AUTO);
  ~Reactor();
  bool if_exit();
  void defer(const Callback& cb, void* data = nullptr);
  void set_end_callback(EndCallback id, const std::function<void(Reactor*)>& fn);
  void erase_end_callback(EndCallback id);
  void set_exit_condition(ExitCondition id, const std::function<bool(Reactor*, size_t&)>& fn);
  void set_handler(int fd_type, int event, ReactorHandler handler);
  bool isset_handler(int fd_type, int event) const;
  void add_destroy_callback(const Callback& cb, void* data = nullptr);
  void execute_begin_callback() const;
  void execute_end_callbacks(bool timed_out = false);

  bool ready() const {
    return ready_;
  }

  bool is_running() const {
    return running;
  }

  int remove_write_event(network::Socket* socket) const {
    if (socket->events & PP_EVENT_READ) {
      socket->events &= (~PP_EVENT_WRITE);
      return set(socket, socket->events);
    } else {
      return del(socket);
    }
  }

  int add_write_event(network::Socket* socket) const {
    if (socket->events & PP_EVENT_READ) {
      socket->events |= PP_EVENT_WRITE;
      return set(socket, socket->events);
    } else {
      return add(socket, PP_EVENT_WRITE);
    }
  }

  int get_timeout_msec() const {
    return defer_tasks == nullptr ? timeout_msec : 0;
  }

  size_t get_event_num() const {
    return sockets_.size();
  }

  ReactorHandler get_handler(const FdType fd_type, const EventType event) const {
    switch (event) {
    case PP_EVENT_READ:
      return read_handler[fd_type];
    case PP_EVENT_WRITE:
      return write_handler[fd_type] ? write_handler[fd_type] : default_write_handler;
    case PP_EVENT_ERROR:
      return error_handler[fd_type] ? error_handler[fd_type] : default_error_handler;
    default:
      abort();
    }
  }

  ReactorHandler get_error_handler(const FdType fd_type) const {
    ReactorHandler handler = get_handler(fd_type, PP_EVENT_ERROR);
    // error callback is not set, try to use readable or writable callback
    if (handler == nullptr) {
      handler = get_handler(fd_type, PP_EVENT_READ);
      if (handler == nullptr) {
        handler = get_handler(fd_type, PP_EVENT_WRITE);
      }
    }
    return handler;
  }

  void before_wait() {
    running = true;
    if (timeout_msec == 0) {
      timeout_msec = -1;
    }
  }

  int trigger_close_event(Event* event) {
    return default_error_handler(this, event);
  }

  void add_socket(network::Socket* socket, const int events) {
    socket->events = events;
    socket->removed = 0;
    sockets_[socket->fd] = socket;
  }

  void set_socket(network::Socket* socket, const int events) {
    socket->events = events;
  }

  void del_socket(network::Socket* socket) {
    socket->events = 0;
    socket->removed = 1;
    sockets_.erase(socket->fd);
  }

  bool catch_error() const {
    return pp_classify_errno(errno) == PP_CONTINUE;
  }

  static ssize_t default_write(Reactor* reactor, network::Socket* socket, const void* buf, size_t n);
  static ssize_t default_writev(Reactor* reactor, network::Socket* socket, const iovec* iov, size_t iovcnt);
  static int default_close(Reactor* reactor, network::Socket* socket);
  static int default_writable_callback(Reactor* reactor, Event* ev);
  static ssize_t write_func(const Reactor* reactor, network::Socket* socket, size_t len, const std::function<ssize_t()>& send_fn, const std::function<void(Buffer* buffer)>& append_fn);

  static bool isset_read_event(const int events) {
    return (events < PP_EVENT_DEFAULT) || (events & PP_EVENT_READ);
  }

  static bool isset_write_event(const int events) {
    return events & PP_EVENT_WRITE;
  }

  static bool isset_error_event(const int events) {
    return events & PP_EVENT_ERROR;
  }
};

} // namespace pp
