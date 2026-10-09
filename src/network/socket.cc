
#include <memory>
#include <utility>

#include "pp_socket.h"
#include "pp_util.h"

namespace pp {
namespace network {

  double Socket::default_connect_timeout = PP_SOCKET_DEFAULT_CONNECT_TIMEOUT;
  double Socket::default_read_timeout = PP_SOCKET_DEFAULT_READ_TIMEOUT;
  double Socket::default_write_timeout = PP_SOCKET_DEFAULT_WRITE_TIMEOUT;
  uint32_t Socket::default_buffer_size = PP_SOCKET_BUFFER_SIZE;

  int Socket::what_event_want(int default_event) const {
    return default_event;
  }

#define CHECK_RETURN_VALUE(rv, be_zero_return)                                                                                                                                                         \
  if (rv < 0) {                                                                                                                                                                                        \
    if (pp_errno() == EINTR || catch_error(pp_errno()) == PP_WAIT) {                                                                                                                                   \
      return PP_CONTINUE;                                                                                                                                                                              \
    }                                                                                                                                                                                                  \
    pp_set_last_error(pp_errno());                                                                                                                                                                     \
    return PP_ERROR;                                                                                                                                                                                   \
  } else if (rv == 0) {                                                                                                                                                                                \
    return be_zero_return;                                                                                                                                                                             \
  }

  bool Socket::wait_for(const std::function<ReturnCode()>& fn, int event, int timeout_msec) {
    double deadline = 0;
    if (timeout_msec > 0) {
      deadline = microtime() + msec2sec(timeout_msec);
    }

    if (!nonblock) {
      set_nonblock();
    }

    while (true) {
      switch (fn()) {
      case PP_ERROR:
        return false;
      case PP_READY:
        return true;
      case PP_CONTINUE:
        /**
         * The ENOBUFS error indicates that the operating system currently lacks sufficient available memory,
         * requiring waiting for the kernel to reclaim memory. Event listening for writes is also ineffective—the
         * only recourse is to sleep for 10 milliseconds while awaiting kernel memory recovery.
         */
        if (has_kernel_nobufs()) {
          if (timeout_msec > 0 && microtime() >= deadline) {
            pp_set_last_error(ETIMEDOUT);
            pp_set_errno(ETIMEDOUT);
            return false;
          }
          usleep(10 * 1000);
          continue;
        }
        break;
      default:
        break;
      }

      int current_timeout_msec = timeout_msec;
      if (timeout_msec > 0) {
        double remaining = deadline - microtime();
        if (remaining <= 0) {
          pp_set_last_error(ETIMEDOUT);
          pp_set_errno(ETIMEDOUT);
          return false;
        }
        current_timeout_msec = sec2msec(remaining);
        pp_trace_log(PP_TRACE_CLIENT, "timeout_ms=%d", current_timeout_msec);
      }

      auto rv = wait_event(current_timeout_msec, what_event_want(event));
      if (rv == PP_ERR && ((pp_errno() == EINTR && dont_restart) || pp_errno() != EINTR)) {
        return false;
      }
    }

    return false;
  }

  /**
   * clear socket buffer.
   */

  /**
   * Wait socket can read or write.
   */
  int Socket::wait_event(int timeout_ms, int events) const {
    pollfd event;
    event.fd = fd;
    event.events = translate_events_to_poll(events);

    if (timeout_ms < 0) {
      timeout_ms = -1;
    }

    while (true) {
      int ret = poll(&event, 1, timeout_ms);
      if (ret == 0) {
        pp_set_last_error(PP_ERROR_SOCKET_POLL_TIMEOUT);
        pp_set_errno(EAGAIN);
        return PP_ERR;
      }
      if (ret < 0) {
        if (pp_errno() != EINTR) {
          pp_set_last_error(pp_errno());
          return PP_ERR;
        }
        if (dont_restart) {
          pp_set_last_error(pp_errno());
          return PP_ERR;
        }
        pp_signal_dispatch();
        continue;
      }
      return PP_OK;
    }

    return PP_OK;
  }

  ssize_t Socket::send_sync(const void* data, size_t len, int flags) {
    ssize_t bytes = 0;

    auto rv = wait_for(
      [this, data, len, flags, &bytes]() {
        ssize_t n = send((char*)data + bytes, len - bytes, flags);
        CHECK_RETURN_VALUE(n, PP_READY);
        bytes += n;
        return bytes == (ssize_t)len ? PP_READY : PP_CONTINUE;
      },
      PP_EVENT_WRITE, sec2msec(write_timeout));

    return rv ? bytes : -1;
  }

  static void socket_free_defer(void* ptr) {
    auto* sock = static_cast<Socket*>(ptr);
    if (sock->fd != PP_BAD_SOCKET && close(sock->fd) != 0) {
      pp_sys_warning("close(%d) failed", sock->fd);
    }
    delete sock;
  }

  void Socket::free() {
    if (recv_timer) {
      pp_timer_del(recv_timer);
    }
    if (send_timer) {
      pp_timer_del(send_timer);
    }

    delete out_buffer;

    if (pp_event_is_available()) {
      if (fd != -1 && !removed) {
        pp_event_del(this);
      }
      pp_event_defer(socket_free_defer, this);
    } else {
      socket_free_defer(this);
    }
  }

  bool Socket::set_buffer_size(uint32_t buffer_size) const {
    return set_send_buffer_size(buffer_size) && set_recv_buffer_size(buffer_size);
  }

  bool Socket::set_recv_buffer_size(uint32_t buffer_size) const {
    return set_option(SOL_SOCKET, SO_RCVBUF, buffer_size) == 0;
  }

  bool Socket::set_send_buffer_size(uint32_t buffer_size) const {
    return set_option(SOL_SOCKET, SO_SNDBUF, buffer_size) == 0;
  }

  static bool fcntl_set_option(int sock, int nonblock, int cloexec) {
    int opts = 0;
    int ret = 0;

    if (nonblock >= 0) {
      do {
        opts = fcntl(sock, F_GETFL);
      } while (opts < 0 && errno == EINTR);

      if (opts < 0) {
        pp_sys_warning("fcntl(%d, GETFL) failed", sock);
        return false;
      }

      if (nonblock) {
        opts = opts | O_NONBLOCK;
      } else {
        opts = opts & ~O_NONBLOCK;
      }

      do {
        ret = fcntl(sock, F_SETFL, opts);
      } while (ret < 0 && errno == EINTR);

      if (ret < 0) {
        pp_sys_warning("fcntl(%d, SETFL, opts) failed", sock);
        return false;
      }
    }

#ifdef FD_CLOEXEC
    if (cloexec >= 0) {
      do {
        opts = fcntl(sock, F_GETFD);
      } while (opts < 0 && errno == EINTR);

      if (opts < 0) {
        pp_sys_warning("fcntl(%d, GETFD) failed", sock);
        return false;
      }

      if (cloexec) {
        opts = opts | FD_CLOEXEC;
      } else {
        opts = opts & ~FD_CLOEXEC;
      }

      do {
        ret = fcntl(sock, F_SETFD, opts);
      } while (ret < 0 && errno == EINTR);

      if (ret < 0) {
        pp_sys_warning("fcntl(%d, SETFD, opts) failed", sock);
        return false;
      }
    }
#endif

    return true;
  }

  bool Socket::set_fd_option(int nonblock, int cloexec) {
    if (fcntl_set_option(fd, nonblock, cloexec)) {
      this->nonblock = nonblock;
      this->cloexec = cloexec;
      return true;
    } else {
      return false;
    }
  }

  void Socket::set_timeout(double timeout, int type) {
    if (timeout == 0) {
      return;
    }
    if (type & PP_TIMEOUT_CONNECT) {
      connect_timeout = timeout;
    }
    if (type & PP_TIMEOUT_READ) {
      read_timeout = timeout;
    }
    if (type & PP_TIMEOUT_WRITE) {
      write_timeout = timeout;
    }
  }

  bool Socket::has_kernel_nobufs() {
    return std::exchange(kernel_nobufs, 0);
  }

  /**
   * send buffer to client
   */
  int Socket::handle_send() {
    Buffer* buffer = out_buffer;
    BufferChunk* chunk = buffer->front();
    uint32_t sendn = chunk->length - chunk->offset;

    if (sendn == 0) {
      buffer->pop();
      return PP_OK;
    }

    ssize_t ret = send(chunk->value.str + chunk->offset, sendn, 0);
    if (ret < 0) {
      switch (catch_write_error(pp_errno())) {
      case PP_ERROR:
        pp_sys_warning("send to fd[%d] failed", fd);
        break;
      case PP_CLOSE:
        close_wait = 1;
        return PP_ERR;
      case PP_WAIT:
        send_wait = 1;
        return PP_ERR;
      default:
        break;
      }
      return PP_OK;
    }
    // chunk full send
    else if (ret == sendn) {
      buffer->pop();
    } else {
      chunk->offset += ret;
      // kernel is not fully processing and socket buffer is full
      if (ret < sendn) {
        send_wait = 1;
        return PP_ERR;
      }
    }
    return PP_OK;
  }

  ssize_t Socket::send(const void* buf, size_t n, int flags) {
    ssize_t retval = 0;

    do {
      { retval = ::send(fd, static_cast<const char*>(buf), n, flags); }
    } while (retval < 0 && (pp_errno() == EINTR && !dont_restart));

    if (retval > 0) {
      total_send_bytes += retval;
      if (send_timer) {
        last_sent_time = time<std::chrono::milliseconds>(true);
      }
    }

    pp_trace_log(PP_TRACE_SOCKET, "send %ld/%ld bytes, errno=%d", retval, n, pp_errno());

    return retval;
  }

  ssize_t Socket::read_sync(void* buf, size_t len) {
    ssize_t bytes = 0;

    auto rv = wait_for(
      [this, buf, len, &bytes]() {
        ssize_t n = read((char*)buf + bytes, len - bytes);
        CHECK_RETURN_VALUE(n, PP_READY);
        bytes += n;
        return PP_READY;
      },
      PP_EVENT_READ, sec2msec(read_timeout));

    return rv ? bytes : -1;
  }

  ssize_t Socket::write_sync(const void* buf, size_t len) {
    ssize_t bytes = 0;

    auto rv = wait_for(
      [this, buf, len, &bytes]() {
        ssize_t n = write((char*)buf + bytes, len - bytes);
        CHECK_RETURN_VALUE(n, PP_READY);
        bytes += n;
        return PP_READY;
      },
      PP_EVENT_WRITE, sec2msec(write_timeout));

    return rv ? bytes : -1;
  }

  int Socket::catch_error(const int err) {
    switch (err) {
    case EFAULT:
      abort();
      return PP_ERROR;
    case ENOBUFS:
      kernel_nobufs = true;
      return PP_WAIT;
    case EINTR:
      // EINTR is classified as PP_CONTINUE by pp_classify_errno; the socket
      // layers handle retries in their own loops, so surface it as an error.
      return PP_ERROR;
    default:
      return pp_classify_errno(err);
    }
  }

} // namespace network

using network::Socket;

Socket* make_socket(ppSocketFd fd, FdType fd_type) {
  auto* socket = new Socket();
  socket->fd = fd;
  socket->fd_type = fd_type;
  socket->removed = 1;
  return socket;
}

} // namespace pp
