
#pragma once

#include "pp_socket.h"

enum ppPipe_close_which
{
  PP_PIPE_CLOSE_MASTER = 1,
  PP_PIPE_CLOSE_WORKER = 2,
};

namespace pp {
class SocketPair {
protected:
  bool blocking_;
  double timeout_;

  /**
   * master : socks[1], for write operation
   * worker : socks[0], for read operation
   */
  ppSocketFd socks_[2] {};

  network::Socket* master_socket_ = nullptr;
  network::Socket* worker_socket_ = nullptr;

  void init_socket(ppSocketFd master_fd, ppSocketFd worker_fd);

public:
  explicit SocketPair(bool blocking) {
    blocking_ = blocking;
    timeout_ = network::Socket::default_read_timeout;
  }
  virtual ~SocketPair();

  ssize_t read(void* buf, size_t length) const;
  ssize_t write(const void* buf, size_t length) const;
  bool close(int which = 0);

  network::Socket* get_socket(bool master) const {
    return master ? master_socket_ : worker_socket_;
  }

  bool ready() const {
    return master_socket_ != nullptr && worker_socket_ != nullptr;
  }

  // Contract: only call after ready() is true; constructors leave sockets null when OS setup fails.
  void set_timeout(double timeout) {
    timeout_ = timeout;
    master_socket_->set_timeout(timeout);
    worker_socket_->set_timeout(timeout);
  }

  // Contract: only call after ready() is true; constructors leave sockets null when OS setup fails.
  void set_blocking(bool blocking);
};

class Pipe : public SocketPair {
public:
  explicit Pipe(bool blocking);
};

class UnixSocket : public SocketPair {
  int protocol_;

public:
  UnixSocket(bool blocking, int protocol);
  bool set_buffer_size(size_t size) const;
};

} // namespace pp
