

#include "pp_pipe.h"
#include "pp_socket.h"

namespace pp {
using network::Socket;

void SocketPair::init_socket(ppSocketFd master_fd, ppSocketFd worker_fd) {
  master_socket_ = make_socket(master_fd, PP_FD_PIPE);
  worker_socket_ = make_socket(worker_fd, PP_FD_PIPE);
  set_blocking(blocking_);
}

Pipe::Pipe(bool blocking) : SocketPair(blocking) {
  if (pipe(socks_) < 0) {
    pp_sys_warning("pipe() failed");
    return;
  }
  // socks[0]: (read end)
  // socks[1]: (write end)
  init_socket(socks_[1], socks_[0]);
}

void SocketPair::set_blocking(bool blocking) {
  if (blocking) {
    worker_socket_->set_block();
    master_socket_->set_block();
  } else {
    worker_socket_->set_nonblock();
    master_socket_->set_nonblock();
  }
  blocking_ = blocking;
}

ssize_t SocketPair::read(void* data, size_t length) const {
  if (blocking_) {
    return worker_socket_->read_sync(data, length);
  } else {
    return worker_socket_->read(data, length);
  }
}

ssize_t SocketPair::write(const void* data, size_t length) const {
  if (blocking_) {
    return master_socket_->write_sync(data, length);
  } else {
    return master_socket_->write(data, length);
  }
}

bool SocketPair::close(int which) {
  if (which == PP_PIPE_CLOSE_MASTER) {
    if (master_socket_ == nullptr) {
      return false;
    }
    master_socket_->free();
    master_socket_ = nullptr;
  } else if (which == PP_PIPE_CLOSE_WORKER) {
    if (worker_socket_ == nullptr) {
      return false;
    }
    worker_socket_->free();
    worker_socket_ = nullptr;
  } else {
    close(PP_PIPE_CLOSE_MASTER);
    close(PP_PIPE_CLOSE_WORKER);
  }
  return true;
}

SocketPair::~SocketPair() {
  if (master_socket_) {
    close(PP_PIPE_CLOSE_MASTER);
  }
  if (worker_socket_) {
    close(PP_PIPE_CLOSE_WORKER);
  }
}

} // namespace pp
