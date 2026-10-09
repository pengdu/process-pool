

#include "pp_pipe.h"
#include "pp_socket.h"

namespace pp {
UnixSocket::UnixSocket(bool blocking, int protocol) : SocketPair(blocking), protocol_(protocol) {
  if (socketpair(AF_UNIX, protocol_, 0, socks_) < 0) {
    pp_sys_warning("socketpair() failed");
    return;
  }
  init_socket(socks_[1], socks_[0]);
  set_buffer_size(network::Socket::default_buffer_size);
}

bool UnixSocket::set_buffer_size(size_t size) const {
  if (!master_socket_->set_buffer_size(size)) {
    return false;
  }
  if (!worker_socket_->set_buffer_size(size)) {
    return false;
  }
  return true;
}
} // namespace pp
