

#pragma once

#include "pp_socket.h"

namespace pp {
struct SendData {
  DataHead info;
  const char* data;
};

struct RecvData {
  DataHead info;
  const char* data;

  SessionId session_id() const {
    return info.fd;
  }

  uint32_t length() const {
    return info.len;
  }
};
} // namespace pp
