

#include "pp_socket.h"

namespace pp {
namespace network {
  static thread_local char tmp_address[INET6_ADDRSTRLEN];

  bool Address::verify_port(const int port, const bool for_connect) {
    if (port < 0 || port > 65535) {
      return false;
    }
    if (for_connect && port == 0) {
      return false;
    }
    return true;
  }

  bool Address::assign(SocketType type, const std::string& host, int port, bool resolve_name) {
    this->type = type;
    const char* host_str = host.c_str();

    if (!verify_port(port)) {
      pp_set_last_error(PP_ERROR_BAD_PORT);
      return false;
    }

    if (Socket::is_inet4(type)) {
      addr.inet_v4.sin_family = AF_INET;
      addr.inet_v4.sin_port = htons(port);
      len = sizeof(addr.inet_v4);

      if (inet_pton(AF_INET, host_str, &addr.inet_v4.sin_addr.s_addr) != 1) {
        if (!resolve_name) {
          pp_set_last_error(PP_ERROR_BAD_HOST_ADDR);
          return false;
        }
        pp_set_last_error(PP_ERROR_BAD_HOST_ADDR);
        return false;
      }
    } else if (Socket::is_inet6(type)) {
      addr.inet_v6.sin6_family = AF_INET6;
      addr.inet_v6.sin6_port = htons(port);
      len = sizeof(addr.inet_v6);
      if (inet_pton(AF_INET6, host_str, addr.inet_v6.sin6_addr.s6_addr) != 1) {
        if (!resolve_name) {
          pp_set_last_error(PP_ERROR_BAD_HOST_ADDR);
          return false;
        }
        pp_set_last_error(PP_ERROR_BAD_HOST_ADDR);
        return false;
      }
    } else if (Socket::is_local(type)) {
      if (host.length() >= sizeof(addr.un.sun_path) - 1) {
        pp_set_last_error(PP_ERROR_NAME_TOO_LONG);
        return false;
      }
      addr.un.sun_family = AF_UNIX;
      pp_strlcpy(addr.un.sun_path, host_str, sizeof(addr.un.sun_path));
      addr.un.sun_path[sizeof(addr.un.sun_path) - 1] = 0;
      len = sizeof(addr.un.sun_path);
    } else {
      pp_set_last_error(PP_ERROR_BAD_SOCKET_TYPE);
      return false;
    }

    return true;
  }

} // namespace network
} // namespace pp
