

#pragma once

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/ip6.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>

#include <string>
#include <vector>

#include "pp.h"
#include "pp_buffer.h"

#ifndef SOCK_NONBLOCK
#define SOCK_NONBLOCK O_NONBLOCK
#endif

namespace pp {

namespace network {
  struct Address {
    union {
      sockaddr ss;
      sockaddr_in inet_v4;
      sockaddr_in6 inet_v6;
      sockaddr_un un;
    } addr;
    socklen_t len;
    SocketType type;

    /**
     * Assign an address based on the socket type and host/port.
     * For IPv4, the host can be an IP address like "192.168.1.100"
     * or a domain name like "www.example.com".
     * For IPv6, the host can be an IP address like "2001:0db8:85a3:0000:0000:8a2e:0370:7334"
     * or a domain name like "ipv6.example.com".
     * For UNIX socket, the host is the path to the socket file.
     * If port is 0, it will not set the port.
     * If resolve_name is false, it will not resolve the domain name.
     *
     * Returns true on success, false on failure.
     */
    bool assign(SocketType type, const std::string& host, int port = 0, bool resolve_name = true);

    /**
     * Convert the address to a string representation.
     * For IPv4, it will be in the format "192.168.1.100"
     * For IPv6, it will be in the format "2001:0db8:85a3:0000:0000:8a2e:0370:7334"
     * For UNIX socket, it will be the path of the socket file.
     * The returned pointer is a static buffer, so it should not be freed.
     */
    static bool verify_port(int port, bool for_connect = false);
  };

  struct Socket {
    static double default_connect_timeout;
    static double default_read_timeout;
    static double default_write_timeout;
    static uint32_t default_buffer_size;

    ppSocketFd fd;
    FdType fd_type;
    SocketType socket_type;
    int events;
    bool enable_tcp_nodelay;
    bool kernel_nobufs;

    uchar removed : 1;
    uchar silent_remove : 1;
    uchar nonblock : 1;
    uchar cloexec : 1;
    uchar listened : 1;
    uchar close_wait : 1;
    uchar send_wait : 1;
    uchar event_hup : 1;
    /**
     * The default setting is false, meaning that system calls interrupted by signals will be automatically retried. If
     * set to true, the call will not be retried but will immediately return -1, setting errno to EINTR. In this case,
     * the caller must explicitly handle this error.
     */
    uchar dont_restart : 1;
    uchar close_notified : 1;

    /**
     * Only used for getsockname, written by the OS, not user. This is the exact actual address.
     */
    Address info;
    // memory buffer size [user space]
    uint32_t buffer_size;
    uint32_t chunk_size;

    double last_sent_time;

    Buffer* out_buffer;

    double connect_timeout = default_connect_timeout;
    double read_timeout = default_read_timeout;
    double write_timeout = default_write_timeout;

    TimerNode* recv_timer;
    TimerNode* send_timer;

    size_t total_send_bytes;

    // for reactor
    int handle_send();
    // user space memory buffer
    void set_memory_buffer_size(uint32_t size) {
      buffer_size = size;
    }
    // socket option [kernel space]
    bool set_buffer_size(uint32_t size) const;
    bool set_recv_buffer_size(uint32_t size) const;
    bool set_send_buffer_size(uint32_t size) const;

    // socket option [user space]
    void set_timeout(double timeout, int type = PP_TIMEOUT_ALL);
    bool has_kernel_nobufs();

    bool set_nonblock() {
      return set_fd_option(1, -1);
    }

    bool set_block() {
      return set_fd_option(0, -1);
    }

    bool set_fd_option(int nonblock, int cloexec);

    int set_option(int level, int optname, int optval) const {
      return setsockopt(fd, level, optname, reinterpret_cast<const char*>(&optval), sizeof(optval));
    }

    ppSocketFd get_fd() const {
      return fd;
    }

    ssize_t writev(const iovec* iov, size_t iovcnt) const {
      return ::writev(fd, iov, iovcnt);
    }

    ssize_t send(const void* buf, size_t n, int flags);
    ssize_t send_sync(const void* data, size_t len, int flags = 0);

    bool isset_writable_event() const {
      return events & PP_EVENT_WRITE;
    }

    int what_event_want(int default_event) const;
    int wait_event(int timeout_ms, int events) const;
    bool wait_for(const std::function<ReturnCode()>& fn, int event, int timeout_msec = -1);
    void free();

    static inline bool is_inet4(SocketType type) {
      return type == PP_SOCK_TCP || type == PP_SOCK_UDP || type == PP_SOCK_RAW;
    }

    static inline bool is_inet6(SocketType type) {
      return type == PP_SOCK_TCP6 || type == PP_SOCK_UDP6 || type == PP_SOCK_RAW6;
    }

    static inline bool is_local(SocketType type) {
      return type == PP_SOCK_UNIX_STREAM || type == PP_SOCK_UNIX_DGRAM;
    }

    ssize_t write(const void* buf, size_t len) const {
      return ::write(fd, buf, len);
    }

    ssize_t read(void* buf, size_t len) const {
      return ::read(fd, buf, len);
    }

    /**
     * Read data from the socket synchronously without setting non-blocking or blocking IO,
     * and allow interruptions by signals.
     */
    ssize_t read_sync(void* buf, size_t len);

    /**
     * Write data to the socket synchronously without setting non-blocking or blocking IO,
     * and allow interruptions by signals.
     */
    ssize_t write_sync(const void* buf, size_t len);

    int catch_error(int err);

    int catch_write_error(const int err) {
      return catch_error(err);
    }
  };

} // namespace network

/**
 * This function will never return NULL; if memory allocation fails, a C++ exception will be thrown.
 * Must use the `socket->free()` function to release the object pointer instead of the `delete` operator.
 * When the socket is released, it will close the file descriptor (fd).
 * If you do not want the fd to be closed, use `socket->move_fd()` to relinquish ownership of the fd.
 */
network::Socket* make_socket(ppSocketFd fd, FdType fd_type);
} // namespace pp
