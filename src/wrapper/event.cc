

#include "pp_reactor.h"

using pp::Callback;
using pp::Reactor;
using pp::ReactorHandler;
using pp::network::Socket;

int pp_event_init(int flags) {
  auto* reactor = new Reactor(PP_REACTOR_MAXEVENTS);
  if (!reactor->ready()) {
    delete reactor;
    return PP_ERR;
  }

  if (flags & PP_EVENTLOOP_WAIT_EXIT) {
    reactor->wait_exit = true;
  }

  PPTG.reactor = reactor;

  return PP_OK;
}

int pp_event_add(Socket* socket, int events) {
  return PPTG.reactor->add(socket, events);
}

int pp_event_del(Socket* socket) {
  return PPTG.reactor->del(socket);
}

int pp_event_wait() {
  Reactor* reactor = PPTG.reactor;
  int retval = 0;
  if (!reactor->wait_exit || !reactor->if_exit()) {
    retval = reactor->wait();
  }
  pp_event_free();
  return retval;
}

int pp_event_free() {
  if (!PPTG.reactor) {
    return PP_ERR;
  }
  delete PPTG.reactor;
  PPTG.reactor = nullptr;
  return PP_OK;
}

void pp_event_defer(const Callback& cb, void* private_data) {
  PPTG.reactor->defer(cb, private_data);
}

ssize_t pp_event_write(Socket* socket, const void* data, size_t len) {
  return PPTG.reactor->write(PPTG.reactor, socket, data, len);
}

void pp_event_set_handler(const int fd_type, const int event, const ReactorHandler handler) {
  PPTG.reactor->set_handler(fd_type, event, handler);
}

bool pp_event_isset_handler(const int fd_type, const int event) {
  return PPTG.reactor->isset_handler(fd_type, event);
}

bool pp_event_is_available() {
  return PPTG.reactor && !PPTG.reactor->destroyed;
}
