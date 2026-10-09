
#include "pp_reactor.h"
#include "pp_signal.h"
#include "pp_socket.h"

#ifdef HAVE_SIGNALFD
#include <sys/signalfd.h>
#endif

#include <csignal>

#ifdef HAVE_KQUEUE
#ifdef USE_KQUEUE_IDE_HELPER
#include "helper/kqueue.h"
#else
#include <sys/event.h>
#endif
#endif
#ifdef __NetBSD__
#include <sys/param.h>
#endif

using pp::Event;
using pp::Reactor;
using pp::Signal;
using pp::SignalHandler;
using pp::network::Socket;

#ifdef HAVE_SIGNALFD
static SignalHandler pp_signalfd_set(int signo, SignalHandler handler);
static bool pp_signalfd_create();
static bool pp_signalfd_setup(Reactor* reactor);
static int pp_signalfd_event_callback(Reactor* reactor, Event* event);
static void pp_signalfd_clear();
#endif

#ifdef HAVE_KQUEUE
static SignalHandler pp_signal_kqueue_set(int signo, SignalHandler handler);
static void pp_signal_kqueue_clear();
#endif

static void signal_handler_safety(int signo);
static void signal_handler_simple(int signo);

#ifdef HAVE_SIGNALFD
static sigset_t signalfd_mask;
static int signal_fd = -1;
static pid_t signalfd_create_pid;
static Socket* signal_socket = nullptr;
static inline bool pp_signalfd_is_available() {
  return signal_fd != -1;
}
#endif
static Signal signals[PP_SIGNO_MAX];
static bool triggered_signals[PP_SIGNO_MAX];

char* pp_signal_to_str(int sig) {
  static char buf[64];
  snprintf(buf, sizeof(buf), "%s", strsignal(sig));
  if (strchr(buf, ':') == nullptr) {
    size_t len = strlen(buf);
    snprintf(buf + len, sizeof(buf) - len, ": %d", sig);
  }
  return buf;
}

void pp_signal_block_all() {
  if (PPTG.signal_blocking_all) {
    return;
  }
  sigset_t mask = 0;
  sigfillset(&mask);
  int ret = pthread_sigmask(SIG_BLOCK, &mask, nullptr);
  if (ret < 0) {
    pp_sys_warning("pthread_sigmask(SIG_BLOCK) failed");
  } else {
    PPTG.signal_blocking_all = true;
  }
}

/**
 * set new signal handler and return origin signal handler
 */
SignalHandler pp_signal_set(int signo, SignalHandler func, int restart, int mask) {
  // ignore
  if (func == nullptr) {
    func = SIG_IGN;
  }
  // clear
  else if ((long)func == -1) {
    func = SIG_DFL;
  }

  if (func == SIG_IGN || func == SIG_DFL) {
    signals[signo].handler = nullptr;
    signals[signo].activated = false;
  }

  struct sigaction act {};
  struct sigaction oact {};
  act.sa_handler = func;
  if (mask) {
    sigfillset(&act.sa_mask);
  } else {
    sigemptyset(&act.sa_mask);
  }
  act.sa_flags = 0;
  if (restart) {
    act.sa_flags |= SA_RESTART;
  }
  if (sigaction(signo, &act, &oact) < 0) {
    return nullptr;
  }
  return oact.sa_handler;
}

/**
 * set new signal handler and return origin signal handler
 */
SignalHandler pp_signal_set(int signo, SignalHandler handler, bool safety) {
#ifdef HAVE_SIGNALFD
  if (PPG.enable_signalfd && pp_event_is_available()) {
    return pp_signalfd_set(signo, handler);
  }
#endif
#ifdef HAVE_KQUEUE
  // SIGCHLD can not be monitored by kqueue, if blocked by SIG_IGN
  // see https://www.freebsd.org/cgi/man.cgi?kqueue
  // if there's no main reactor, signals cannot be monitored either
  if (PPG.enable_kqueue && pp_event_is_available() && signo != SIGCHLD) {
    return pp_signal_kqueue_set(signo, handler);
  }
#endif

  signals[signo].handler = handler;
  signals[signo].activated = true;
  signals[signo].signo = signo;
  return pp_signal_set(signo, safety ? signal_handler_safety : signal_handler_simple, 0, 0);
}

static void signal_handler_safety(int signo) {
  triggered_signals[signo] = true;
  PPG.signal_dispatch = true;
}

static void signal_handler_simple(int signo) {
  if (pp_reactor()) {
    signal_handler_safety(signo);
  } else {
    static int reentry_lock = 0;
    // discard signal
    if (reentry_lock) {
      return;
    }
    reentry_lock = 1;
    pp_signal_callback(signo);
    reentry_lock = 0;
  }
}

void pp_signal_dispatch() {
  if (!PPG.signal_dispatch) {
    return;
  }
  PP_LOOP_N(PP_SIGNO_MAX) {
    if (triggered_signals[i]) {
      pp_signal_callback(i);
      triggered_signals[i] = false;
    }
  }
  PPG.signal_dispatch = false;
}

void pp_signal_callback(int signo) {
  if (signo >= PP_SIGNO_MAX) {
    pp_warning("signal[%d] numberis invalid", signo);
    return;
  }
  SignalHandler callback = signals[signo].handler;
  if (!callback) {
    pp_error_log(PP_LOG_WARNING, PP_ERROR_UNREGISTERED_SIGNAL, PP_UNREGISTERED_SIGNAL_FMT, pp_signal_to_str(signo));
    return;
  }
  if (callback == SIG_IGN || callback == SIG_DFL) {
    return;
  }
  callback(signo);
}

void pp_signal_clear() {
#ifdef HAVE_SIGNALFD
  if (PPG.enable_signalfd && pp_signalfd_is_available()) {
    pp_signalfd_clear();
    return;
  }
#endif

#ifdef HAVE_KQUEUE
  if (PPG.enable_kqueue) {
    pp_signal_kqueue_clear();
    return;
  }
#endif

  PP_LOOP_N(PP_SIGNO_MAX) {
    if (signals[i].activated) {
      pp_signal_set(signals[i].signo, reinterpret_cast<SignalHandler>(-1), 1, 0);
    }
  }
  pp_memset_zero(signals, sizeof(signals));
}

#ifdef HAVE_SIGNALFD
void pp_signalfd_init() {
  sigemptyset(&signalfd_mask);
  pp_memset_zero(signals, sizeof(signals));
}

/**
 * set new signal handler and return origin signal handler
 */
static SignalHandler pp_signalfd_set(int signo, SignalHandler handler) {
  SignalHandler origin_handler = nullptr;

  if (handler == nullptr && signals[signo].activated) {
    sigdelset(&signalfd_mask, signo);
    pp_memset_zero(&signals[signo], sizeof(Signal));
  } else {
    sigaddset(&signalfd_mask, signo);
    origin_handler = signals[signo].handler;
    signals[signo].handler = handler;
    signals[signo].signo = signo;
    signals[signo].activated = true;
  }

  if (pp_signalfd_is_available()) {
    sigprocmask(SIG_SETMASK, &signalfd_mask, nullptr);
    signalfd(signal_fd, &signalfd_mask, SFD_NONBLOCK | SFD_CLOEXEC);
  }
  pp_signalfd_setup(pp_reactor());

  return origin_handler;
}

static void pp_signalfd_close() {
  if (!pp_signalfd_is_available()) {
    return;
  }
  signal_socket->fd = PP_BAD_SOCKET;
  signal_socket->free();
  close(signal_fd);
  signal_socket = nullptr;
  signal_fd = -1;
}

static bool pp_signalfd_create() {
  if (pp_signalfd_is_available()) {
    return false;
  }

  signal_fd = signalfd(-1, &signalfd_mask, SFD_NONBLOCK | SFD_CLOEXEC);
  if (signal_fd < 0) {
    pp_sys_warning("signalfd() failed");
    return false;
  }
  signal_socket = pp::make_socket(signal_fd, PP_FD_SIGNAL);
  if (sigprocmask(SIG_BLOCK, &signalfd_mask, nullptr) == -1) {
    pp_sys_warning("sigprocmask() failed");
    pp_signalfd_close();
    return false;
  }
  signalfd_create_pid = getpid();

  return true;
}

bool pp_signalfd_setup(Reactor* reactor) {
  if (!pp_signalfd_is_available() && !pp_signalfd_create()) {
    return false;
  }
  if (!pp_event_isset_handler(PP_FD_SIGNAL, PP_EVENT_READ)) {
    pp_event_set_handler(PP_FD_SIGNAL, PP_EVENT_READ, pp_signalfd_event_callback);
    reactor->set_exit_condition(Reactor::EXIT_CONDITION_SIGNALFD, [](Reactor* reactor, size_t& event_num) -> bool {
      event_num--;
      return true;
    });
    reactor->add_destroy_callback([](void*) {
      // child process removes signal socket, parent process will not be able to trigger signal
      if (signal_socket && signalfd_create_pid == getpid()) {
        pp_event_del(signal_socket);
      }
    });
  }
  if (!(signal_socket->events & PP_EVENT_READ) && pp_event_add(signal_socket, PP_EVENT_READ) < 0) {
    return false;
  }
  reactor->erase_end_callback(Reactor::PRIORITY_SIGNAL_CALLBACK);
  return true;
}

static void pp_signalfd_clear() {
  if (!pp_signalfd_is_available()) {
    return;
  }

  if (sigprocmask(SIG_UNBLOCK, &signalfd_mask, nullptr) < 0) {
    pp_sys_warning("sigprocmask(SIG_UNBLOCK) failed");
  }
  pp_memset_zero(signals, sizeof(signals));
  pp_memset_zero(&signalfd_mask, sizeof(signalfd_mask));

  pp_signalfd_close();
}

static int pp_signalfd_event_callback(Reactor* reactor, Event* event) {
  signalfd_siginfo siginfo;
  ssize_t n = read(event->fd, &siginfo, sizeof(siginfo));
  if (n < 0) {
    pp_sys_warning("read from signalfd failed");
    return PP_OK;
  }
  if (siginfo.ssi_signo >= PP_SIGNO_MAX) {
    pp_warning("unknown signal[%d]", siginfo.ssi_signo);
    return PP_OK;
  }
  if (signals[siginfo.ssi_signo].activated) {
    SignalHandler handler = signals[siginfo.ssi_signo].handler;
    if (handler == SIG_IGN) {
      return PP_OK;
    } else if (handler) {
      handler(siginfo.ssi_signo);
    } else {
      pp_error_log(PP_LOG_WARNING, PP_ERROR_UNREGISTERED_SIGNAL, PP_UNREGISTERED_SIGNAL_FMT, pp_signal_to_str(siginfo.ssi_signo));
    }
  }

  return PP_OK;
}
#endif

#ifdef HAVE_KQUEUE
/**
 * set new signal handler and return origin signal handler
 */
static SignalHandler pp_signal_kqueue_set(int signo, SignalHandler handler) {
  struct kevent ev;
  SignalHandler origin_handler = nullptr;
  Reactor* reactor = pp_reactor();

  // clear signal
  if (handler == nullptr) {
    signal(signo, SIG_DFL);
    pp_memset_zero(&signals[signo], sizeof(Signal));
    EV_SET(&ev, signo, EVFILT_SIGNAL, EV_DELETE, 0, 0, NULL);
  }
  // add/update signal
  else {
    signal(signo, SIG_IGN);
    origin_handler = signals[signo].handler;
    signals[signo].handler = handler;
    signals[signo].signo = signo;
    signals[signo].activated = true;
#if !defined(__NetBSD__) || (defined(__NetBSD__) && __NetBSD_Version__ >= 1000000000)
    auto sigptr = &signals[signo];
#else
    auto sigptr = reinterpret_cast<intptr_t>(&signals[signo]);
#endif
    // save ppSignal* as udata
    EV_SET(&ev, signo, EVFILT_SIGNAL, EV_ADD, 0, 0, sigptr);
  }
  int n = kevent(reactor->native_handle, &ev, 1, nullptr, 0, nullptr);
  if (n < 0 && pp_unlikely(handler)) {
    pp_sys_warning("kevent set signal[%d] error", signo);
  }

  return origin_handler;
}

static void pp_signal_kqueue_clear() {
  PP_LOOP_N(PP_SIGNO_MAX) {
    if (signals[i].activated && pp_event_is_available()) {
      signals[i].activated = false;
      signals[i].handler = nullptr;
      pp_signal_kqueue_set(signals[i].signo, nullptr);
    }
  }
  pp_memset_zero(signals, sizeof(signals));
}
#endif
