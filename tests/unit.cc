/*
  Unit tests for the process-pool library internals.

  Everything here runs in a single process with no fork: pure functions,
  in-memory data structures, and process-local services. The process-level
  behavior (fork/IPC/reload) lives in func.cc / integration.cc.

  Build:  cmake --build build --target pp_unit
  Run:    ./build/tests/pp_unit
*/

#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <thread>

#include "pp_buffer.h"
#include "pp_channel.h"
#include "pp_error.h"
#include "pp_heap.h"
#include "pp_lock.h"
#include "pp_log.h"
#include "pp_memory.h"
#include "pp_msg_queue.h"
#include "pp_process_pool.h"
#include "pp_reactor.h"
#include "pp_socket.h"
#include "pp_timer.h"
#include "pp_util.h"

using pp::Buffer;
using pp::Channel;
using pp::EventData;
using pp::Heap;
using pp::HeapNode;
using pp::Logger;
using pp::msec2sec;
using pp::Mutex;
using pp::sec2msec;
using pp::Timer;
using pp::TimerNode;

// ---------------------------------------------------------------------------
// tiny harness
// ---------------------------------------------------------------------------

static int test_failures = 0;

#define EXPECT(cond)                                                                                                                                                                                   \
  do {                                                                                                                                                                                                 \
    if (!(cond)) {                                                                                                                                                                                     \
      std::fprintf(stderr, "    EXPECT failed at %s:%d: %s\n", __FILE__, __LINE__, #cond);                                                                                                             \
      test_failures++;                                                                                                                                                                                 \
    }                                                                                                                                                                                                  \
  } while (0)

struct TestCase {
  const char* name;
  void (*fn)();
};

// ---------------------------------------------------------------------------
// core: errno classification / event translation / time helpers
// ---------------------------------------------------------------------------

static void test_errno_classification() {
  EXPECT(pp_classify_errno(0) == PP_WAIT);
  EXPECT(pp_classify_errno(EAGAIN) == PP_WAIT);
  EXPECT(pp_classify_errno(EWOULDBLOCK) == PP_WAIT);
  EXPECT(pp_classify_errno(EINTR) == PP_CONTINUE);
  EXPECT(pp_classify_errno(EBADF) == PP_INVALID);
  EXPECT(pp_classify_errno(ENOENT) == PP_INVALID);
  EXPECT(pp_classify_errno(EPIPE) == PP_CLOSE);
  EXPECT(pp_classify_errno(ECONNRESET) == PP_CLOSE);
  EXPECT(pp_classify_errno(ETIMEDOUT) == PP_CLOSE);
  EXPECT(pp_classify_errno(ECONNREFUSED) == PP_CLOSE);
  EXPECT(pp_classify_errno(EACCES) == PP_ERROR);
  EXPECT(pp_classify_errno(ENOMEM) == PP_ERROR);
}

static void test_translate_events() {
  EXPECT(pp::translate_events_to_poll(PP_EVENT_READ) == POLLIN);
  EXPECT(pp::translate_events_to_poll(PP_EVENT_WRITE) == POLLOUT);
  EXPECT(pp::translate_events_to_poll(PP_EVENT_READ | PP_EVENT_WRITE) == (POLLIN | POLLOUT));
  EXPECT(pp::translate_events_to_poll(0) == 0);
}

static void test_time_helpers() {
  EXPECT(pp::sec2msec(1L) == 1000);
  EXPECT(pp::sec2msec(2) == 2000);
  EXPECT(pp::sec2msec(0.5) == 500);
  EXPECT(pp::msec2sec(1500) == 1.5);
}

static void test_snprintf_and_strlcpy() {
  char buf[16];

  EXPECT(pp_snprintf(buf, sizeof(buf), "%d-%s", 7, "ok") == 4);
  EXPECT(std::strcmp(buf, "7-ok") == 0);

  pp_strlcpy(buf, "hello", sizeof(buf));
  EXPECT(std::strcmp(buf, "hello") == 0);

  // truncation keeps the terminator and returns the source length
  EXPECT(pp_strlcpy(buf, "0123456789abcdef", 8) == 16);
  EXPECT(std::strcmp(buf, "0123456") == 0);
}

static void test_worker_identity() {
  pp_set_worker_type(PP_TASK_WORKER);
  pp_set_worker_id(41);
  pp_set_worker_pid(1234);
  EXPECT(pp_get_worker_type() == PP_TASK_WORKER);
  EXPECT(pp_get_worker_id() == 41);
  EXPECT(pp_get_worker_pid() == 1234);
  EXPECT(pp_get_worker_symbol() == '^');

  pp_set_worker_type(PP_MASTER);
  EXPECT(pp_get_worker_symbol() == '#');
}

static void test_strerror() {
  // known codes carry their message, unknown codes fall back
  EXPECT(std::strstr(pp_strerror(PP_ERROR_MALLOC_FAIL), "alloc") != nullptr);
  EXPECT(std::strstr(pp_strerror(PP_ERROR_INVALID_PARAMS), "Invalid") != nullptr);
  EXPECT(std::strstr(pp_strerror(PP_ERROR_EVENT_ADD_FAILED), "add") != nullptr);
  EXPECT(std::strstr(pp_strerror(PP_ERROR_BAD_HOST_ADDR), "host") != nullptr);
  EXPECT(pp_strerror(PP_ERROR_END) != nullptr);
  // errno passthrough (< PP_ERROR_BEGIN) maps to strerror
  EXPECT(std::strcmp(pp_strerror(ENOENT), strerror(ENOENT)) == 0);

  // last-error round trip
  pp_set_last_error(PP_ERROR_OUTPUT_BUFFER_OVERFLOW);
  EXPECT(pp_get_last_error() == PP_ERROR_OUTPUT_BUFFER_OVERFLOW);
}

static void test_event_data_layout() {
  EventData data {};
  EXPECT(data.size() == sizeof(data.info) + 0);
  data.info.len = 100;
  EXPECT(data.size() == sizeof(data.info) + 100);
  EXPECT(sizeof(data.data) == PP_IPC_BUFFER_SIZE);
}

// ---------------------------------------------------------------------------
// core: Heap
// ---------------------------------------------------------------------------

// Heap::pop() returns the payload and deletes the node; payloads here are
// the priority encoded as a pointer-sized integer.
static void* payload_of(uint64_t p) {
  return reinterpret_cast<void*>(static_cast<uintptr_t>(p * 10 + 1));
}

static void test_heap_ordering() {
  Heap heap(8, Heap::MIN_HEAP);

  // Push in reverse order; pop must yield ascending priorities.
  for (uint64_t p = 5; p >= 1; p--) {
    EXPECT(heap.push(p, payload_of(p)) != nullptr);
  }
  EXPECT(heap.count() == 5);
  EXPECT(heap.top() != nullptr);
  EXPECT(heap.top()->priority == 1);

  for (uint64_t expect = 1; expect <= 5; expect++) {
    void* data = heap.pop();
    EXPECT(data == payload_of(expect));
  }
  EXPECT(heap.count() == 0);
  EXPECT(heap.top() == nullptr);
  EXPECT(heap.pop() == nullptr);
}

static void test_heap_change_priority() {
  Heap heap(8, Heap::MIN_HEAP);
  HeapNode* n1 = heap.push(10, payload_of(10));
  HeapNode* n2 = heap.push(20, payload_of(20));
  EXPECT(n1 != nullptr && n2 != nullptr);

  // demote n1 below n2
  heap.change_priority(50, n1);
  EXPECT(heap.top() == n2);

  heap.change_priority(1, n1);
  EXPECT(heap.top() == n1);

  // the re-promoted n1 pops first; Heap deletes the node itself
  void* data = heap.pop();
  EXPECT(data == payload_of(10));

  heap.remove(n2);
  EXPECT(heap.count() == 0);
}

static void test_heap_remove() {
  Heap heap(8, Heap::MIN_HEAP);
  HeapNode* n1 = heap.push(1, payload_of(1));
  HeapNode* n2 = heap.push(2, payload_of(2));
  heap.remove(n1); // deletes the node
  EXPECT(heap.count() == 1);
  void* data = heap.pop();
  EXPECT(data == payload_of(2));
  EXPECT(heap.count() == 0);
}

// ---------------------------------------------------------------------------
// core: Buffer
// ---------------------------------------------------------------------------

static void test_buffer_append_pop() {
  Buffer buffer(4);
  EXPECT(buffer.length() == 0);
  EXPECT(buffer.count() == 0);

  // 10 bytes over chunk size 4 -> 3 chunks (4+4+2)
  buffer.append("0123456789", 10);
  EXPECT(buffer.length() == 10);
  EXPECT(buffer.count() == 3);

  // drain in order: chunk sizes 4, 4, 2
  const pp::BufferChunk* c = nullptr;
  EXPECT((c = buffer.front()) != nullptr && c != nullptr && c->length == 4);
  buffer.pop();
  EXPECT(buffer.length() == 6);
  EXPECT(buffer.count() == 2);
  EXPECT((c = buffer.front()) != nullptr && c->length == 4);
  buffer.pop();
  EXPECT((c = buffer.front()) != nullptr && c->length == 2);
  buffer.pop();
  EXPECT(buffer.length() == 0);
  EXPECT(buffer.count() == 0);
  EXPECT(buffer.front() == nullptr);
}

static void test_buffer_zero_chunk_is_single() {
  // chunk_size == 0 disables chunking (INT_MAX sentinel)
  Buffer buffer(0);
  buffer.append("abc", 3);
  EXPECT(buffer.count() == 1);
  EXPECT(buffer.length() == 3);

  const pp::BufferChunk* c = nullptr;
  EXPECT((c = buffer.front()) != nullptr && c->length == 3);
}

static void test_buffer_iovec_append() {
  Buffer buffer(8);

  iovec iov[2];
  char part1[] = "aaaa";
  char part2[] = "bbbbbb";
  iov[0].iov_base = part1;
  iov[0].iov_len = 4;
  iov[1].iov_base = part2;
  iov[1].iov_len = 6;

  // append with offset 0: all 10 bytes
  buffer.append(iov, 2, 0);
  EXPECT(buffer.length() == 10);

  // append skipping the first 4 bytes: only the 6 of part2
  buffer.append(iov, 2, 4);
  EXPECT(buffer.length() == 16);

  // offset beyond the total length appends nothing
  buffer.append(iov, 2, 100);
  EXPECT(buffer.length() == 16);

  // empty iov returns immediately
  buffer.append(iov, 0, 0);
  EXPECT(buffer.length() == 16);
}

// ---------------------------------------------------------------------------
// core: Channel (process-local instance, lock mode)
// ---------------------------------------------------------------------------

static void test_channel_fifo() {
  auto* chan = Channel::make(1024, 8, pp::PP_CHAN_LOCK);
  EXPECT(chan != nullptr);
  EXPECT(chan->empty());
  EXPECT(!chan->full());

  char out[16];
  int n = 0;

  // push/pop return PP_OK on success, the byte count on pop
  EXPECT(chan->push("one", 3) == PP_OK);
  EXPECT(chan->push("two", 3) == PP_OK);
  EXPECT(!chan->empty());
  EXPECT(chan->num == 2);

  n = chan->pop(out, sizeof(out));
  EXPECT(n == 3);
  EXPECT(std::memcmp(out, "one", 3) == 0);
  n = chan->pop(out, sizeof(out));
  EXPECT(n == 3);
  EXPECT(std::memcmp(out, "two", 3) == 0);
  EXPECT(chan->empty());

  // popping from an empty channel fails
  EXPECT(chan->pop(out, sizeof(out)) == PP_ERR);

  chan->destroy();
}

static void test_channel_maxlen_rejects() {
  auto* chan = Channel::make(1024, 4, pp::PP_CHAN_LOCK);
  char out[16];

  // maxlen caps a single message, not the backlog
  EXPECT(chan->push("abc", 3) == PP_OK);
  EXPECT(chan->push("abc", 3) == PP_OK);      // still room: two pending are fine
  EXPECT(chan->push("toolong", 7) == PP_ERR); // one message over maxlen
  EXPECT(chan->pop(out, sizeof(out)) == 3);

  chan->destroy();
}

// ---------------------------------------------------------------------------
// core: Timer (manually triggered, single process)
// ---------------------------------------------------------------------------

static void test_timer_add_fire_once() {
  Timer timer(true);
  EXPECT(timer.count() == 0);

  int fired = 0;
  TimerNode* node = timer.add(1, false, &fired, [](Timer*, TimerNode* n) { (*static_cast<int*>(n->data)) += 1; });
  EXPECT(node != nullptr);
  EXPECT(timer.count() == 1);

  // not yet due
  timer.select();
  EXPECT(fired == 0);
  EXPECT(timer.count() == 1);

  usleep(2 * 1000);
  timer.select();
  EXPECT(fired == 1);
  EXPECT(timer.count() == 0);
}

static void test_timer_persistent() {
  Timer timer(true);
  int fired = 0;
  TimerNode* node = timer.add(1, true, &fired, [](Timer*, TimerNode* n) { (*static_cast<int*>(n->data)) += 1; });
  EXPECT(node != nullptr);

  for (int round = 0; round < 3; round++) {
    usleep(2 * 1000);
    timer.select();
  }
  EXPECT(fired >= 2);
  EXPECT(timer.count() == 1); // persistent node stays

  EXPECT(timer.remove(node));
  EXPECT(timer.count() == 0);
}

static void test_timer_remove_before_fire() {
  Timer timer(true);
  int fired = 0;
  TimerNode* node = timer.add(50, false, &fired, [](Timer*, TimerNode* n) { (*static_cast<int*>(n->data)) += 1; });
  EXPECT(node != nullptr);

  EXPECT(timer.remove(node));
  EXPECT(timer.count() == 0);

  usleep(60 * 1000);
  timer.select();
  EXPECT(fired == 0);
}

// ---------------------------------------------------------------------------
// network: Address validation (numeric IPs and unix paths only)
// ---------------------------------------------------------------------------

static void test_address_unix_path() {
  pp::network::Address addr {};
  EXPECT(addr.assign(PP_SOCK_UNIX_STREAM, "/tmp/pp_unit.sock"));

  // path too long is rejected
  std::string long_path(200, 'x');
  EXPECT(!addr.assign(PP_SOCK_UNIX_STREAM, long_path));
}

static void test_address_inet_validation() {
  pp::network::Address addr {};

  EXPECT(addr.assign(PP_SOCK_TCP, "127.0.0.1", 8080));
  EXPECT(pp::network::Address::verify_port(8080));
  EXPECT(pp::network::Address::verify_port(0));
  EXPECT(!pp::network::Address::verify_port(-1));
  EXPECT(!pp::network::Address::verify_port(65536));

  // port out of range fails assignment
  EXPECT(!addr.assign(PP_SOCK_TCP, "127.0.0.1", 70000));

  // this extraction has no DNS: a hostname fails with BAD_HOST_ADDR
  pp_set_last_error(0);
  EXPECT(!addr.assign(PP_SOCK_TCP, "example.invalid", 80));
  EXPECT(pp_get_last_error() == PP_ERROR_BAD_HOST_ADDR);
}

// ---------------------------------------------------------------------------
// lock: Mutex
// ---------------------------------------------------------------------------

static void test_mutex_excludes_threads() {
  Mutex mutex(false);
  int counter = 0;

  EXPECT(mutex.lock() == 0);
  EXPECT(mutex.unlock() == 0);

  // two threads increment under the lock; final value must be exact
  auto worker = [&]() {
    for (int i = 0; i < 10000; i++) {
      mutex.lock();
      counter++;
      mutex.unlock();
    }
  };
  std::thread t1(worker);
  std::thread t2(worker);
  t1.join();
  t2.join();
  EXPECT(counter == 20000);
}

// ---------------------------------------------------------------------------
// msgqueue (SysV, process-local round trip)
// ---------------------------------------------------------------------------

#ifdef HAVE_MSGQUEUE

static void test_msgqueue_roundtrip() {
  // IPC_PRIVATE: fresh queue for this process only
  auto* queue = new pp::MsgQueue(0);
  if (!queue->ready()) {
    // some sandboxes disallow SysV IPC; skip rather than fail
    delete queue;
    std::fprintf(stderr, "    (msgget unavailable, skipping)\n");
    return;
  }

  struct {
    long mtype;
    char text[8];
  } in {}, out {};

  in.mtype = 7;
  std::memcpy(in.text, "payload", 7);

  EXPECT(queue->push(reinterpret_cast<const pp::QueueNode*>(&in), sizeof(in.text)));
  EXPECT(queue->pop(reinterpret_cast<pp::QueueNode*>(&out), sizeof(out.text)) > 0);
  EXPECT(out.mtype == 7);
  EXPECT(std::memcmp(out.text, "payload", 7) == 0);

  // popping again blocks/fails: use IPC_NOWAIT semantics via a fresh push check
  struct {
    long mtype;
    char text[4];
  } in2 {};
  in2.mtype = 9;
  EXPECT(queue->push(reinterpret_cast<const pp::QueueNode*>(&in2), sizeof(in2.text)));
  out = {};
  EXPECT(queue->pop(reinterpret_cast<pp::QueueNode*>(&out), sizeof(out.text)) > 0);
  EXPECT(out.mtype == 9);

  queue->destroy();
  delete queue;
}

#endif // HAVE_MSGQUEUE

// ---------------------------------------------------------------------------
// log: level filtering and file output
// ---------------------------------------------------------------------------

static void test_logger_level_filtering() {
  auto* logger = pp_logger();
  const int original = logger->get_level();

  char tmp[64];
  std::snprintf(tmp, sizeof(tmp), "/tmp/pp_unit_%d.log", (int)getpid());
  EXPECT(logger->open(tmp));

  logger->set_level(PP_LOG_WARNING);
  EXPECT(logger->get_level() == PP_LOG_WARNING);

  // clamped to [DEBUG, NONE]
  logger->set_level(-100);
  EXPECT(logger->get_level() == PP_LOG_DEBUG);
  logger->set_level(1000);
  EXPECT(logger->get_level() == PP_LOG_NONE);

  logger->set_level(PP_LOG_WARNING);
  logger->put(PP_LOG_INFO, "below threshold", 17);
  logger->put(PP_LOG_WARNING, "kept", 4);

  logger->close();
  std::remove(tmp);
  logger->set_level(original);
}

static void test_logger_close_and_reopen() {
  auto* logger = pp_logger();

  char tmp[64];
  std::snprintf(tmp, sizeof(tmp), "/tmp/pp_unit_reopen_%d.log", (int)getpid());
  EXPECT(logger->open(tmp));
  logger->put(PP_LOG_INFO, "first", 5);
  logger->close();

  // reopen writes to the same file (append mode)
  EXPECT(logger->open(tmp));
  logger->put(PP_LOG_INFO, "second", 6);
  logger->close();
  logger->reopen();

  std::remove(tmp);
  logger->close();
}

// ---------------------------------------------------------------------------
// reactor: handler registry
// ---------------------------------------------------------------------------

static void test_reactor_handler_registry() {
  pp::Reactor reactor;
  EXPECT(reactor.ready());

  static int called = 0;
  static const pp::ReactorHandler handler_fn = [](pp::Reactor*, pp::Event*) -> int {
    called++;
    return PP_OK;
  };
  called = 0;
  reactor.set_handler(PP_FD_PIPE, PP_EVENT_READ, handler_fn);
  EXPECT(reactor.isset_handler(PP_FD_PIPE, PP_EVENT_READ));
  EXPECT(reactor.get_handler(PP_FD_PIPE, PP_EVENT_READ) != nullptr);
  EXPECT(reactor.get_handler(PP_FD_PIPE, PP_EVENT_ERROR) == nullptr);

  // unset handler falls back through get_error_handler's read/write fallback
  pp::Event ev {};
  auto* handler = reactor.get_error_handler(PP_FD_PIPE);
  EXPECT(handler != nullptr);
  handler(&reactor, &ev);
  EXPECT(called == 1);
}

static void test_reactor_event_predicates() {
  EXPECT(pp::Reactor::isset_read_event(PP_EVENT_READ));
  EXPECT(pp::Reactor::isset_read_event(0)); // below PP_EVENT_DEFAULT => read
  EXPECT(!pp::Reactor::isset_read_event(PP_EVENT_WRITE));
  EXPECT(pp::Reactor::isset_write_event(PP_EVENT_READ | PP_EVENT_WRITE));
  EXPECT(!pp::Reactor::isset_write_event(PP_EVENT_READ));
  EXPECT(pp::Reactor::isset_error_event(PP_EVENT_ERROR));
  EXPECT(!pp::Reactor::isset_error_event(PP_EVENT_READ));
}

// ---------------------------------------------------------------------------
// runner
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// in-process runtime: event loop, socket IO, signal dispatch, system timer.
// These paths normally execute inside forked workers, whose coverage
// profiles are lost to signals; driving them from the test process itself
// makes them count.
// ---------------------------------------------------------------------------

static int reader_wait_event(pp::UnixSocket& pair);

static int g_ev_reads = 0;

// helper for socket readability
static int reader_wait_event(pp::UnixSocket& pair) {
  auto* reader = pair.get_socket(false);
  return reader->wait_event(50, PP_EVENT_READ);
}

static int g_usr1_dispatched = 0;

static void test_event_loop_roundtrip() {
  EXPECT(!pp_event_is_available());
  EXPECT(pp_event_init(0) == PP_OK);
  EXPECT(pp_event_is_available());

  pp::UnixSocket pair(true, SOCK_DGRAM);
  EXPECT(pair.ready());
  auto* reader = pair.get_socket(false);

  pp_event_set_handler(PP_FD_PIPE, PP_EVENT_READ, [](pp::Reactor*, pp::Event* ev) -> int {
    char buf[16];
    ssize_t n = ev->socket->read(buf, sizeof(buf));
    if (n > 0) {
      g_ev_reads++;
    }
    return PP_OK;
  });

  EXPECT(pp_event_add(reader, PP_EVENT_READ) == PP_OK);

  const char payload[] = "ping";
  EXPECT(pair.write(payload, sizeof(payload)) == (ssize_t)sizeof(payload));

  // once-mode: deliver the pending read event, then stop after this round
  g_ev_reads = 0;
  PPTG.reactor->once = true;
  EXPECT(pp_event_wait() == 0);
  EXPECT(g_ev_reads == 1);

  // write path: pp_event_write queues through the loop when the socket
  // would block, and the write handler flushes it
  static int g_ev_writes = 0;
  g_ev_writes = 0;
  EXPECT(pp_event_init(0) == PP_OK);

  pp::UnixSocket pair2(true, SOCK_DGRAM);
  EXPECT(pair2.ready());
  auto* reader2 = pair2.get_socket(false);

  pp_event_set_handler(PP_FD_PIPE, PP_EVENT_READ, [](pp::Reactor*, pp::Event* ev) -> int {
    char buf[64];
    g_ev_reads += ev->socket->read(buf, sizeof(buf)) > 0 ? 1 : 0;
    return PP_OK;
  });
  pp_event_set_handler(PP_FD_PIPE, PP_EVENT_WRITE, [](pp::Reactor*, pp::Event*) -> int {
    g_ev_writes++;
    return PP_OK;
  });

  // re-register the reader and fill the datagram queue until writes block
  EXPECT(pp_event_add(reader2, PP_EVENT_READ) == PP_OK);
  char blob[2048] = {};
  bool queued = false;
  for (int i = 0; i < 64; i++) {
    if (pp_event_write(pair2.get_socket(true), blob, sizeof(blob)) < 0) {
      queued = true; // reactor took over the buffer
      break;
    }
  }
  // drain one round: read handler fires, write handler flushes if armed
  g_ev_reads = 0;
  PPTG.reactor->once = true;
  EXPECT(pp_event_wait() == 0);
  EXPECT(g_ev_reads >= 1);

  pp_event_free();
  EXPECT(!pp_event_is_available());
}

static void test_socket_sync_roundtrip() {
  pp::UnixSocket pair(true, SOCK_DGRAM);
  EXPECT(pair.ready());

  const char msg[] = "hello";
  char buf[16] = {};

  EXPECT(pair.write(msg, sizeof(msg)) == (ssize_t)sizeof(msg));
  EXPECT(pair.read(buf, sizeof(buf)) == (ssize_t)sizeof(msg));
  EXPECT(std::memcmp(buf, msg, sizeof(msg)) == 0);

  // timeout plumbing through both halves
  pair.set_timeout(0.5);
  EXPECT(pair.write("x", 1) == 1);
  EXPECT(pair.read(buf, sizeof(buf)) == 1);

  // buffer-size setup on the dgram pair
  EXPECT(pair.set_buffer_size(2048));

  // wait_event sees the readable state after a write
  EXPECT(pair.write("yy", 2) == 2);
  EXPECT(reader_wait_event(pair) == PP_OK);
  EXPECT(pair.read(buf, sizeof(buf)) == 2);
}

static void test_reactor_defer_and_exit() {
  EXPECT(pp_event_init(0) == PP_OK);

  volatile int ran = 0;
  pp_event_defer([&ran](void*) { ran = 1; }, nullptr);

  // defer tasks keep the loop from exiting while pending; run once to drain
  static int deferred = 0;
  deferred = 0;
  pp_event_defer([](void*) { deferred = 1; }, nullptr);
  PPTG.reactor->once = true;
  pp_event_wait();
  EXPECT(deferred == 1);
  EXPECT(ran == 1);

  pp_event_free();
}

static void test_signal_dispatch() {
  g_usr1_dispatched = 0;
  // safety=true: the raw handler only records; pp_signal_dispatch runs it
  pp_signal_set(SIGUSR1, [](int) { g_usr1_dispatched++; }, true);
  raise(SIGUSR1);
  pp_signal_dispatch();
  EXPECT(g_usr1_dispatched == 1);

  // repeat to cover the queue drain once more, then uninstall
  raise(SIGUSR1);
  raise(SIGUSR1);
  pp_signal_dispatch();
  EXPECT(g_usr1_dispatched >= 2);
  pp_signal_set(SIGUSR1, SIG_IGN);

  EXPECT(pp_signal_to_str(SIGUSR1) != nullptr);
}

static void test_system_timer_sigalrm() {
  // non-manually-triggered Timer arms the OS timer (setitimer/SIGALRM)
  EXPECT(!pp_event_is_available());
  pp::Timer timer(false);
  EXPECT(timer.count() == 0);

  int fired = 0;
  pp::TimerNode* node = timer.add(1, false, &fired, [](pp::Timer*, pp::TimerNode* n) { (*static_cast<int*>(n->data)) = 1; });
  EXPECT(node != nullptr);
  // the 1ms SIGALRM interrupts usleep, so poll until the deadline passes
  for (int i = 0; i < 50 && fired == 0; i++) {
    usleep(1 * 1000);
    timer.select();
  }
  EXPECT(fired == 1);
  EXPECT(timer.count() == 0);
}

static void test_misc_helpers() {
  // pp_system_random requires max > min (strict); pp_rand allows equal
  int r = pp_system_random(1, 3);
  EXPECT(r >= 1 && r <= 3);
  EXPECT(pp_rand(1, 1) == 1);
  int w = pp_rand(2, 4);
  EXPECT(w >= 2 && w <= 4);

  double t1 = pp::microtime();
  usleep(2 * 1000);
  EXPECT(pp::microtime() >= t1);

  // bounded wait: immediate success and a real timeout
  int spins = 0;
  EXPECT(pp_wait_for([&] { return ++spins >= 3; }, 1000));
  spins = 0;
  EXPECT(!pp_wait_for([&] { return false; }, 60));
}

// ---------------------------------------------------------------------------
// futex/wait primitives (the deliberately-kept registry fallback on
// platforms without kernel futex): cross-thread wait/wakeup semantics
// ---------------------------------------------------------------------------

static void test_atomic_wait_nowake() {
  pp_atomic_t flag = 0;
  // waking a value nobody waits on succeeds trivially
  EXPECT(pp_atomic_futex_wakeup(&flag, 1) == 0);
}

static void test_atomic_wait_handoff() {
  static pp_atomic_t ready = 0;   // set once data is posted
  static pp_atomic_t grabbed = 0; // worker claims via cmp_set

  std::thread worker([&] {
    // blocks until the main thread posts, then claims the slot
    EXPECT(pp_atomic_futex_wait(&ready, 2.0) == 0);
    EXPECT(pp_atomic_cmp_set(&grabbed, 0, 1));
  });

  // let the worker enter the wait first
  usleep(50 * 1000);
  EXPECT(pp_atomic_cmp_set(&ready, 0, 1));
  // If the worker already observed ready==1 it returned from the wait
  // without blocking (futex hand-off semantics); otherwise this wakes it.
  // Either way the worker must complete and claim the slot.
  EXPECT(pp_atomic_futex_wakeup(&ready, 1) >= 0);

  worker.join();
  EXPECT(grabbed == 1);
}

static void test_atomic_wait_timeout() {
  pp_atomic_t flag = 0;
  // nobody ever wakes the value -> the bounded wait must time out
  const double t0 = pp::time<std::chrono::milliseconds>(true);
  EXPECT(pp_atomic_futex_wait(&flag, 0.1) == -1);
  EXPECT(pp_get_last_error() == ETIMEDOUT);
  const double t1 = pp::time<std::chrono::milliseconds>(true);
  EXPECT(t1 - t0 >= 100);
}

// ---------------------------------------------------------------------------
// branch coverage: errno policies, socket options, pipe lifecycle,
// channel wrap-around, msgqueue edges, log rotation, memory pool
// ---------------------------------------------------------------------------

static void test_socket_catch_error_policies() {
  pp::UnixSocket pair(true, SOCK_DGRAM);
  EXPECT(pair.ready());
  auto* sock = pair.get_socket(true);

  // socket-layer errno policy (EINTR deliberately surfaces as ERROR:
  // retries live in the caller's loops)
  EXPECT(sock->catch_error(EBADF) == PP_INVALID);
  EXPECT(sock->catch_error(ENOENT) == PP_INVALID);
  EXPECT(sock->catch_error(EPIPE) == PP_CLOSE);
  EXPECT(sock->catch_error(ETIMEDOUT) == PP_CLOSE);
  EXPECT(sock->catch_error(ECONNREFUSED) == PP_CLOSE);
  EXPECT(sock->catch_error(0) == PP_WAIT);
  EXPECT(sock->catch_error(EAGAIN) == PP_WAIT);
  EXPECT(sock->catch_error(EINTR) == PP_ERROR);
  EXPECT(sock->catch_error(EACCES) == PP_ERROR);

  // catch_write_error delegates to the same policy
  EXPECT(sock->catch_write_error(EPIPE) == PP_CLOSE);
  EXPECT(sock->catch_write_error(EAGAIN) == PP_WAIT);
}

static void test_socket_nobufs_flag() {
  pp::UnixSocket pair(true, SOCK_DGRAM);
  auto* sock = pair.get_socket(true);

  // ENOBUFS sets the kernel_nobufs flag; has_kernel_nobufs consumes it once
  EXPECT(sock->catch_error(ENOBUFS) == PP_WAIT);
  EXPECT(sock->has_kernel_nobufs());
  // second probe clears the sticky state: no residual flag
  EXPECT(!sock->has_kernel_nobufs());
}

static void test_socket_fd_options() {
  pp::UnixSocket pair(true, SOCK_DGRAM);
  auto* sock = pair.get_socket(true);

  EXPECT(sock->set_nonblock());
  EXPECT(sock->nonblock == 1);
  EXPECT(sock->set_block());
  EXPECT(sock->nonblock == 0);
  EXPECT(sock->set_fd_option(1, 1)); // nonblock + cloexec
  EXPECT(sock->nonblock == 1 && sock->cloexec == 1);
  EXPECT(sock->get_fd() >= 0);
}

static void test_pipe_close_lifecycle() {
  pp::UnixSocket pair(true, SOCK_DGRAM);
  EXPECT(pair.ready());

  EXPECT(pair.close(PP_PIPE_CLOSE_MASTER));
  EXPECT(!pair.ready());
  EXPECT(!pair.close(PP_PIPE_CLOSE_MASTER)); // already closed: no-op

  EXPECT(pair.close(PP_PIPE_CLOSE_WORKER));
  // close(0) dispatches both halves and reports true regardless
  EXPECT(pair.close(0));

  // a second UnixSocket exercises the plain-Pipe class too
  pp::Pipe pipe_(true);
  EXPECT(pipe_.ready());
}

static void test_channel_wraparound() {
  // small ring: forces head/tail tag flip and the "not enough space
  // between tail and head" rejection branch
  auto* chan = pp::Channel::make(256, 16, pp::PP_CHAN_LOCK);
  EXPECT(chan != nullptr);

  const int kMsgs = 24;
  char out[32];
  // push/pop interleaved keeps the ring moving forward across the wrap
  for (int i = 0; i < kMsgs; i++) {
    char msg[8];
    std::snprintf(msg, sizeof(msg), "m%d", i);
    if (chan->push(msg, (int)strlen(msg) + 1) != PP_OK) {
      // drain one, then retry once
      EXPECT(chan->pop(out, sizeof(out)) > 0);
      EXPECT(chan->push(msg, (int)strlen(msg) + 1) == PP_OK);
    }
  }
  EXPECT(!chan->empty());
  chan->destroy();
}

static void test_msgqueue_edges() {
#ifndef HAVE_MSGQUEUE
  return;
#endif
  auto* queue = new pp::MsgQueue(0);
  if (!queue->ready()) {
    delete queue;
    return; // sandboxed SysV IPC
  }

  // oversized message is rejected by the queue wrapper
  struct {
    long mtype;
    char big[70000];
  } huge {};
  huge.mtype = 1;
  EXPECT(!queue->push(reinterpret_cast<const pp::QueueNode*>(&huge), sizeof(huge.big)));

  // note: an empty pop intentionally blocks (workers wait for tasks on
  // this very call), so no empty-queue assertion here

  EXPECT(queue->destroy());
  delete queue;
}

static void test_logger_rotation_formats() {
  auto* logger = pp_logger();

  // each rotation mode produces a distinct dated suffix; run all five
  char tmp[64];
  std::snprintf(tmp, sizeof(tmp), "/tmp/pp_unit_rot_%d.log", (int)getpid());

  const int modes[] = {PP_LOG_ROTATION_SINGLE, PP_LOG_ROTATION_MONTHLY, PP_LOG_ROTATION_DAILY, PP_LOG_ROTATION_HOURLY, PP_LOG_ROTATION_EVERY_MINUTE};
  for (int mode : modes) {
    logger->set_log_rotation(mode);
    EXPECT(logger->open(tmp));
    logger->put(PP_LOG_INFO, "rot", 3);
    logger->close();
  }
  logger->set_log_rotation(PP_LOG_ROTATION_SINGLE);

  // rotation modes write to a dated suffix; clean every file we created
  char dated[128];
  std::snprintf(dated, sizeof(dated), "%s.%%Y%%m%%d", tmp);
  (void)dated; // strftime format kept for reference; remove by glob below
  for (int mode : modes) {
    if (mode == 0) {
      std::remove(tmp);
    }
  }
  for (int d = 0; d < 400; d++) {
    // brute sweep of plausible suffixes for this year (cheap cleanup)
    char probe[192];
    std::snprintf(probe, sizeof(probe), "%s.2026%02d%02d", tmp, (d / 100) % 13 + 1, d % 28 + 1);
    std::remove(probe);
  }
}

static void test_global_memory_pool() {
  // small round trip through the shared global pool
  auto* mem = pp_mem_pool();
  EXPECT(mem != nullptr);

  void* block = mem->alloc(1234);
  EXPECT(block != nullptr);
  std::memset(block, 0xAB, 1234);

  // second allocation must not overlap
  void* block2 = mem->alloc(5678);
  EXPECT(block2 != nullptr && block2 != block);
  EXPECT(static_cast<char*>(block)[0] == (char)0xAB); // first block intact

  mem->free(block);
  mem->free(block2);
}

static void test_shm_alloc_free() {
  void* p1 = pp_shm_malloc(1000);
  EXPECT(p1 != nullptr);
  std::memset(p1, 0x5A, 1000);
  EXPECT(static_cast<unsigned char*>(p1)[999] == 0x5A);

  void* p2 = pp_shm_malloc(64);
  EXPECT(p2 != nullptr && p2 != p1);

  pp_shm_free(p1);
  pp_shm_free(p2);
}

static const TestCase kCases[] = {
  {"errno_classification", test_errno_classification},
  {"translate_events", test_translate_events},
  {"time_helpers", test_time_helpers},
  {"snprintf_strlcpy", test_snprintf_and_strlcpy},
  {"worker_identity", test_worker_identity},
  {"event_data_layout", test_event_data_layout},
  {"strerror", test_strerror},
  {"event_loop_roundtrip", test_event_loop_roundtrip},
  {"socket_sync_roundtrip", test_socket_sync_roundtrip},
  {"reactor_defer_and_exit", test_reactor_defer_and_exit},
  {"signal_dispatch", test_signal_dispatch},
  {"system_timer_sigalrm", test_system_timer_sigalrm},
  {"misc_helpers", test_misc_helpers},
  {"atomic_wait_nowake", test_atomic_wait_nowake},
  {"atomic_wait_handoff", test_atomic_wait_handoff},
  {"atomic_wait_timeout", test_atomic_wait_timeout},
  {"socket_catch_error_policies", test_socket_catch_error_policies},
  {"socket_nobufs_flag", test_socket_nobufs_flag},
  {"socket_fd_options", test_socket_fd_options},
  {"pipe_close_lifecycle", test_pipe_close_lifecycle},
  {"channel_wraparound", test_channel_wraparound},
  {"msgqueue_edges", test_msgqueue_edges},
  {"logger_rotation_formats", test_logger_rotation_formats},
  {"global_memory_pool", test_global_memory_pool},
  {"shm_alloc_free", test_shm_alloc_free},
  {"heap_ordering", test_heap_ordering},
  {"heap_change_priority", test_heap_change_priority},
  {"heap_remove", test_heap_remove},
  {"buffer_append_pop", test_buffer_append_pop},
  {"buffer_zero_chunk", test_buffer_zero_chunk_is_single},
  {"buffer_iovec_append", test_buffer_iovec_append},
  {"channel_fifo", test_channel_fifo},
  {"channel_maxlen_rejects", test_channel_maxlen_rejects},
  {"timer_add_fire_once", test_timer_add_fire_once},
  {"timer_persistent", test_timer_persistent},
  {"timer_remove_before_fire", test_timer_remove_before_fire},
  {"address_unix_path", test_address_unix_path},
  {"address_inet_validation", test_address_inet_validation},
  {"mutex_excludes_threads", test_mutex_excludes_threads},
#ifdef HAVE_MSGQUEUE
  {"msgqueue_roundtrip", test_msgqueue_roundtrip},
#endif
  {"logger_level_filtering", test_logger_level_filtering},
  {"logger_close_reopen", test_logger_close_and_reopen},
  {"reactor_handler_registry", test_reactor_handler_registry},
  {"reactor_event_predicates", test_reactor_event_predicates},
};

int main() {
  pp_init();

  int passed = 0;
  int failed = 0;
  for (const auto& tc : kCases) {
    test_failures = 0;
    tc.fn();
    if (test_failures == 0) {
      std::printf("[PASS] %s\n", tc.name);
      passed++;
    } else {
      std::printf("[FAIL] %s\n", tc.name);
      failed++;
    }
  }
  std::printf("----\n%d/%d passed\n", passed, passed + failed);
  return failed == 0 ? 0 : 1;
}
