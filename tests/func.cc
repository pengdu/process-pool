/*
  Functional tests for the standalone process-pool library.

  Each TEST runs in a freshly forked child so pool state, signal handlers and
  PPWG globals never leak between cases; the parent only collects exit
  codes. Worker-side observations are written to a shared-memory counter
  block, so every assertion checks facts that really crossed the process
  boundary.

  Covered surface (the API retained by the extraction):
    create/start/dispatch/dispatch_sync/shutdown/wait/destroy/reload,
    PP_IPC_UNIXSOCK + PP_IPC_MSGQUEUE, max_request worker recycling,
    crash respawn (onWorkerError), push_message/onWorkerMessage,
    round-robin scheduling, and negative paths.
*/

#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <set>
#include <string>

#include "pp.h"
#include "pp_process_pool.h"
#include "pp_signal.h"
#include "pp_timer.h"

using pp::EventData;
using pp::ExitStatus;
using pp::ProcessPool;
using pp::Timer;
using pp::TimerNode;
using pp::Worker;

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
  int (*fn)();
};

// Shared-memory counters, inherited by the pool's workers after fork.
static int* g_counters = nullptr;
static const int kCounterSlots = 64;
static ProcessPool* g_pool = nullptr;

static const int16_t kAsyncMsgType = 321; // marker written into DataHead.reactor_id

static void counter_incr(int slot, int v = 1) {
  __sync_fetch_and_add(&g_counters[slot], v);
}

static int counter_get(int slot) {
  return g_counters[slot];
}

// ---------------------------------------------------------------------------
// 1. unixsock: dispatch() spreads tasks across every worker
// ---------------------------------------------------------------------------

static int test_unixsock_scheduling() {
  const uint32_t worker_num = 3;
  const int jobs = 12;

  ProcessPool pool {};
  g_pool = &pool;
  EXPECT(pool.create(worker_num, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);

  pool.onTask = [](ProcessPool*, Worker* worker, EventData*) -> int {
    counter_incr(worker->id); // slot[worker_id] = tasks handled there
    worker->add_request_count();
    return 0;
  };

  EXPECT(pool.start() == PP_OK);

  int delivered = 0;
  for (int i = 0; i < jobs; i++) {
    EventData data {};
    data.info.len = sizeof(int);
    std::memcpy(data.data, &i, sizeof(int));
    int dst = -1; // let the pool schedule
    if (pool.dispatch(&data, &dst) == PP_OK) {
      delivered++;
      EXPECT(dst >= 0 && dst < (int)worker_num);
    } else {
      usleep(10 * 1000);
      --i;
    }
  }
  usleep(300 * 1000);

  pool.shutdown();
  EXPECT(pool.wait() == PP_OK);
  pool.destroy();

  int total = 0;
  int busy_workers = 0;
  for (uint32_t i = 0; i < worker_num; i++) {
    total += counter_get(i);
    if (counter_get(i) > 0) {
      busy_workers++;
    }
  }
  EXPECT(delivered == jobs);
  EXPECT(total == jobs);
  EXPECT(busy_workers >= 2); // round-robin must involve more than one worker
  return test_failures;
}

// ---------------------------------------------------------------------------
// 2. msgqueue IPC: dispatch_sync over a SysV message queue, targeted worker
// ---------------------------------------------------------------------------

static int test_msgqueue_ipc() {
  const uint32_t worker_num = 2;
  const int jobs = 6;
  const key_t key = 0x9500 + (getpid() & 0xff);

  ProcessPool pool {};
  g_pool = &pool;
  EXPECT(pool.create(worker_num, key, PP_IPC_MSGQUEUE) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);

  pool.onTask = [](ProcessPool*, Worker* worker, EventData* task) -> int {
    int value = 0;
    std::memcpy(&value, task->data, sizeof(int));
    counter_incr(0);        // tasks done
    counter_incr(1, value); // payload sum
    counter_incr(10 + worker->id);
    worker->add_request_count();
    return 0;
  };

  EXPECT(pool.start() == PP_OK);

  int expected_sum = 0;
  for (int i = 1; i <= jobs; i++) {
    EventData data {};
    data.info.len = sizeof(int);
    std::memcpy(data.data, &i, sizeof(int));
    int dst = i % worker_num; // address both workers explicitly
    EXPECT(pool.dispatch_sync(&data, &dst) == PP_OK);
    expected_sum += i;
  }
  usleep(300 * 1000);

  pool.shutdown();
  EXPECT(pool.wait() == PP_OK);
  if (pool.queue) {
    pool.queue->destroy(); // remove the SysV queue from the system
  }
  pool.destroy();

  EXPECT(counter_get(0) == jobs);
  EXPECT(counter_get(1) == expected_sum);
  EXPECT(counter_get(10) > 0); // both addressed workers really ran tasks
  EXPECT(counter_get(11) > 0);
  return test_failures;
}

// ---------------------------------------------------------------------------
// 3. max_request: the worker exits after N tasks and wait() respawns it
// ---------------------------------------------------------------------------

static int test_max_request_recycle() {
  ProcessPool pool {};
  g_pool = &pool;
  EXPECT(pool.create(1, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);

  pool.onWorkerStart = [](ProcessPool*, Worker* worker) {
    // PPWG.max_request lives in the worker process, so it has to be
    // armed here (the pool-level field is not propagated by the pool).
    worker->set_max_request(2, 0);
    int gen = __sync_fetch_and_add(&g_counters[0], 1);
    if (gen < 8) {
      g_counters[8 + gen] = (int)getpid();
    }
  };
  pool.onTask = [](ProcessPool*, Worker* worker, EventData*) -> int {
    counter_incr(1);
    worker->add_request_count();
    return 0;
  };
  pool.onStart = [](ProcessPool* pool) {
    // 4 tasks with max_request=2 force at least one worker recycle
    for (int i = 0; i < 4; i++) {
      EventData data {};
      data.info.len = 1;
      data.data[0] = 'x';
      int dst = 0;
      pool->dispatch_sync(&data, &dst);
    }
    pp_timer_tick(50, [pool](Timer*, TimerNode*) {
      if (counter_get(1) >= 4) {
        pool->running = false;
      }
    });
    pp_timer_after(5000, [pool](Timer*, TimerNode*) { pool->running = false; });
  };

  EXPECT(pool.start() == PP_OK);
  EXPECT(pool.wait() == PP_OK);
  pool.destroy();

  EXPECT(counter_get(1) == 4); // all tasks processed
  EXPECT(counter_get(0) >= 2); // at least two worker generations
  EXPECT(g_counters[8] != 0);
  EXPECT(g_counters[9] != 0);
  EXPECT(g_counters[8] != g_counters[9]); // different pids => real respawn
  return test_failures;
}

// ---------------------------------------------------------------------------
// 4. reload: every worker is replaced, callbacks fire exactly once
// ---------------------------------------------------------------------------

static int test_reload() {
  const uint32_t worker_num = 2;

  ProcessPool pool {};
  g_pool = &pool;
  EXPECT(pool.create(worker_num, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);
  pool.max_wait_time = 1; // escalate to SIGKILL after 1s

  pool.onWorkerStart = [](ProcessPool* pool, Worker*) {
    int gen = __sync_fetch_and_add(&g_counters[0], 1);
    if (gen < 8) {
      g_counters[8 + gen] = (int)getpid();
    }
    if (gen < (int)pool->worker_num) {
      // first generation ignores SIGTERM: reload must escalate via
      // the ReloadTask timeout killer (SIGKILL)
      signal(SIGTERM, SIG_IGN);
    }
  };
  pool.onBeforeReload = [](ProcessPool*) { counter_incr(1); };
  pool.onAfterReload = [](ProcessPool* pool) {
    counter_incr(2);
    pp_timer_after(100, [pool](Timer*, TimerNode*) { pool->running = false; });
  };
  pool.onStart = [](ProcessPool* pool) {
    pp_timer_after(100, [pool](Timer*, TimerNode*) { EXPECT(pool->reload()); });
    pp_timer_after(150, [pool](Timer*, TimerNode*) {
      if (pool->reload_task) {
        EXPECT(!pool->reload()); // rejected while in flight
      }
    });
    pp_timer_after(10000, [pool](Timer*, TimerNode*) { pool->running = false; });
  };

  EXPECT(pool.start() == PP_OK);
  EXPECT(pool.wait() == PP_OK);
  pool.destroy();

  EXPECT(counter_get(1) == 1);
  EXPECT(counter_get(2) == 1);
  EXPECT(counter_get(0) >= (int)(2 * worker_num)); // both workers replaced

  std::set<int> first_gen;
  for (uint32_t i = 0; i < worker_num; i++) {
    first_gen.insert(g_counters[8 + i]);
  }
  for (uint32_t i = 0; i < worker_num; i++) {
    int pid = g_counters[8 + worker_num + i];
    EXPECT(pid != 0);
    EXPECT(first_gen.count(pid) == 0); // second generation has new pids
  }
  return test_failures;
}

// ---------------------------------------------------------------------------
// 5. crash respawn: abnormal exit triggers onWorkerError and a respawn
// ---------------------------------------------------------------------------

static int test_crash_respawn() {
  ProcessPool pool {};
  g_pool = &pool;
  EXPECT(pool.create(1, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);

  pool.onWorkerStart = [](ProcessPool*, Worker*) {
    int gen = __sync_fetch_and_add(&g_counters[0], 1);
    if (gen == 0) {
      raise(SIGKILL); // first generation dies abnormally
    }
  };
  pool.onWorkerError = [](ProcessPool*, Worker*, const ExitStatus& st) {
    counter_incr(1);
    EXPECT(!st.is_normal_exit());
    EXPECT(st.get_signal() == SIGKILL);
  };
  pool.onStart = [](ProcessPool* pool) {
    pp_timer_tick(50, [pool](Timer*, TimerNode*) {
      if (counter_get(0) >= 2) { // the respawned generation is alive
        pool->running = false;
      }
    });
    pp_timer_after(5000, [pool](Timer*, TimerNode*) { pool->running = false; });
  };

  EXPECT(pool.start() == PP_OK);
  EXPECT(pool.wait() == PP_OK);
  pool.destroy();

  EXPECT(counter_get(0) >= 2); // respawned
  EXPECT(counter_get(1) >= 1); // onWorkerError fired
  return test_failures;
}

// ---------------------------------------------------------------------------
// 5b. crash while processing a task: SIGSEGV is reported, the worker is
//     respawned, and the pool keeps serving subsequent tasks
// ---------------------------------------------------------------------------

static int test_crash_in_task() {
  ProcessPool pool {};
  g_pool = &pool;
  EXPECT(pool.create(1, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);

  // counters: 0 = worker generations, 1 = tasks completed,
  //           2 = onWorkerError count, 3 = follow-up dispatched (master flag)
  pool.onWorkerStart = [](ProcessPool*, Worker*) { counter_incr(0); };
  pool.onTask = [](ProcessPool*, Worker* worker, EventData* task) -> int {
    if (task->data[0] == 'C') {
      raise(SIGSEGV); // die mid-task, after the packet left the pipe
    }
    counter_incr(1);
    worker->add_request_count();
    return 0;
  };
  pool.onWorkerError = [](ProcessPool*, Worker*, const ExitStatus& st) {
    counter_incr(2);
    EXPECT(!st.is_normal_exit());
    EXPECT(st.get_signal() == SIGSEGV);
  };
  pool.onStart = [](ProcessPool* pool) {
    EventData data {};
    data.info.len = 1;
    data.data[0] = 'C';
    int dst = 0;
    pool->dispatch_sync(&data, &dst);

    pp_timer_tick(50, [pool](Timer*, TimerNode*) {
      // after the respawn, prove the pool still serves tasks
      if (counter_get(0) >= 2 && counter_get(3) == 0) {
        g_counters[3] = 1; // master-only flag: dispatch exactly once
        EventData data {};
        data.info.len = 1;
        data.data[0] = 'N';
        int dst = 0;
        pool->dispatch_sync(&data, &dst);
      }
      if (counter_get(1) >= 1) {
        pool->running = false;
      }
    });
    pp_timer_after(5000, [pool](Timer*, TimerNode*) { pool->running = false; });
  };

  EXPECT(pool.start() == PP_OK);
  EXPECT(pool.wait() == PP_OK);
  pool.destroy();

  EXPECT(counter_get(2) >= 1); // SIGSEGV surfaced via onWorkerError
  EXPECT(counter_get(0) >= 2); // worker respawned
  EXPECT(counter_get(1) == 1); // task after the crash was processed
  return test_failures;
}

// ---------------------------------------------------------------------------
// 5c. non-zero exit code: WIFEXITED is a "normal" exit, so the worker is
//     respawned silently -- onWorkerError must NOT fire
// ---------------------------------------------------------------------------

static int test_exit_nonzero_respawn() {
  ProcessPool pool {};
  g_pool = &pool;
  EXPECT(pool.create(1, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);

  pool.onWorkerStart = [](ProcessPool*, Worker*) {
    int gen = __sync_fetch_and_add(&g_counters[0], 1);
    if (gen == 0) {
      _exit(7); // first generation quits with a non-zero code
    }
  };
  pool.onWorkerError = [](ProcessPool*, Worker*, const ExitStatus&) { counter_incr(1); };
  pool.onStart = [](ProcessPool* pool) {
    pp_timer_tick(50, [pool](Timer*, TimerNode*) {
      if (counter_get(0) >= 2) {
        pool->running = false;
      }
    });
    pp_timer_after(5000, [pool](Timer*, TimerNode*) { pool->running = false; });
  };

  EXPECT(pool.start() == PP_OK);
  EXPECT(pool.wait() == PP_OK);
  pool.destroy();

  EXPECT(counter_get(0) >= 2); // respawned
  EXPECT(counter_get(1) == 0); // exit(7) is WIFEXITED: no onWorkerError
  return test_failures;
}

// ---------------------------------------------------------------------------
// 6. push_message / onWorkerMessage: worker -> master message box
// ---------------------------------------------------------------------------

static const char kMsg[] = "hello-master";

static int test_worker_message() {
  ProcessPool pool {};
  g_pool = &pool;
  EXPECT(pool.create(1, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);

  // push_message() signals the master with SIGIO; the handler is the
  // embedder's job, exactly like upstream usage.
  pp_signal_set(SIGIO, [](int) { g_pool->trigger_read_message_event(); });

  pool.onWorkerStart = [](ProcessPool* pool, Worker*) { pool->push_message(PP_WORKER_MESSAGE_STOP + 1, kMsg, sizeof(kMsg)); };
  pool.onWorkerMessage = [](ProcessPool* pool, EventData* msg) {
    if (msg->info.type == PP_WORKER_MESSAGE_STOP + 1 && msg->info.len == sizeof(kMsg) && std::memcmp(msg->data, kMsg, sizeof(kMsg)) == 0) {
      counter_incr(0);
    }
    pool->running = false;
  };
  pool.onStart = [](ProcessPool* pool) {
    // periodic SIGALRM guarantees the blocking waitpid() in wait() gets
    // interrupted even if SIGIO arrived before wait() started
    pp_timer_tick(50, [](Timer*, TimerNode*) {});
    pp_timer_after(5000, [pool](Timer*, TimerNode*) { pool->running = false; });
  };

  EXPECT(pool.start() == PP_OK);
  EXPECT(pool.wait() == PP_OK);
  pool.destroy();

  EXPECT(counter_get(0) == 1); // message content verified in the master
  return test_failures;
}

// ---------------------------------------------------------------------------
// 7. negative paths & small API guarantees
// ---------------------------------------------------------------------------

static int test_negative_paths() {
  {
    // PP_IPC_SOCKET support was physically removed from this build
    ProcessPool pool {};
    EXPECT(pool.create(1, 0, PP_IPC_SOCKET) == PP_ERR);
  }

  {
    ProcessPool pool {};
    EXPECT(pool.create(2, 0, PP_IPC_UNIXSOCK) == PP_OK);

    // stream-protocol stubs must report NOT_SUPPORT
    EXPECT(pool.dispatch_sync("x", 1) == PP_ERR);
    EXPECT(pp_get_last_error() == PP_ERROR_OPERATION_NOT_SUPPORT);
    EXPECT(pool.response("x", 1) == PP_ERR);

    // worker-id range helpers
    EXPECT(pool.is_worker_id_valid(0));
    EXPECT(pool.is_worker_id_valid(1));
    EXPECT(!pool.is_worker_id_valid(2));

    // oversized push_message must be rejected
    std::string big(sizeof(EventData::data) + 1, 'a');
    EXPECT(pool.push_message(9, big.data(), big.size()) == PP_ERR);

    // must not signal process-group 0 when nothing was spawned
    pool.kill_all_workers(SIGTERM);

    pool.destroy();
  }

  return test_failures;
}

// ---------------------------------------------------------------------------
// driver: run every case in a forked child
// ---------------------------------------------------------------------------

// async workers: each worker runs its own reactor and receives dispatched
// packets through the onMessage callback instead of the task-protocol loop.
static void on_async_message(ProcessPool* pool, pp::RecvData* msg) {
  if (msg->info.reactor_id == kAsyncMsgType && msg->info.len >= sizeof(int)) {
    int slot = 0;
    std::memcpy(&slot, msg->data, sizeof(int));
    counter_incr(20 + slot);
  }
}

static int test_async_worker() {
  ProcessPool pool {};
  EXPECT(pool.create(2, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_async(true);
  pool.onMessage = on_async_message;
  pool.set_protocol(PP_PROTOCOL_TASK);

  EXPECT(pool.start() == PP_OK);

  EventData data {};
  data.info.reactor_id = kAsyncMsgType;
  data.info.len = sizeof(int);
  int slot = 0;
  std::memcpy(data.data, &slot, sizeof(int));
  int dst = -1;
  EXPECT(pool.dispatch_sync(&data, &dst) == PP_OK);

  // poll until the async worker's reactor delivered the packet
  for (int i = 0; i < 200 && counter_get(20) == 0; i++) {
    usleep(10 * 1000);
  }
  EXPECT(counter_get(20) == 1);

  pool.shutdown();
  pool.wait();
  pool.destroy();
  g_pool = nullptr;
  return test_failures == 0 ? 0 : 1;
}

static const TestCase kCases[] = {
  {"unixsock_scheduling", test_unixsock_scheduling},
  {"msgqueue_ipc", test_msgqueue_ipc},
  {"max_request_recycle", test_max_request_recycle},
  {"reload", test_reload},
  {"crash_respawn", test_crash_respawn},
  {"crash_in_task", test_crash_in_task},
  {"exit_nonzero_respawn", test_exit_nonzero_respawn},
  {"worker_message", test_worker_message},
  {"negative_paths", test_negative_paths},
  {"async_worker", test_async_worker},
};

int main() {
  int passed = 0;
  int failed = 0;
  const int total = (int)(sizeof(kCases) / sizeof(kCases[0]));

  for (const TestCase& tc : kCases) {
    std::fflush(stdout);
    std::fflush(stderr);
    pid_t pid = fork();
    if (pid == 0) {
      pp_init();
      g_counters = static_cast<int*>(pp_shm_malloc(sizeof(int) * kCounterSlots));
      if (!g_counters) {
        std::fprintf(stderr, "shm alloc failed\n");
        _exit(120);
      }
      std::memset(g_counters, 0, sizeof(int) * kCounterSlots);
      int rc = tc.fn();
      _exit(rc == 0 ? 0 : 1);
    }

    int status = 0;
    waitpid(pid, &status, 0);
    const bool ok = WIFEXITED(status) && WEXITSTATUS(status) == 0;
    std::printf("[%s] %s\n", ok ? "PASS" : "FAIL", tc.name);
    if (WIFSIGNALED(status)) {
      std::printf("    terminated by signal %d\n", WTERMSIG(status));
    }
    ok ? passed++ : failed++;
  }

  std::printf("----\n%d/%d passed\n", passed, total);
  return failed == 0 ? 0 : 1;
}
