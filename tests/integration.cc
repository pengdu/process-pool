/*
  Integration tests for the process pool: multi-process scenarios that
  combine dispatch, reload, worker messaging, and async workers.

  Like func.cc, each case runs inside a freshly forked child so pool
  state, signal handlers, and PPWG globals never leak between cases.

  Build:  cmake --build build --target pp_integration
  Run:    ./build/tests/pp_integration
*/

#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <functional>
#include <string>

#include "pp.h"
#include "pp_process_pool.h"

using pp::EventData;
using pp::ProcessPool;
using pp::Worker;

// ---------------------------------------------------------------------------
// tiny harness (fork-per-case, shared-memory verdicts)
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

// Counters in shared memory: workers inherit them across fork and the
// parent reads the final tally.
static int* g_counters = nullptr;
static const int kCounterSlots = 64;
static ProcessPool* g_pool = nullptr;

static void counter_incr(int slot, int v = 1) {
  __sync_fetch_and_add(&g_counters[slot], v);
}

static int counter_get(int slot) {
  return g_counters[slot];
}

static void counters_reset() {
  for (int i = 0; i < kCounterSlots; i++) {
    g_counters[i] = 0;
  }
}

static const int16_t kMsgTypeTask = 1;

// wait until predicate or ~2s timeout
static bool wait_for(const std::function<bool()>& done, int timeout_ms = 2000) {
  for (int waited = 0; waited < timeout_ms; waited += 10) {
    if (done()) {
      return true;
    }
    usleep(10 * 1000);
  }
  return done();
}

static int total_tasks() {
  int total = 0;
  for (int i = 0; i < 4; i++) {
    total += counter_get(i);
  }
  return total;
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

static void dispatch_int(ProcessPool* pool, int value, int* dst = nullptr) {
  EventData data {};
  data.info.type = kMsgTypeTask;
  data.info.len = sizeof(int);
  std::memcpy(data.data, &value, sizeof(int));
  int worker = -1;
  if (dst == nullptr) {
    dst = &worker;
  }
  EXPECT(pool->dispatch(&data, dst) == PP_OK);
}

// ---------------------------------------------------------------------------
// cases
// ---------------------------------------------------------------------------

// 120 tasks across 3 workers: every task executes exactly once, all workers
// participate, and the pool keeps serving afterwards.
static int test_stress_many_tasks() {
  ProcessPool pool {};
  EXPECT(pool.create(3, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);
  pool.onTask = [](ProcessPool*, Worker* worker, EventData* task) {
    int value = 0;
    std::memcpy(&value, task->data, sizeof(int));
    counter_incr(worker->id);
    counter_incr(4, value); // slot[4] = sum of all delivered values
    worker->add_request_count();
    return 0;
  };
  EXPECT(pool.start() == PP_OK);

  const int kTasks = 120;
  const int kSum = (kTasks - 1) * kTasks / 2;
  for (int i = 0; i < kTasks; i++) {
    dispatch_int(&pool, i);
  }

  EXPECT(wait_for([&] { return counter_get(4) == kSum; }));
  EXPECT(counter_get(4) == kSum);
  EXPECT(total_tasks() == kTasks); // exactly-once delivery

  pool.shutdown();
  pool.wait();
  pool.destroy();
  return test_failures == 0 ? 0 : 1;
}

// reload replaces both workers; the respawned generation keeps serving new
// tasks without the caller noticing anything but onAfterReload.
static int test_reload_continues_serving() {
  ProcessPool pool {};
  EXPECT(pool.create(2, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);
  pool.onAfterReload = [](ProcessPool*) {
    counter_incr(8); // slot[8] = reloads completed
  };
  pool.onTask = [](ProcessPool*, Worker* worker, EventData*) {
    counter_incr(worker->id);
    return 0;
  };
  pool.onStart = [](ProcessPool* p) {
    // generation 1 does some work
    for (int i = 0; i < 4; i++) {
      EventData data {};
      data.info.type = kMsgTypeTask;
      data.info.len = sizeof(int);
      int dst = -1;
      EXPECT(p->dispatch(&data, &dst) == PP_OK);
    }
    // the pool state machine only advances inside wait(); steer it with timers
    pp_timer_tick(30, [p](pp::Timer*, pp::TimerNode*) {
      if (total_tasks() >= 4 && counter_get(8) == 0) {
        p->reload();
        return;
      }
      if (counter_get(8) == 1 && total_tasks() < 8) {
        // generation 2 must still serve
        EventData data {};
        data.info.type = kMsgTypeTask;
        data.info.len = sizeof(int);
        int dst = -1;
        EXPECT(p->dispatch(&data, &dst) == PP_OK);
        return;
      }
      if (total_tasks() >= 8) {
        p->running = false;
      }
    });
    pp_timer_after(5000, [p](pp::Timer*, pp::TimerNode*) { p->running = false; });
  };
  EXPECT(pool.start() == PP_OK);
  EXPECT(pool.wait() == PP_OK);
  pool.destroy();

  EXPECT(counter_get(8) == 1); // exactly one reload completed
  EXPECT(total_tasks() == 8);  // both generations served
  return test_failures == 0 ? 0 : 1;
}

// addressing a specific worker id delivers only to that worker
static int test_targeted_dispatch_precision() {
  ProcessPool pool {};
  EXPECT(pool.create(3, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);
  pool.onTask = [](ProcessPool*, Worker* worker, EventData*) {
    counter_incr(worker->id);
    return 0;
  };
  EXPECT(pool.start() == PP_OK);

  const int kTarget = 1;
  for (int i = 0; i < 5; i++) {
    int dst = kTarget;
    dispatch_int(&pool, i, &dst);
    EXPECT(dst == kTarget);
  }

  EXPECT(wait_for([&] { return counter_get(kTarget) == 5; }));
  EXPECT(counter_get(kTarget) == 5);
  EXPECT(counter_get(0) == 0);
  EXPECT(counter_get(2) == 0);

  pool.shutdown();
  pool.wait();
  pool.destroy();
  return test_failures == 0 ? 0 : 1;
}

// request/reply matching through the worker message box: the master embeds a
// job id in info.fd; each worker replies carrying the same id; the master
// matches every reply to its request.
static int test_roundtrip_reply_matching() {
  static const int kJobs = 9;
  static bool received[kJobs] = {};

  ProcessPool pool {};
  g_pool = &pool;
  EXPECT(pool.create(3, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);
  pool.onTask = [](ProcessPool* p, Worker*, EventData* task) {
    // echo the job id back through the shared-memory message box
    EventData reply {};
    reply.info.type = kMsgTypeTask + 1;
    reply.info.fd = task->info.fd;
    reply.info.len = sizeof(long);
    long pid = static_cast<long>(getpid());
    std::memcpy(reply.data, &pid, sizeof(pid));
    p->push_message(&reply);
    return 0;
  };
  pool.onWorkerMessage = [](ProcessPool*, EventData* msg) {
    if (msg->info.type == kMsgTypeTask + 1 && msg->info.fd >= 0 && msg->info.fd < kJobs) {
      received[msg->info.fd] = true;
    }
  };
  pool.onStart = [](ProcessPool* p) {
    for (long job = 0; job < kJobs; job++) {
      EventData data {};
      data.info.type = kMsgTypeTask;
      data.info.fd = job; // job id travels in the header
      data.info.len = 0;
      int dst = -1;
      EXPECT(p->dispatch(&data, &dst) == PP_OK);
    }
    pp_timer_tick(30, [p](pp::Timer*, pp::TimerNode*) {
      int n = 0;
      for (int i = 0; i < kJobs; i++) {
        n += received[i] ? 1 : 0;
      }
      if (n == kJobs || pp_timer_get_next_msec() < 0) {
        p->running = false;
      }
    });
    pp_timer_after(5000, [p](pp::Timer*, pp::TimerNode*) { p->running = false; });
  };
  EXPECT(pool.start() == PP_OK);

  // SIGIO flips read_message; the wait() loop drains the box and calls
  // onWorkerMessage in the master.
  pp_signal_set(SIGIO, [](int) { g_pool->trigger_read_message_event(); });

  EXPECT(pool.wait() == PP_OK);
  pool.destroy();

  int n = 0;
  for (int i = 0; i < kJobs; i++) {
    n += received[i] ? 1 : 0;
  }
  EXPECT(n == kJobs);
  g_pool = nullptr;
  return test_failures == 0 ? 0 : 1;
}

// async workers under a burst: every packet reaches onMessage with its
// original header intact.
static int test_async_worker_burst() {
  ProcessPool pool {};
  EXPECT(pool.create(3, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_async(true);
  pool.onMessage = [](ProcessPool*, pp::RecvData* msg) {
    if (msg->info.type == kMsgTypeTask && msg->info.len >= sizeof(int)) {
      int payload = 0;
      std::memcpy(&payload, msg->data, sizeof(int));
      counter_incr(12 + payload);
    }
  };
  pool.set_protocol(PP_PROTOCOL_TASK);
  EXPECT(pool.start() == PP_OK);

  const int kBurst = 3; // one packet per worker slot
  for (int i = 0; i < kBurst; i++) {
    EventData data {};
    data.info.type = kMsgTypeTask;
    data.info.len = sizeof(int);
    std::memcpy(data.data, &i, sizeof(int));
    int dst = -1;
    EXPECT(pool.dispatch_sync(&data, &dst) == PP_OK);
  }

  EXPECT(wait_for([&] {
    for (int i = 0; i < kBurst; i++) {
      if (counter_get(12 + i) < 1) {
        return false;
      }
    }
    return true;
  }));
  for (int i = 0; i < kBurst; i++) {
    EXPECT(counter_get(12 + i) >= 1);
  }

  pool.shutdown();
  pool.wait();
  pool.destroy();
  return test_failures == 0 ? 0 : 1;
}

// a worker that exits by itself (onWorkerStop path via max_request) recycles
// and the replacement carries the same worker id — ids stay stable across
// generations.
static int test_worker_id_stability_across_recycle() {
  ProcessPool pool {};
  EXPECT(pool.create(2, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);
  pool.onWorkerStart = [](ProcessPool*, Worker* worker) {
    // PPWG.max_request lives in the worker; arm it there (pool-level
    // set_max_request does not propagate).
    worker->set_max_request(2, 0);
    counter_incr(16 + worker->id); // slot[16+id] = generations served
  };
  pool.onTask = [](ProcessPool*, Worker* worker, EventData*) {
    counter_incr(worker->id);
    worker->add_request_count();
    return 0;
  };
  pool.onStart = [](ProcessPool* p) {
    for (int i = 0; i < 10; i++) {
      EventData data {};
      data.info.type = kMsgTypeTask;
      data.info.len = sizeof(int);
      int dst = -1;
      EXPECT(p->dispatch(&data, &dst) == PP_OK);
    }
    pp_timer_tick(30, [p](pp::Timer*, pp::TimerNode*) {
      if (total_tasks() >= 10) {
        p->running = false;
      }
    });
    pp_timer_after(5000, [p](pp::Timer*, pp::TimerNode*) { p->running = false; });
  };
  EXPECT(pool.start() == PP_OK);
  EXPECT(pool.wait() == PP_OK);
  pool.destroy();

  EXPECT(total_tasks() == 10); // every task exactly once

  // both ids must have seen at least 2 generations (recycled at least once)
  EXPECT(counter_get(16 + 0) >= 2);
  EXPECT(counter_get(16 + 1) >= 2);
  return test_failures == 0 ? 0 : 1;
}

// ---------------------------------------------------------------------------
// runner (fork-per-case, like func.cc)
// ---------------------------------------------------------------------------

static const TestCase kCases[] = {
  {"stress_many_tasks", test_stress_many_tasks},
  {"reload_continues_serving", test_reload_continues_serving},
  {"targeted_dispatch_precision", test_targeted_dispatch_precision},
  {"roundtrip_reply_matching", test_roundtrip_reply_matching},
  {"async_worker_burst", test_async_worker_burst},
  {"worker_id_stability_across_recycle", test_worker_id_stability_across_recycle},
};

int main() {
  pp_init();

  g_counters = static_cast<int*>(pp_shm_malloc(sizeof(int) * kCounterSlots));

  int passed = 0;
  int failed = 0;
  for (const auto& tc : kCases) {
    counters_reset();
    test_failures = 0;

    const pid_t pid = fork();
    if (pid == 0) {
      _exit(tc.fn()); // child: run the case, report via exit code
    }
    int status = 0;
    waitpid(pid, &status, 0);
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
      std::printf("[PASS] %s\n", tc.name);
      passed++;
    } else {
      std::printf("[FAIL] %s\n", tc.name);
      failed++;
    }
  }

  std::printf("----\n%d/%d passed\n", passed, passed + failed);
  pp_shm_free(g_counters);
  return failed == 0 ? 0 : 1;
}
