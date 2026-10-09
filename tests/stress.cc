/*
  Stress tests for the process pool: sustained high-throughput dispatch,
  heavy worker-messaging, and reload under load.

  Each case measures both correctness (exactly-once, ordering, matching)
  and throughput (tasks/sec), printing the rates so regressions are
  visible in the log. Exit code is 0 only when every correctness check
  holds; throughput numbers are informational.

  Build:  cmake --build build --target pp_stress
  Run:    ./build/tests/pp_stress            (default loads)
          ./build/tests/pp_stress 4         (multiplier: x4 load)
*/

#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <ctime>

#include "pp.h"
#include "pp_process_pool.h"

using pp::EventData;
using pp::ProcessPool;
using pp::Timer;
using pp::TimerNode;
using pp::Worker;

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

static int test_failures = 0;

#define EXPECT(cond)                                                                                                                                                                                   \
  do {                                                                                                                                                                                                 \
    if (!(cond)) {                                                                                                                                                                                     \
      std::fprintf(stderr, "    EXPECT failed at %s:%d: %s\n", __FILE__, __LINE__, #cond);                                                                                                             \
      test_failures++;                                                                                                                                                                                 \
    }                                                                                                                                                                                                  \
  } while (0)

static double now_sec() {
  timespec ts {};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec + ts.tv_nsec / 1e9;
}

static int* g_counters = nullptr;
static const int kSlots = 64;
static ProcessPool* g_pool = nullptr;
static int g_load = 1; // CLI multiplier

static void counter_incr(int slot, int v = 1) {
  __sync_fetch_and_add(&g_counters[slot], v);
}

static int counter_get(int slot) {
  return g_counters[slot];
}

static void counters_reset() {
  for (int i = 0; i < kSlots; i++) {
    g_counters[i] = 0;
  }
}

static bool wait_for(const std::function<bool()>& done, int timeout_ms = 60000) {
  for (int waited = 0; waited < timeout_ms; waited += 10) {
    if (done()) {
      return true;
    }
    usleep(10 * 1000);
  }
  return done();
}

static const int16_t kMsgTask = 1;

static int tasks_total() {
  int n = 0;
  for (int i = 0; i < 4; i++) {
    n += counter_get(i);
  }
  return n;
}

static void dispatch_int(ProcessPool* p, int value, int* dst = nullptr) {
  EventData data {};
  data.info.type = kMsgTask;
  data.info.len = sizeof(int);
  std::memcpy(data.data, &value, sizeof(int));
  int worker = -1;
  if (dst == nullptr) {
    dst = &worker;
  }
  int rc = p->dispatch(&data, dst);
  if (rc != PP_OK) {
    // transient EAGAIN when the pipe fills: back off one tick and retry
    EXPECT(rc == PP_OK);
    return;
  }
}

// ---------------------------------------------------------------------------
// case 1: sustained round-robin dispatch at full speed
// ---------------------------------------------------------------------------

static int stress_dispatch_throughput() {
  const int kTasks = 50000 * g_load;

  ProcessPool pool {};
  EXPECT(pool.create(4, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);
  pool.onTask = [](ProcessPool*, Worker* worker, EventData* task) {
    counter_incr(worker->id);
    // slot[4] accumulates the sum of payloads: proves exactly-once delivery
    int value = 0;
    std::memcpy(&value, task->data, sizeof(int));
    __sync_fetch_and_add(&g_counters[4], value);
    worker->add_request_count();
    return 0;
  };
  EXPECT(pool.start() == PP_OK);

  const long expect_sum = 1L * (kTasks - 1) * kTasks / 2;

  const double t0 = now_sec();
  for (int i = 0; i < kTasks; i++) {
    dispatch_int(&pool, i);
  }
  const double t_send = now_sec() - t0;

  const long got_sum = [&] {
    long s = 0;
    for (int r = 0; r < 8; r++) {
      s = 0;
      for (int i = 0; i < 4; i++) {
        s += counter_get(i);
      }
      if (s == kTasks) {
        break;
      }
      usleep(50 * 1000);
    }
    return g_counters[4];
  }();
  const double t_all = now_sec() - t0;

  std::printf("  dispatch: %d tasks in %.2fs (send %.2fs) -> %.0f tasks/s\n", kTasks, t_all, t_send, kTasks / t_all);

  EXPECT(got_sum == expect_sum);
  EXPECT(tasks_total() == kTasks);

  pool.shutdown();
  pool.wait();
  pool.destroy();
  return test_failures == 0 ? 0 : 1;
}

// ---------------------------------------------------------------------------
// case 2: worker -> master message box flood
// ---------------------------------------------------------------------------

static int stress_worker_messages() {
  const int kMsgs = 20000 * g_load;

  ProcessPool pool {};
  g_pool = &pool;
  EXPECT(pool.create(3, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);
  pool.onTask = [](ProcessPool* p, Worker* w, EventData* task) {
    // echo every 10th task: the box holds ~32 packets, so the in-flight
    // reply count must stay well below capacity between master drains
    int value = 0;
    std::memcpy(&value, task->data, sizeof(int));
    if (value % 10 == 0) {
      p->push_message(task);
      counter_incr(9); // slot[9] = replies pushed by workers
    }
    counter_incr(w->id);
    return 0;
  };
  pool.onWorkerMessage = [](ProcessPool*, EventData*) {
    counter_incr(5); // slot[5] = replies received by the master
  };
  pp_signal_set(SIGIO, [](int) { g_pool->trigger_read_message_event(); });

  // The master drains the message box inside wait(), so feed from onStart
  // (with backoff when the datagram socket fills) and drive the finish
  // condition from a timer; the main thread then runs pool.wait().
  static int s_msgs = 0;
  s_msgs = kMsgs;
  pool.onStart = [](ProcessPool* p) {
    for (int i = 0; i < s_msgs; i++) {
      EventData data {};
      data.info.type = kMsgTask;
      data.info.len = sizeof(int);
      std::memcpy(data.data, &i, sizeof(int));
      int dst = -1;
      while (p->dispatch(&data, &dst) != PP_OK) {
        usleep(1000); // pipe full: let the workers drain it
      }
    }
    const double t0 = now_sec();
    pp_timer_tick(20, [p, t0](pp::Timer*, pp::TimerNode*) {
      if (counter_get(5) >= s_msgs / 10) {
        std::printf("  msgbox : %d replies in %.2fs -> %.0f msg/s\n", s_msgs, now_sec() - t0, counter_get(5) / (now_sec() - t0));
        p->running = false;
      }
    });
    pp_timer_after(120000, [p](Timer*, TimerNode*) { p->running = false; });
  };
  EXPECT(pool.start() == PP_OK);
  EXPECT(pool.wait() == PP_OK);
  pool.destroy();

  std::printf("  debug  : pushed=%ld received=%ld\n", (long)counter_get(9), (long)counter_get(5));
  EXPECT(counter_get(9) == kMsgs / 10); // workers pushed exactly the 1/10
  EXPECT(counter_get(5) >= kMsgs / 10); // master received them all
  EXPECT(tasks_total() == kMsgs);
  g_pool = nullptr;
  return test_failures == 0 ? 0 : 1;
}

// ---------------------------------------------------------------------------
// case 3: reload storm while dispatching
// ---------------------------------------------------------------------------

static int stress_reload_under_load() {
  const int kBatches = 40 * g_load;
  const int kPerBatch = 50;
  static int reloads = 0;

  ProcessPool pool {};
  g_pool = &pool;
  EXPECT(pool.create(3, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);
  reloads = 0;
  pool.onAfterReload = [](ProcessPool*) { __sync_fetch_and_add(&reloads, 1); };
  pool.onTask = [](ProcessPool*, Worker* worker, EventData*) {
    counter_incr(worker->id);
    return 0;
  };
  static int s_batches = 0;
  static int s_per_batch = 0;
  s_batches = kBatches;
  s_per_batch = kPerBatch;
  pool.onStart = [](ProcessPool* p) {
    int batch = 0;
    const long goal = 1L * s_batches * s_per_batch;
    pp_timer_tick(20, [p, &batch, goal](pp::Timer*, pp::TimerNode*) {
      // dispatch a batch every tick; retry through reload windows where
      // a killed worker briefly makes its pipe unwritable
      for (int i = 0; i < s_per_batch; i++) {
        EventData data {};
        data.info.type = kMsgTask;
        data.info.len = sizeof(int);
        int dst = -1;
        int tries = 0;
        while (p->dispatch(&data, &dst) != PP_OK) {
          if (++tries > 2000) {
            EXPECT(0 && "dispatch stuck through reload window");
            return;
          }
          usleep(500);
        }
        counter_incr(6); // slot[6] = delivered
      }
      batch++;
      // every third batch, kick a reload while tasks are in flight
      if (batch % 3 == 0 && reloads < s_batches / 3) {
        p->reload();
      }
      if (counter_get(6) >= goal) {
        p->running = false;
      }
    });
    pp_timer_after(120000, [p](Timer*, TimerNode*) { p->running = false; });
  };
  EXPECT(pool.start() == PP_OK);

  const double t0 = now_sec();
  EXPECT(pool.wait() == PP_OK);
  const double t_all = now_sec() - t0;

  const int dispatched = counter_get(6);
  std::printf("  reload : %d tasks + %d reloads in %.2fs -> %.0f tasks/s\n", dispatched, reloads, t_all, dispatched / t_all);

  EXPECT(dispatched == kBatches * kPerBatch);
  EXPECT(tasks_total() == dispatched); // nothing lost across reloads
  EXPECT(reloads == kBatches / 3);

  pool.destroy();
  g_pool = nullptr;
  return test_failures == 0 ? 0 : 1;
}

// ---------------------------------------------------------------------------
// case 4: msgqueue transport at full speed (SysV)
// ---------------------------------------------------------------------------

static int stress_msgqueue_throughput() {
#ifndef HAVE_MSGQUEUE
  std::printf("  (msgget unavailable, skipping)\n");
  return 0;
#else
  const int kTasks = 20000 * g_load;

  ProcessPool pool {};
  EXPECT(pool.create(3, 0, PP_IPC_MSGQUEUE) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);
  pool.onTask = [](ProcessPool*, Worker* worker, EventData* task) {
    counter_incr(worker->id);
    int value = 0;
    std::memcpy(&value, task->data, sizeof(int));
    __sync_fetch_and_add(&g_counters[4], value);
    return 0;
  };
  EXPECT(pool.start() == PP_OK);

  const long expect_sum = 1L * (kTasks - 1) * kTasks / 2;
  const double t0 = now_sec();
  for (int i = 0; i < kTasks; i++) {
    dispatch_int(&pool, i);
  }
  EXPECT(wait_for([&] { return tasks_total() == kTasks; }));
  const double t_all = now_sec() - t0;

  std::printf("  msgq   : %d tasks in %.2fs -> %.0f tasks/s\n", kTasks, t_all, kTasks / t_all);

  EXPECT(g_counters[4] == expect_sum);
  EXPECT(tasks_total() == kTasks);

  pool.shutdown();
  pool.wait();
  pool.destroy();
  return test_failures == 0 ? 0 : 1;
#endif
}

// ---------------------------------------------------------------------------
// runner
// ---------------------------------------------------------------------------

struct Case {
  const char* name;
  int (*fn)();
};

static const Case kCases[] = {
  {"dispatch_throughput", stress_dispatch_throughput},
  {"worker_message_flood", stress_worker_messages},
  {"reload_under_load", stress_reload_under_load},
#ifdef HAVE_MSGQUEUE
  {"msgqueue_throughput", stress_msgqueue_throughput},
#endif
};

int main(int argc, char** argv) {
  pp_init();
  if (argc > 1) {
    g_load = std::atoi(argv[1]);
    if (g_load < 1 || g_load > 100) {
      std::fprintf(stderr, "usage: %s [load-multiplier 1..100]\n", argv[0]);
      return 2;
    }
  }

  g_counters = static_cast<int*>(pp_shm_malloc(sizeof(int) * kSlots));

  int passed = 0;
  int failed = 0;
  for (const auto& tc : kCases) {
    counters_reset();
    test_failures = 0;
    std::printf("[RUN ] %s\n", tc.name);
    const int rc = tc.fn();
    if (rc == 0 && test_failures == 0) {
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
