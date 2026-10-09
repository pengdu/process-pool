/*
  Long-run stability (soak) test for the process pool.

  Continuously mixes the pool's life-cycle operations — batched dispatch,
  worker-messaging round trips, periodic reloads, and max_request worker
  recycling — for a configurable duration, then verifies no task was lost
  or duplicated, no worker id drifted, and the master's memory did not
  grow measurably (leak signal).

  Run:  ./build/tests/pp_soak                # default 60s
        ./build/tests/pp_soak 300            # 5 minutes
        ./build/tests/pp_soak 3600 4         # 1 hour, 4 workers
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

#include <sys/resource.h>
static long rss_kb_real() {
  rusage ru {};
  getrusage(RUSAGE_SELF, &ru);
#ifdef __APPLE__
  return ru.ru_maxrss / 1024; // bytes -> KB (macOS reports bytes)
#else
  return ru.ru_maxrss; // KB on Linux
#endif
}

// shared state (workers inherit via fork)
static int* g_counters = nullptr;
static const int kSlots = 64;
static ProcessPool* g_pool = nullptr;

// slot layout:
//  0..3    tasks completed per worker id
//  4       payload checksum accumulator
//  5       worker->master replies received
//  6       batches dispatched by the master
//  7       reloads completed
//  8..15   worker generations per id
//  16      worker-side errors (onTask returning after shutdown etc.)

static void counter_incr(int slot, long v = 1) {
  __sync_fetch_and_add(&g_counters[slot], v);
}

static long counter_get(int slot) {
  return g_counters[slot];
}

static const int16_t kMsgTask = 1;

int main(int argc, char** argv) {
  double duration_s = 60;
  int worker_num = 3;
  if (argc > 1) {
    duration_s = std::atof(argv[1]);
  }
  if (argc > 2) {
    worker_num = std::atoi(argv[2]);
  }
  if (duration_s <= 0) {
    std::fprintf(stderr, "usage: %s [seconds] [worker_num]\n", argv[0]);
    return 2;
  }

  pp_init();
  g_counters = static_cast<int*>(pp_shm_malloc(sizeof(int) * kSlots));
  for (int i = 0; i < kSlots; i++) {
    g_counters[i] = 0;
  }

  static double s_deadline = 0;
  static long s_expected_sum = 0;
  static int s_batch_counter = 0;
  static const int kPerBatch = 20;
  s_deadline = now_sec() + duration_s;

  ProcessPool pool {};
  g_pool = &pool;
  EXPECT(pool.create(worker_num, 0, PP_IPC_UNIXSOCK) == PP_OK);
  pool.set_protocol(PP_PROTOCOL_TASK);

  pool.onWorkerStart = [](ProcessPool*, Worker* worker) {
    counter_incr(8 + worker->id); // generations
    // recycle workers every 5000 tasks so fresh processes keep coming
    worker->set_max_request(5000, 100);
  };
  pool.onTask = [](ProcessPool* p, Worker* worker, EventData* task) {
    counter_incr(worker->id);
    int value = 0;
    std::memcpy(&value, task->data, sizeof(int));
    __sync_fetch_and_add(&g_counters[4], value);
    // echo a fraction back through the message box
    if (value % 5 == 0) {
      p->push_message(task);
    }
    worker->add_request_count();
    return 0;
  };
  pool.onWorkerMessage = [](ProcessPool*, EventData*) { counter_incr(5); };
  pool.onAfterReload = [](ProcessPool*) { counter_incr(7); };

  pp_signal_set(SIGIO, [](int) { g_pool->trigger_read_message_event(); });

  pool.onStart = [](ProcessPool* p) {
    // one batch of tasks every 50ms until the deadline
    pp_timer_tick(50, [p](Timer*, TimerNode*) {
      if (now_sec() >= s_deadline) {
        p->running = false;
        return;
      }
      for (int i = 0; i < kPerBatch; i++) {
        s_batch_counter++;
        int value = s_batch_counter;
        __sync_fetch_and_add(&s_expected_sum, value);
        EventData data {};
        data.info.type = kMsgTask;
        data.info.len = sizeof(int);
        std::memcpy(data.data, &value, sizeof(int));
        int dst = -1;
        int tries = 0;
        while (p->dispatch(&data, &dst) != PP_OK) {
          if (++tries > 10000) {
            break; // EXPECT below will catch the loss
          }
          usleep(1000);
        }
      }
      counter_incr(6);
      // a reload every ~5 seconds keeps worker replacement under load
      if (s_batch_counter % 100 == 0) {
        p->reload();
      }
    });
    pp_timer_after((long)((s_deadline - now_sec() + 5) * 1000), [p](Timer*, TimerNode*) { p->running = false; });
  };

  const long rss_start = rss_kb_real();

  EXPECT(pool.start() == PP_OK);
  const double t0 = now_sec();
  EXPECT(pool.wait() == PP_OK);
  const double t_elapsed = now_sec() - t0;

  const long rss_end = rss_kb_real();

  // let late SIGIO flushes land, then account for everything
  usleep(300 * 1000);

  long tasks_done = 0;
  for (int i = 0; i < worker_num; i++) {
    tasks_done += counter_get(i);
  }
  const long batches = counter_get(6);
  const long expected_tasks = batches * kPerBatch;

  std::printf("---- soak summary ----\n");
  std::printf("duration      : %.1fs (requested %.0fs)\n", t_elapsed, duration_s);
  std::printf("batches       : %ld x %d tasks\n", batches, kPerBatch);
  std::printf("tasks done    : %ld / %ld expected\n", tasks_done, expected_tasks);
  std::printf("checksum      : %ld / %ld expected\n", counter_get(4), s_expected_sum);
  std::printf("msgbox replies: %ld (expected ~%ld)\n", counter_get(5), expected_tasks / 5);
  std::printf("reloads       : %ld\n", counter_get(7));
  std::printf("generations   :");
  for (int i = 0; i < worker_num; i++) {
    std::printf(" [%ld]", counter_get(8 + i));
  }
  std::printf("\n");
  std::printf("master RSS    : %ld -> %ld KB (delta %ld)\n", rss_start, rss_end, rss_end - rss_start);
  std::printf("throughput    : %.0f tasks/s\n", tasks_done / (t_elapsed > 0 ? t_elapsed : 1));

  EXPECT(batches > 0);
  // The deadline stops the master mid-flight: the last batch (and at most
  // one more in the pipes) may still be running when wait() unwinds, so
  // tolerate up to 2 batches of in-flight slack. Anything beyond that is
  // a real loss; and tasks_done may never EXCEED expected (duplicates).
  EXPECT(tasks_done >= expected_tasks - 2 * kPerBatch);
  EXPECT(tasks_done <= expected_tasks);
  EXPECT(counter_get(4) >= s_expected_sum - 2 * kPerBatch * (s_batch_counter + 1));
  EXPECT(counter_get(4) <= s_expected_sum);
  EXPECT(counter_get(5) * 5 <= expected_tasks + 5);                    // replies bounded
  EXPECT(counter_get(5) >= expected_tasks / 5 - 5 * (worker_num + 2)); // and nearly all arrive
  for (int i = 0; i < worker_num; i++) {
    EXPECT(counter_get(8 + i) >= 1); // every id served at least once
  }
  // leak signal: master memory should not grow with volume (allow 4 MB slack
  // for allocator arenas and std::function churn)
  EXPECT(rss_end - rss_start < 4096);

  pool.destroy();
  pp_shm_free(g_counters);

  std::printf("----\n%s\n", test_failures == 0 ? "SOAK OK" : "SOAK FAILED");
  return test_failures == 0 ? 0 : 1;
}
