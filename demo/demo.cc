/*
  Minimal demo: a process pool that computes squares.

  The master dispatches numbers to a fixed set of worker processes; each worker
  computes the square and writes the result into a shared result array that the
  master reads back after the pool has been drained.

  Build:  cmake --build build --target pp_demo
  Run:    ./build/demo/pp_demo
*/

#include <unistd.h>

#include <cstdio>
#include <cstring>

#include "pp.h"
#include "pp_process_pool.h"

using pp::EventData;
using pp::ProcessPool;
using pp::Worker;

static const uint32_t kWorkerNum = 4;
static const int kJobs = 16;

// Results land in shared memory so the master can observe them after the fork.
static int* shared_results = nullptr;

static int on_task(ProcessPool* pool, Worker* worker, EventData* task) {
  int value = 0;
  if (task->info.len >= sizeof(int)) {
    std::memcpy(&value, task->data, sizeof(int));
  }
  int squared = value * value;

  if (shared_results && value >= 0 && value < kJobs) {
    shared_results[value] = squared;
  }

  std::printf("[worker %u pid=%d] %d*%d = %d\n", worker->id, (int)getpid(), value, value, squared);
  std::fflush(stdout);

  worker->add_request_count();
  return 0;
}

int main() {
  pp_init();

  shared_results = static_cast<int*>(pp_shm_malloc(sizeof(int) * kJobs));
  if (!shared_results) {
    std::fprintf(stderr, "demo: shared memory allocation failed\n");
    return 1;
  }
  std::memset(shared_results, 0, sizeof(int) * kJobs);

  ProcessPool pool {};
  if (pool.create(kWorkerNum, 0, PP_IPC_UNIXSOCK) < 0) {
    std::fprintf(stderr, "demo: pool.create() failed\n");
    return 1;
  }

  pool.onTask = on_task;
  pool.set_type(PP_USER_WORKER);
  pool.set_start_id(0);
  pool.set_max_request(0, 0);
  pool.set_protocol(PP_PROTOCOL_TASK);

  if (pool.start() < 0) {
    std::fprintf(stderr, "demo: pool.start() failed\n");
    return 1;
  }
  std::printf("pool started with %u workers\n", kWorkerNum);

  int dispatched = 0;
  for (int i = 0; i < kJobs; i++) {
    EventData data {};
    data.info.len = sizeof(int);
    std::memcpy(data.data, &i, sizeof(int));

    // dispatch() (as opposed to dispatch_sync()) hands the task to the next
    // idle worker without blocking for a reply, so work is spread across the
    // whole pool instead of being serialised through one worker at a time.
    int dst = -1; // -1 lets the pool pick the next idle worker
    if (pool.dispatch(&data, &dst) == PP_OK) {
      dispatched++;
    } else {
      // The pipe is full: back off and let the workers drain it.
      usleep(10 * 1000);
      --i;
    }
  }

  // Let the workers drain the pipe before tearing the pool down.
  usleep(500 * 1000);

  pool.shutdown();
  pool.wait();
  pool.destroy();

  int correct = 0;
  for (int i = 0; i < kJobs; i++) {
    if (shared_results[i] == i * i) {
      correct++;
    }
  }

  std::printf("dispatched=%d, correct results=%d/%d\n", dispatched, correct, kJobs);

  pp_shm_free(shared_results);

  return (dispatched == kJobs && correct == kJobs) ? 0 : 1;
}
