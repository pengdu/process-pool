/*
  Sample: master dispatches typed jobs to a worker pool and collects replies.

  Data flow
  =========

            dispatch()  (unix-socket pipe, round-robin)
    master ------------------------------------------> worker[0..N-1]
                                                          |
            push_message() + SIGIO  (shared-memory box)   |
    master <----------------------------------------------+
      |
      `-- onWorkerMessage() aggregates the results inside wait()

  Protocol
  ========
  Every job is one EventData packet. `info.type` carries the job type and
  `info.fd` carries the job id, so the reply can be matched to the request:

    JOB_SQUARE   payload: int32         reply: "<id>:square=<n*n>"
    JOB_REVERSE  payload: string        reply: "<id>:reverse=<gnirts>"
    JOB_SLEEP    payload: int32 (ms)    reply: "<id>:slept=<ms>"

  Shutdown: once the master has received every reply it stops the pool.

  Build:  cmake --build build --target pp_task_dispatch
  Run:    ./build/demo/pp_task_dispatch
*/

#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>

#include "pp.h"
#include "pp_process_pool.h"
#include "pp_signal.h"
#include "pp_timer.h"

using pp::EventData;
using pp::ProcessPool;
using pp::Timer;
using pp::TimerNode;
using pp::Worker;

enum JobType
{
  JOB_SQUARE = 1,
  JOB_REVERSE = 2,
  JOB_SLEEP = 3,
};

// One reply message type, distinct from the pool's internal STOP message.
static const uint8_t kMsgJobDone = PP_WORKER_MESSAGE_STOP + 1;

static const int kJobs = 9;

static ProcessPool* g_pool = nullptr;
static int g_replies = 0;

// ---------------------------------------------------------------------------
// worker side
// ---------------------------------------------------------------------------

static void reply(ProcessPool* pool, long job_id, const std::string& text) {
  std::string payload = std::to_string(job_id) + ":" + text;
  pool->push_message(kMsgJobDone, payload.data(), payload.size());
}

static int on_task(ProcessPool* pool, Worker* worker, EventData* task) {
  const long job_id = ProcessPool::get_task_id(task);

  switch (task->info.type) {
  case JOB_SQUARE: {
    int n = 0;
    std::memcpy(&n, task->data, sizeof(n));
    reply(pool, job_id, "square=" + std::to_string((long)n * n));
    break;
  }
  case JOB_REVERSE: {
    std::string s(task->data, task->info.len);
    std::reverse(s.begin(), s.end());
    reply(pool, job_id, "reverse=" + s);
    break;
  }
  case JOB_SLEEP: {
    int ms = 0;
    std::memcpy(&ms, task->data, sizeof(ms));
    usleep(ms * 1000);
    reply(pool, job_id, "slept=" + std::to_string(ms));
    break;
  }
  default:
    std::fprintf(stderr, "[worker %u] unknown job type %d\n", worker->id, task->info.type);
    return -1;
  }

  std::printf("[worker %u pid=%d] job#%ld type=%d done\n", worker->id, (int)getpid(), job_id, task->info.type);
  std::fflush(stdout);
  worker->add_request_count();
  return 0;
}

// ---------------------------------------------------------------------------
// master side
// ---------------------------------------------------------------------------

static bool dispatch_job(ProcessPool* pool, long job_id, JobType type, const void* payload, uint32_t len) {
  EventData data {};
  data.info.fd = job_id; // job id travels in info.fd (== get_task_id())
  data.info.type = type;
  data.info.len = len;
  std::memcpy(data.data, payload, len);

  int dst = -1; // -1: let the pool pick the next idle worker
  if (pool->dispatch(&data, &dst) != PP_OK) {
    std::fprintf(stderr, "dispatch job#%ld failed\n", job_id);
    return false;
  }
  std::printf("[master] job#%ld type=%d -> worker %d\n", job_id, type, dst);
  // onStart() runs before the workers are forked; flush so the children
  // don't inherit (and later re-flush) this buffered output.
  std::fflush(stdout);
  return true;
}

static void dispatch_all_jobs(ProcessPool* pool) {
  long id = 0;
  for (int i = 1; i <= 3; i++) { // 3 square jobs
    dispatch_job(pool, id++, JOB_SQUARE, &i, sizeof(i));
  }
  const char* words[] = {"hello", "proc", "pool"};
  for (const char* w : words) { // 3 reverse jobs
    dispatch_job(pool, id++, JOB_REVERSE, w, (uint32_t)strlen(w));
  }
  for (int ms = 10; ms <= 30; ms += 10) { // 3 sleep jobs
    dispatch_job(pool, id++, JOB_SLEEP, &ms, sizeof(ms));
  }
}

static void on_worker_message(ProcessPool* pool, EventData* msg) {
  if (msg->info.type != kMsgJobDone) {
    return;
  }
  std::printf("[master] reply: %.*s\n", (int)msg->info.len, msg->data);
  if (++g_replies == kJobs) {
    pool->running = false; // all replies in: leave wait()
  }
}

int main() {
  pp_init();

  ProcessPool pool {};
  g_pool = &pool;

  if (pool.create(3, 0, PP_IPC_UNIXSOCK) < 0) {
    std::fprintf(stderr, "pool.create() failed\n");
    return 1;
  }
  pool.set_protocol(PP_PROTOCOL_TASK);
  pool.onTask = on_task;
  pool.onWorkerMessage = on_worker_message;

  // push_message() notifies the master with SIGIO; wiring the handler is
  // the embedder's responsibility.
  pp_signal_set(SIGIO, [](int) { g_pool->trigger_read_message_event(); });

  pool.onStart = [](ProcessPool* pool) {
    dispatch_all_jobs(pool);
    // Safety net: give up after 5s if replies get lost.
    pp_timer_after(5000, [pool](Timer*, TimerNode*) { pool->running = false; });
  };

  if (pool.start() < 0) {
    std::fprintf(stderr, "pool.start() failed\n");
    return 1;
  }

  pool.wait(); // drives respawn + onWorkerMessage until running == false
  pool.destroy();

  std::printf("---\nreplies received: %d/%d -> %s\n", g_replies, kJobs, g_replies == kJobs ? "OK" : "FAILED");
  return g_replies == kJobs ? 0 : 1;
}
