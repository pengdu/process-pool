/*
  Smoke test for the standalone process-pool library.

  It exercises the real extraction boundary rather than just linking:
    1. pp_init() boots the vendored core (memory pool, logger, signals).
    2. A ProcessPool is created in PP_IPC_UNIXSOCK mode with the task protocol.
    3. Workers are forked; each runs the task main loop in its own process.
    4. The parent dispatches one EventData per worker over the unix-socket IPC.
    5. Each worker writes proof (its own pid + the payload it received) to a
       per-worker file, so the test verifies the payload actually crossed the
       process boundary -- not merely that dispatch() returned success.
    6. The pool is shut down and every child is reaped.

  Exit code 0 means the whole path worked.
*/

#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "pp.h"
#include "pp_process_pool.h"

using pp::EventData;
using pp::ProcessPool;
using pp::Worker;

static const uint32_t kWorkerNum = 3;
static const char* kPayloads[] = {"alpha", "bravo", "charlie"};

static std::string proof_path(uint16_t worker_id) {
  return "/tmp/pp_pp_smoke_" + std::to_string(worker_id) + ".txt";
}

// Runs inside the forked child process.
static int on_task(ProcessPool* pool, Worker* worker, EventData* task) {
  FILE* f = std::fopen(proof_path(worker->id).c_str(), "w");
  if (f) {
    std::fprintf(f, "pid=%d id=%u payload=%.*s\n", (int)getpid(), worker->id, (int)task->info.len, task->data);
    std::fclose(f);
  }
  worker->add_request_count();
  return 0;
}

static bool read_file(const std::string& path, std::string* out) {
  FILE* f = std::fopen(path.c_str(), "r");
  if (!f) {
    return false;
  }
  char buf[256] = {0};
  size_t n = std::fread(buf, 1, sizeof(buf) - 1, f);
  std::fclose(f);
  out->assign(buf, n);
  return true;
}

int main() {
  pp_init();
  const pid_t parent_pid = getpid();

  for (uint32_t i = 0; i < kWorkerNum; i++) {
    std::remove(proof_path((uint16_t)i).c_str());
  }

  ProcessPool pool {};
  if (pool.create(kWorkerNum, 0, PP_IPC_UNIXSOCK) < 0) {
    std::fprintf(stderr, "FAIL: pool.create() returned error\n");
    return 1;
  }

  pool.onTask = on_task;
  pool.set_type(PP_USER_WORKER);
  pool.set_start_id(0);
  pool.set_max_request(0, 0);
  pool.set_protocol(PP_PROTOCOL_TASK);
  pool.set_max_packet_size(PP_IPC_MAX_SIZE);

  if (pool.start() < 0) {
    std::fprintf(stderr, "FAIL: pool.start() returned error\n");
    return 1;
  }

  int delivered = 0;
  for (uint32_t i = 0; i < kWorkerNum; i++) {
    EventData data {};
    data.info.type = 0;
    data.info.len = (uint32_t)std::snprintf(data.data, sizeof(data.data), "%s", kPayloads[i]);

    int dst = (int)i;
    if (pool.dispatch_sync(&data, &dst) == PP_OK) {
      delivered++;
    }
  }

  // Give the children a moment to drain the pipe before the master tears the
  // pool down; shutdown() sends SIGTERM, which would otherwise cut the read
  // loop short. This is a property of the pool's shutdown timing, not a bug.
  usleep(300 * 1000);

  pool.shutdown();
  pool.wait();
  pool.destroy();

  int verified = 0;
  for (uint32_t i = 0; i < kWorkerNum; i++) {
    std::string content;
    if (!read_file(proof_path((uint16_t)i), &content)) {
      std::fprintf(stderr, "FAIL: worker %u wrote no proof file\n", i);
      continue;
    }

    char expected[128];
    std::snprintf(expected, sizeof(expected), "id=%u payload=%s\n", i, kPayloads[i]);

    const char* pid_field = std::strstr(content.c_str(), "pid=");
    int child_pid = pid_field ? std::atoi(pid_field + 4) : -1;

    if (child_pid > 0 && child_pid != parent_pid && std::strstr(content.c_str(), expected) != nullptr) {
      verified++;
      std::printf("worker %u: child_pid=%d payload=%s\n", i, child_pid, kPayloads[i]);
    } else {
      std::fprintf(stderr, "FAIL: worker %u proof mismatch: %s", i, content.c_str());
    }
  }

  std::printf("dispatched=%d/%u verified=%d/%u\n", delivered, kWorkerNum, verified, kWorkerNum);

  const bool ok = delivered == (int)kWorkerNum && verified == (int)kWorkerNum;
  std::printf("%s\n", ok ? "SMOKE OK" : "SMOKE FAILED");
  return ok ? 0 : 1;
}
