

#pragma once

#include <sys/wait.h>

#include <csignal>
#include <queue>
#include <unordered_map>
#include <unordered_set>

#include "pp_channel.h"
#include "pp_lock.h"
#include "pp_msg_queue.h"
#include "pp_pipe.h"
#include "pp_protocol.h"
#include "pp_signal.h"

enum ppWorkerStatus
{
  PP_WORKER_BUSY = 1,
  PP_WORKER_IDLE = 2,
  PP_WORKER_EXIT = 3,
};

enum ppWorkerType
{
  PP_MASTER = 1,
  PP_WORKER = 2,
  PP_MANAGER = 3,
  PP_TASK_WORKER = 4,
  PP_USER_WORKER = 5,
};

enum ppIPCMode
{
  PP_IPC_NONE = 0,
  PP_IPC_UNIXSOCK = 1,
  PP_IPC_MSGQUEUE = 2,
  PP_IPC_SOCKET = 3,
};

enum ppWorkerMessageType
{
  PP_WORKER_MESSAGE_STOP = 1,
};

enum ppProtocolType
{
  PP_PROTOCOL_TASK = 1,
};

namespace pp {
struct WorkerStopMessage {
  pid_t pid;
  uint16_t worker_id;
};

class ExitStatus {
  pid_t pid_;
  int status_;

public:
  ExitStatus(pid_t pid, int status) : pid_(pid), status_(status) {
  }

  pid_t get_pid() const {
    return pid_;
  }

  int get_code() const {
    return WEXITSTATUS(status_);
  }

  int get_signal() const {
    return WTERMSIG(status_);
  }

  bool is_normal_exit() const {
    return WIFEXITED(status_);
  }
};

static inline ExitStatus wait_process() {
  int status = 0;
  pid_t pid = ::wait(&status);
  return {pid, status};
}

struct ProcessPool;
struct Worker;

struct WorkerGlobal {
  WorkerId id;
  uint8_t type;
  pid_t pid;
  bool shutdown;
  bool running;
  uint32_t max_request;
  /**
   * worker is shared memory, visible in other work processes.
   * When a worker process restarts, it may be held by both the old and new processes simultaneously,
   * necessitating careful handling of the state.
   */
  Worker* worker;
};

struct Worker {
  pid_t pid;
  WorkerId id;
  ProcessPool* pool;
  MsgQueue* queue;

  /**
   * worker status, IDLE or BUSY
   */
  uint8_t status;
  uint8_t type;

  pp_atomic_long_t request_count;

  Mutex* lock;
  UnixSocket* pipe_object;

  network::Socket* pipe_master;
  network::Socket* pipe_worker;

  ssize_t send_message(const void* buf, size_t n, int flags) const;
  bool has_exceeded_max_request() const;
  void set_max_request(uint32_t max_request, uint32_t max_request_grace);
  void report_error(const ExitStatus& exit_status) const;
  /**
   * Init global state for worker process.
   * Must be called after the process is spawned and before the main loop is executed.
   */
  void init();
  void shutdown();
  bool is_shutdown();

  void set_status(ppWorkerStatus status) {
    this->status = status;
  }

  void set_status_to_idle() {
    set_status(PP_WORKER_IDLE);
  }

  void add_request_count() {
    request_count++;
  }

  bool is_idle() const {
    return status == PP_WORKER_IDLE;
  }
};

struct ReloadTask {
  std::unordered_map<pid_t, Worker*> workers;
  std::queue<pid_t> kill_queue;
  TimerNode* timer = nullptr;

  size_t count() const {
    return workers.size();
  }

  bool is_completed() const {
    return workers.empty();
  }

  ReloadTask() = default;
  ~ReloadTask();
  void kill_one(int signal_number = SIGTERM);
  void kill_all(int signal_number = SIGKILL);
  void add_workers(Worker* list, size_t n);
  void add_timeout_killer(int timeout);
  bool remove(pid_t pid);
  void clear_queue();
};

struct ProcessPool {
  bool running;
  bool reload_init;
  bool read_message;
  bool async;

  uint8_t ipc_mode;
  pid_t master_pid;
  uint32_t max_wait_time;

  /**
   * process type
   */
  uint8_t type;

  /**
   * worker->id = start_id + i
   */
  uint16_t start_id;

  /**
   * use stream socket IPC
   */

  char* packet_buffer;
  uint32_t max_packet_size_;

  /**
   * message queue key
   */
  key_t msgqueue_key;

  uint32_t worker_num;
  uint32_t max_request;
  uint32_t max_request_grace;

  /**
   * No idle task work process is available.
   */
  void (*onStart)(ProcessPool* pool);
  void (*onShutdown)(ProcessPool* pool);
  void (*onBeforeReload)(ProcessPool* pool);
  void (*onAfterReload)(ProcessPool* pool);
  int (*onTask)(ProcessPool* pool, Worker* worker, EventData* task);
  void (*onWorkerStart)(ProcessPool* pool, Worker* worker);
  void (*onMessage)(ProcessPool* pool, RecvData* msg);
  void (*onWorkerStop)(ProcessPool* pool, Worker* worker);
  void (*onWorkerError)(ProcessPool* pool, Worker* worker, const ExitStatus& exit_status);
  void (*onWorkerMessage)(ProcessPool* pool, EventData* msg);
  int (*onWorkerNotFound)(ProcessPool* pool, const ExitStatus& exit_status);
  int (*main_loop)(ProcessPool* pool, Worker* worker);

  pp_atomic_t round_id;

  Worker* workers;
  std::vector<std::shared_ptr<UnixSocket>>* pipes;
  MsgQueue* queue;
  std::unordered_map<pid_t, Worker*>* map_;
  Channel* message_box = nullptr;
  ReloadTask* reload_task = nullptr;

  bool is_worker_id_valid(WorkerId worker_id) const {
    return worker_id >= start_id && worker_id < (uint64_t)start_id + worker_num;
  }

  Worker* get_worker(WorkerId worker_id) const {
    return &(workers[worker_id - start_id]);
  }

  static TaskId get_task_id(const EventData* task) {
    return task->info.fd;
  }

  void set_max_packet_size(uint32_t max_packet_size) {
    max_packet_size_ = max_packet_size;
  }

  bool is_master() const {
    return pp_get_worker_type() == PP_MASTER;
  }

  void set_protocol(ppProtocolType protocol_type);
  void set_type(int type);
  void set_start_id(int start_id);
  /**
   * Event-driven workers: instead of the blocking task protocol loop, each
   * worker runs its own reactor (epoll/kqueue) and delivers incoming pipe
   * data through the `onMessage` callback. Set before `start()`.
   */
  void set_async(bool enable = true) {
    async = enable;
  }
  void set_max_request(uint32_t max_request, uint32_t max_request_grace);
  int wait();
  int start_check();
  int start();
  bool shutdown();
  bool reload();
  void reopen_logger();

  void trigger_read_message_event() {
    read_message = true;
  }

  pid_t spawn(Worker* worker);
  void kill_all_workers(int signo = SIGKILL);
  ppResultCode dispatch(EventData* data, int* worker_id);
  int response(const char* data, uint32_t length) const;
  ppResultCode dispatch_sync(EventData* data, int* dst_worker_id);
  ppResultCode dispatch_sync(const char* data, uint32_t len) const;
  Worker* get_worker_by_pid(pid_t pid) const;
  void destroy();
  int create(uint32_t worker_num, key_t msgqueue_key = 0, ppIPCMode ipc_mode = PP_IPC_NONE);
  int create_message_box(size_t memory_size);
  int push_message(uint8_t type, const void* data, size_t length) const;
  int push_message(const EventData* msg) const;
  int pop_message(void* data, size_t size) const;
  int schedule();
  bool is_worker_running(Worker* worker) const;

private:
  static int recv_packet(Reactor* reactor, Event* event);
  static int run_with_task_protocol(ProcessPool* pool, Worker* worker);
  static int run_async(ProcessPool* pool, Worker* worker);
  void at_worker_enter(Worker* worker) const;
  void at_worker_exit(Worker* worker);
  bool handle_worker_exit(const ExitStatus& exit_status, std::unordered_set<pid_t>& detached_workers);

  bool wait_detached_worker(std::unordered_set<pid_t>& detached_workers, pid_t pid);
};
}; // namespace pp

static pp_inline int pp_kill(pid_t pid, int sig) {
  return kill(pid, sig);
}

extern PP_THREAD_LOCAL pp::WorkerGlobal PPWG;
