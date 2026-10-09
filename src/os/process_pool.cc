
#include "pp_memory.h"
#include "pp_msg_queue.h"
#include "pp_pipe.h"
#include "pp_process_pool.h"
#include "pp_reactor.h"
#include "pp_socket.h"
#include "pp_timer.h"
#include "pp_util.h"

PP_THREAD_LOCAL pp::WorkerGlobal PPWG = {};

namespace pp {

using network::Socket;

static inline long worker_id_to_mtype(WorkerId id) {
  // SysV message queues use mtype >= 1; worker ids start at 0, so reserve mtype 0 for broadcast.
  return id + 1;
}

static inline void worker_end_callback() {
  pp_timer_select();
  pp_signal_dispatch();
}

/**
 * Process manager
 */
int ProcessPool::create(uint32_t worker_num, key_t msgqueue_key, ppIPCMode ipc_mode) {
#ifndef HAVE_MSGQUEUE
  if (ipc_mode == PP_IPC_MSGQUEUE) {
    pp_warning("current platform does not support `sysvmsg`");
    return PP_ERR;
  }
#endif
  this->worker_num = worker_num;
  /**
   * Shared memory is used here
   */
  workers = static_cast<Worker*>(pp_mem_pool()->alloc(worker_num * sizeof(Worker)));
  if (workers == nullptr) {
    pp_sys_warning("malloc[1] failed");
    return PP_ERR;
  }

  if (create_message_box(PP_MESSAGE_BOX_SIZE) < 0) {
    return PP_ERR;
  }

  if (ipc_mode == PP_IPC_MSGQUEUE) {
    this->msgqueue_key = msgqueue_key;
    queue = new MsgQueue(msgqueue_key);
    if (!queue->ready()) {
      delete queue;
      queue = nullptr;
      return PP_ERR;
    }
  } else if (ipc_mode == PP_IPC_UNIXSOCK) {
    pipes = new std::vector<std::shared_ptr<UnixSocket>>;
    PP_LOOP_N(worker_num) {
      auto sock = new UnixSocket(true, SOCK_DGRAM);
      if (!sock->ready()) {
        delete sock;
        delete pipes;
        pipes = nullptr;
        return PP_ERR;
      }
      pipes->emplace_back(sock);
      workers[i].pipe_master = sock->get_socket(true);
      workers[i].pipe_worker = sock->get_socket(false);
      workers[i].pipe_object = sock;
    }
  } else if (ipc_mode == PP_IPC_SOCKET) {
    pp_error_log(PP_LOG_WARNING, PP_ERROR_OPERATION_NOT_SUPPORT, "PP_IPC_SOCKET is not compiled in");
    return PP_ERR;
  } else {
    ipc_mode = PP_IPC_NONE;
  }

  map_ = new std::unordered_map<pid_t, Worker*>;
  this->ipc_mode = ipc_mode;
  main_loop = run_with_task_protocol;

  max_packet_size_ = PP_INPUT_BUFFER_SIZE;
  max_wait_time = PP_WORKER_MAX_WAIT_TIME;

  PP_LOOP_N(worker_num) {
    workers[i].pool = this;
  }

  return PP_OK;
}

int ProcessPool::create_message_box(size_t memory_size) {
  message_box = Channel::make(memory_size, sizeof(EventData), PP_CHAN_LOCK | PP_CHAN_SHM);
  if (message_box == nullptr) {
    return PP_ERR;
  }
  return PP_OK;
}

void ProcessPool::set_protocol(ppProtocolType protocol_type) {
  switch (protocol_type) {
  case PP_PROTOCOL_TASK:
    main_loop = run_with_task_protocol;
    break;
  default:
    abort();
    break;
  }
}

int ProcessPool::start_check() {

  running = true;
  master_pid = getpid();
  pp_set_worker_type(PP_MASTER);

  if (async) {
    main_loop = run_async;
  }

  PP_LOOP_N(worker_num) {
    workers[i].pool = this;
    workers[i].id = start_id + i;
    workers[i].type = type;
    if (workers[i].pipe_worker) {
      workers[i].pipe_worker->buffer_size = UINT_MAX;
    }
    if (workers[i].pipe_master) {
      workers[i].pipe_master->buffer_size = UINT_MAX;
    }
  }

  return PP_OK;
}

/**
 * start workers
 */
int ProcessPool::start() {
  if (start_check() < 0) {
    return PP_ERR;
  }

  if (onStart) {
    onStart(this);
  }

  PP_LOOP_N(worker_num) {
    if (spawn(&(workers[i])) < 0) {
      return PP_ERR;
    }
  }

  return PP_OK;
}

int ProcessPool::schedule() {
  uint32_t target_worker_id = 0;
  uint8_t found = 0;

  for (uint32_t i = 0; i < worker_num + 1; i++) {
    target_worker_id = pp_atomic_fetch_add(&round_id, 1) % worker_num;
    if (workers[target_worker_id].is_idle()) {
      found = 1;
      break;
    }
  }
  if (found == 0) {
    pp_warning("no idle worker, dispatch to worker#%u anyway", target_worker_id);
  }
  return target_worker_id;
}

int ProcessPool::response(const char* data, uint32_t length) const {
  pp_set_last_error(PP_ERROR_OPERATION_NOT_SUPPORT);
  return PP_ERR;
}

int ProcessPool::push_message(const EventData* msg) const {
  if (message_box->push(msg, msg->size()) < 0) {
    return PP_ERR;
  }
  return pp_kill(master_pid, SIGIO);
}

int ProcessPool::push_message(uint8_t type, const void* data, size_t length) const {
  if (!message_box) {
    return PP_ERR;
  }

  EventData msg;
  if (pp_unlikely((data == nullptr && length > 0) || length > sizeof(msg.data))) {
    pp_set_last_error(PP_ERROR_INVALID_PARAMS);
    return PP_ERR;
  }

  msg.info = {};
  msg.info.type = type;
  msg.info.len = length;
  if (length > 0) {
    memcpy(msg.data, data, length);
  }

  return push_message(&msg);
}

int ProcessPool::pop_message(void* data, size_t size) const {
  if (!message_box) {
    return PP_ERR;
  }
  return message_box->pop(data, size);
}

static ppResultCode send_to_worker(ProcessPool* pool, EventData* data, int* dst_worker_id, int flags) {
  if (*dst_worker_id < 0) {
    *dst_worker_id = pool->schedule();
  }

  *dst_worker_id += pool->start_id;
  Worker* worker = pool->get_worker(*dst_worker_id);

  if (worker->send_message(data, data->size(), flags) < 0) {
    pp_warning("send %d bytes to worker#%d failed", data->size(), *dst_worker_id);
    return PP_ERR;
  }
  return PP_OK;
}

ppResultCode ProcessPool::dispatch(EventData* data, int* dst_worker_id) {
  return send_to_worker(this, data, dst_worker_id, PP_PIPE_MASTER | PP_PIPE_NONBLOCK);
}

ppResultCode ProcessPool::dispatch_sync(const char* data, uint32_t len) const {
  pp_set_last_error(PP_ERROR_OPERATION_NOT_SUPPORT);
  return PP_ERR;
}

ppResultCode ProcessPool::dispatch_sync(EventData* data, int* dst_worker_id) {
  return send_to_worker(this, data, dst_worker_id, PP_PIPE_MASTER);
}

bool ProcessPool::reload() {
  if (reload_task) {
    return false;
  }
  if (onBeforeReload) {
    onBeforeReload(this);
  }
  reload_task = new ReloadTask();
  if (max_wait_time) {
    reload_task->add_timeout_killer(max_wait_time);
  }
  reload_init = true;

  return true;
}

void ProcessPool::reopen_logger() {
  pp_logger()->reopen();

  if (is_master()) {
    kill_all_workers(SIGWINCH);
  }
}

void ProcessPool::kill_all_workers(int signo) {
  // reopen_logger() can run from a signal handler before start() has spawned every worker.
  // Unspawned slots still hold pid 0, and kill(0, ...) would signal the whole process group.
  PP_LOOP_N(worker_num) {
    if (workers[i].pid > 0) {
      pp_kill(workers[i].pid, signo);
    }
  }
}

bool ProcessPool::shutdown() {
  if (is_master()) {
    running = false;
    return true;
  } else {
    return pp_kill(master_pid, SIGTERM) == 0;
  }
}

pid_t ProcessPool::spawn(Worker* worker) {
  map_->erase(worker->pid);
  pid_t pid = pp_fork(0);
  int ret_code = 0;

  if (pid == 0) {
    worker->init();
    worker->pid = getpid();
    pp_set_worker_type(PP_WORKER);
    pp_set_worker_id(worker->id);
    pp_set_worker_pid(worker->pid);
    PPWG.worker = worker;
    if (async) {
      // 0 flags: no WAIT_EXIT condition — an empty reactor would otherwise
      // satisfy the defer-task exit check and stop immediately. The async
      // worker's event loop runs until the process is terminated (SIGTERM).
      if (pp_event_init(0) < 0) {
        exit(254);
      }
      pp_reactor()->ptr = this;
    }
    if (onWorkerStart != nullptr) {
      onWorkerStart(this, worker);
    }
    if (main_loop) {
      ret_code = main_loop(this, worker);
    }
    if (onWorkerStop != nullptr) {
      onWorkerStop(this, worker);
    }
    exit(ret_code);
  } else {
    worker->pid = pid;
    // insert new process
    map_->emplace(std::make_pair(pid, worker));
  }

  return pid;
}

void ProcessPool::set_max_request(uint32_t max_request, uint32_t max_request_grace) {
  this->max_request = max_request;
  this->max_request_grace = max_request_grace;
}

bool ProcessPool::is_worker_running(Worker* worker) const {
  return running && !worker->is_shutdown() && !worker->has_exceeded_max_request();
}

void ProcessPool::at_worker_enter(Worker* worker) const {
  if (worker->pipe_worker) {
    worker->pipe_worker->dont_restart = 1;
  }
  if (ipc_mode == PP_IPC_UNIXSOCK) {
    if (pp_timer_is_available()) {
      pp_timer()->reinit(true);
    } else {
      pp_timer_create(true);
    }
  }
}

void ProcessPool::at_worker_exit(Worker* worker) {
  if (pp_timer_is_available()) {
    pp_timer_free();
  }
}

int ProcessPool::run_with_task_protocol(ProcessPool* pool, Worker* worker) {
  struct {
    long mtype;
    EventData buf;
  } out {};

  ssize_t n = 0;

  if (pp_unlikely(pool->ipc_mode == PP_IPC_NONE)) {
    pp_warning("PP_IPC_NONE pools cannot run the task protocol; provide a custom main_loop() instead");
    return PP_ERR;
  }

  out.mtype = worker_id_to_mtype(worker->id);

  pool->at_worker_enter(worker);
  while (pool->is_worker_running(worker)) {
    /**
     * fetch task
     */
    if (pool->ipc_mode == PP_IPC_MSGQUEUE) {
      n = pool->queue->pop((QueueNode*)&out, sizeof(out.buf));
      if (n < 0 && pp_classify_errno(errno) == PP_ERROR) {
        pp_sys_warning("[Worker#%d] msgrcv(%d) failed", worker->id, pool->queue->get_id());
        break;
      }
    } else {
      worker->pipe_worker->set_timeout(msec2sec(pp_timer_get_next_msec()), PP_TIMEOUT_READ);
      n = worker->pipe_worker->read_sync(&out.buf, sizeof(out.buf));
      if (n < 0 && pp_classify_errno(errno) == PP_ERROR) {
        pp_sys_warning("[Worker#%d] read(%d) failed", worker->id, worker->pipe_worker->fd);
        break;
      }
    }

    if (n < 0) {
      worker_end_callback();
      continue;
    }
    if (n != (ssize_t)out.buf.size()) {
      pp_warning("[Worker#%d] bad task packet, The received data-length[%ld] is inconsistent with the "
                 "packet-length[%ld]",
        worker->id, n, out.buf.info.len + sizeof(out.buf.info));
    }
    if (pool->onTask(pool, worker, &out.buf) < 0) {
      pp_warning("[Worker#%d] the execution of task#%ld has failed", worker->id, pool->get_task_id(&out.buf));
    }

    worker_end_callback();
  }
  pool->at_worker_exit(worker);

  return PP_OK;
}

int ProcessPool::recv_packet(Reactor* reactor, Event* event) {
  auto* pool = static_cast<ProcessPool*>(reactor->ptr);
  ssize_t n = event->socket->read(pool->packet_buffer, pool->max_packet_size_);
  if (pp_unlikely(n < 0)) {
    if (errno != EINTR && errno != EAGAIN) {
      pp_sys_warning("failed to read(%d) pipe", event->fd);
      return PP_ERR;
    }
    return PP_OK;
  }
  if (pp_unlikely(n < (ssize_t)sizeof(DataHead))) {
    pp_warning("[Reactor#%d] short packet on fd=%d: %ld bytes", reactor->id, event->fd, (long)n);
    return PP_OK;
  }
  // The wire format is EventData{DataHead info; char data[]}: restore the
  // original header and point at the payload instead of fabricating one.
  RecvData msg {};
  std::memcpy(&msg.info, pool->packet_buffer, sizeof(DataHead));
  msg.info.len = n - sizeof(DataHead);
  msg.data = pool->packet_buffer + sizeof(DataHead);
  pool->onMessage(pool, &msg);
  return PP_OK;
}

int ProcessPool::run_async(ProcessPool* pool, Worker* worker) {
  if (pool->ipc_mode == PP_IPC_UNIXSOCK && pool->onMessage) {
    pp_event_add(worker->pipe_worker, PP_EVENT_READ);
    {
      pool->packet_buffer = new char[pool->max_packet_size_];
      pp_event_set_handler(PP_FD_PIPE, PP_EVENT_READ, recv_packet);
    }
  }
  return pp_event_wait();
}

Worker* ProcessPool::get_worker_by_pid(pid_t pid) const {
  const auto iter = map_->find(pid);
  if (iter == map_->end()) {
    return nullptr;
  }
  return iter->second;
}

void ProcessPool::set_type(int type) {
  this->type = type;
  for (uint32_t i = 0; i < worker_num; i++) {
    workers[i].type = type;
  }
}

void ProcessPool::set_start_id(int start_id) {
  this->start_id = start_id;
  for (uint32_t i = 0; i < worker_num; i++) {
    workers[i].id = start_id + i;
  }
}

bool ProcessPool::wait_detached_worker(std::unordered_set<pid_t>& detached_workers, pid_t pid) {
  auto iter = detached_workers.find(pid);
  if (iter == detached_workers.end()) {
    pp_warning("received an exit signal from an unknown child process[pid=%d]", pid);
    return false;
  }
  detached_workers.erase(iter);
  return true;
}

/**
 * Drain the shared-memory message box: deliver worker messages, and respawn
 * workers that asked for a stop (PP_WORKER_MESSAGE_STOP) via a detached exit.
 */
static void read_worker_messages(ProcessPool* pool, std::unordered_set<pid_t>& detached_workers) {
  EventData msg;
  while (pool->pop_message(&msg, sizeof(msg)) > 0) {
    if (!pool->running) {
      continue;
    }
    // PP_WORKER_MESSAGE_STOP is a reserved wire type: require BOTH the
    // type and the exact payload size so a user message that merely
    // reuses the numeric type is still delivered, not misparsed as stop.
    if (!(msg.info.type == PP_WORKER_MESSAGE_STOP && msg.info.len == sizeof(WorkerStopMessage)) && pool->onWorkerMessage) {
      pool->onWorkerMessage(pool, &msg);
      continue;
    }
    WorkerStopMessage worker_stop_msg;
    memcpy(&worker_stop_msg, msg.data, sizeof(worker_stop_msg));
    Worker* exit_worker = pool->get_worker_by_pid(worker_stop_msg.pid);
    if (exit_worker == nullptr) {
      continue;
    }
    pool->spawn(exit_worker);
    detached_workers.insert(worker_stop_msg.pid);
    pool->map_->erase(worker_stop_msg.pid);
  }
  pool->read_message = false;
  // Re-check after clearing: a SIGIO that fired between the last successful
  // pop and the clear would otherwise leave fresh messages undrained until
  // the next signal (lost wakeup). Re-arm when the box is not empty.
  if (!pool->message_box->empty()) {
    pool->read_message = true;
  }
}

/**
 * Report the exit of a known worker, fire the callbacks, respawn it, and drop
 * it from the pid map / active reload task.
 * Returns false when the pid is not a known worker (nothing was respawned).
 */
bool ProcessPool::handle_worker_exit(const ExitStatus& exit_status, std::unordered_set<pid_t>& detached_workers) {
  Worker* exit_worker = get_worker_by_pid(exit_status.get_pid());
  if (exit_worker == nullptr) {
    if (onWorkerNotFound) {
      onWorkerNotFound(this, exit_status);
    } else {
      wait_detached_worker(detached_workers, exit_status.get_pid());
    }
    return false;
  }

  if (!exit_status.is_normal_exit()) {
    exit_worker->report_error(exit_status);
    if (onWorkerError) {
      onWorkerError(this, exit_worker, exit_status);
    }
  }

  spawn(exit_worker);
  map_->erase(exit_status.get_pid());
  if (reload_task) {
    reload_task->remove(exit_status.get_pid());
  }
  return true;
}

int ProcessPool::wait() {
  std::unordered_set<pid_t> detached_workers;

  while (running) {
    ExitStatus exit_status = wait_process();
    const auto wait_error = errno;

    pp_signal_dispatch();

    if (pp_timer()) {
      pp_timer()->select();
    }

    if (read_message) {
      read_worker_messages(this, detached_workers);
    }

    if (exit_status.get_pid() < 0) {
      if (!running) {
        break;
      }
      if (!reload_task) {
        if (wait_error > 0 && wait_error != EINTR) {
          pp_sys_warning("wait() failed");
        }
        continue;
      }
      // No child exited, but a reload is pending: fall through and kick the
      // next reload kill below.
    }

    if (running) {
      if (reload_init) {
        reload_init = false;
        reload_task->add_workers(workers, worker_num);
      } else if (exit_status.get_pid() < 0) {
        continue;
      } else if (!handle_worker_exit(exit_status, detached_workers)) {
        continue;
      }
    }

    if (reload_task) {
      if (reload_task->is_completed()) {
        delete reload_task;
        reload_task = nullptr;
        if (onAfterReload) {
          onAfterReload(this);
        }
      } else {
        reload_task->kill_one();
      }
    }
  }

  uint32_t i = 0;
  int status = 0;
  Worker* worker = nullptr;
  running = false;

  delete reload_task;

  if (onShutdown) {
    onShutdown(this);
  }

  // concurrent kill
  for (i = 0; i < worker_num; i++) {
    worker = &workers[i];
    if (pp_kill(worker->pid, SIGTERM) < 0) {
      pp_sys_warning("kill(%d, SIGTERM) failed", worker->pid);
      continue;
    }
  }
  if (max_wait_time) {
    pp_timer_add((long)max_wait_time * 1000, false, [this](Timer*, TimerNode*) { kill_all_workers(); });
  }
  for (i = 0; i < worker_num; i++) {
    worker = &workers[i];
    if (pp_waitpid(worker->pid, &status, 0) < 0) {
      pp_sys_warning("waitpid(%d) failed", worker->pid);
    }
  }

  return PP_OK;
}

void ProcessPool::destroy() {
  if (pipes) {
    delete pipes;
    pipes = nullptr;
  }

  if (queue) {
    delete queue;
    queue = nullptr;
  }

  if (packet_buffer) {
    delete[] packet_buffer;
    packet_buffer = nullptr;
  }

  if (map_) {
    delete map_;
    map_ = nullptr;
  }

  if (message_box) {
    message_box->destroy();
    message_box = nullptr;
  }

  pp_mem_pool()->free(workers);
}

void Worker::init() {

  request_count = 0;
  set_status_to_idle();
  PPWG.running = true;
  PPWG.shutdown = false;
}

void Worker::set_max_request(uint32_t max_request, uint32_t max_request_grace) {
  if (max_request > 0 && max_request_grace > 0) {
    max_request += pp_system_random(1, max_request_grace);
  }
  PPWG.max_request = max_request;
}

bool Worker::has_exceeded_max_request() const {
  return PPWG.max_request > 0 && request_count >= PPWG.max_request;
}

void Worker::shutdown() {
  status = PP_WORKER_EXIT;
  PPWG.shutdown = true;
}

bool Worker::is_shutdown() {
  return PPWG.shutdown;
}

ssize_t Worker::send_message(const void* buf, size_t n, int flags) const {
  Socket* pipe_sock = nullptr;

  if (flags & PP_PIPE_MASTER) {
    pipe_sock = pipe_master;
  } else {
    pipe_sock = pipe_worker;
  }

  // message-queue
  if (pool->ipc_mode == PP_IPC_MSGQUEUE) {
    struct {
      long mtype;
      EventData buf;
    } msg;

    msg.mtype = worker_id_to_mtype(id);
    memcpy(&msg.buf, buf, n);

    pp_trace_log(PP_TRACE_WORKER, "push to MsgQ#%d %lu bytes", pool->queue->get_id(), (ulong_t)n);

    return pool->queue->push((QueueNode*)&msg, n) ? n : -1;
  }

  // PP_IPC_NONE pools never create worker pipes; dispatching a task over
  // them is a configuration error, not a crash.
  if (pp_unlikely(pipe_sock == nullptr)) {
    pp_set_last_error(PP_ERROR_OPERATION_NOT_SUPPORT);
    pp_warning("IPC mode PP_IPC_NONE has no transport for dispatching tasks");
    return -1;
  }

  if ((flags & PP_PIPE_NONBLOCK) && pp_event_is_available()) {
    return pp_event_write(pipe_sock, buf, n);
  } else {
    return pipe_sock->send_sync(buf, n);
  }
}

void Worker::report_error(const ExitStatus& exit_status) const {
  pp_warning("worker(pid=%d, id=%d) abnormal exit, status=%d, signal=%d"
             "%s",
    exit_status.get_pid(), id, exit_status.get_code(), exit_status.get_signal(), "");
}

void ReloadTask::add_workers(Worker* list, size_t n) {
  PP_LOOP_N(n) {
    if (list[i].pid <= 0) {
      pp_warning("skip invalid worker(pid=%d, id=%d) for reload", list[i].pid, list[i].id);
      continue;
    }
    workers[list[i].pid] = &list[i];
    kill_queue.push(list[i].pid);
  }
}

void ReloadTask::add_timeout_killer(int timeout) {
  timer = pp_timer_add(sec2msec(timeout), false, [this](Timer*, TimerNode*) {
    kill_all();
    timer = nullptr;
  });
}

bool ReloadTask::remove(pid_t pid) {
  auto iter = workers.find(pid);
  if (iter != workers.end()) {
    workers.erase(iter);
    return true;
  } else {
    return false;
  }
}

ReloadTask::~ReloadTask() {
  if (timer) {
    pp_timer_del(timer);
    timer = nullptr;
  }
}

void ReloadTask::kill_all(int signal_number) {
  for (auto iter = workers.begin(); iter != workers.end();) {
    if (pp_kill(iter->first, signal_number) < 0) {
      if (errno == ECHILD || errno == ESRCH) {
        iter = workers.erase(iter);
        continue;
      }
      pp_sys_warning("failed to kill(%d, %d) worker#[%d]", iter->first, signal_number, iter->second->id);
    } else if (signal_number == SIGKILL) {
      pp_warning("force kill worker process(pid=%d, id=%d)", iter->first, iter->second->id);
    }
    iter++;
  }

  clear_queue();
}

void ReloadTask::kill_one(int signal_number) {
  while (!kill_queue.empty()) {
    auto pid = kill_queue.front();
    kill_queue.pop();
    auto iter = workers.find(pid);
    if (iter == workers.end()) {
      continue;
    }
    if (pp_kill(pid, signal_number) < 0) {
      if (errno == ECHILD || errno == ESRCH) {
        workers.erase(iter);
        continue;
      }
      pp_sys_warning("kill(%d, %d) [%d] failed", pid, signal_number, iter->second->id);
    }
    break;
  }
}

void ReloadTask::clear_queue() {
  while (!kill_queue.empty()) {
    kill_queue.pop();
  }
}
} // namespace pp

pp::WorkerId pp_get_worker_id() {
  return PPWG.id;
}

pid_t pp_get_worker_pid() {
  return PPWG.pid;
}

int pp_get_worker_type() {
  return PPWG.type;
}

void pp_set_worker_id(pp::WorkerId worker_id) {
  PPWG.id = worker_id;
}

void pp_set_worker_pid(pid_t pid) {
  PPWG.pid = pid;
}

void pp_set_worker_type(int type) {
  PPWG.type = type;
}

char pp_get_worker_symbol() {
  switch (pp_get_worker_type()) {
  case PP_MASTER:
    return '#';
  case PP_MANAGER:
    return '$';
  case PP_WORKER:
    return '*';
  case PP_TASK_WORKER:
    return '^';
  case PP_USER_WORKER:
    return '@';
  default:
    return '%';
  }
}
