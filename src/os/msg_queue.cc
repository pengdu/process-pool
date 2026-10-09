

#include "pp_msg_queue.h"

namespace pp {
#ifdef HAVE_MSGQUEUE
#include <sys/ipc.h>
#include <sys/msg.h>

bool MsgQueue::destroy() {
  if (msgctl(msg_id_, IPC_RMID, nullptr) < 0) {
    pp_sys_warning("msgctl(%d, IPC_RMID) failed", msg_id_);
    return false;
  }
  msg_id_ = -1;
  return true;
}

void MsgQueue::set_blocking(const bool blocking) {
  if (blocking == 0) {
    flags_ = flags_ | IPC_NOWAIT;
  } else {
    flags_ = flags_ & (~IPC_NOWAIT);
  }
}

MsgQueue::MsgQueue(key_t msg_key, bool blocking, int perms) {
  if (perms <= 0 || perms >= 01000) {
    perms = 0666;
  }
  msg_key_ = msg_key;
  flags_ = 0;
  perms_ = perms;
  blocking_ = blocking;
  msg_id_ = msgget(msg_key, IPC_CREAT | perms);
  if (msg_id_ < 0) {
    pp_sys_warning("msgget() failed");
  } else {
    set_blocking(blocking);
  }
}

MsgQueue::~MsgQueue() {
  // private queue must be destroyed
  if (msg_key_ == IPC_PRIVATE && msg_id_ >= 0) {
    destroy();
  }
}

ssize_t MsgQueue::pop(QueueNode* data, size_t mdata_size) const {
  ssize_t ret = msgrcv(msg_id_, data, mdata_size, data->mtype, flags_);
  if (ret < 0) {
    pp_set_last_error(errno);
    if (errno != ENOMSG && errno != EINTR) {
      pp_sys_warning("msgrcv(%d, %zu, %ld) failed", msg_id_, mdata_size, data->mtype);
    }
  }
  return ret;
}

bool MsgQueue::push(const QueueNode* in, size_t mdata_length) const {
  while (true) {
    if (msgsnd(msg_id_, in, mdata_length, flags_) == 0) {
      return true;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno != EAGAIN) {
      pp_sys_warning("msgsnd(%d, %lu, %ld) failed", msg_id_, mdata_length, in->mtype);
    }
    pp_set_last_error(errno);
    break;
  }
  return false;
}

bool MsgQueue::stat(size_t* queue_num, size_t* queue_bytes) const {
  msqid_ds stat_buf;
  if (msgctl(msg_id_, IPC_STAT, &stat_buf) == 0) {
    *queue_num = stat_buf.msg_qnum;
#ifndef __NetBSD__
    *queue_bytes = stat_buf.msg_cbytes;
#else
    *queue_bytes = stat_buf._msg_cbytes;
#endif
    return true;
  }
  return false;
}

bool MsgQueue::set_capacity(size_t queue_bytes) const {
  msqid_ds stat_buf;
  if (msgctl(msg_id_, IPC_STAT, &stat_buf) != 0) {
    return false;
  }
  stat_buf.msg_qbytes = queue_bytes;
  if (msgctl(msg_id_, IPC_SET, &stat_buf)) {
    pp_sys_warning("msgctl(msqid=%d, IPC_SET, msg_qbytes=%lu) failed", msg_id_, queue_bytes);
    return false;
  }
  return true;
}
#else
MsgQueue::MsgQueue(key_t msg_key, bool blocking, int perms) {
  pp_error("current platform does not support `sysvmsg`");
}

void MsgQueue::set_blocking(bool blocking) {
}

bool MsgQueue::set_capacity(size_t queue_bytes) const {
  return false;
}

bool MsgQueue::push(const QueueNode* in, size_t mdata_length) const {
  return false;
}

ssize_t MsgQueue::pop(QueueNode* out, size_t mdata_size) const {
  return -1;
}

bool MsgQueue::stat(size_t* queue_num, size_t* queue_bytes) const {
  return false;
}

bool MsgQueue::destroy() {
  return false;
}

MsgQueue::~MsgQueue() {
}
#endif
} // namespace pp
