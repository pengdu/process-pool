
#pragma once

#include <sys/types.h>

#include "pp.h"

enum
{
  PP_MSGQUEUE_ORIENT = 1,
  PP_MSGQUEUE_BALANCE = 2,
};

namespace pp {
struct QueueNode {
  long mtype;                    /* type of received/sent message */
  char mdata[sizeof(EventData)]; /* text of the message */
};

class MsgQueue {
private:
  bool blocking_;
  int msg_id_;
  key_t msg_key_;
  int flags_;
  int perms_;

public:
  explicit MsgQueue(key_t msg_key, bool blocking = true, int perms = 0);
  ~MsgQueue();

  bool ready() const {
    return msg_id_ >= 0;
  }

  int get_id() const {
    return msg_id_;
  }

  void set_blocking(bool blocking);
  bool set_capacity(size_t queue_bytes) const;
  bool push(const QueueNode* in, size_t mdata_length) const;
  ssize_t pop(QueueNode* out, size_t mdata_size) const;
  bool stat(size_t* queue_num, size_t* queue_bytes) const;
  bool destroy();
};
} // namespace pp
