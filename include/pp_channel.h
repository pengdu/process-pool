

#pragma once

#include "pp_lock.h"

namespace pp {

enum ChannelFlag
{
  PP_CHAN_LOCK = 1u << 1,
  PP_CHAN_SHM = 1u << 3,
};

struct Channel {
  off_t head;
  off_t tail;
  size_t size;
  char head_tag;
  char tail_tag;
  int num;
  int max_num;
  /**
   * Data length, excluding structure
   */
  size_t bytes;
  int flags;
  int maxlen;
  /**
   * memory point
   */
  void* mem;
  Lock* lock;

  bool empty() const {
    return num == 0;
  }
  bool full() const {
    return ((head == tail && tail_tag != head_tag) || (bytes + sizeof(int) * num == size));
  }
  int pop(void* out_buf, int buffer_length);
  int push(const void* in_data, int data_length);
  int out(void* out_buf, int buffer_length);
  int in(const void* in_data, int data_length);
  void destroy();
  static Channel* make(size_t size, size_t maxlen, int flags);
};
} // namespace pp
