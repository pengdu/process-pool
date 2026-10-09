
#include "pp_channel.h"
#include "pp_lock.h"
#include "pp_memory.h"
#include "pp_pipe.h"

namespace pp {

struct ChannelSlice {
  int length;
  char data[0];
};

Channel* Channel::make(size_t size, size_t maxlen, int flags) {
  assert(size >= maxlen);
  void* mem = nullptr;

  // use shared memory
  if (flags & PP_CHAN_SHM) {
    /**
     * overflow space
     */
    mem = pp_shm_malloc(size + sizeof(Channel) + maxlen + sizeof(ChannelSlice));
  } else {
    mem = pp_malloc(size + sizeof(Channel) + maxlen + sizeof(ChannelSlice));
  }

  if (mem == nullptr) {
    pp_warning("alloc(%ld) failed", size);
    return nullptr;
  }

  auto* object = static_cast<Channel*>(mem);
  mem = static_cast<char*>(mem) + sizeof(Channel);

  *object = {};

  // overflow space
  object->size = size;
  object->mem = mem;
  object->maxlen = maxlen;
  object->flags = flags;

  if (flags & PP_CHAN_LOCK) {
    object->lock = new Mutex(true);
  }

  return object;
}

/**
 * push data(no lock)
 */

/**
 * pop data(no lock)
 */

/**
 * peek data
 */

/**
 * wait notify
 */

/**
 * new data coming, notify to customer
 */

/**
 * push data (lock)
 */
int Channel::push(const void* in_data, int data_length) {
  assert(flags & PP_CHAN_LOCK);
  lock->lock();
  int ret = in(in_data, data_length);
  lock->unlock();
  return ret;
}

int Channel::in(const void* in_data, int data_length) {
  if (pp_unlikely(data_length < 0 || data_length > maxlen)) {
    return PP_ERR;
  }
  assert(data_length <= maxlen);
  if (pp_unlikely(full())) {
    return PP_ERR;
  }
  ChannelSlice* item = nullptr;
  int msize = sizeof(item->length) + data_length;

  if (tail < head) {
    // no enough memory space
    if (pp_unlikely((head - tail) < msize)) {
      return PP_ERR;
    }
    item = reinterpret_cast<ChannelSlice*>(static_cast<char*>(mem) + tail);
    tail += msize;
  } else {
    item = reinterpret_cast<ChannelSlice*>(static_cast<char*>(mem) + tail);
    tail += msize;
    if (tail >= (off_t)size) {
      tail = 0;
      tail_tag = 1 - tail_tag;
    }
  }
  num++;
  bytes += data_length;
  item->length = data_length;
  memcpy(item->data, in_data, data_length);
  return PP_OK;
}

int Channel::out(void* out_buf, int buffer_length) {
  if (pp_unlikely(empty())) {
    return PP_ERR;
  }

  auto* item = reinterpret_cast<ChannelSlice*>(static_cast<char*>(mem) + head);
  if (pp_unlikely(buffer_length < item->length)) {
    return PP_ERR;
  }
  assert(buffer_length >= item->length);
  memcpy(out_buf, item->data, item->length);
  head += (item->length + sizeof(item->length));
  if (head >= (off_t)size) {
    head = 0;
    head_tag = 1 - head_tag;
  }
  num--;
  bytes -= item->length;
  return item->length;
}

/**
 * free channel
 */
void Channel::destroy() {
  if (flags & PP_CHAN_LOCK) {
    delete lock;
  }
  if (flags & PP_CHAN_SHM) {
    pp_shm_free(this);
  } else {
    pp_free(this);
  }
}

/**
 * pop data (lock)
 */
int Channel::pop(void* out_buf, int buffer_length) {
  assert(flags & PP_CHAN_LOCK);
  lock->lock();
  int n = out(out_buf, buffer_length);
  lock->unlock();
  return n;
}

} // namespace pp
