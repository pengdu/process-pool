
#pragma once

#include <sys/uio.h>

#include <queue>

namespace pp {

struct BufferChunk {
  enum Type
  {
    TYPE_DATA,
    TYPE_CLOSE,
  };

  Type type;
  uint32_t length = 0;
  uint32_t offset = 0;
  union {
    char* str;
    void* ptr;
    uint32_t u32;
    uint64_t u64;
  } value {};
  uint32_t size = 0;

  BufferChunk(Type type, uint32_t size);
  ~BufferChunk();

  void (*destroy)(BufferChunk* chunk) = nullptr;
};

class Buffer {
  // 0: don't use chunk
  uint32_t chunk_size_;
  uint32_t total_length_ = 0;
  std::queue<BufferChunk*> queue_;

public:
  explicit Buffer(uint32_t chunk_size);
  ~Buffer();

  BufferChunk* alloc(BufferChunk::Type type, uint32_t size);

  BufferChunk* front() const {
    return queue_.front();
  }

  void pop();
  void append(const char* data, uint32_t size);
  void append(const struct iovec* iov, size_t iovcnt, off_t offset);

  uint32_t length() const {
    return total_length_;
  }

  size_t count() const {
    return queue_.size();
  }

  bool empty() const {
    return queue_.empty();
  }

  static bool empty(const Buffer* buffer) {
    return buffer == nullptr || buffer->queue_.empty();
  }
};

} // namespace pp
