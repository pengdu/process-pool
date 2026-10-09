

#include "pp.h"
#include "pp_buffer.h"

namespace pp {

BufferChunk::BufferChunk(Type type, uint32_t size) : type(type), size(size) {
  if (type == TYPE_DATA && size > 0) {
    value.str = new char[size];
  }
}

BufferChunk::~BufferChunk() {
  if (type == TYPE_DATA) {
    delete[] value.str;
  }
  if (destroy) {
    destroy(this);
  }
}

Buffer::Buffer(uint32_t chunk_size) {
  chunk_size_ = chunk_size == 0 ? INT_MAX : chunk_size;
}

BufferChunk* Buffer::alloc(BufferChunk::Type type, uint32_t size) {
  auto* chunk = new BufferChunk(type, size);
  queue_.push(chunk);
  return chunk;
}

void Buffer::pop() {
  BufferChunk* chunk = queue_.front();
  total_length_ -= chunk->length;
  delete chunk;
  queue_.pop();
}

Buffer::~Buffer() {
  while (!queue_.empty()) {
    pop();
  }
}

void Buffer::append(const char* data, uint32_t size) {
  uint32_t length = size;
  auto pos = data;

  assert(size > 0);

  // buffer enQueue
  while (length > 0) {
    uint32_t n = length >= chunk_size_ ? chunk_size_ : length;

    BufferChunk* chunk = alloc(BufferChunk::TYPE_DATA, n);

    total_length_ += n;

    memcpy(chunk->value.str, pos, n);
    chunk->length = n;

    pp_trace_log(PP_TRACE_BUFFER, "chunk_n=%lu|size=%u|chunk_len=%u|chunk=%p", count(), n, chunk->length, chunk);

    pos += n;
    length -= n;
  }
}

void Buffer::append(const iovec* iov, size_t iovcnt, off_t offset) {
  size_t length = 0;

  if (pp_unlikely(iovcnt == 0)) {
    return;
  }

  PP_LOOP_N(iovcnt) {
    assert(iov[i].iov_len > 0);
    assert(iov[i].iov_base != nullptr);
    length += iov[i].iov_len;
  }

  if (pp_unlikely(offset < 0 || (size_t)offset >= length)) {
    return;
  }

  auto pos = static_cast<char*>(iov[0].iov_base);
  BufferChunk* chunk = nullptr;
  size_t iov_remain_len = iov[0].iov_len;
  size_t chunk_remain_len = 0;
  size_t i = 0;

  while (true) {
    if (chunk) {
      if (chunk->size == chunk->length) {
        chunk = nullptr;
        continue;
      } else {
        chunk_remain_len = chunk->size - chunk->length;
      }
    } else {
      if (offset > 0) {
        if (offset >= (off_t)iov[i].iov_len) {
          // the offset spans past this whole iovec: skip it and switch the
          // cursors to the next one
          offset -= iov[i].iov_len;
          length -= iov[i].iov_len;
          i++;
          pos = static_cast<char*>(iov[i].iov_base);
          iov_remain_len = iov[i].iov_len;
          continue;
        } else {
          pos = static_cast<char*>(iov[i].iov_base) + offset;
          iov_remain_len = iov[i].iov_len - offset;
          length -= offset;
          offset = 0;
        }
      }
      chunk_remain_len = length >= chunk_size_ ? chunk_size_ : length;
      chunk = alloc(BufferChunk::TYPE_DATA, chunk_remain_len);
    }

    size_t n = std::min(iov_remain_len, chunk_remain_len);
    memcpy(chunk->value.str + chunk->length, pos, n);
    total_length_ += n;
    length -= n;

    pp_trace_log(PP_TRACE_BUFFER, "chunk_n=%lu|size=%lu|chunk_len=%u|chunk=%p", count(), n, chunk->length, chunk);

    chunk->length += n;
    iov_remain_len -= n;

    if (iov_remain_len == 0) {
      i++;
      if (i == iovcnt) {
        break;
      }
      iov_remain_len = iov[i].iov_len;
      pos = (char*)iov[i].iov_base;
    } else {
      pos += n;
    }
  }
}
} // namespace pp
