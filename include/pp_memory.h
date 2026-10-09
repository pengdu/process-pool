
#pragma once

#include "pp.h"

namespace pp {

class MemoryPool {
public:
  virtual ~MemoryPool() = default;
  virtual void* alloc(uint32_t size) = 0;
  virtual void free(void* ptr) = 0;

protected:
  MemoryPool() {};
};

struct FixedPoolImpl;

struct RingBufferImpl;

// RingBuffer, In order for malloc / free

struct GlobalMemoryImpl;

// Global memory, the program life cycle only malloc / free one time
class GlobalMemory : public MemoryPool {
private:
  GlobalMemoryImpl* impl;

public:
  GlobalMemory(uint32_t page_size, bool shared);
  ~GlobalMemory() override;
  void* alloc(uint32_t size) override;
  void free(void* ptr) override;
};
} // namespace pp

void* pp_shm_malloc(size_t size);
void pp_shm_free(void* ptr);
