
#pragma once

#include <sys/file.h>

#include <system_error>

#include "pp_memory.h"

namespace pp {

class Lock {
public:
  enum Type
  {
    MUTEX = 3,
  };
  virtual ~Lock() = default;
  virtual int lock(int operation = LOCK_EX, int timeout_msec = -1) = 0;
  virtual int unlock() = 0;

protected:
  explicit Lock(bool shared) {
    shared_ = shared;
  }
  template <typename T> void free_ptr(T* ptr) {
    if (shared_) {
      pp_mem_pool()->free((void*)ptr);
    } else {
      delete ptr;
    }
  }
  bool shared_;
};

struct MutexImpl;

class Mutex final : public Lock {
  MutexImpl* impl;

public:
  explicit Mutex(bool shared);
  ~Mutex() override;
  int lock(int operation = LOCK_EX, int timeout_msec = -1) override;
  int unlock() override;
};

} // namespace pp
