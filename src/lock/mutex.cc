
#include "pp_lock.h"

namespace pp {

struct MutexImpl {
  pthread_mutex_t lock_;
  pthread_mutexattr_t attr_;
};

Mutex::Mutex(bool shared) : Lock(shared) {
  if (shared) {
    impl = (MutexImpl*)pp_mem_pool()->alloc(sizeof(*impl));
    if (impl == nullptr) {
      throw std::bad_alloc();
    }
  } else {
    impl = new MutexImpl();
  }

  int err = pthread_mutexattr_init(&impl->attr_);
  if (err != 0) {
    free_ptr(impl);
    throw std::system_error(err, std::generic_category(), "pthread_mutexattr_init() failed");
  }
  if (shared) {
    err = pthread_mutexattr_setpshared(&impl->attr_, PTHREAD_PROCESS_SHARED);
    if (err != 0) {
      pthread_mutexattr_destroy(&impl->attr_);
      free_ptr(impl);
      throw std::system_error(err, std::generic_category(), "pthread_mutexattr_setpshared() failed");
    }
  }
  err = pthread_mutex_init(&impl->lock_, &impl->attr_);
  if (err != 0) {
    pthread_mutexattr_destroy(&impl->attr_);
    free_ptr(impl);
    throw std::system_error(err, std::generic_category(), "pthread_mutex_init() failed");
  }
}

int Mutex::lock(int operation, int timeout_msec) {
  if (operation & LOCK_NB) {
    return pthread_mutex_trylock(&impl->lock_);
  }
  if (timeout_msec > 0) {
#ifdef HAVE_MUTEX_TIMEDLOCK
    timespec timeo;
    realtime_get(&timeo);
    realtime_add(&timeo, timeout_msec);
    return pthread_mutex_timedlock(&impl->lock_, &timeo);
#else
    return pp_wait_for([this]() { return pthread_mutex_trylock(&impl->lock_) == 0; }, timeout_msec) ? 0 : ETIMEDOUT;
#endif
  }
  return pthread_mutex_lock(&impl->lock_);
}

int Mutex::unlock() {
  return pthread_mutex_unlock(&impl->lock_);
}

Mutex::~Mutex() {
  pthread_mutexattr_destroy(&impl->attr_);
  pthread_mutex_destroy(&impl->lock_);
  free_ptr(impl);
}

} // namespace pp
