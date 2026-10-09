
#include "pp.h"

#ifndef HAVE_FUTEX
#include <algorithm>
#include <chrono>

#include "pp_memory.h"
#endif

#include <thread>

#ifdef HAVE_FUTEX
#include <linux/futex.h>
#include <sys/syscall.h>

int pp_atomic_futex_wait(pp_atomic_t* atomic, double timeout) {
  if (pp_atomic_cmp_set(atomic, 1, 0)) {
    return 0;
  }

  int ret;
  timespec ts;

  if (timeout > 0) {
    ts.tv_sec = static_cast<long>(timeout);
    ts.tv_nsec = (timeout - ts.tv_sec) * 1000 * 1000 * 1000;
    ret = syscall(SYS_futex, atomic, FUTEX_WAIT, 0, &ts, NULL, 0);
  } else {
    ret = syscall(SYS_futex, atomic, FUTEX_WAIT, 0, NULL, NULL, 0);
  }
  if (ret == 0 && pp_atomic_cmp_set(atomic, 1, 0)) {
    return 0;
  } else {
    return -1;
  }
}

int pp_atomic_futex_wakeup(pp_atomic_t* atomic, int n) {
  if (pp_atomic_cmp_set(atomic, 0, 1)) {
    return syscall(SYS_futex, atomic, FUTEX_WAKE, n, NULL, NULL, 0);
  } else {
    return 0;
  }
}
#else
using AtomicWaitClock = std::chrono::steady_clock;

constexpr size_t PP_ATOMIC_WAIT_ENTRY_COUNT = 1024;

// This registry is allocated before any worker is forked. Its entries are
// keyed by the shared atomic address, keeping wait metadata out of Atomic
// objects while still making it visible to every descendant process.
struct AtomicWaitEntry {
  uintptr_t atomic_address;
  uint32_t waiters;
  uint32_t generation;
  uint32_t notifications;
};

struct AtomicWaitRegistry {
  pp_atomic_t lock;
  AtomicWaitEntry entries[PP_ATOMIC_WAIT_ENTRY_COUNT];
};

static AtomicWaitRegistry* atomic_wait_registry;

void pp_atomic_wait_init() {
  atomic_wait_registry = static_cast<AtomicWaitRegistry*>(pp_mem_pool()->alloc(sizeof(AtomicWaitRegistry)));
  if (atomic_wait_registry == nullptr) {
    pp_fatal_error(PP_ERROR_MALLOC_FAIL, "failed to allocate atomic wait registry");
  }
}

static void atomic_wait_registry_lock() {
  while (!pp_atomic_cmp_set(&atomic_wait_registry->lock, 0, 1)) {
    pp_atomic_cpu_pause();
    std::this_thread::yield();
  }
}

static void atomic_wait_registry_unlock() {
  pp_spinlock_release(&atomic_wait_registry->lock);
}

static AtomicWaitEntry* atomic_wait_entry_get(pp_atomic_t* atomic, bool create) {
  const auto address = reinterpret_cast<uintptr_t>(atomic);
  const size_t start = (address >> 3) % PP_ATOMIC_WAIT_ENTRY_COUNT;
  AtomicWaitEntry* reusable = nullptr;

  for (size_t i = 0; i < PP_ATOMIC_WAIT_ENTRY_COUNT; i++) {
    AtomicWaitEntry* entry = &atomic_wait_registry->entries[(start + i) % PP_ATOMIC_WAIT_ENTRY_COUNT];
    if (entry->atomic_address == address) {
      return entry;
    }
    if (entry->atomic_address == 0) {
      if (!create) {
        return nullptr;
      }
      entry = reusable ? reusable : entry;
      entry->atomic_address = address;
      entry->waiters = 0;
      entry->generation = 0;
      entry->notifications = 0;
      return entry;
    }
    if (create && reusable == nullptr && entry->waiters == 0 && entry->notifications == 0) {
      reusable = entry;
    }
  }

  if (reusable) {
    reusable->atomic_address = address;
    reusable->waiters = 0;
    reusable->generation = 0;
    reusable->notifications = 0;
    return reusable;
  }
  return nullptr;
}

int pp_atomic_futex_wait(pp_atomic_t* atomic, double timeout) {
  if (pp_atomic_cmp_set(atomic, 1, 0)) {
    return 0;
  }

  const bool finite = timeout > 0;
  const auto deadline = finite ? AtomicWaitClock::now() + std::chrono::duration_cast<AtomicWaitClock::duration>(std::chrono::duration<double>(timeout)) : AtomicWaitClock::time_point::max();

  atomic_wait_registry_lock();
  // Close the race between the initial acquire and registering this waiter.
  if (pp_atomic_cmp_set(atomic, 1, 0)) {
    atomic_wait_registry_unlock();
    return 0;
  }
  AtomicWaitEntry* entry = atomic_wait_entry_get(atomic, true);
  if (entry == nullptr) {
    atomic_wait_registry_unlock();
    pp_set_last_error(ENOSPC);
    return -1;
  }
  uint32_t generation = entry->generation;
  entry->waiters++;
  atomic_wait_registry_unlock();

  while (true) {
    atomic_wait_registry_lock();

    if (entry->generation != generation) {
      generation = entry->generation;
      if (entry->notifications > 0) {
        entry->notifications--;
        entry->waiters--;
        const bool acquired = pp_atomic_cmp_set(atomic, 1, 0);
        atomic_wait_registry_unlock();
        return acquired ? 0 : -1;
      }
    } else {
      if (pp_atomic_cmp_set(atomic, 1, 0)) {
        entry->waiters--;
        atomic_wait_registry_unlock();
        return 0;
      }
    }

    if (finite && AtomicWaitClock::now() >= deadline) {
      entry->waiters--;
      atomic_wait_registry_unlock();
      pp_set_last_error(ETIMEDOUT);
      return -1;
    }

    atomic_wait_registry_unlock();
    usleep(1000);
  }
}

int pp_atomic_futex_wakeup(pp_atomic_t* atomic, int n) {
  if (n < 1) {
    pp_set_last_error(EINVAL);
    return -1;
  }

  atomic_wait_registry_lock();
  if (!pp_atomic_cmp_set(atomic, 0, 1)) {
    atomic_wait_registry_unlock();
    return 0;
  }

  AtomicWaitEntry* entry = atomic_wait_entry_get(atomic, false);
  if (entry == nullptr) {
    atomic_wait_registry_unlock();
    return 0;
  }
  // Notifications belong only to waiters that are already registered. This
  // prevents a later waiter from consuming an earlier wakeup.
  const uint32_t available = entry->waiters > entry->notifications ? entry->waiters - entry->notifications : 0;
  const uint32_t notified = std::min<uint32_t>(n, available);
  if (notified > 0) {
    entry->notifications += notified;
    entry->generation++;
  }
  atomic_wait_registry_unlock();
  return notified;
}
#endif
