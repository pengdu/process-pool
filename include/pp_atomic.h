
#pragma once

using pp_atomic_int32_t = int32_t;
using pp_atomic_uint32_t = uint32_t;
using pp_atomic_int64_t = int64_t;
using pp_atomic_uint64_t = uint64_t;

using pp_atomic_long_t = pp_atomic_int64_t;
using pp_atomic_ulong_t = pp_atomic_uint64_t;
using pp_atomic_t = pp_atomic_uint32_t;

#define pp_atomic_cmp_set(lock, old, set) __sync_bool_compare_and_swap(lock, old, set)
#define pp_atomic_fetch_add(value, add) __sync_fetch_and_add(value, add)

#define pp_spinlock_release(lock) __sync_lock_release(lock)

#if defined(__x86_64__)
#define pp_atomic_cpu_pause() __asm__ __volatile__("pause")
#elif defined(__aarch64__)
#define pp_atomic_cpu_pause() __asm__ __volatile__("yield")
#else
#define pp_atomic_cpu_pause()
#endif

#ifndef HAVE_FUTEX
void pp_atomic_wait_init();
#endif
int pp_atomic_futex_wait(pp_atomic_t* atomic, double timeout);
int pp_atomic_futex_wakeup(pp_atomic_t* atomic, int n);
