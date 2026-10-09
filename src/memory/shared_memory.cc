
#include <sys/mman.h>

#include <limits>

#include "pp_memory.h"

#if defined(MAP_ANON) && !defined(MAP_ANONYMOUS)
#define MAP_ANONYMOUS MAP_ANON
#endif

namespace pp {

struct SharedMemory {
  size_t size_;

  static void* alloc(size_t size);
  static void free(void* ptr);

  static SharedMemory* fetch_object(void* ptr) {
    return reinterpret_cast<SharedMemory*>(static_cast<char*>(ptr) - sizeof(SharedMemory));
  }
};

void* SharedMemory::alloc(size_t size) {
  void* mem = nullptr;
  SharedMemory object;

  if (pp_unlikely(size > std::numeric_limits<size_t>::max() - PP_DEFAULT_ALIGNMENT + 1)) {
    return nullptr;
  }
  size = PP_MEM_ALIGNED_SIZE(size);
  if (pp_unlikely(size > std::numeric_limits<size_t>::max() - sizeof(SharedMemory))) {
    return nullptr;
  }
  size += sizeof(SharedMemory);

  int tmpfd = -1;
  int flags = MAP_SHARED | MAP_ANONYMOUS;
  mem = mmap(nullptr, size, PROT_READ | PROT_WRITE, flags, tmpfd, 0);
#ifdef MAP_FAILED
  if (mem == MAP_FAILED)
#else
  if (!mem)
#endif
  {
    pp_sys_warning("mmap(%lu) failed", size);
    return nullptr;
  } else {
    object.size_ = size;
    memcpy(mem, &object, sizeof(object));
    return static_cast<char*>(mem) + sizeof(object);
  }
}

void SharedMemory::free(void* ptr) {
  SharedMemory* object = SharedMemory::fetch_object(ptr);
  size_t size = object->size_;
  if (munmap(object, size) < 0) {
    pp_sys_warning("munmap(%p, %lu) failed", object, size);
  }
}

} // namespace pp

using pp::SharedMemory;

void* pp_shm_malloc(size_t size) {
  return SharedMemory::alloc(size);
}

void pp_shm_free(void* ptr) {
  SharedMemory::free(ptr);
}
