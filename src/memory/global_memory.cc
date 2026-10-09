

#include <mutex>
#include <vector>

#include "pp_memory.h"

#define PP_MIN_PAGE_SIZE 4096

namespace pp {

struct GlobalMemoryImpl {
  bool shared;
  uint32_t pagesize;
  std::mutex lock;
  std::vector<char*> pages;
  uint32_t alloc_offset = 0;
  pid_t create_pid;

  GlobalMemoryImpl(uint32_t pagesize, bool shared);
  ~GlobalMemoryImpl();
  char* new_page();
};

struct MemoryBlock {
  uint32_t size;
  uint32_t reserved;
  char memory[0];
};

/**
 * After the memory is allocated,
 * it will not be released until it is recycled by OS when the process exits
 */
GlobalMemory::GlobalMemory(uint32_t pagesize, bool shared) {
  assert(pagesize >= PP_MIN_PAGE_SIZE);
  impl = new GlobalMemoryImpl(pagesize, shared);
}

GlobalMemoryImpl::GlobalMemoryImpl(uint32_t pagesize, bool shared) {
  this->shared = shared;
  this->pagesize = PP_MEM_ALIGNED_SIZE_EX(pagesize, pp_pagesize());
  create_pid = getpid();

  if (new_page() == nullptr) {
    throw std::bad_alloc();
  }
}

GlobalMemoryImpl::~GlobalMemoryImpl() {
  for (auto page : pages) {
    shared ? ::pp_shm_free(page) : ::pp_free(page);
  }
}

char* GlobalMemoryImpl::new_page() {
  auto page = static_cast<char*>(shared ? pp_shm_malloc(pagesize) : pp_malloc(pagesize));
  if (page == nullptr) {
    return nullptr;
  }

  pages.push_back(page);
  alloc_offset = 0;

  return page;
}

/**
 * The returned memory must be initialized to 0
 */
void* GlobalMemory::alloc(uint32_t size) {
  MemoryBlock* block = nullptr;
  size = PP_MEM_ALIGNED_SIZE(size);
  uint32_t alloc_size = sizeof(*block) + size;
  std::unique_lock<std::mutex> lock(impl->lock);

  if (alloc_size > impl->pagesize) {
    pp_warning("failed to alloc %d bytes, exceed the maximum size[%d]", size, impl->pagesize);
    return nullptr;
  }

  if (impl->shared && impl->create_pid != getpid()) {
    GlobalMemoryImpl* old_impl = impl;
    impl = new GlobalMemoryImpl(old_impl->pagesize, old_impl->shared);
  }

  pp_trace("alloc_size=%u, size=%u", alloc_size, size);

  if (impl->alloc_offset + alloc_size > impl->pagesize) {
    char* page = impl->new_page();
    if (page == nullptr) {
      pp_warning("alloc memory error");
      return nullptr;
    }
  }

  block = reinterpret_cast<MemoryBlock*>(impl->pages.back() + impl->alloc_offset);
  impl->alloc_offset += alloc_size;

  block->size = size;

  pp_memset_zero(block->memory, size);
  return block->memory;
}

void GlobalMemory::free(void* ptr) {
}

GlobalMemory::~GlobalMemory() {
  delete impl;
}

} // namespace pp
