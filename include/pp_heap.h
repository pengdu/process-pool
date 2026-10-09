

#pragma once

#include <cstddef>
#include <cstdint>

namespace pp {

struct HeapNode {
  uint64_t priority;
  uint32_t position;
  void* data;
};

class Heap {
public:
  enum Type
  {
    MIN_HEAP,
  };

  Heap(size_t n, Type type);
  ~Heap();

  HeapNode* push(uint64_t priority, void* data);
  void* pop();
  void change_priority(uint64_t new_priority, HeapNode* ptr) const;
  void remove(HeapNode* node);
  size_t count() const {
    return num - 1;
  }

  int compare(uint64_t a, uint64_t b) const;

  HeapNode* top() const {
    if (num == 1) {
      return nullptr;
    }
    return nodes[1];
  }

private:
  uint32_t num;
  uint32_t size;
  enum Type type;
  HeapNode** nodes;

  void bubble_up(uint32_t i) const;
  uint32_t maxchild(uint32_t i) const;
  void percolate_down(uint32_t i) const;
};
} // namespace pp
