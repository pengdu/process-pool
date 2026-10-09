

#include "pp.h"
#include "pp_heap.h"

#define left(i) ((i) << 1)
#define right(i) (((i) << 1) + 1)
#define parent(i) ((i) >> 1)

namespace pp {

Heap::Heap(size_t n, Type type) {
  if (!((nodes = static_cast<HeapNode**>(pp_malloc((n + 1) * sizeof(void*)))))) {
    throw std::bad_alloc();
  }
  num = 1;
  size = (n + 1);
  this->type = type;
}

Heap::~Heap() {
  for (uint32_t i = 1; i < num; i++) {
    if (nodes[i]) {
      delete nodes[i];
    }
  }
  pp_free(nodes);
}

int Heap::compare(uint64_t a, uint64_t b) const {
  if (type == MIN_HEAP) {
    return a > b;
  } else {
    return a < b;
  }
}

uint32_t Heap::maxchild(uint32_t i) const {
  uint32_t child_i = left(i);
  if (child_i >= num) {
    return 0;
  }
  HeapNode* child_node = nodes[child_i];
  if ((child_i + 1) < num && compare(child_node->priority, nodes[child_i + 1]->priority)) {
    child_i++;
  }
  return child_i;
}

void Heap::bubble_up(uint32_t i) const {
  HeapNode* moving_node = nodes[i];
  uint32_t parent_i = 0;

  for (parent_i = parent(i); (i > 1) && compare(nodes[parent_i]->priority, moving_node->priority); i = parent_i, parent_i = parent(i)) {
    nodes[i] = nodes[parent_i];
    nodes[i]->position = i;
  }

  nodes[i] = moving_node;
  moving_node->position = i;
}

void Heap::percolate_down(uint32_t i) const {
  uint32_t child_i = 0;
  HeapNode* moving_node = nodes[i];

  while (((child_i = maxchild(i))) && compare(moving_node->priority, nodes[child_i]->priority)) {
    nodes[i] = nodes[child_i];
    nodes[i]->position = i;
    i = child_i;
  }

  nodes[i] = moving_node;
  moving_node->position = i;
}

HeapNode* Heap::push(uint64_t priority, void* data) {
  HeapNode** tmp = nullptr;
  uint32_t i = 0;

  if (num >= size) {
    uint32_t newsize = size * 2;
    if (!((tmp = static_cast<HeapNode**>(pp_realloc(nodes, sizeof(HeapNode*) * newsize))))) {
      return nullptr;
    }
    nodes = tmp;
    size = newsize;
  }

  auto* node = new HeapNode;
  node->priority = priority;
  node->data = data;
  i = num++;
  nodes[i] = node;
  bubble_up(i);
  return node;
}

void Heap::change_priority(uint64_t new_priority, HeapNode* node) const {
  uint32_t pos = node->position;
  uint64_t old_pri = node->priority;

  node->priority = new_priority;
  if (compare(old_pri, new_priority)) {
    bubble_up(pos);
  } else {
    percolate_down(pos);
  }
}

void Heap::remove(HeapNode* node) {
  uint32_t pos = node->position;
  uint32_t last = --num;

  if (pp_unlikely(pos == last)) {
    nodes[pos] = nullptr;
    delete node;
    return;
  }

  nodes[pos] = nodes[last];

  if (compare(node->priority, nodes[pos]->priority)) {
    bubble_up(pos);
  } else {
    percolate_down(pos);
  }
  delete node;
}

void* Heap::pop() {
  if (count() == 0) {
    return nullptr;
  }

  HeapNode* head = nodes[1];
  nodes[1] = nodes[--num];
  percolate_down(1);

  void* data = head->data;
  delete head;
  return data;
}

} // namespace pp
