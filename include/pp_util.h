
#pragma once

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <memory>
#include <string>
#include <thread>

namespace pp {
// Keep parameter 'steady' as false for backward compatibility.
template <typename T> static inline long time(bool steady = false) {
  if (pp_likely(steady)) {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<T>(now.time_since_epoch()).count();
  } else {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<T>(now.time_since_epoch()).count();
  }
}

} // namespace pp
