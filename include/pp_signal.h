#pragma once

#include <csignal>

#include "pp.h"

namespace pp {

struct Signal {
  SignalHandler handler;
  uint16_t signo;
  bool activated;
};

} // namespace pp
