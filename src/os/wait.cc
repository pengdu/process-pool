

#include <list>
#include <unordered_map>

#include "pp_process_pool.h"
#include "pp_signal.h"
#include "pp_timer.h"

pid_t pp_waitpid(pid_t pid, int* stat_loc, int options) {
  pid_t retval = 0;
  PP_LOOP {
    retval = waitpid(pid, stat_loc, options);
    if (retval >= 0 || errno != EINTR) {
      break;
    }
    pp_signal_dispatch();
    if (pp_timer()) {
      pp_timer()->select();
    }
  }
  return retval;
}
