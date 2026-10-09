

#include "pp.h"

const char* pp_strerror(int code) {
  if (code < PP_ERROR_BEGIN) {
    return strerror(code);
  }
  static thread_local char buffer[32];
  /* strerror {{{*/
  switch (code) {
  case PP_ERROR_MALLOC_FAIL:
    return "Malloc fail";
  case PP_ERROR_SYSTEM_CALL_FAIL:
    return "System call fail";
  case PP_ERROR_NAME_TOO_LONG:
    return "Name too long";
  case PP_ERROR_INVALID_PARAMS:
    return "Invalid params";
  case PP_ERROR_OPERATION_NOT_SUPPORT:
    return "Operation not support";
  case PP_ERROR_UNREGISTERED_SIGNAL:
    return "Unregistered signal";
  case PP_ERROR_BAD_HOST_ADDR:
    return "Bad host addr";
  case PP_ERROR_BAD_PORT:
    return "Bad port";
  case PP_ERROR_BAD_SOCKET_TYPE:
    return "Bad socket type";
  case PP_ERROR_EVENT_REMOVE_FAILED:
    return "Event remove failed";
  case PP_ERROR_EVENT_ADD_FAILED:
    return "Event add failed";
  case PP_ERROR_EVENT_UPDATE_FAILED:
    return "Event update failed";
  case PP_ERROR_EVENT_UNKNOWN_DATA:
    return "Event unknown data";
  case PP_ERROR_OUTPUT_BUFFER_OVERFLOW:
    return "Output buffer overflow";
  case PP_ERROR_PACKAGE_LENGTH_TOO_LARGE:
    return "Package length too large";
  case PP_ERROR_SOCKET_POLL_TIMEOUT:
    return "Socket poll timeout";
  default:
#ifndef __MACH__
    snprintf(buffer, sizeof(buffer), "Unknown error %d", code);
#else
    snprintf(buffer, sizeof(buffer), "Unknown error: %d", code);
#endif
    return buffer;
  }
}
