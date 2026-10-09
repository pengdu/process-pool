

#pragma once

enum ppErrorCode
{
  /**
   * Prevent repetition with errno [syscall error]
   */
  PP_ERROR_BEGIN = 500,

  /**
   * common error
   */
  PP_ERROR_MALLOC_FAIL = 501,
  PP_ERROR_SYSTEM_CALL_FAIL,
  PP_ERROR_NAME_TOO_LONG,
  PP_ERROR_INVALID_PARAMS,
  PP_ERROR_OPERATION_NOT_SUPPORT,
  PP_ERROR_UNREGISTERED_SIGNAL,
  PP_ERROR_BAD_HOST_ADDR,
  PP_ERROR_BAD_PORT,
  PP_ERROR_BAD_SOCKET_TYPE,

  // EventLoop
  PP_ERROR_EVENT_REMOVE_FAILED = 800,
  PP_ERROR_EVENT_ADD_FAILED,
  PP_ERROR_EVENT_UPDATE_FAILED,
  PP_ERROR_EVENT_UNKNOWN_DATA,

  /**
   * connection error
   */
  PP_ERROR_OUTPUT_BUFFER_OVERFLOW,

  PP_ERROR_PACKAGE_LENGTH_TOO_LARGE = 1201,

  /**
   * task error
   */
  /**
   * AIO
   */
  /**
   * Client
   */
  /**
   * Socket
   */
  PP_ERROR_SOCKET_POLL_TIMEOUT,

  /**
   * server global error
   */
  /**
   * Process exit timeout, forced to end.
   */
  PP_ERROR_END
};
