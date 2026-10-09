
#ifndef PP_CONFIG_H_
#define PP_CONFIG_H_

#ifndef __clang__
// gcc version check
#if defined(__GNUC__) && (__GNUC__ < 3 || (__GNUC__ == 4 && __GNUC_MINOR__ < 8))
#error "GCC 4.8 or later required"
#endif
#endif

#define PP_MAX_FDTYPE 32 // 32 kinds of event
#define PP_ERROR_MSG_SIZE 16384

#ifdef HAVE_MALLOC_TRIM
#define PP_USE_MALLOC_TRIM 1
#endif
#define PP_MALLOC_TRIM_INTERVAL 60
#define PP_MALLOC_TRIM_PAD 0

#define PP_SOCKET_BUFFER_SIZE 8388608
#define PP_SOCKET_RETRY_COUNT 10

#define PP_SOCKET_DEFAULT_CONNECT_TIMEOUT 10
#define PP_SOCKET_DEFAULT_READ_TIMEOUT 60
#define PP_SOCKET_DEFAULT_WRITE_TIMEOUT 60

#define PP_GLOBAL_MEMORY_PAGESIZE (2 * 1024 * 1024) // global memory page

// !!!Don't modify.----------------------------------------------------------
#ifdef __MACH__
#define PP_IPC_MAX_SIZE 2048 // MacOS
#else
#define PP_IPC_MAX_SIZE 8192 // for IPC, dgram and message-queue max size
#endif
#define PP_IPC_BUFFER_SIZE (PP_IPC_MAX_SIZE - sizeof(pp::DataHead))
// !!!End.-------------------------------------------------------------------

#define PP_WORKER_MAX_WAIT_TIME 3

#define PP_REACTOR_MAXEVENTS 4096

#define PP_MESSAGE_BOX_SIZE 65536

#define PP_INPUT_BUFFER_SIZE (2 * 1024 * 1024)

#define PP_BACKLOG 512

#define PP_SIGNO_MAX 128
#define PP_UNREGISTERED_SIGNAL_FMT "Unable to find callback function for signal %s"

#ifdef PP_DEBUG
#ifndef PP_LOG_TRACE_OPEN
#define PP_LOG_TRACE_OPEN 1
#endif
#endif

#endif /* PP_CONFIG_H_ */
