add_executable (pp_demo demo.cc)
target_link_libraries (pp_demo PRIVATE pp_process_pool)

add_executable (pp_task_dispatch task_dispatch.cc)
target_link_libraries (pp_task_dispatch PRIVATE pp_process_pool)
