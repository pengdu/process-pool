# Standalone smoke test (no gtest dependency, exercised via ctest).
add_executable (pp_smoke smoke.cc)
target_link_libraries (pp_smoke PRIVATE pp_process_pool)
add_test (NAME pp_smoke COMMAND pp_smoke)

# Functional tests covering the retained API surface.
add_executable (pp_func func.cc)
target_link_libraries (pp_func PRIVATE pp_process_pool)
add_test (NAME pp_func COMMAND pp_func)
set_tests_properties (pp_func PROPERTIES TIMEOUT 60)

# Unit tests: single-process coverage of pure functions and in-memory structures.
add_executable (pp_unit unit.cc)
target_link_libraries (pp_unit PRIVATE pp_process_pool)
add_test (NAME pp_unit COMMAND pp_unit)
set_tests_properties (pp_unit PROPERTIES TIMEOUT 30)

# Integration tests: multi-process scenarios (stress, reload, reply matching).
add_executable (pp_integration integration.cc)
target_link_libraries (pp_integration PRIVATE pp_process_pool)
add_test (NAME pp_integration COMMAND pp_integration)
set_tests_properties (pp_integration PROPERTIES TIMEOUT 90)

# Stress tests: throughput floors + correctness under load (heavy; not in default ctest).
add_executable (pp_stress stress.cc)
target_link_libraries (pp_stress PRIVATE pp_process_pool)

# Soak/long-run stability: continuous mixed workload, leak signal via RSS.
add_executable (pp_soak soak.cc)
target_link_libraries (pp_soak PRIVATE pp_process_pool)
