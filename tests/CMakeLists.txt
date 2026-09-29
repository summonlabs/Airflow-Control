# Test suite.
#
# No test is given a timeout. CTest timeouts, timeout wrappers, and watchdog
# processes are prohibited in this repository: a test that does not finish is a
# defect to diagnose, not to bound.

# The probe is a real program that the crash and multiprocess tests start as an
# independent operating-system process, and that terminates itself at chosen
# durable stages.
add_executable(airflow_probe support/airflow_probe.cpp)
target_link_libraries(airflow_probe PRIVATE airflow_control)
target_include_directories(airflow_probe PRIVATE "${CMAKE_SOURCE_DIR}/src")
airflow_control_apply_strict_options(airflow_probe)

add_library(airflow_test_support STATIC
  support/test_harness.cpp
  support/fixture.cpp
  support/proc.cpp)
target_include_directories(airflow_test_support PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_CURRENT_SOURCE_DIR}/support"
  "${CMAKE_SOURCE_DIR}/src")
target_link_libraries(airflow_test_support PUBLIC airflow_control)
target_compile_definitions(airflow_test_support PUBLIC
  "AIRFLOW_PROBE_EXECUTABLE=\"$<TARGET_FILE:airflow_probe>\"")
add_dependencies(airflow_test_support airflow_probe)
airflow_control_apply_strict_options(airflow_test_support)

if(TARGET airflow_control_cli)
  target_compile_definitions(airflow_test_support PUBLIC
    "AIRFLOW_CLI_EXECUTABLE=\"$<TARGET_FILE:airflow_control_cli>\"")
  add_dependencies(airflow_test_support airflow_control_cli)
endif()

function(airflow_control_add_test name)
  add_executable(${name} ${ARGN})
  target_link_libraries(${name} PRIVATE airflow_test_support)
  airflow_control_apply_strict_options(${name})
  add_test(NAME ${name} COMMAND ${name})
  set_tests_properties(${name} PROPERTIES
    WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
endfunction()

airflow_control_add_test(airflow_test_identity test_identity.cpp)
airflow_control_add_test(airflow_test_lifecycle test_lifecycle.cpp)
airflow_control_add_test(airflow_test_model test_model.cpp)
airflow_control_add_test(airflow_test_evidence test_evidence.cpp)
airflow_control_add_test(airflow_test_authority test_authority.cpp)
airflow_control_add_test(airflow_test_control test_control.cpp)
airflow_control_add_test(airflow_test_idempotency test_idempotency.cpp)
airflow_control_add_test(airflow_test_adapter test_adapter.cpp)
airflow_control_add_test(airflow_test_persistence test_persistence.cpp)
airflow_control_add_test(airflow_test_recovery test_recovery.cpp)
airflow_control_add_test(airflow_test_multiprocess test_multiprocess.cpp)
airflow_control_add_test(airflow_test_property test_property.cpp)
airflow_control_add_test(airflow_test_adversarial test_adversarial.cpp)

if(TARGET airflow_control_cli)
  airflow_control_add_test(airflow_test_cli test_cli.cpp)
endif()

# The example programs are registered as tests by the examples directory itself,
# exactly as every other subproject registers its own; this file does not reach
# across subdirectories to add them a second time.

# The downstream consumer is configured, built, and run out of tree against an
# installed package. It is enabled by pointing AIRFLOW_CONTROL_DOWNSTREAM_PREFIX
# at an install prefix produced by this project.
if(AIRFLOW_CONTROL_DOWNSTREAM_PREFIX)
  add_test(NAME airflow_test_downstream_install
    COMMAND "${CMAKE_COMMAND}"
      -DAIRFLOW_SOURCE_DIR=${CMAKE_SOURCE_DIR}
      -DAIRFLOW_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}/downstream
      -DAIRFLOW_PREFIX=${AIRFLOW_CONTROL_DOWNSTREAM_PREFIX}
      -DAIRFLOW_GENERATOR=${CMAKE_GENERATOR}
      -DAIRFLOW_CXX_COMPILER=${CMAKE_CXX_COMPILER}
      -DAIRFLOW_CONFIG=$<CONFIG>
      -P "${CMAKE_SOURCE_DIR}/cmake/RunDownstreamCheck.cmake")
  set_tests_properties(airflow_test_downstream_install PROPERTIES
    WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
endif()
