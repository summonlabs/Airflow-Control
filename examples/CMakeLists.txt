# Runnable examples. Each example is a real executable check: it takes exactly
# one argument, the directory in which it may create its own durable store,
# prints its steps, and returns non-zero when an expectation does not hold.
#
# No test carries a timeout: a timeout would turn a slow machine into a
# reported semantic failure, and every one of these checks is bounded by its own
# finite workload.

set(AIRFLOW_CONTROL_EXAMPLES
  pressure_recovery
  ack_without_effect
  containment_precedence
  stale_evidence
  crash_reopen)

foreach(example IN LISTS AIRFLOW_CONTROL_EXAMPLES)
  set(target "airflow_example_${example}")
  add_executable(${target} "${example}.cpp")
  target_link_libraries(${target} PRIVATE airflow_control)
  airflow_control_apply_strict_options(${target})
  set(store_directory "${CMAKE_CURRENT_BINARY_DIR}/example-${example}")
  file(MAKE_DIRECTORY "${store_directory}")
  add_test(NAME ${target} COMMAND ${target} "${store_directory}")
  set_tests_properties(${target} PROPERTIES WORKING_DIRECTORY "${store_directory}")
endforeach()
