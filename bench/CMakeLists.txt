# The benchmark of the completed control operation. It is not registered as a
# test: it measures throughput rather than asserting a property, and it takes its
# own workload arguments.

add_executable(airflow_bench_control airflow_bench_control.cpp)
target_link_libraries(airflow_bench_control PRIVATE airflow_control)
airflow_control_apply_strict_options(airflow_bench_control)
