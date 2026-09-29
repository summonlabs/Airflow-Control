cmake_minimum_required(VERSION 3.21)

# Independent out-of-tree consumer of the installed Airflow Control package.
#
# This project is not part of the Airflow Control build tree. It is configured
# separately with CMAKE_PREFIX_PATH pointing at an install prefix, and it sees
# only the installed headers, the installed library, and the installed package
# configuration file.

project(AirflowControlConsumer
  VERSION 1.0.0
  DESCRIPTION "Out-of-tree consumer of the installed Airflow Control package"
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(AirflowControl 1.0 REQUIRED)

add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE AirflowControl::airflow_control)

if(MSVC)
  target_compile_options(consumer PRIVATE /W4 /permissive- /utf-8 /Zc:__cplusplus /EHsc /WX)
else()
  target_compile_options(consumer PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -Wshadow
    -Wconversion
    -Wsign-conversion
    -Wnon-virtual-dtor
    -Wcast-qual
    -Wdouble-promotion
    -Werror)
endif()

# Runs the consumer through a real minimal lifecycle against a store under this
# project's own build directory: open a durable store, adopt an epoch, register a
# device, set the lifecycle and a fan policy, define a pressure relationship and
# a containment element, declare and satisfy a protected interlock, add a grant,
# accept an initial observation, evaluate and issue a raise_airflow command
# through a SyntheticAirflowAdapter, verify the effect from a fresh observation,
# close, reopen, and prove that the attempt journal and the verified effect
# survived.
add_custom_target(run_consumer
  COMMAND consumer "${CMAKE_CURRENT_BINARY_DIR}/consumer-store"
  DEPENDS consumer
  COMMENT "Running the installed-package consumer lifecycle"
  VERBATIM)
