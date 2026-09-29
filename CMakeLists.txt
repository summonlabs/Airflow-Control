cmake_minimum_required(VERSION 3.21)

project(AirflowControl
  VERSION 1.0.0
  DESCRIPTION "Vendor-neutral airflow-oriented control semantics for rooms, rows, racks, containment, and pressure relationships"
  HOMEPAGE_URL "https://github.com/summonlabs/Airflow-Control"
  LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(AIRFLOW_CONTROL_IS_TOP_LEVEL ON)
else()
  set(AIRFLOW_CONTROL_IS_TOP_LEVEL OFF)
endif()

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 20)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(AIRFLOW_CONTROL_IS_TOP_LEVEL)
  option(AIRFLOW_CONTROL_BUILD_TESTS "Build the Airflow Control test suite" ON)
  option(AIRFLOW_CONTROL_BUILD_EXAMPLES "Build the Airflow Control examples" ON)
  option(AIRFLOW_CONTROL_BUILD_BENCHMARKS "Build the Airflow Control benchmarks" ON)
  option(AIRFLOW_CONTROL_BUILD_TOOLS "Build the Airflow Control command line tool" ON)
else()
  option(AIRFLOW_CONTROL_BUILD_TESTS "Build the Airflow Control test suite" OFF)
  option(AIRFLOW_CONTROL_BUILD_EXAMPLES "Build the Airflow Control examples" OFF)
  option(AIRFLOW_CONTROL_BUILD_BENCHMARKS "Build the Airflow Control benchmarks" OFF)
  option(AIRFLOW_CONTROL_BUILD_TOOLS "Build the Airflow Control command line tool" OFF)
endif()

option(AIRFLOW_CONTROL_WARNINGS_AS_ERRORS "Treat first-party compiler warnings as errors" ON)
option(AIRFLOW_CONTROL_ENABLE_ADDRESS_SANITIZER
  "Build first-party targets with AddressSanitizer where the toolchain supports it" OFF)

set(AIRFLOW_CONTROL_DOWNSTREAM_PREFIX "" CACHE PATH
  "Install prefix of a previous Airflow Control installation; enables the downstream consumer test")

# ---------------------------------------------------------------------------
# First-party compile options
# ---------------------------------------------------------------------------
function(airflow_control_apply_strict_options target)
  if(MSVC)
    target_compile_options(${target} PRIVATE /W4 /permissive- /utf-8 /Zc:__cplusplus /EHsc)
    if(AIRFLOW_CONTROL_WARNINGS_AS_ERRORS)
      target_compile_options(${target} PRIVATE /WX)
    endif()
  else()
    target_compile_options(${target} PRIVATE
      -Wall
      -Wextra
      -Wpedantic
      -Wshadow
      -Wconversion
      -Wsign-conversion
      -Wnon-virtual-dtor
      -Wcast-qual
      -Wdouble-promotion)
    if(AIRFLOW_CONTROL_WARNINGS_AS_ERRORS)
      target_compile_options(${target} PRIVATE -Werror)
    endif()
  endif()
  if(AIRFLOW_CONTROL_ENABLE_ADDRESS_SANITIZER)
    if(MSVC)
      # Debug information is emitted because the sanitizer needs it for a usable
      # report; building with the sanitizer and no debug info is itself a
      # diagnostic warning, and the honest fix is to emit the information rather
      # than to silence the warning.
      # The sanitizer flag is a compiler option: the compiler records the runtime
      # libraries it needs, and link.exe does not accept the flag itself.
      target_compile_options(${target} PRIVATE /fsanitize=address /Zi)
      target_link_options(${target} PRIVATE /DEBUG /INCREMENTAL:NO)
    else()
      target_compile_options(${target} PRIVATE -fsanitize=address -fno-omit-frame-pointer -g)
      target_link_options(${target} PRIVATE -fsanitize=address)
    endif()
  endif()
endfunction()

# ---------------------------------------------------------------------------
# Generated version header
# ---------------------------------------------------------------------------
set(AIRFLOW_CONTROL_GENERATED_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(AIRFLOW_CONTROL_STORE_FORMAT_VERSION 1)
configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/include/airflow_control/version.hpp.in"
  "${AIRFLOW_CONTROL_GENERATED_INCLUDE_DIR}/airflow_control/version.hpp"
  @ONLY)

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------
add_library(airflow_control
  src/status.cpp
  src/ids.cpp
  src/units.cpp
  src/lifecycle.cpp
  src/model.cpp
  src/evidence.cpp
  src/authority.cpp
  src/adapter.cpp
  src/attempt.cpp
  src/audit.cpp
  src/synthetic_adapter.cpp
  src/store.cpp
  src/engine.cpp
  src/detail/codec.cpp
  src/detail/crash_point.cpp
  src/detail/crc32c.cpp
  src/detail/file_io.cpp
  src/detail/path.cpp
  src/detail/process.cpp
  src/detail/store_file.cpp
  src/detail/serialization.cpp)
add_library(AirflowControl::airflow_control ALIAS airflow_control)

target_include_directories(airflow_control
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<BUILD_INTERFACE:${AIRFLOW_CONTROL_GENERATED_INCLUDE_DIR}>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
  PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/src")

target_compile_features(airflow_control PUBLIC cxx_std_20)
airflow_control_apply_strict_options(airflow_control)

set_target_properties(airflow_control PROPERTIES
  OUTPUT_NAME "airflow_control"
  VERSION "${PROJECT_VERSION}"
  SOVERSION "${PROJECT_VERSION_MAJOR}"
  CXX_STANDARD 20
  CXX_STANDARD_REQUIRED ON)

if(BUILD_SHARED_LIBS AND WIN32)
  set_target_properties(airflow_control PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)
endif()

# ---------------------------------------------------------------------------
# CLI tool
# ---------------------------------------------------------------------------
if(AIRFLOW_CONTROL_BUILD_TOOLS)
  add_executable(airflow_control_cli tools/airflow_cli.cpp tools/cli_json.cpp)
  set_target_properties(airflow_control_cli PROPERTIES OUTPUT_NAME "airflow-control")
  target_link_libraries(airflow_control_cli PRIVATE airflow_control)
  target_include_directories(airflow_control_cli PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tools")
  airflow_control_apply_strict_options(airflow_control_cli)
endif()

# ---------------------------------------------------------------------------
# Install / export
# ---------------------------------------------------------------------------
install(TARGETS airflow_control
  EXPORT AirflowControlTargets
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

if(AIRFLOW_CONTROL_BUILD_TOOLS)
  install(TARGETS airflow_control_cli
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
endif()

install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/airflow_control"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  FILES_MATCHING PATTERN "*.hpp")
install(FILES "${AIRFLOW_CONTROL_GENERATED_INCLUDE_DIR}/airflow_control/version.hpp"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/airflow_control")

set(AIRFLOW_CONTROL_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/AirflowControl")

install(EXPORT AirflowControlTargets
  NAMESPACE AirflowControl::
  FILE AirflowControlTargets.cmake
  DESTINATION "${AIRFLOW_CONTROL_INSTALL_CMAKEDIR}")

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/AirflowControlConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/AirflowControlConfig.cmake"
  INSTALL_DESTINATION "${AIRFLOW_CONTROL_INSTALL_CMAKEDIR}")

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/AirflowControlConfigVersion.cmake"
  VERSION "${PROJECT_VERSION}"
  COMPATIBILITY SameMajorVersion)

install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/AirflowControlConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/AirflowControlConfigVersion.cmake"
  DESTINATION "${AIRFLOW_CONTROL_INSTALL_CMAKEDIR}")

install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE" "${CMAKE_CURRENT_SOURCE_DIR}/NOTICE"
  DESTINATION "${CMAKE_INSTALL_DATADIR}/AirflowControl")

# ---------------------------------------------------------------------------
# Subprojects
# ---------------------------------------------------------------------------
if(AIRFLOW_CONTROL_IS_TOP_LEVEL AND (AIRFLOW_CONTROL_BUILD_TESTS OR AIRFLOW_CONTROL_BUILD_EXAMPLES))
  enable_testing()
endif()

if(AIRFLOW_CONTROL_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(AIRFLOW_CONTROL_BUILD_BENCHMARKS)
  add_subdirectory(bench)
endif()

if(AIRFLOW_CONTROL_BUILD_TESTS)
  add_subdirectory(tests)
endif()
