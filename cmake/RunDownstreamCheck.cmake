# Configures, builds, and runs the out-of-tree downstream consumer against an
# installed Airflow Control package.
#
# Invoked with:
#   -DAIRFLOW_SOURCE_DIR=<Airflow Control source tree>
#   -DAIRFLOW_BINARY_DIR=<directory for the nested configure/build, and for the
#                         consumer's store>
#   -DAIRFLOW_PREFIX=<install prefix of a previous installation>
#   -DAIRFLOW_GENERATOR=<generator to pass through, optional>
#   -DAIRFLOW_CXX_COMPILER=<compiler to pass through, optional>
#   -DAIRFLOW_CONFIG=<configuration to build and run, optional>
#
# The consumer project is independent: it is configured from
# <AIRFLOW_SOURCE_DIR>/downstream/consumer with CMAKE_PREFIX_PATH pointing at the
# install prefix, so it sees only installed artifacts. Every step must succeed;
# a step that cannot complete fails the check with its exact reason rather than
# reporting a pass it did not earn. No step is given a timeout.

foreach(required AIRFLOW_SOURCE_DIR AIRFLOW_BINARY_DIR AIRFLOW_PREFIX)
  if(NOT DEFINED ${required} OR "${${required}}" STREQUAL "")
    message(FATAL_ERROR "RunDownstreamCheck.cmake requires -D${required}=...")
  endif()
endforeach()

set(airflow_downstream_source "${AIRFLOW_SOURCE_DIR}/downstream/consumer")
if(NOT EXISTS "${airflow_downstream_source}/CMakeLists.txt")
  message(FATAL_ERROR
          "the downstream consumer project was not found at "
          "'${airflow_downstream_source}'. RunDownstreamCheck.cmake expects "
          "-DAIRFLOW_SOURCE_DIR to name an Airflow Control source tree.")
endif()

set(airflow_configuration "${AIRFLOW_CONFIG}")
set(airflow_generator "${AIRFLOW_GENERATOR}")
set(airflow_compiler "${AIRFLOW_CXX_COMPILER}")

# One process per step, with its combined output reported on failure.
function(airflow_downstream_step description)
  execute_process(COMMAND ${ARGN}
    RESULT_VARIABLE airflow_result
    OUTPUT_VARIABLE airflow_output
    ERROR_VARIABLE airflow_output)
  if(NOT airflow_result EQUAL 0)
    if(airflow_output MATCHES "Could not find a package configuration file provided by \"AirflowControl\"")
      message(FATAL_ERROR
              "${description} failed because no Airflow Control package was found under "
              "'${AIRFLOW_PREFIX}'. Install the project into that prefix first, for example "
              "'cmake --install <build-dir> --prefix ${AIRFLOW_PREFIX}'.\n${airflow_output}")
    endif()
    if(airflow_output MATCHES "No CMAKE_CXX_COMPILER could be found" OR
       airflow_output MATCHES "CMAKE_CXX_COMPILER not set")
      message(FATAL_ERROR
              "${description} could not run because no C++ compiler is reachable from this "
              "environment. Run the check from a developer environment (for MSVC, after "
              "vcvars64.bat) to execute the installed-package check for real.\n${airflow_output}")
    endif()
    message(FATAL_ERROR "${description} failed with exit ${airflow_result}:\n${airflow_output}")
  endif()
  set(airflow_last_output "${airflow_output}" PARENT_SCOPE)
endfunction()

file(REMOVE_RECURSE "${AIRFLOW_BINARY_DIR}")
file(MAKE_DIRECTORY "${AIRFLOW_BINARY_DIR}")

set(airflow_configure_command
  "${CMAKE_COMMAND}"
  -S "${airflow_downstream_source}"
  -B "${AIRFLOW_BINARY_DIR}"
  "-DCMAKE_PREFIX_PATH=${AIRFLOW_PREFIX}")
if(airflow_generator)
  list(APPEND airflow_configure_command -G "${airflow_generator}")
endif()
if(airflow_compiler)
  list(APPEND airflow_configure_command "-DCMAKE_CXX_COMPILER=${airflow_compiler}")
endif()
if(airflow_configuration)
  list(APPEND airflow_configure_command "-DCMAKE_BUILD_TYPE=${airflow_configuration}")
endif()

airflow_downstream_step("downstream configure" ${airflow_configure_command})
message(STATUS "downstream configure:\n${airflow_last_output}")

# A multi-configuration generator ignores CMAKE_BUILD_TYPE, so the configuration
# is named explicitly here as well.
set(airflow_build_command "${CMAKE_COMMAND}" --build "${AIRFLOW_BINARY_DIR}")
if(airflow_configuration)
  list(APPEND airflow_build_command --config "${airflow_configuration}")
endif()

airflow_downstream_step("downstream build" ${airflow_build_command})
message(STATUS "downstream build:\n${airflow_last_output}")

# The consumer binary is located rather than assumed: a single-configuration
# generator writes it beside the build files, a multi-configuration one writes it
# under a per-configuration directory.
set(airflow_consumer_suffix "")
if(CMAKE_HOST_WIN32)
  set(airflow_consumer_suffix ".exe")
endif()
set(airflow_consumer_candidates
  "${AIRFLOW_BINARY_DIR}/consumer${airflow_consumer_suffix}")
foreach(airflow_candidate_configuration IN ITEMS
    "${airflow_configuration}" "Release" "RelWithDebInfo" "MinSizeRel" "Debug")
  if(airflow_candidate_configuration)
    list(APPEND airflow_consumer_candidates
      "${AIRFLOW_BINARY_DIR}/${airflow_candidate_configuration}/consumer${airflow_consumer_suffix}")
  endif()
endforeach()

set(airflow_consumer_binary "")
foreach(airflow_candidate IN LISTS airflow_consumer_candidates)
  if(airflow_consumer_binary STREQUAL "" AND EXISTS "${airflow_candidate}")
    set(airflow_consumer_binary "${airflow_candidate}")
  endif()
endforeach()
if(airflow_consumer_binary STREQUAL "")
  message(FATAL_ERROR
          "the downstream build succeeded but no consumer binary was found under "
          "'${AIRFLOW_BINARY_DIR}'. Looked for: ${airflow_consumer_candidates}")
endif()

# The consumer creates its durable store under the nested build directory, which
# belongs to this check and is removed at the start of the next one.
set(airflow_consumer_store "${AIRFLOW_BINARY_DIR}/consumer-store")
airflow_downstream_step("downstream run" "${airflow_consumer_binary}" "${airflow_consumer_store}")
message(STATUS "downstream consumer output:\n${airflow_last_output}")
message(STATUS "downstream check passed against prefix '${AIRFLOW_PREFIX}'")
