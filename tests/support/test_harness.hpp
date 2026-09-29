#pragma once

// Minimal deterministic test harness. No third-party dependency, no timeouts,
// no watchdog logic: a check either passes, fails, or the program does not
// finish, and an unfinished program is a defect to diagnose rather than to
// bound.
//
// A suite that performs no check at all fails rather than passing vacuously: a
// run that asserts nothing cannot tell a working runtime from a broken one.

#include <cstddef>
#include <functional>
#include <ostream>
#include <sstream>
#include <string>
#include <typeinfo>
#include <vector>

#include "airflow_control/adapter.hpp"
#include "airflow_control/attempt.hpp"
#include "airflow_control/audit.hpp"
#include "airflow_control/authority.hpp"
#include "airflow_control/engine.hpp"
#include "airflow_control/evidence.hpp"
#include "airflow_control/ids.hpp"
#include "airflow_control/lifecycle.hpp"
#include "airflow_control/model.hpp"
#include "airflow_control/status.hpp"
#include "airflow_control/store.hpp"
#include "airflow_control/synthetic_adapter.hpp"
#include "airflow_control/units.hpp"

namespace airflow_control {

// The library renders its enumerations through to_string(enum), so a check that
// prints one writes the token a caller branches on rather than an ordinal that
// a build could renumber.
#define AIRFLOW_CONTROL_TOKEN_OPERATOR(type)                          \
  inline std::ostream& operator<<(std::ostream& stream, type value) { \
    return stream << to_string(value);                                \
  }

AIRFLOW_CONTROL_TOKEN_OPERATOR(StatusCode)
AIRFLOW_CONTROL_TOKEN_OPERATOR(DeviceLifecycle)
AIRFLOW_CONTROL_TOKEN_OPERATOR(TransitionClass)
AIRFLOW_CONTROL_TOKEN_OPERATOR(PressurePolarity)
AIRFLOW_CONTROL_TOKEN_OPERATOR(ContainmentState)
AIRFLOW_CONTROL_TOKEN_OPERATOR(ContainmentKind)
AIRFLOW_CONTROL_TOKEN_OPERATOR(ObligationScope)
AIRFLOW_CONTROL_TOKEN_OPERATOR(ObligationClass)
AIRFLOW_CONTROL_TOKEN_OPERATOR(ObligationBinding)
AIRFLOW_CONTROL_TOKEN_OPERATOR(ControlIntent)
AIRFLOW_CONTROL_TOKEN_OPERATOR(RequestClass)
AIRFLOW_CONTROL_TOKEN_OPERATOR(SetpointKind)
AIRFLOW_CONTROL_TOKEN_OPERATOR(Quality)
AIRFLOW_CONTROL_TOKEN_OPERATOR(ObservationKind)
AIRFLOW_CONTROL_TOKEN_OPERATOR(FreshnessVerdict)
AIRFLOW_CONTROL_TOKEN_OPERATOR(ControlAction)
AIRFLOW_CONTROL_TOKEN_OPERATOR(InterlockClass)
AIRFLOW_CONTROL_TOKEN_OPERATOR(InterlockState)
AIRFLOW_CONTROL_TOKEN_OPERATOR(AdapterDisposition)
AIRFLOW_CONTROL_TOKEN_OPERATOR(AttemptState)
AIRFLOW_CONTROL_TOKEN_OPERATOR(EffectState)
AIRFLOW_CONTROL_TOKEN_OPERATOR(AuditKind)
AIRFLOW_CONTROL_TOKEN_OPERATOR(PressureState)
AIRFLOW_CONTROL_TOKEN_OPERATOR(ObligationState)
AIRFLOW_CONTROL_TOKEN_OPERATOR(ObservationPayload)
AIRFLOW_CONTROL_TOKEN_OPERATOR(SetpointRequest)

#undef AIRFLOW_CONTROL_TOKEN_OPERATOR

inline std::ostream& operator<<(std::ostream& stream, const Status& value) {
  return stream << value.to_string();
}

inline std::ostream& operator<<(std::ostream& stream, const CheckTrace& value) {
  return stream << value.check << "=" << to_string(value.outcome);
}

/// Identities and ordinals print their own value, so a failure names the object
/// it is about instead of an address.
template <typename Tag, std::size_t MaxLength>
std::ostream& operator<<(std::ostream& stream, const BasicId<Tag, MaxLength>& value) {
  return stream << value.str();
}

template <typename Tag>
std::ostream& operator<<(std::ostream& stream, const Ordinal<Tag>& value) {
  return stream << value.to_string();
}

inline std::ostream& operator<<(std::ostream& stream, const LogicalTick& value) {
  return stream << value.to_string();
}

}  // namespace airflow_control

namespace airflow_test {

using Body = std::function<void()>;

struct TestCase {
  std::string name;
  Body body;
};

std::vector<TestCase>& registry();

class Registrar {
 public:
  Registrar(const char* name, Body body);
};

/// Records a failure for the currently running case.
void report_failure(const char* file, int line, const std::string& expression,
                    const std::string& detail);

/// Counts one assertion.
void count_check();

/// Renders a value for a failure message. A type without a stream operator is
/// named by its RTTI name rather than silently omitted, because an omitted value
/// turns a diagnosable failure into a mystery.
template <typename T>
std::string display(const T& value) {
  if constexpr (requires(std::ostream& stream, const T& item) { stream << item; }) {
    std::ostringstream stream;
    stream << value;
    return stream.str();
  } else {
    return std::string("<value of type ") + typeid(T).name() + ">";
  }
}

std::string display(const std::string& value);
std::string display(const char* value);
std::string display(bool value);
std::string display(std::nullptr_t value);

/// The code of either a Status or a Result<T>, so a check reads the same way for
/// both. Returned by value so that a temporary in the caller's full expression
/// cannot dangle.
inline ::airflow_control::StatusCode status_code_of(const ::airflow_control::Status& value) {
  return value.code();
}

template <typename T>
::airflow_control::StatusCode status_code_of(const ::airflow_control::Result<T>& value) {
  return value.code();
}

/// The explanatory message of either a Status or a Result<T>, by the same rule.
inline std::string status_message_of(const ::airflow_control::Status& value) {
  return value.message();
}

template <typename T>
std::string status_message_of(const ::airflow_control::Result<T>& value) {
  return value.message();
}

/// Runs every registered case. Returns 1 when any check failed or when the suite
/// performed no check at all.
int run_all();

}  // namespace airflow_test

/// Declares one case. Names are unique within a test binary.
#define AIRFLOW_TEST(name)                                                     \
  static void airflow_test_body_##name();                                      \
  static const ::airflow_test::Registrar airflow_test_registrar_##name(        \
      #name, airflow_test_body_##name);                                        \
  static void airflow_test_body_##name()

/// The harness owns main(), so a test source states that fact and defines no
/// main of its own. This expands to nothing.
#define AIRFLOW_TEST_MAIN

#define CHECK(expression)                                                       \
  do {                                                                          \
    ::airflow_test::count_check();                                              \
    if (!(expression)) {                                                        \
      ::airflow_test::report_failure(__FILE__, __LINE__, #expression, "");       \
    }                                                                           \
  } while (false)

#define CHECK_EQ(left, right)                                                      \
  do {                                                                             \
    ::airflow_test::count_check();                                                 \
    /* Copied, not bound by reference: an expression such as                      \
       "engine.device(id).value().revision" yields a reference into a temporary    \
       that would already be destroyed by the time the comparison runs. */         \
    const auto airflow_left_value = (left);                                        \
    const auto airflow_right_value = (right);                                      \
    if (!(airflow_left_value == airflow_right_value)) {                            \
      ::airflow_test::report_failure(                                              \
          __FILE__, __LINE__, #left " == " #right,                                 \
          "left=" + ::airflow_test::display(airflow_left_value) +                  \
              " right=" + ::airflow_test::display(airflow_right_value));           \
    }                                                                              \
  } while (false)

#define CHECK_NE(left, right)                                                 \
  do {                                                                        \
    ::airflow_test::count_check();                                            \
    const auto airflow_left_value = (left);                                   \
    const auto airflow_right_value = (right);                                 \
    if (airflow_left_value == airflow_right_value) {                          \
      ::airflow_test::report_failure(__FILE__, __LINE__, #left " != " #right,  \
                                     "both=" +                               \
                                         ::airflow_test::display(airflow_left_value)); \
    }                                                                         \
  } while (false)

/// The expression must report exactly this status code. Accepts either a Status
/// or a Result<T>.
#define CHECK_STATUS(expression, expected)                                          \
  do {                                                                              \
    ::airflow_test::count_check();                                                  \
    const ::airflow_control::StatusCode airflow_expected_code = (expected);          \
    const ::airflow_control::StatusCode airflow_actual_code =                        \
        ::airflow_test::status_code_of(expression);                                  \
    if (airflow_actual_code != airflow_expected_code) {                              \
      ::airflow_test::report_failure(                                                \
          __FILE__, __LINE__, #expression " reports " #expected,                     \
          "expected=" + ::airflow_test::display(airflow_expected_code) +             \
              " actual=" + ::airflow_test::display(airflow_actual_code) + " (" +     \
              ::airflow_test::status_message_of(expression) + ")");                  \
    }                                                                                \
  } while (false)

/// The expression must fail with exactly this code. Spelled separately from
/// CHECK_STATUS because the intent at the call site is a refusal, and a reader
/// should not have to read the expected code to see that.
#define AIRFLOW_TEST_FAILS_WITH(expression, expected) CHECK_STATUS(expression, expected)

/// States a condition the rest of the case depends on. When it does not hold the
/// failure is recorded and the case returns, because every later check in it
/// would be about state that was never established. Nothing is thrown.
#define REQUIRE(expression)                                                        \
  do {                                                                             \
    ::airflow_test::count_check();                                                 \
    if (!(expression)) {                                                           \
      ::airflow_test::report_failure(__FILE__, __LINE__, #expression,               \
                                     "required for the rest of this case");         \
      return;                                                                      \
    }                                                                              \
  } while (false)
