#include "test_harness.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

// Model-layer proof suite: the device lifecycle state machine.
//
// The transition table is the whole contract. These cases walk it rather than
// sampling it, so a state or pair added without a class, or removed without a
// migration, cannot pass unnoticed.

namespace {

using namespace airflow_control;  // NOLINT(google-build-using-namespace)

const DeviceLifecycle kStates[] = {
    DeviceLifecycle::provisioned, DeviceLifecycle::active,      DeviceLifecycle::maintenance,
    DeviceLifecycle::degraded,    DeviceLifecycle::isolated,    DeviceLifecycle::faulted,
    DeviceLifecycle::retired,
};

constexpr std::size_t kStateCount = sizeof(kStates) / sizeof(kStates[0]);

const TransitionClass kClasses[] = {
    TransitionClass::commission, TransitionClass::service, TransitionClass::recovery,
    TransitionClass::administrative, TransitionClass::fault,
};

constexpr std::size_t kClassCount = sizeof(kClasses) / sizeof(kClasses[0]);

/// A token comparison by text, never by string-literal address.
std::string_view token(DeviceLifecycle state) { return to_string(state); }

}  // namespace

AIRFLOW_TEST(declared_transition_table_is_consistent) {
  const LifecycleTransition* table = declared_transitions();
  REQUIRE(table != nullptr);
  const std::size_t count = declared_transition_count();
  REQUIRE(count > 0);

  std::size_t self_entries = 0;
  for (std::size_t index = 0; index < count; ++index) {
    const LifecycleTransition& entry = table[index];
    // Every declared pair is accepted, and its class is the one written down.
    CHECK(is_declared_transition(entry.from, entry.to));
    const std::optional<TransitionClass> klass = transition_class(entry.from, entry.to);
    REQUIRE(klass.has_value());
    CHECK(klass.value() == entry.klass);
    if (entry.from == entry.to) {
      ++self_entries;
    }
    // The class is one of the five declared classes and round trips by name.
    CHECK(parse_transition_class(to_string(entry.klass)).has_value());
    CHECK(parse_transition_class(to_string(entry.klass)).value() == entry.klass);
  }
  // The self transition is legal for every state but is not an entry: it is a
  // no-op rather than a change, so it is not part of the table.
  CHECK_EQ(self_entries, std::size_t{0});

  // The accepted pairs are exactly the table plus the self transitions.
  std::size_t accepted = 0;
  for (const DeviceLifecycle from : kStates) {
    for (const DeviceLifecycle to : kStates) {
      if (is_declared_transition(from, to)) {
        ++accepted;
      }
    }
  }
  CHECK_EQ(accepted, count + kStateCount);
}

AIRFLOW_TEST(self_transitions_are_legal_and_classless) {
  for (const DeviceLifecycle state : kStates) {
    CHECK(is_declared_transition(state, state));
    // A self transition changes nothing, so it carries no class and therefore
    // requires no permission action.
    CHECK(!transition_class(state, state).has_value());
  }
}

AIRFLOW_TEST(undeclared_transitions_are_refused) {
  const DeviceLifecycle undeclared[][2] = {
      {DeviceLifecycle::retired, DeviceLifecycle::active},
      {DeviceLifecycle::retired, DeviceLifecycle::maintenance},
      {DeviceLifecycle::retired, DeviceLifecycle::isolated},
      {DeviceLifecycle::provisioned, DeviceLifecycle::degraded},
      {DeviceLifecycle::provisioned, DeviceLifecycle::faulted},
      {DeviceLifecycle::provisioned, DeviceLifecycle::maintenance},
      {DeviceLifecycle::provisioned, DeviceLifecycle::isolated},
      {DeviceLifecycle::active, DeviceLifecycle::provisioned},
      {DeviceLifecycle::degraded, DeviceLifecycle::provisioned},
      {DeviceLifecycle::isolated, DeviceLifecycle::provisioned},
      {DeviceLifecycle::isolated, DeviceLifecycle::degraded},
      {DeviceLifecycle::maintenance, DeviceLifecycle::faulted},
      {DeviceLifecycle::faulted, DeviceLifecycle::provisioned},
      {DeviceLifecycle::faulted, DeviceLifecycle::degraded},
  };
  for (const auto& pair : undeclared) {
    CHECK(!is_declared_transition(pair[0], pair[1]));
    CHECK(!transition_class(pair[0], pair[1]).has_value());
  }

  // Every state except the terminal one has somewhere to go: a state with no
  // outgoing transition except retirement-by-fiat would be a trap in the model.
  for (const DeviceLifecycle from : kStates) {
    std::size_t outgoing = 0;
    for (const DeviceLifecycle to : kStates) {
      if (!(from == to) && is_declared_transition(from, to)) {
        ++outgoing;
      }
    }
    if (is_terminal(from)) {
      CHECK_EQ(outgoing, std::size_t{0});
    } else {
      CHECK(outgoing > 0);
    }
  }
}

AIRFLOW_TEST(state_predicates_match_the_documented_states) {
  for (const DeviceLifecycle state : kStates) {
    const bool controls = permits_control(state);
    const bool maintenance = requires_maintenance_override(state);
    const bool terminal = is_terminal(state);

    CHECK_EQ(controls, state == DeviceLifecycle::active || state == DeviceLifecycle::degraded);
    CHECK_EQ(maintenance, state == DeviceLifecycle::maintenance);
    CHECK_EQ(terminal, state == DeviceLifecycle::retired);

    // The three predicates are independent, and a state that permits control is
    // never one that needs an override or has been withdrawn.
    CHECK(!(controls && maintenance));
    CHECK(!(controls && terminal));
    CHECK(!(maintenance && terminal));
  }
}

AIRFLOW_TEST(tokens_round_trip_and_out_of_range_parses_to_nothing) {
  for (const DeviceLifecycle state : kStates) {
    const std::string_view rendered = token(state);
    CHECK(!rendered.empty());
    const std::optional<DeviceLifecycle> parsed = parse_lifecycle(rendered);
    REQUIRE(parsed.has_value());
    CHECK(parsed.value() == state);
  }
  for (std::size_t left = 0; left < kStateCount; ++left) {
    for (std::size_t right = left + 1; right < kStateCount; ++right) {
      CHECK(token(kStates[left]) != token(kStates[right]));
    }
  }
  for (const TransitionClass klass : kClasses) {
    const std::string_view rendered = to_string(klass);
    CHECK(!rendered.empty());
    const std::optional<TransitionClass> parsed = parse_transition_class(rendered);
    REQUIRE(parsed.has_value());
    CHECK(parsed.value() == klass);
  }
  for (std::size_t left = 0; left < kClassCount; ++left) {
    for (std::size_t right = left + 1; right < kClassCount; ++right) {
      CHECK(std::string_view(to_string(kClasses[left])) !=
            std::string_view(to_string(kClasses[right])));
    }
  }

  // A numeric token is not a name, and an unknown name is not a state: an
  // ordinal here would let a durable store's numbering become a contract.
  CHECK(!parse_lifecycle("").has_value());
  CHECK(!parse_lifecycle("7").has_value());
  CHECK(!parse_lifecycle("0").has_value());
  CHECK(!parse_lifecycle("Active").has_value());
  CHECK(!parse_lifecycle("active ").has_value());
  CHECK(!parse_lifecycle("unknown").has_value());
  CHECK(!parse_transition_class("5").has_value());
  CHECK(!parse_transition_class("").has_value());
  CHECK(!parse_transition_class("unknown").has_value());

  // An out-of-range numeric value renders as an explicit non-token rather than
  // as a plausible state name.
  CHECK_EQ(to_string(static_cast<DeviceLifecycle>(kStateCount)), std::string_view("unknown"));
  CHECK_EQ(to_string(static_cast<DeviceLifecycle>(99)), std::string_view("unknown"));
  CHECK_EQ(to_string(static_cast<TransitionClass>(kClassCount)), std::string_view("unknown"));
  CHECK_EQ(to_string(static_cast<TransitionClass>(99)), std::string_view("unknown"));
}
