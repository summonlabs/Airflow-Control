#include "airflow_control/lifecycle.hpp"

#include <array>
#include <cstddef>
#include <string_view>

namespace airflow_control {
namespace {

// The complete declared transition table. Nothing outside it is legal, and the
// self transition is legal for every state because re-asserting the current
// state is a no-op rather than a change.
constexpr std::array<LifecycleTransition, 24> kTransitions{{
    {DeviceLifecycle::provisioned, DeviceLifecycle::active, TransitionClass::commission},
    {DeviceLifecycle::provisioned, DeviceLifecycle::retired, TransitionClass::administrative},

    {DeviceLifecycle::active, DeviceLifecycle::degraded, TransitionClass::fault},
    {DeviceLifecycle::active, DeviceLifecycle::maintenance, TransitionClass::service},
    {DeviceLifecycle::active, DeviceLifecycle::isolated, TransitionClass::administrative},
    {DeviceLifecycle::active, DeviceLifecycle::faulted, TransitionClass::fault},
    {DeviceLifecycle::active, DeviceLifecycle::retired, TransitionClass::administrative},

    {DeviceLifecycle::degraded, DeviceLifecycle::active, TransitionClass::recovery},
    {DeviceLifecycle::degraded, DeviceLifecycle::maintenance, TransitionClass::service},
    {DeviceLifecycle::degraded, DeviceLifecycle::isolated, TransitionClass::administrative},
    {DeviceLifecycle::degraded, DeviceLifecycle::faulted, TransitionClass::fault},
    {DeviceLifecycle::degraded, DeviceLifecycle::retired, TransitionClass::administrative},

    {DeviceLifecycle::maintenance, DeviceLifecycle::active, TransitionClass::service},
    {DeviceLifecycle::maintenance, DeviceLifecycle::degraded, TransitionClass::fault},
    {DeviceLifecycle::maintenance, DeviceLifecycle::isolated, TransitionClass::administrative},
    {DeviceLifecycle::maintenance, DeviceLifecycle::retired, TransitionClass::administrative},

    {DeviceLifecycle::isolated, DeviceLifecycle::active, TransitionClass::recovery},
    {DeviceLifecycle::isolated, DeviceLifecycle::maintenance, TransitionClass::service},
    {DeviceLifecycle::isolated, DeviceLifecycle::faulted, TransitionClass::fault},
    {DeviceLifecycle::isolated, DeviceLifecycle::retired, TransitionClass::administrative},

    {DeviceLifecycle::faulted, DeviceLifecycle::active, TransitionClass::recovery},
    {DeviceLifecycle::faulted, DeviceLifecycle::isolated, TransitionClass::administrative},
    {DeviceLifecycle::faulted, DeviceLifecycle::maintenance, TransitionClass::service},
    {DeviceLifecycle::faulted, DeviceLifecycle::retired, TransitionClass::administrative},
}};

constexpr std::array<const char*, 7> kLifecycleNames{{
    "provisioned", "active", "maintenance", "degraded", "isolated", "faulted", "retired",
}};

constexpr std::array<const char*, 5> kTransitionClassNames{{
    "commission", "service", "recovery", "administrative", "fault",
}};

}  // namespace

const LifecycleTransition* declared_transitions() noexcept { return kTransitions.data(); }

std::size_t declared_transition_count() noexcept { return kTransitions.size(); }

bool is_declared_transition(DeviceLifecycle from, DeviceLifecycle to) noexcept {
  if (from == to) {
    return true;
  }
  return transition_class(from, to).has_value();
}

std::optional<TransitionClass> transition_class(DeviceLifecycle from, DeviceLifecycle to) noexcept {
  for (const LifecycleTransition& entry : kTransitions) {
    if (entry.from == from && entry.to == to) {
      return entry.klass;
    }
  }
  return std::nullopt;
}

bool permits_control(DeviceLifecycle state) noexcept {
  return state == DeviceLifecycle::active || state == DeviceLifecycle::degraded;
}

bool requires_maintenance_override(DeviceLifecycle state) noexcept {
  return state == DeviceLifecycle::maintenance;
}

bool is_terminal(DeviceLifecycle state) noexcept { return state == DeviceLifecycle::retired; }

const char* to_string(DeviceLifecycle state) noexcept {
  const auto index = static_cast<std::size_t>(state);
  if (index >= kLifecycleNames.size()) {
    return "unknown";
  }
  return kLifecycleNames[index];
}

const char* to_string(TransitionClass klass) noexcept {
  const auto index = static_cast<std::size_t>(klass);
  if (index >= kTransitionClassNames.size()) {
    return "unknown";
  }
  return kTransitionClassNames[index];
}

std::optional<DeviceLifecycle> parse_lifecycle(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kLifecycleNames.size(); ++index) {
    if (token == kLifecycleNames[index]) {
      return static_cast<DeviceLifecycle>(index);
    }
  }
  return std::nullopt;
}

std::optional<TransitionClass> parse_transition_class(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kTransitionClassNames.size(); ++index) {
    if (token == kTransitionClassNames[index]) {
      return static_cast<TransitionClass>(index);
    }
  }
  return std::nullopt;
}

}  // namespace airflow_control
