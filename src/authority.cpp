#include "airflow_control/authority.hpp"

#include <array>
#include <cstddef>
#include <string>
#include <string_view>

namespace airflow_control {
namespace {

constexpr std::array<const char*, kControlActionCount> kActionNames{{
    "apply_setpoint", "raise_airflow", "lower_airflow", "change_pressure_target",
    "service_containment", "clear_breach", "emergency_purge", "release_hold",
    "set_lifecycle", "set_policy",
}};

constexpr std::array<const char*, 2> kInterlockClassNames{{"protected", "advisory"}};
constexpr std::array<const char*, 3> kInterlockStateNames{{"satisfied", "open", "unknown"}};

}  // namespace

const char* to_string(ControlAction action) noexcept {
  const auto index = static_cast<std::size_t>(action);
  if (index >= kActionNames.size()) {
    return "unknown";
  }
  return kActionNames[index];
}

std::optional<ControlAction> parse_control_action(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kActionNames.size(); ++index) {
    if (token == kActionNames[index]) {
      return static_cast<ControlAction>(index);
    }
  }
  return std::nullopt;
}

ControlAction required_action(ControlIntent intent) noexcept {
  switch (intent) {
    case ControlIntent::hold_setpoint:
      return ControlAction::apply_setpoint;
    case ControlIntent::raise_airflow:
      return ControlAction::raise_airflow;
    case ControlIntent::lower_airflow:
      return ControlAction::lower_airflow;
    case ControlIntent::restore_pressure_relationship:
      return ControlAction::change_pressure_target;
    case ControlIntent::rebalance_rows:
      return ControlAction::apply_setpoint;
    case ControlIntent::trim_for_efficiency:
      return ControlAction::lower_airflow;
    case ControlIntent::emergency_purge:
      return ControlAction::emergency_purge;
    case ControlIntent::release_to_policy:
      return ControlAction::release_hold;
  }
  return ControlAction::apply_setpoint;
}

ActionSet ActionSet::all() noexcept {
  return ActionSet((1u << kControlActionCount) - 1u);
}

Result<ActionSet> ActionSet::from_mask(std::uint32_t mask) noexcept {
  const std::uint32_t permitted = (1u << kControlActionCount) - 1u;
  if ((mask & ~permitted) != 0u) {
    return Status::failure(StatusCode::invalid_argument,
                           "action mask " + std::to_string(mask) + " sets an undefined action bit");
  }
  return ActionSet(mask);
}

ActionSet ActionSet::of(std::initializer_list<ControlAction> actions) noexcept {
  std::uint32_t mask = 0;
  for (const ControlAction action : actions) {
    mask |= (1u << static_cast<std::uint32_t>(action));
  }
  return ActionSet(mask);
}

bool ActionSet::contains(ControlAction action) const noexcept {
  const std::size_t index = static_cast<std::size_t>(action);
  if (index >= kControlActionCount) {
    return false;
  }
  return (mask_ & (1u << index)) != 0u;
}

std::string ActionSet::to_string() const {
  std::string rendered;
  for (std::size_t index = 0; index < kControlActionCount; ++index) {
    if ((mask_ & (1u << index)) == 0u) {
      continue;
    }
    if (!rendered.empty()) {
      rendered += ',';
    }
    rendered += kActionNames[index];
  }
  return rendered;
}

const char* to_string(InterlockClass klass) noexcept {
  const auto index = static_cast<std::size_t>(klass);
  if (index >= kInterlockClassNames.size()) {
    return "unknown";
  }
  return kInterlockClassNames[index];
}

const char* to_string(InterlockState state) noexcept {
  const auto index = static_cast<std::size_t>(state);
  if (index >= kInterlockStateNames.size()) {
    return "unknown";
  }
  return kInterlockStateNames[index];
}

std::optional<InterlockClass> parse_interlock_class(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kInterlockClassNames.size(); ++index) {
    if (token == kInterlockClassNames[index]) {
      return static_cast<InterlockClass>(index);
    }
  }
  return std::nullopt;
}

std::optional<InterlockState> parse_interlock_state(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kInterlockStateNames.size(); ++index) {
    if (token == kInterlockStateNames[index]) {
      return static_cast<InterlockState>(index);
    }
  }
  return std::nullopt;
}

}  // namespace airflow_control
