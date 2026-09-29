#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

namespace airflow_control {

/// Lifecycle of an airflow device.
///
/// The states are ordered by nothing: the safety meaning of a state is carried
/// by an explicit predicate, never by its numeric value or by its name.
enum class DeviceLifecycle : std::uint32_t {
  provisioned = 0,
  active = 1,
  maintenance = 2,
  degraded = 3,
  isolated = 4,
  faulted = 5,
  retired = 6,
};

/// The class of a lifecycle transition. The class decides which permission
/// action the caller must hold, so the action is never inferred from state
/// names.
enum class TransitionClass : std::uint32_t {
  commission = 0,
  service = 1,
  recovery = 2,
  administrative = 3,
  fault = 4,
};

struct LifecycleTransition {
  DeviceLifecycle from;
  DeviceLifecycle to;
  TransitionClass klass;
};

/// The complete declared transition table. No transition outside it is legal.
[[nodiscard]] const LifecycleTransition* declared_transitions() noexcept;
/// Number of entries in declared_transitions().
[[nodiscard]] std::size_t declared_transition_count() noexcept;

/// True when the pair is a declared transition, including the self transition
/// (which is always legal and is a no-op).
[[nodiscard]] bool is_declared_transition(DeviceLifecycle from, DeviceLifecycle to) noexcept;

/// The class of a declared transition, or nothing when the pair is not declared.
[[nodiscard]] std::optional<TransitionClass> transition_class(DeviceLifecycle from,
                                                              DeviceLifecycle to) noexcept;

/// True when the state permits control at all. active and degraded do;
/// maintenance does only with a live scoped override; the rest never do.
[[nodiscard]] bool permits_control(DeviceLifecycle state) noexcept;

/// True when the state is terminal.
[[nodiscard]] bool is_terminal(DeviceLifecycle state) noexcept;

[[nodiscard]] const char* to_string(DeviceLifecycle state) noexcept;
[[nodiscard]] const char* to_string(TransitionClass klass) noexcept;

[[nodiscard]] std::optional<DeviceLifecycle> parse_lifecycle(std::string_view token) noexcept;
[[nodiscard]] std::optional<TransitionClass> parse_transition_class(std::string_view token) noexcept;

/// True when the state requires a live maintenance override to permit control.
[[nodiscard]] bool requires_maintenance_override(DeviceLifecycle state) noexcept;

}  // namespace airflow_control
