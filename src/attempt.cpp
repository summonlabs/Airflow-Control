#include "airflow_control/attempt.hpp"

#include <array>
#include <cstddef>
#include <string_view>

namespace airflow_control {
namespace {

constexpr std::array<const char*, 13> kAttemptStateNames{{
    "accepted", "dispatched", "acknowledged", "refused", "unavailable", "faulted", "indeterminate",
    "effect_established", "effect_contradicted", "effect_indeterminate", "recovery_required",
    "resolved_without_effect", "superseded",
}};

constexpr std::array<const char*, 4> kEffectStateNames{{
    "unverified", "effective", "contradicted", "indeterminate",
}};

}  // namespace

const char* to_string(AttemptState state) noexcept {
  const auto index = static_cast<std::size_t>(state);
  if (index >= kAttemptStateNames.size()) {
    return "accepted";
  }
  return kAttemptStateNames[index];
}

std::optional<AttemptState> parse_attempt_state(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kAttemptStateNames.size(); ++index) {
    if (token == kAttemptStateNames[index]) {
      return static_cast<AttemptState>(index);
    }
  }
  return std::nullopt;
}

bool is_unresolved(AttemptState state) noexcept {
  // The list is deliberately conservative. effect_indeterminate is unresolved
  // because verification ran and could not decide, which is not the same as
  // knowing the device did not move; a caller resolves it explicitly.
  switch (state) {
    case AttemptState::accepted:
    case AttemptState::dispatched:
    case AttemptState::acknowledged:
    case AttemptState::indeterminate:
    case AttemptState::effect_indeterminate:
    case AttemptState::recovery_required:
      return true;
    case AttemptState::refused:
    case AttemptState::unavailable:
    case AttemptState::faulted:
    case AttemptState::effect_established:
    case AttemptState::effect_contradicted:
    case AttemptState::resolved_without_effect:
    case AttemptState::superseded:
      return false;
  }
  return true;
}

bool is_definite(AttemptState state) noexcept {
  return state == AttemptState::refused || state == AttemptState::unavailable ||
         state == AttemptState::faulted;
}

const char* to_string(EffectState state) noexcept {
  const auto index = static_cast<std::size_t>(state);
  if (index >= kEffectStateNames.size()) {
    return "unverified";
  }
  return kEffectStateNames[index];
}

DeliveryComparison compare_delivery(const SetpointRequest& candidate,
                                    const SetpointRequest& incumbent) noexcept {
  // A percent request and an airflow request are not ordered here: relating
  // them needs the fan curve, which belongs to the device, not to this runtime.
  // Refusing to order them is the honest answer.
  if (const auto* candidate_percent = std::get_if<SetpointPercent>(&candidate)) {
    const auto* incumbent_percent = std::get_if<SetpointPercent>(&incumbent);
    if (incumbent_percent == nullptr) {
      return DeliveryComparison::incomparable;
    }
    const std::uint32_t lhs = candidate_percent->percent.basis_points();
    const std::uint32_t rhs = incumbent_percent->percent.basis_points();
    if (lhs == rhs) {
      return DeliveryComparison::equal;
    }
    return lhs > rhs ? DeliveryComparison::increases : DeliveryComparison::decreases;
  }

  const auto* candidate_airflow = std::get_if<SetpointAirflow>(&candidate);
  const auto* incumbent_airflow = std::get_if<SetpointAirflow>(&incumbent);
  if (incumbent_airflow == nullptr) {
    return DeliveryComparison::incomparable;
  }
  const std::int64_t lhs = candidate_airflow->airflow.cubic_metres_per_hour();
  const std::int64_t rhs = incumbent_airflow->airflow.cubic_metres_per_hour();
  if (lhs == rhs) {
    return DeliveryComparison::equal;
  }
  return lhs > rhs ? DeliveryComparison::increases : DeliveryComparison::decreases;
}

bool supersession_permitted(RequestClass candidate_class, bool has_safety_permit,
                            DeliveryComparison comparison) noexcept {
  // An emergency request may take over an unresolved attempt, because waiting
  // for the previous command's effect is itself the hazard, and the authority
  // that owns emergency policy has said so in an explicit permit.
  if (candidate_class == RequestClass::safety_directed && has_safety_permit) {
    return true;
  }
  // Otherwise the new command must not reduce delivered airflow. A reduction
  // could leave the room with less air than the unresolved command may already
  // have established, and no evidence exists to say which state the plant is
  // in.
  return comparison == DeliveryComparison::increases || comparison == DeliveryComparison::equal;
}

}  // namespace airflow_control
