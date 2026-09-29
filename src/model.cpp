#include "airflow_control/model.hpp"

#include <array>
#include <cstddef>
#include <string_view>

namespace airflow_control {
namespace {

constexpr std::array<const char*, 3> kPolarityNames{{"positive", "negative", "neutral"}};
constexpr std::array<const char*, 4> kContainmentStateNames{{
    "intact", "open_for_service", "breached", "unknown",
}};
constexpr std::array<const char*, 6> kContainmentKindNames{{
    "aisle_containment", "rack_chimney", "blanking_panel", "subfloor_barrier", "door", "other",
}};
constexpr std::array<const char*, 3> kObligationScopeNames{{"room", "row", "rack"}};
constexpr std::array<const char*, 2> kObligationClassNames{{"protected", "advisory"}};
constexpr std::array<const char*, 2> kObligationBindingNames{{"metered", "device_sum"}};
constexpr std::array<const char*, 8> kIntentNames{{
    "hold_setpoint", "raise_airflow", "lower_airflow", "restore_pressure_relationship",
    "rebalance_rows", "trim_for_efficiency", "emergency_purge", "release_to_policy",
}};
constexpr std::array<const char*, 3> kRequestClassNames{{"optimization", "protective", "safety_directed"}};
constexpr std::array<const char*, 2> kSetpointKindNames{{"fan_percent", "airflow"}};

std::int64_t absolute(std::int64_t value) noexcept {
  return value < 0 ? -value : value;
}

}  // namespace

// ---------------------------------------------------------------------------
// Pressure polarities and bands
// ---------------------------------------------------------------------------

const char* to_string(PressurePolarity polarity) noexcept {
  const auto index = static_cast<std::size_t>(polarity);
  if (index >= kPolarityNames.size()) {
    return "unknown";
  }
  return kPolarityNames[index];
}

std::optional<PressurePolarity> parse_pressure_polarity(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kPolarityNames.size(); ++index) {
    if (token == kPolarityNames[index]) {
      return static_cast<PressurePolarity>(index);
    }
  }
  return std::nullopt;
}

PressurePolarity classify(Pressure differential) noexcept {
  if (differential.millipascals() > 0) {
    return PressurePolarity::positive;
  }
  if (differential.millipascals() < 0) {
    return PressurePolarity::negative;
  }
  return PressurePolarity::neutral;
}

Result<PressureBand> PressureBand::create(PressurePolarity polarity, Pressure lower, Pressure upper,
                                          Pressure tolerance) noexcept {
  if (!is_representable(lower) || !is_representable(upper) || !is_representable(tolerance)) {
    return Status::failure(StatusCode::pressure_band_invalid,
                           "pressure band contains a value outside the representable range");
  }
  if (lower > upper) {
    return Status::failure(StatusCode::pressure_band_invalid,
                           "pressure band lower bound " + lower.to_string() +
                               " exceeds upper bound " + upper.to_string());
  }
  if (tolerance.millipascals() < 0) {
    return Status::failure(StatusCode::pressure_band_invalid, "pressure tolerance is negative");
  }
  if (tolerance.millipascals() > PhysicalBounds::max_pressure_tolerance_millipascals) {
    return Status::failure(StatusCode::pressure_band_invalid,
                           "pressure tolerance " + tolerance.to_string() +
                               " exceeds the structural maximum");
  }

  const std::int64_t low = lower.millipascals();
  const std::int64_t high = upper.millipascals();
  switch (polarity) {
    case PressurePolarity::positive:
      if (low <= 0) {
        return Status::failure(StatusCode::pressure_band_invalid,
                               "a positive pressure band must lie strictly above zero, but its lower "
                               "bound is " + lower.to_string());
      }
      break;
    case PressurePolarity::negative:
      if (high >= 0) {
        return Status::failure(StatusCode::pressure_band_invalid,
                               "a negative pressure band must lie strictly below zero, but its upper "
                               "bound is " + upper.to_string());
      }
      break;
    case PressurePolarity::neutral:
      if (low > 0 || high < 0) {
        return Status::failure(StatusCode::pressure_band_invalid,
                               "a neutral pressure band must straddle zero, but the band " +
                                   lower.to_string() + " .. " + upper.to_string() + " does not");
      }
      if (absolute(low) > PhysicalBounds::max_neutral_band_millipascals ||
          absolute(high) > PhysicalBounds::max_neutral_band_millipascals) {
        return Status::failure(StatusCode::pressure_band_invalid,
                               "a neutral pressure band may not exceed the structural neutral bound");
      }
      break;
  }
  return PressureBand(polarity, lower, upper, tolerance);
}

bool PressureBand::contains(Pressure differential) const noexcept {
  return differential >= lower_ && differential <= upper_;
}

bool PressureBand::sign_matches(Pressure differential) const noexcept {
  return classify(differential) == polarity_;
}

bool PressureBand::satisfied_by(Pressure differential) const noexcept {
  return sign_matches(differential) && contains(differential);
}

std::string PressureBand::to_string() const {
  return std::string(airflow_control::to_string(polarity_)) + " " + lower_.to_string() + " .. " +
         upper_.to_string() + " tolerance " + tolerance_.to_string();
}

// ---------------------------------------------------------------------------
// Containment
// ---------------------------------------------------------------------------

std::uint32_t containment_rank(ContainmentState state) noexcept {
  switch (state) {
    case ContainmentState::intact:
      return 0;
    case ContainmentState::open_for_service:
      return 1;
    case ContainmentState::unknown:
      return 2;
    case ContainmentState::breached:
      return 3;
  }
  return 3;
}

bool is_known(ContainmentState state) noexcept { return state != ContainmentState::unknown; }

const char* to_string(ContainmentState state) noexcept {
  const auto index = static_cast<std::size_t>(state);
  if (index >= kContainmentStateNames.size()) {
    return "unknown";
  }
  return kContainmentStateNames[index];
}

std::optional<ContainmentState> parse_containment_state(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kContainmentStateNames.size(); ++index) {
    if (token == kContainmentStateNames[index]) {
      return static_cast<ContainmentState>(index);
    }
  }
  return std::nullopt;
}

const char* to_string(ContainmentKind kind) noexcept {
  const auto index = static_cast<std::size_t>(kind);
  if (index >= kContainmentKindNames.size()) {
    return "other";
  }
  return kContainmentKindNames[index];
}

std::optional<ContainmentKind> parse_containment_kind(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kContainmentKindNames.size(); ++index) {
    if (token == kContainmentKindNames[index]) {
      return static_cast<ContainmentKind>(index);
    }
  }
  return std::nullopt;
}

// ---------------------------------------------------------------------------
// Obligations
// ---------------------------------------------------------------------------

const char* to_string(ObligationScope scope) noexcept {
  const auto index = static_cast<std::size_t>(scope);
  if (index >= kObligationScopeNames.size()) {
    return "unknown";
  }
  return kObligationScopeNames[index];
}

const char* to_string(ObligationClass klass) noexcept {
  const auto index = static_cast<std::size_t>(klass);
  if (index >= kObligationClassNames.size()) {
    return "unknown";
  }
  return kObligationClassNames[index];
}

const char* to_string(ObligationBinding binding) noexcept {
  const auto index = static_cast<std::size_t>(binding);
  if (index >= kObligationBindingNames.size()) {
    return "unknown";
  }
  return kObligationBindingNames[index];
}

std::optional<ObligationScope> parse_obligation_scope(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kObligationScopeNames.size(); ++index) {
    if (token == kObligationScopeNames[index]) {
      return static_cast<ObligationScope>(index);
    }
  }
  return std::nullopt;
}

std::optional<ObligationClass> parse_obligation_class(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kObligationClassNames.size(); ++index) {
    if (token == kObligationClassNames[index]) {
      return static_cast<ObligationClass>(index);
    }
  }
  return std::nullopt;
}

std::optional<ObligationBinding> parse_obligation_binding(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kObligationBindingNames.size(); ++index) {
    if (token == kObligationBindingNames[index]) {
      return static_cast<ObligationBinding>(index);
    }
  }
  return std::nullopt;
}

// ---------------------------------------------------------------------------
// Envelopes
// ---------------------------------------------------------------------------

Status validate_envelope(const OperatingEnvelope& envelope) {
  if (envelope.min_fan_percent > envelope.max_fan_percent) {
    return Status::failure(StatusCode::limit_invalid,
                           "envelope fan interval is inverted: " + envelope.min_fan_percent.to_string() +
                               " .. " + envelope.max_fan_percent.to_string());
  }
  if (envelope.default_fan_percent < envelope.min_fan_percent ||
      envelope.default_fan_percent > envelope.max_fan_percent) {
    return Status::failure(StatusCode::limit_invalid,
                           "envelope default fan setpoint " + envelope.default_fan_percent.to_string() +
                               " lies outside " + envelope.min_fan_percent.to_string() + " .. " +
                               envelope.max_fan_percent.to_string());
  }
  if (envelope.min_airflow > envelope.max_airflow) {
    return Status::failure(StatusCode::limit_invalid,
                           "envelope airflow interval is inverted: " + envelope.min_airflow.to_string() +
                               " .. " + envelope.max_airflow.to_string());
  }
  if (!is_representable(envelope.min_airflow) || !is_representable(envelope.max_airflow)) {
    return Status::failure(StatusCode::limit_invalid,
                           "envelope airflow limit is outside the representable range");
  }
  // SlewBasisPoints and SetpointBasisPoints cannot hold an unrepresentable
  // value: their constructors are the only way to make one, and they bound the
  // value. Nothing further to check here, which is the point of the types.
  return Status::success();
}

// ---------------------------------------------------------------------------
// Intents
// ---------------------------------------------------------------------------

const char* to_string(ControlIntent intent) noexcept {
  const auto index = static_cast<std::size_t>(intent);
  if (index >= kIntentNames.size()) {
    return "unknown";
  }
  return kIntentNames[index];
}

const char* to_string(RequestClass klass) noexcept {
  const auto index = static_cast<std::size_t>(klass);
  if (index >= kRequestClassNames.size()) {
    return "unknown";
  }
  return kRequestClassNames[index];
}

std::optional<ControlIntent> parse_control_intent(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kIntentNames.size(); ++index) {
    if (token == kIntentNames[index]) {
      return static_cast<ControlIntent>(index);
    }
  }
  return std::nullopt;
}

RequestClass classify(ControlIntent intent) noexcept {
  // A reduction of delivered airflow, or a redistribution of it, is an
  // optimization and is held to the strictest evidence standard. Anything that
  // maintains, increases, or restores airflow is protective. Emergency
  // ventilation is directed by the authority that owns emergency policy and
  // needs its explicit permit.
  switch (intent) {
    case ControlIntent::hold_setpoint:
    case ControlIntent::raise_airflow:
    case ControlIntent::restore_pressure_relationship:
    case ControlIntent::release_to_policy:
      return RequestClass::protective;
    case ControlIntent::lower_airflow:
    case ControlIntent::rebalance_rows:
    case ControlIntent::trim_for_efficiency:
      return RequestClass::optimization;
    case ControlIntent::emergency_purge:
      return RequestClass::safety_directed;
  }
  return RequestClass::protective;
}

bool requires_pressure_proof(ControlIntent intent) noexcept {
  return intent == ControlIntent::restore_pressure_relationship;
}

bool requires_containment_proof(ControlIntent intent) noexcept {
  return classify(intent) == RequestClass::optimization;
}

bool requires_safety_permit(ControlIntent intent) noexcept {
  return classify(intent) == RequestClass::safety_directed;
}

bool changes_setpoint(ControlIntent intent) noexcept { return intent != ControlIntent::release_to_policy; }

// ---------------------------------------------------------------------------
// Setpoints
// ---------------------------------------------------------------------------

SetpointKind setpoint_kind(const SetpointRequest& request) noexcept {
  return std::holds_alternative<SetpointPercent>(request) ? SetpointKind::fan_percent
                                                          : SetpointKind::airflow;
}

const char* to_string(SetpointKind kind) noexcept {
  const auto index = static_cast<std::size_t>(kind);
  if (index >= kSetpointKindNames.size()) {
    return "unknown";
  }
  return kSetpointKindNames[index];
}

std::string to_string(const SetpointRequest& request) {
  if (const auto* percent = std::get_if<SetpointPercent>(&request)) {
    return "fan_percent " + percent->percent.to_string();
  }
  const auto* airflow = std::get_if<SetpointAirflow>(&request);
  return "airflow " + airflow->airflow.to_string();
}

std::optional<SetpointKind> parse_setpoint_kind(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kSetpointKindNames.size(); ++index) {
    if (token == kSetpointKindNames[index]) {
      return static_cast<SetpointKind>(index);
    }
  }
  return std::nullopt;
}

SetpointRequest policy_default_setpoint(const FanPolicy& policy) {
  return SetpointPercent{policy.envelope.default_fan_percent};
}

}  // namespace airflow_control
