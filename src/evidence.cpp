#include "airflow_control/evidence.hpp"

#include <array>
#include <cstddef>
#include <string_view>

namespace airflow_control {
namespace {

constexpr std::array<const char*, 4> kQualityNames{{"good", "suspect", "bad", "unknown"}};
constexpr std::array<const char*, 3> kKindNames{{"airflow", "pressure", "fan_setpoint"}};

}  // namespace

const char* to_string(Quality quality) noexcept {
  const auto index = static_cast<std::size_t>(quality);
  if (index >= kQualityNames.size()) {
    return "unknown";
  }
  return kQualityNames[index];
}

std::optional<Quality> parse_quality(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kQualityNames.size(); ++index) {
    if (token == kQualityNames[index]) {
      return static_cast<Quality>(index);
    }
  }
  return std::nullopt;
}

ObservationKind observation_kind(const ObservationPayload& payload) noexcept {
  if (std::holds_alternative<AirflowReading>(payload)) {
    return ObservationKind::airflow;
  }
  if (std::holds_alternative<PressureReading>(payload)) {
    return ObservationKind::pressure;
  }
  return ObservationKind::fan_setpoint;
}

const char* to_string(ObservationKind kind) noexcept {
  const auto index = static_cast<std::size_t>(kind);
  if (index >= kKindNames.size()) {
    return "unknown";
  }
  return kKindNames[index];
}

std::optional<ObservationKind> parse_observation_kind(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kKindNames.size(); ++index) {
    if (token == kKindNames[index]) {
      return static_cast<ObservationKind>(index);
    }
  }
  return std::nullopt;
}

std::string to_string(const ObservationPayload& payload) {
  if (const auto* airflow = std::get_if<AirflowReading>(&payload)) {
    return "airflow " + airflow->value.to_string();
  }
  if (const auto* pressure = std::get_if<PressureReading>(&payload)) {
    return "pressure " + pressure->differential.to_string();
  }
  const auto* fan = std::get_if<FanReading>(&payload);
  return "fan_setpoint " + fan->percent.to_string();
}

const char* to_string(FreshnessVerdict verdict) noexcept {
  switch (verdict) {
    case FreshnessVerdict::fresh:
      return "fresh";
    case FreshnessVerdict::stale:
      return "stale";
    case FreshnessVerdict::future:
      return "future";
    case FreshnessVerdict::quality_insufficient:
      return "quality_insufficient";
    case FreshnessVerdict::wrong_device_generation:
      return "wrong_device_generation";
    case FreshnessVerdict::wrong_evidence_generation:
      return "wrong_evidence_generation";
    case FreshnessVerdict::recovered:
      return "recovered";
  }
  return "unknown";
}

StatusCode freshness_status(FreshnessVerdict verdict) noexcept {
  switch (verdict) {
    case FreshnessVerdict::fresh:
      return StatusCode::ok;
    case FreshnessVerdict::stale:
      return StatusCode::evidence_stale;
    case FreshnessVerdict::future:
      return StatusCode::evidence_future;
    case FreshnessVerdict::quality_insufficient:
      return StatusCode::evidence_quality_insufficient;
    case FreshnessVerdict::wrong_device_generation:
      return StatusCode::generation_mismatch;
    case FreshnessVerdict::wrong_evidence_generation:
      return StatusCode::evidence_generation_mismatch;
    case FreshnessVerdict::recovered:
      return StatusCode::evidence_stale;
  }
  return StatusCode::evidence_unknown;
}

FreshnessVerdict assess_freshness(const Observation& observation,
                                  const FreshnessRequirements& requirements) noexcept {
  // The order is fixed, so the same observation and the same requirement always
  // produce the same verdict rather than the first one a caller happened to
  // check.
  if (observation.draft.evidence_generation != requirements.evidence_generation) {
    return FreshnessVerdict::wrong_evidence_generation;
  }
  if (observation.draft.device_generation != requirements.device_generation) {
    return FreshnessVerdict::wrong_device_generation;
  }
  if (observation.draft.quality != Quality::good) {
    return FreshnessVerdict::quality_insufficient;
  }
  if (observation.recovered && !requirements.allow_recovered) {
    return FreshnessVerdict::recovered;
  }
  if (observation.draft.measured_at > requirements.now) {
    return FreshnessVerdict::future;
  }
  const std::uint64_t age = requirements.now.value() - observation.draft.measured_at.value();
  if (age > requirements.max_age_ticks.value()) {
    return FreshnessVerdict::stale;
  }
  return FreshnessVerdict::fresh;
}

bool is_proof(const FreshnessVerdict verdict) noexcept { return verdict == FreshnessVerdict::fresh; }

bool is_bound(const Observation& observation, const EvidenceBinding& binding) noexcept {
  if (binding.measured_at_or_after.has_value() &&
      observation.draft.measured_at < *binding.measured_at_or_after) {
    return false;
  }
  if (binding.accepted_after.has_value() && !(observation.accepted_at > *binding.accepted_after)) {
    return false;
  }
  return true;
}

}  // namespace airflow_control
