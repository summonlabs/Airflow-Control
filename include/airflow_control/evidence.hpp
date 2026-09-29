#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "airflow_control/ids.hpp"
#include "airflow_control/status.hpp"
#include "airflow_control/units.hpp"

namespace airflow_control {

/// What a source says about the reliability of its own reading.
///
/// Only good evidence can establish a safety condition. suspect, bad, and
/// unknown are recorded and reported but never promoted.
enum class Quality : std::uint32_t {
  good = 0,
  suspect = 1,
  bad = 2,
  unknown = 3,
};

[[nodiscard]] const char* to_string(Quality quality) noexcept;
[[nodiscard]] std::optional<Quality> parse_quality(std::string_view token) noexcept;

struct AirflowReading {
  Airflow value;
};

struct PressureReading {
  Pressure differential;
};

struct FanReading {
  SetpointBasisPoints percent;
};

using ObservationPayload = std::variant<AirflowReading, PressureReading, FanReading>;

enum class ObservationKind : std::uint32_t {
  airflow = 0,
  pressure = 1,
  fan_setpoint = 2,
};

[[nodiscard]] ObservationKind observation_kind(const ObservationPayload& payload) noexcept;
[[nodiscard]] const char* to_string(ObservationKind kind) noexcept;
[[nodiscard]] std::optional<ObservationKind> parse_observation_kind(std::string_view token) noexcept;
[[nodiscard]] std::string to_string(const ObservationPayload& payload);

/// A reading as reported by a source, before this runtime has accepted it.
///
/// A draft carries no identity and no acceptance instant: an adapter cannot
/// fabricate an ObservationId, and it cannot claim that its reading was
/// accepted at a time this runtime did not choose.
struct ObservationDraft {
  ObservationPayload payload;
  AirflowDeviceId device;
  std::optional<PressureRelationshipId> relationship;
  SpaceRefId point;
  SourceId source;
  EvidenceSequence sequence;
  LogicalTick measured_at;
  DeviceGeneration device_generation;
  EvidenceGeneration evidence_generation;
  Quality quality = Quality::good;
};

/// A reading accepted into the evidence set.
///
/// recovered is true when the value was restored from durable state rather than
/// produced by a source in this incarnation. A recovered observation is never
/// fresh: recovered dynamic state is not current physical evidence.
struct Observation {
  ObservationId id;
  ObservationDraft draft;
  LogicalTick accepted_at;
  bool recovered = false;
};

/// What must hold for an observation to count as proof of a physical effect.
struct FreshnessRequirements {
  LogicalTick now;
  LogicalTick max_age_ticks;
  DeviceGeneration device_generation;
  EvidenceGeneration evidence_generation;
  bool allow_recovered = false;
};

/// Why an observation is or is not usable as proof.
enum class FreshnessVerdict : std::uint32_t {
  fresh = 0,
  stale = 1,
  future = 2,
  quality_insufficient = 3,
  wrong_device_generation = 4,
  wrong_evidence_generation = 5,
  recovered = 6,
};

[[nodiscard]] const char* to_string(FreshnessVerdict verdict) noexcept;
[[nodiscard]] StatusCode freshness_status(FreshnessVerdict verdict) noexcept;

/// Assesses an observation against a freshness requirement.
///
/// The order of the checks is fixed, so the same observation and the same
/// requirement always produce the same verdict.
[[nodiscard]] FreshnessVerdict assess_freshness(const Observation& observation,
                                                const FreshnessRequirements& requirements) noexcept;

/// True when the verdict permits the observation to be used as proof.
[[nodiscard]] bool is_proof(const FreshnessVerdict verdict) noexcept;

/// The ordering an observation must satisfy to be bound to one attempt.
///
/// Freshness alone is not enough to verify a command: a reading that was
/// already in hand when the command was issued is fresh but is not evidence
/// about the command. Binding states the two orderings that make a reading
/// evidence about a specific attempt.
struct EvidenceBinding {
  /// The reading must have been measured at or after this instant. Absent means
  /// the reading is not required to post-date anything.
  std::optional<LogicalTick> measured_at_or_after;
  /// The reading must have been accepted strictly after this instant. Absent
  /// means the reading is not required to have been accepted later.
  std::optional<LogicalTick> accepted_after;
};

/// True when the observation satisfies the binding.
[[nodiscard]] bool is_bound(const Observation& observation, const EvidenceBinding& binding) noexcept;

}  // namespace airflow_control
