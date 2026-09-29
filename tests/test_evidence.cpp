#include "test_harness.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

// Model-layer proof suite: evidence freshness, freshness verdicts, the binding
// orderings, and observation kinds.
//
// assess_freshness and is_bound are total functions of an observation and a
// requirement, so every ordering between the two is checked directly, including
// the exact boundary of each comparison.

namespace {

using namespace airflow_control;  // NOLINT(google-build-using-namespace)

const LogicalTick kNow = LogicalTick::from(1000);
const DeviceGeneration kDeviceGeneration = DeviceGeneration::from(7);
const EvidenceGeneration kEvidenceGeneration = EvidenceGeneration::from(3);
constexpr std::uint64_t kMaxAge = 100;

ObservationDraft make_draft(ObservationPayload payload, LogicalTick measured_at,
                            DeviceGeneration device_generation,
                            EvidenceGeneration evidence_generation, Quality quality) {
  return ObservationDraft{
      .payload = std::move(payload),
      .device = AirflowDeviceId::parse("dev-1").value(),
      .relationship = std::nullopt,
      .point = SpaceRefId::parse("space-1").value(),
      .source = SourceId::parse("source-1").value(),
      .sequence = EvidenceSequence::from(1),
      .measured_at = measured_at,
      .device_generation = device_generation,
      .evidence_generation = evidence_generation,
      .quality = quality,
  };
}

Observation make_observation(ObservationPayload payload, LogicalTick measured_at,
                             LogicalTick accepted_at, bool recovered) {
  return Observation{
      .id = ObservationId::from(1),
      .draft = make_draft(std::move(payload), measured_at, kDeviceGeneration, kEvidenceGeneration,
                          Quality::good),
      .accepted_at = accepted_at,
      .recovered = recovered,
  };
}

FreshnessRequirements requirements(bool allow_recovered = false) {
  return FreshnessRequirements{kNow, LogicalTick::from(kMaxAge), kDeviceGeneration,
                               kEvidenceGeneration, allow_recovered};
}

/// A reading of the kind a device reports, so the payload never matters to a
/// freshness question.
ObservationPayload reading() { return AirflowReading{Airflow::from_cubic_metres_per_hour(5000)}; }

}  // namespace

AIRFLOW_TEST(freshness_check_order_is_fixed) {
  const FreshnessRequirements need = requirements();

  // A current, correctly stamped, good reading is proof.
  CHECK_EQ(assess_freshness(make_observation(reading(), kNow, kNow, false), need),
           FreshnessVerdict::fresh);

  // The evidence generation is checked first: an observation that is wrong in
  // every other way as well is still reported against its generation.
  Observation everything_wrong =
      make_observation(reading(), LogicalTick::from(kNow.value() + 500), kNow, false);
  everything_wrong.draft.evidence_generation = EvidenceGeneration::from(4);
  everything_wrong.draft.device_generation = DeviceGeneration::from(9);
  everything_wrong.draft.quality = Quality::bad;
  CHECK_EQ(assess_freshness(everything_wrong, need),
           FreshnessVerdict::wrong_evidence_generation);

  // Then the device generation, before quality and before the clock.
  Observation wrong_device =
      make_observation(reading(), LogicalTick::from(kNow.value() + 500), kNow, false);
  wrong_device.draft.device_generation = DeviceGeneration::from(9);
  wrong_device.draft.quality = Quality::bad;
  CHECK_EQ(assess_freshness(wrong_device, need), FreshnessVerdict::wrong_device_generation);

  // Then quality, before recovery and before the clock.
  Observation bad_quality =
      make_observation(reading(), LogicalTick::from(kNow.value() + 500), kNow, true);
  bad_quality.draft.quality = Quality::bad;
  CHECK_EQ(assess_freshness(bad_quality, need), FreshnessVerdict::quality_insufficient);

  // Then recovery, before the clock: a recovered value is not current physical
  // evidence even when its instant would be in the future.
  Observation recovered_future =
      make_observation(reading(), LogicalTick::from(kNow.value() + 500), kNow, true);
  CHECK_EQ(assess_freshness(recovered_future, need), FreshnessVerdict::recovered);

  // A recovered observation is usable only where the requirement explicitly
  // allows recovered state, and even then the clock still applies.
  CHECK_EQ(assess_freshness(recovered_future, requirements(true)), FreshnessVerdict::future);
  Observation recovered_current = make_observation(reading(), kNow, kNow, true);
  CHECK_EQ(assess_freshness(recovered_current, requirements(true)), FreshnessVerdict::fresh);
  CHECK_EQ(assess_freshness(recovered_current, need), FreshnessVerdict::recovered);

  // Only then the clock: a stale reading with good stamps and quality.
  Observation stale = make_observation(
      reading(), LogicalTick::from(kNow.value() - (kMaxAge + 1)), kNow, false);
  CHECK_EQ(assess_freshness(stale, need), FreshnessVerdict::stale);
  Observation future =
      make_observation(reading(), LogicalTick::from(kNow.value() + 1), kNow, false);
  CHECK_EQ(assess_freshness(future, need), FreshnessVerdict::future);
}

AIRFLOW_TEST(freshness_age_boundary_is_inclusive) {
  const FreshnessRequirements need = requirements();

  // Age zero is fresh, exactly max_age is fresh, one tick past it is stale.
  CHECK_EQ(assess_freshness(make_observation(reading(), kNow, kNow, false), need),
           FreshnessVerdict::fresh);
  CHECK_EQ(assess_freshness(
               make_observation(reading(), LogicalTick::from(kNow.value() - kMaxAge), kNow, false),
               need),
           FreshnessVerdict::fresh);
  CHECK_EQ(assess_freshness(make_observation(
                                reading(), LogicalTick::from(kNow.value() - (kMaxAge + 1)), kNow,
                                false),
                            need),
           FreshnessVerdict::stale);

  // A requirement with a zero window accepts only a reading measured exactly
  // now, and refuses one tick older.
  FreshnessRequirements zero_window{kNow, LogicalTick::from(0), kDeviceGeneration,
                                    kEvidenceGeneration, false};
  CHECK_EQ(assess_freshness(make_observation(reading(), kNow, kNow, false), zero_window),
           FreshnessVerdict::fresh);
  CHECK_EQ(assess_freshness(
               make_observation(reading(), LogicalTick::from(kNow.value() - 1), kNow, false),
               zero_window),
           FreshnessVerdict::stale);

  // A reading measured at or before "now" is never reported as future, and the
  // earliest instant the clock can express is ageable.
  CHECK_EQ(assess_freshness(
               make_observation(reading(), LogicalTick::from(0), LogicalTick::from(0), false),
               FreshnessRequirements{LogicalTick::from(0), LogicalTick::from(0), kDeviceGeneration,
                                     kEvidenceGeneration, false}),
           FreshnessVerdict::fresh);
  CHECK_EQ(assess_freshness(
               make_observation(reading(), LogicalTick::from(1), LogicalTick::from(0), false),
               FreshnessRequirements{LogicalTick::from(0), LogicalTick::from(0), kDeviceGeneration,
                                     kEvidenceGeneration, false}),
           FreshnessVerdict::future);
}

AIRFLOW_TEST(every_quality_other_than_good_is_insufficient) {
  const Quality qualities[] = {Quality::suspect, Quality::bad, Quality::unknown};
  for (const Quality quality : qualities) {
    Observation observation = make_observation(reading(), kNow, kNow, false);
    observation.draft.quality = quality;
    CHECK_EQ(assess_freshness(observation, requirements()), FreshnessVerdict::quality_insufficient);
    CHECK(!is_proof(assess_freshness(observation, requirements())));
  }
  Observation good = make_observation(reading(), kNow, kNow, false);
  CHECK_EQ(assess_freshness(good, requirements()), FreshnessVerdict::fresh);
}

AIRFLOW_TEST(freshness_status_maps_every_verdict) {
  CHECK_EQ(freshness_status(FreshnessVerdict::fresh), StatusCode::ok);
  CHECK_EQ(freshness_status(FreshnessVerdict::stale), StatusCode::evidence_stale);
  CHECK_EQ(freshness_status(FreshnessVerdict::future), StatusCode::evidence_future);
  CHECK_EQ(freshness_status(FreshnessVerdict::quality_insufficient),
           StatusCode::evidence_quality_insufficient);
  CHECK_EQ(freshness_status(FreshnessVerdict::wrong_device_generation),
           StatusCode::generation_mismatch);
  CHECK_EQ(freshness_status(FreshnessVerdict::wrong_evidence_generation),
           StatusCode::evidence_generation_mismatch);
  // A recovered reading is refused as stale: there is no separate code, because
  // the repair a caller makes is the same one, namely take a new reading.
  CHECK_EQ(freshness_status(FreshnessVerdict::recovered), StatusCode::evidence_stale);

  // Every verdict has a code, every verdict has a distinct token, and the only
  // two verdicts that share a status code are the two that mean "not current".
  const FreshnessVerdict verdicts[] = {
      FreshnessVerdict::fresh,
      FreshnessVerdict::stale,
      FreshnessVerdict::future,
      FreshnessVerdict::quality_insufficient,
      FreshnessVerdict::wrong_device_generation,
      FreshnessVerdict::wrong_evidence_generation,
      FreshnessVerdict::recovered,
  };
  constexpr std::size_t kCount = sizeof(verdicts) / sizeof(verdicts[0]);
  for (std::size_t left = 0; left < kCount; ++left) {
    CHECK(!std::string_view(to_string(verdicts[left])).empty());
    for (std::size_t right = left + 1; right < kCount; ++right) {
      CHECK(std::string_view(to_string(verdicts[left])) !=
            std::string_view(to_string(verdicts[right])));
      const bool shared = freshness_status(verdicts[left]) == freshness_status(verdicts[right]);
      const bool both_not_current =
          (verdicts[left] == FreshnessVerdict::stale && verdicts[right] == FreshnessVerdict::recovered) ||
          (verdicts[left] == FreshnessVerdict::recovered && verdicts[right] == FreshnessVerdict::stale);
      CHECK_EQ(shared, both_not_current);
    }
  }
}

AIRFLOW_TEST(is_proof_is_true_only_for_fresh) {
  const FreshnessVerdict verdicts[] = {
      FreshnessVerdict::fresh,
      FreshnessVerdict::stale,
      FreshnessVerdict::future,
      FreshnessVerdict::quality_insufficient,
      FreshnessVerdict::wrong_device_generation,
      FreshnessVerdict::wrong_evidence_generation,
      FreshnessVerdict::recovered,
  };
  for (const FreshnessVerdict verdict : verdicts) {
    CHECK_EQ(is_proof(verdict), verdict == FreshnessVerdict::fresh);
  }
  CHECK(is_proof(FreshnessVerdict::fresh));
  CHECK(!is_proof(FreshnessVerdict::stale));
  CHECK(!is_proof(FreshnessVerdict::recovered));
}

AIRFLOW_TEST(binding_orderings_are_checked_at_the_boundary) {
  const LogicalTick bound = LogicalTick::from(500);

  // Neither ordering required: everything is bound.
  const EvidenceBinding unbound{std::nullopt, std::nullopt};
  CHECK(is_bound(make_observation(reading(), LogicalTick::from(0), LogicalTick::from(0), false),
                 unbound));

  // Measured at or after the bound, inclusive.
  const EvidenceBinding measured{bound, std::nullopt};
  CHECK(is_bound(make_observation(reading(), bound, bound, false), measured));
  CHECK(is_bound(
      make_observation(reading(), LogicalTick::from(bound.value() + 1), bound, false), measured));
  CHECK(!is_bound(
      make_observation(reading(), LogicalTick::from(bound.value() - 1), bound, false), measured));

  // Accepted strictly after the bound: equality is NOT bound.
  const EvidenceBinding accepted{std::nullopt, bound};
  CHECK(!is_bound(make_observation(reading(), LogicalTick::from(0), bound, false), accepted));
  CHECK(is_bound(
      make_observation(reading(), LogicalTick::from(0), LogicalTick::from(bound.value() + 1), false),
      accepted));

  // Both orderings together, for every combination of the three measurement
  // positions and the two acceptance positions.
  const EvidenceBinding both{bound, bound};
  for (std::int64_t delta = -1; delta <= 1; ++delta) {
    for (std::uint64_t after = 0; after <= 1; ++after) {
      const LogicalTick measured_at = LogicalTick::from(
          static_cast<std::uint64_t>(static_cast<std::int64_t>(bound.value()) + delta));
      const LogicalTick accepted_at = LogicalTick::from(bound.value() + after);
      const bool expected = delta >= 0 && after == 1;
      CHECK_EQ(is_bound(make_observation(reading(), measured_at, accepted_at, false), both),
               expected);
    }
  }

  // The two orderings are independent: satisfying one does not satisfy the
  // other, which is what makes a pre-command reading unusable as proof.
  CHECK(!is_bound(make_observation(reading(), LogicalTick::from(bound.value() + 10), bound, false),
                  both));
  CHECK(is_bound(
      make_observation(reading(), bound, LogicalTick::from(bound.value() + 1), false), both));
}

AIRFLOW_TEST(observation_kind_and_rendering_agree_with_the_payload) {
  const ObservationPayload airflow_payload =
      AirflowReading{Airflow::from_cubic_metres_per_hour(5)};
  const ObservationPayload pressure_payload = PressureReading{Pressure::from_millipascals(-20000)};
  const ObservationPayload fan_payload = FanReading{SetpointBasisPoints::create(5000).value()};

  CHECK_EQ(observation_kind(airflow_payload), ObservationKind::airflow);
  CHECK_EQ(observation_kind(pressure_payload), ObservationKind::pressure);
  CHECK_EQ(observation_kind(fan_payload), ObservationKind::fan_setpoint);

  CHECK_EQ(to_string(airflow_payload), std::string("airflow 5 m3/h"));
  CHECK_EQ(to_string(pressure_payload), std::string("pressure -20.000 Pa"));
  CHECK_EQ(to_string(fan_payload), std::string("fan_setpoint 50.00 %"));

  // The variant exposes exactly one alternative, so the reading cannot be
  // silently read as the kind that was not reported.
  CHECK(std::holds_alternative<AirflowReading>(airflow_payload));
  CHECK(!std::holds_alternative<PressureReading>(airflow_payload));
  CHECK(!std::holds_alternative<FanReading>(airflow_payload));
  CHECK_EQ(std::get<AirflowReading>(airflow_payload).value.cubic_metres_per_hour(),
           std::int64_t{5});

  const ObservationKind kinds[] = {ObservationKind::airflow, ObservationKind::pressure,
                                   ObservationKind::fan_setpoint};
  for (const ObservationKind kind : kinds) {
    const std::string_view rendered = to_string(kind);
    CHECK(!rendered.empty());
    const std::optional<ObservationKind> parsed = parse_observation_kind(rendered);
    REQUIRE(parsed.has_value());
    CHECK(parsed.value() == kind);
  }
  CHECK(!parse_observation_kind("fan").has_value());
  CHECK(!parse_observation_kind("").has_value());

  const Quality qualities[] = {Quality::good, Quality::suspect, Quality::bad, Quality::unknown};
  for (const Quality quality : qualities) {
    const std::string_view rendered = to_string(quality);
    CHECK(!rendered.empty());
    const std::optional<Quality> parsed = parse_quality(rendered);
    REQUIRE(parsed.has_value());
    CHECK(parsed.value() == quality);
  }
  CHECK(!parse_quality("Good").has_value());
  CHECK(!parse_quality("").has_value());
}
