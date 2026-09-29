// Adversarial: malformed, hostile, and out-of-range input. Every case asserts the
// exact status code and that the engine is still usable afterwards, because an
// input that is refused must leave nothing behind.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include "test_harness.hpp"
#include "fixture.hpp"

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

using namespace airflow_control;
using namespace airflow_test;

namespace {

ControlRequest request_of(const Scenario& scenario, const std::string& key, ControlIntent intent,
                          std::optional<SetpointRequest> setpoint, LogicalTick at) {
  return ControlRequest{.key = IdempotencyKey::parse(key).value(),
                        .device = scenario.device,
                        .device_generation = scenario.device_generation,
                        .epoch = scenario.epoch,
                        .expected_revision = std::nullopt,
                        .intent = intent,
                        .setpoint = setpoint,
                        .actor = scenario.actor,
                        .requested_at = at,
                        .safety_permit = std::nullopt,
                        .supersede = std::nullopt,
                        .relationship = std::nullopt};
}

SetpointRequest fan_percent(std::uint32_t basis_points) {
  return SetpointRequest{SetpointPercent{.percent = SetpointBasisPoints::create(basis_points).value()}};
}

/// The engine is usable after a refusal: the clock still advances, a well formed
/// request is still eligible, and the authoritative model still renders.
void still_usable(AirflowControlEngine& engine, const Scenario& s) {
  const LogicalTick tick = engine.current_tick();
  CHECK(engine.advance_tick(LogicalTick::from(tick.value() + 1)).ok());
  const Result<Decision> decision =
      engine.evaluate(request_of(s, "usable-1", ControlIntent::hold_setpoint, fan_percent(5'000),
                                 engine.current_tick()));
  CHECK(decision.ok());
  CHECK_EQ(decision.value().code, StatusCode::ok);
  CHECK(!engine.canonical_state().empty());
}

/// A store open that must fail with exactly this code, leaving the path usable.
void expect_store_refusal(const std::string& path, StatusCode expected, const Scenario& s) {
  Result<AirflowControlEngine> opened =
      AirflowControlEngine::open(path, OpenMode::open_or_create, base_options());
  CHECK(!opened.ok());
  if (opened.ok()) {
    std::cout << "  store open unexpectedly succeeded for " << path << std::endl;
    (void)opened.value().close();
    return;
  }
  CHECK_EQ(opened.code(), expected);
  // A different, well formed path still opens: the refusal left no lock behind.
  TempDir scratch("adversarial-usable");
  Result<AirflowControlEngine> usable =
      AirflowControlEngine::open(scratch.store_path(), OpenMode::open_or_create, base_options());
  CHECK(usable.ok());
  if (usable.ok()) {
    CHECK(apply_scenario(usable.value(), s).ok());
    CHECK(usable.value().close().ok());
  }
}

}  // namespace

AIRFLOW_TEST(malformed_evidence_is_refused) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  Result<Fixture> fixture = Fixture::open(ScenarioOptions{});
  REQUIRE(fixture.ok());
  AirflowControlEngine& engine = fixture.value().engine();
  const LogicalTick tick = engine.current_tick();

  const auto draft = [&](ObservationPayload payload, const AirflowDeviceId& device,
                         std::optional<PressureRelationshipId> relationship, EvidenceSequence sequence,
                         LogicalTick measured_at) {
    return ObservationDraft{.payload = payload,
                            .device = device,
                            .relationship = relationship,
                            .point = SpaceRefId::parse("point-1").value(),
                            .source = s.source,
                            .sequence = sequence,
                            .measured_at = measured_at,
                            .device_generation = s.device_generation,
                            .evidence_generation = s.evidence_generation,
                            .quality = Quality::good};
  };

  // An observation for a device that was never declared.
  CHECK_STATUS(engine.observe(draft(FanReading{SetpointBasisPoints::create(5'000).value()},
                                    AirflowDeviceId::parse("dev-undeclared").value(), std::nullopt,
                                    EvidenceSequence::from(1), tick),
                              tick),
               StatusCode::not_found);
  // An observation for a relationship that was never declared.
  CHECK_STATUS(engine.observe(draft(PressureReading{Pressure::from_millipascals(-20'000)}, s.device,
                                    PressureRelationshipId::parse("rel-undeclared").value(),
                                    EvidenceSequence::from(1), tick),
                              tick),
               StatusCode::not_found);
  // A pressure reading with no relationship at all is a shape error.
  CHECK_STATUS(engine.observe(draft(PressureReading{Pressure::from_millipascals(-20'000)}, s.device,
                                    std::nullopt, EvidenceSequence::from(1), tick),
                              tick),
               StatusCode::invalid_argument);
  // A fan reading that names a relationship is a shape error.
  CHECK_STATUS(engine.observe(draft(FanReading{SetpointBasisPoints::create(5'000).value()}, s.device,
                                    s.relationship, EvidenceSequence::from(1), tick),
                              tick),
               StatusCode::invalid_argument);
  // The wrong evidence generation, and the wrong device generation.
  {
    ObservationDraft wrong = draft(PressureReading{Pressure::from_millipascals(-20'000)}, s.device,
                                   s.relationship, EvidenceSequence::from(1), tick);
    wrong.evidence_generation = EvidenceGeneration::from(9);
    CHECK_STATUS(engine.observe(wrong, tick), StatusCode::evidence_generation_mismatch);
    wrong.evidence_generation = s.evidence_generation;
    wrong.device_generation = DeviceGeneration::from(9);
    CHECK_STATUS(engine.observe(wrong, tick), StatusCode::generation_mismatch);
  }
  // A reading measured after the instant it is accepted at, and one accepted in
  // the future of the logical clock.
  CHECK_STATUS(engine.observe(draft(FanReading{SetpointBasisPoints::create(5'000).value()}, s.device,
                                    std::nullopt, EvidenceSequence::from(1),
                                    LogicalTick::from(tick.value() + 1)),
                              tick),
               StatusCode::evidence_future);
  CHECK_STATUS(engine.observe(draft(FanReading{SetpointBasisPoints::create(5'000).value()}, s.device,
                                    std::nullopt, EvidenceSequence::from(1), tick),
                              LogicalTick::from(tick.value() + 1)),
               StatusCode::evidence_future);
  // An out-of-range quantity in the reading itself.
  CHECK_STATUS(engine.observe(draft(AirflowReading{Airflow::from_cubic_metres_per_hour(
                                        PhysicalBounds::max_abs_airflow_cubic_metres_per_hour + 1)},
                                    s.device, std::nullopt, EvidenceSequence::from(1), tick),
                              tick),
               StatusCode::bounds_exceeded);
  CHECK_STATUS(engine.observe(draft(PressureReading{Pressure::from_millipascals(
                                        PhysicalBounds::max_abs_pressure_millipascals + 1)},
                                    s.device, s.relationship, EvidenceSequence::from(1), tick),
                              tick),
               StatusCode::bounds_exceeded);

  // An out-of-order sequence, and the identical sequence repeated.
  CHECK(engine.observe(draft(FanReading{SetpointBasisPoints::create(5'000).value()}, s.device,
                             std::nullopt, EvidenceSequence::from(5), tick),
                       tick)
            .ok());
  CHECK_STATUS(engine.observe(draft(FanReading{SetpointBasisPoints::create(5'000).value()}, s.device,
                                    std::nullopt, EvidenceSequence::from(4), tick),
                              tick),
               StatusCode::evidence_out_of_order);
  CHECK_STATUS(engine.observe(draft(FanReading{SetpointBasisPoints::create(5'000).value()}, s.device,
                                    std::nullopt, EvidenceSequence::from(5), tick),
                              tick),
               StatusCode::evidence_out_of_order);
  CHECK(engine.observe(draft(PressureReading{Pressure::from_millipascals(-20'000)}, s.device,
                             s.relationship, EvidenceSequence::from(3), tick),
                       tick)
            .ok());
  CHECK_STATUS(engine.observe(draft(PressureReading{Pressure::from_millipascals(-20'000)}, s.device,
                                    s.relationship, EvidenceSequence::from(3), tick),
                              tick),
               StatusCode::evidence_out_of_order);

  still_usable(engine, s);
}

AIRFLOW_TEST(malformed_model_declarations_are_refused) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  Result<AirflowControlEngine> opened = AirflowControlEngine::open_in_memory(base_options());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value();
  CHECK(apply_scenario(engine, s).ok());
  const LogicalTick tick = engine.current_tick();

  // A containment report for an element that was never declared.
  CHECK_STATUS(engine.report_containment(ReportContainmentRequest{
                  .element = ContainmentId::parse("cont-undeclared").value(),
                  .state = ContainmentState::intact,
                  .quality = Quality::good,
                  .source = s.source,
                  .sequence = EvidenceSequence::from(1),
                  .evidence_generation = s.evidence_generation,
                  .measured_at = tick}),
               StatusCode::not_found);
  // An out-of-order containment report: the fixture reported sequence one.
  CHECK_STATUS(engine.report_containment(ReportContainmentRequest{
                  .element = s.containment,
                  .state = ContainmentState::intact,
                  .quality = Quality::good,
                  .source = s.source,
                  .sequence = EvidenceSequence::from(1),
                  .evidence_generation = s.evidence_generation,
                  .measured_at = tick}),
               StatusCode::evidence_out_of_order);
  // An interlock report for an interlock that was never declared, and one whose
  // sequence is not ahead of the retained one.
  CHECK_STATUS(engine.report_interlock(ReportInterlockRequest{
                  .id = InterlockId::parse("il-undeclared").value(),
                  .state = InterlockState::satisfied,
                  .sequence = EvidenceSequence::from(1),
                  .epoch = s.epoch,
                  .reported_at = tick}),
               StatusCode::not_found);
  CHECK_STATUS(engine.report_interlock(ReportInterlockRequest{
                  .id = s.interlock,
                  .state = InterlockState::satisfied,
                  .sequence = EvidenceSequence::from(1),
                  .epoch = s.epoch,
                  .reported_at = tick}),
               StatusCode::evidence_out_of_order);
  // A relationship whose controlled and reference spaces are the same object.
  CHECK_STATUS(engine.define_pressure_relationship(DefineRelationshipRequest{
                  .id = PressureRelationshipId::parse("rel-same").value(),
                  .room = s.room,
                  .controlled_space = s.controlled_space,
                  .reference_space = s.controlled_space,
                  .polarity = PressurePolarity::negative,
                  .lower = Pressure::from_millipascals(-30'000),
                  .upper = Pressure::from_millipascals(-10'000),
                  .tolerance = Pressure::from_millipascals(2'000),
                  .evidence_generation = s.evidence_generation,
                  .expected_revision = std::nullopt,
                  .actor = s.actor,
                  .requested_at = tick}),
               StatusCode::invalid_argument);
  // A relationship with no evidence generation, and one with an inverted band.
  CHECK_STATUS(engine.define_pressure_relationship(DefineRelationshipRequest{
                  .id = PressureRelationshipId::parse("rel-zero").value(),
                  .room = s.room,
                  .controlled_space = SpaceRefId::parse("space-a").value(),
                  .reference_space = SpaceRefId::parse("space-b").value(),
                  .polarity = PressurePolarity::negative,
                  .lower = Pressure::from_millipascals(-30'000),
                  .upper = Pressure::from_millipascals(-10'000),
                  .tolerance = Pressure::from_millipascals(2'000),
                  .evidence_generation = EvidenceGeneration::from(0),
                  .expected_revision = std::nullopt,
                  .actor = s.actor,
                  .requested_at = tick}),
               StatusCode::invalid_argument);
  CHECK_STATUS(engine.define_pressure_relationship(DefineRelationshipRequest{
                  .id = PressureRelationshipId::parse("rel-inverted").value(),
                  .room = s.room,
                  .controlled_space = SpaceRefId::parse("space-c").value(),
                  .reference_space = SpaceRefId::parse("space-d").value(),
                  .polarity = PressurePolarity::negative,
                  .lower = Pressure::from_millipascals(-10'000),
                  .upper = Pressure::from_millipascals(-30'000),
                  .tolerance = Pressure::from_millipascals(2'000),
                  .evidence_generation = s.evidence_generation,
                  .expected_revision = std::nullopt,
                  .actor = s.actor,
                  .requested_at = tick}),
               StatusCode::pressure_band_invalid);

  const auto obligation = [&](const char* id, Airflow minimum, Airflow target,
                              ObligationBinding binding, std::optional<SpaceRefId> point,
                              std::vector<AirflowDeviceId> devices) {
    return DeclareObligationRequest{.id = ObligationId::parse(id).value(),
                                    .scope = ObligationScope::room,
                                    .room = s.room,
                                    .row = std::nullopt,
                                    .rack = std::nullopt,
                                    .klass = ObligationClass::protected_obligation,
                                    .binding = binding,
                                    .metered_point = point,
                                    .devices = std::move(devices),
                                    .minimum_airflow = minimum,
                                    .target_airflow = target,
                                    .source = s.source,
                                    .evidence_generation = s.evidence_generation,
                                    .expected_revision = std::nullopt,
                                    .actor = s.actor,
                                    .requested_at = tick};
  };
  // A minimum above the target.
  CHECK_STATUS(engine.declare_obligation(obligation("ob-inverted",
                                                    Airflow::from_cubic_metres_per_hour(9'000),
                                                    Airflow::from_cubic_metres_per_hour(1'000),
                                                    ObligationBinding::device_sum, std::nullopt,
                                                    {s.device})),
               StatusCode::invalid_argument);
  // A metered obligation with no metered point, and a device-sum obligation with
  // no devices: the binding does not match the fields.
  CHECK_STATUS(engine.declare_obligation(obligation("ob-metered",
                                                    Airflow::from_cubic_metres_per_hour(1'000),
                                                    Airflow::from_cubic_metres_per_hour(2'000),
                                                    ObligationBinding::metered_scope, std::nullopt,
                                                    {})),
               StatusCode::invalid_argument);
  CHECK_STATUS(engine.declare_obligation(obligation("ob-nodevices",
                                                    Airflow::from_cubic_metres_per_hour(1'000),
                                                    Airflow::from_cubic_metres_per_hour(2'000),
                                                    ObligationBinding::device_sum, std::nullopt, {})),
               StatusCode::invalid_argument);
  // An obligation that binds a device that was never registered, and one that
  // names the same device twice.
  CHECK_STATUS(engine.declare_obligation(obligation("ob-absent",
                                                    Airflow::from_cubic_metres_per_hour(1'000),
                                                    Airflow::from_cubic_metres_per_hour(2'000),
                                                    ObligationBinding::device_sum, std::nullopt,
                                                    {AirflowDeviceId::parse("dev-absent").value()})),
               StatusCode::not_found);
  CHECK_STATUS(engine.declare_obligation(obligation("ob-duplicate",
                                                    Airflow::from_cubic_metres_per_hour(1'000),
                                                    Airflow::from_cubic_metres_per_hour(2'000),
                                                    ObligationBinding::device_sum, std::nullopt,
                                                    {s.device, s.device})),
               StatusCode::duplicate_identity);
  // An obligation airflow outside the representable range.
  CHECK_STATUS(engine.declare_obligation(obligation(
                  "ob-huge",
                  Airflow::from_cubic_metres_per_hour(PhysicalBounds::max_abs_airflow_cubic_metres_per_hour + 1),
                  Airflow::from_cubic_metres_per_hour(PhysicalBounds::max_abs_airflow_cubic_metres_per_hour + 1),
                  ObligationBinding::device_sum, std::nullopt, {s.device})),
               StatusCode::bounds_exceeded);

  // A grant whose action mask sets an undefined bit cannot even be constructed.
  CHECK_STATUS(ActionSet::from_mask(1u << kControlActionCount), StatusCode::invalid_argument);
  CHECK_STATUS(ActionSet::from_mask(0xFFFFFFFFu), StatusCode::invalid_argument);
  CHECK(ActionSet::from_mask((1u << kControlActionCount) - 1u).ok());
  // A grant with no actions at all, and one bound to a device generation without
  // naming the device.
  CHECK_STATUS(engine.add_grant(PermissionGrant{.id = GrantId::parse("grant-empty").value(),
                                                .issuer = s.issuer,
                                                .epoch = s.epoch,
                                                .device = s.device,
                                                .device_generation = s.device_generation,
                                                .room = s.room,
                                                .actions = ActionSet::none(),
                                                .issued_at = tick,
                                                .expires_at = std::nullopt,
                                                .revoked = false},
                                s.actor, tick),
               StatusCode::invalid_argument);
  CHECK_STATUS(engine.add_grant(PermissionGrant{.id = GrantId::parse("grant-detached").value(),
                                                .issuer = s.issuer,
                                                .epoch = s.epoch,
                                                .device = std::nullopt,
                                                .device_generation = s.device_generation,
                                                .room = s.room,
                                                .actions = ActionSet::all(),
                                                .issued_at = tick,
                                                .expires_at = std::nullopt,
                                                .revoked = false},
                                s.actor, tick),
               StatusCode::invalid_argument);
  // A grant that expires before it is issued, and one from an epoch that was
  // never adopted.
  CHECK_STATUS(engine.add_grant(PermissionGrant{.id = GrantId::parse("grant-backwards").value(),
                                                .issuer = s.issuer,
                                                .epoch = s.epoch,
                                                .device = s.device,
                                                .device_generation = s.device_generation,
                                                .room = s.room,
                                                .actions = ActionSet::all(),
                                                .issued_at = LogicalTick::from(10),
                                                .expires_at = std::optional<LogicalTick>(LogicalTick::from(5)),
                                                .revoked = false},
                                s.actor, tick),
               StatusCode::invalid_argument);
  CHECK_STATUS(engine.add_grant(PermissionGrant{.id = GrantId::parse("grant-zero-epoch").value(),
                                                .issuer = s.issuer,
                                                .epoch = AuthorityEpoch::from(0),
                                                .device = s.device,
                                                .device_generation = s.device_generation,
                                                .room = s.room,
                                                .actions = ActionSet::all(),
                                                .issued_at = tick,
                                                .expires_at = std::nullopt,
                                                .revoked = false},
                                s.actor, tick),
               StatusCode::invalid_argument);
  // An override whose expiry precedes its issue, and one for a device that was
  // never registered.
  CHECK_STATUS(engine.add_maintenance_override(MaintenanceOverride{
                  .id = OverrideId::parse("ov-backwards").value(),
                  .device = s.device,
                  .device_generation = s.device_generation,
                  .epoch = s.epoch,
                  .issued_at = LogicalTick::from(10),
                  .expires_at = LogicalTick::from(5),
                  .reason = "backwards",
                  .revoked = false},
                  s.actor, tick),
               StatusCode::invalid_argument);
  CHECK_STATUS(engine.add_maintenance_override(MaintenanceOverride{
                  .id = OverrideId::parse("ov-absent").value(),
                  .device = AirflowDeviceId::parse("dev-absent").value(),
                  .device_generation = s.device_generation,
                  .epoch = s.epoch,
                  .issued_at = tick,
                  .expires_at = LogicalTick::from(tick.value() + 10),
                  .reason = "absent",
                  .revoked = false},
                  s.actor, tick),
               StatusCode::not_found);
  // A safety permit that expires before it is issued, and one for a device that
  // was never registered.
  CHECK_STATUS(engine.add_safety_permit(SafetyPermit{.id = SafetyPermitId::parse("permit-backwards").value(),
                                                     .issuer = s.issuer,
                                                     .epoch = s.epoch,
                                                     .device = s.device,
                                                     .issued_at = LogicalTick::from(10),
                                                     .expires_at = std::optional<LogicalTick>(LogicalTick::from(5)),
                                                     .reason = "backwards"},
                                        s.actor, tick),
               StatusCode::invalid_argument);
  CHECK_STATUS(engine.add_safety_permit(SafetyPermit{.id = SafetyPermitId::parse("permit-absent").value(),
                                                     .issuer = s.issuer,
                                                     .epoch = s.epoch,
                                                     .device = AirflowDeviceId::parse("dev-absent").value(),
                                                     .issued_at = tick,
                                                     .expires_at = std::nullopt,
                                                     .reason = "absent"},
                                        s.actor, tick),
               StatusCode::not_found);
  // A lifecycle transition that is not declared: active never returns to
  // provisioned.
  CHECK_STATUS(engine.set_device_lifecycle(SetLifecycleRequest{
                  .device = s.device,
                  .generation = s.device_generation,
                  .expected_revision = device_revision(engine, s.device).value(),
                  .target = DeviceLifecycle::provisioned,
                  .epoch = s.epoch,
                  .actor = s.actor,
                  .requested_at = tick}),
               StatusCode::transition_invalid);
  CHECK_EQ(engine.device(s.device).value().lifecycle, DeviceLifecycle::active);
  // A tick that moves the clock backwards.
  const LogicalTick before = engine.current_tick();
  CHECK_STATUS(engine.advance_tick(LogicalTick::from(before.value() - 1)), StatusCode::out_of_range);
  CHECK_EQ(engine.current_tick(), before);
  // An epoch that is already adopted, or older, is not adopted again.
  CHECK_STATUS(engine.adopt_epoch(s.epoch, s.actor, tick), StatusCode::epoch_stale);
  CHECK_STATUS(engine.adopt_epoch(AuthorityEpoch::from(0), s.actor, tick), StatusCode::invalid_argument);

  still_usable(engine, s);
}

AIRFLOW_TEST(malformed_control_requests_are_refused) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  Result<Fixture> fixture = Fixture::open(ScenarioOptions{});
  REQUIRE(fixture.ok());
  AirflowControlEngine& engine = fixture.value().engine();
  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, s);
  const LogicalTick tick = engine.current_tick();

  const auto refused = [&](const ControlRequest& request, StatusCode expected) {
    const Result<Decision> decision = engine.evaluate(request);
    CHECK(decision.ok());
    CHECK_EQ(decision.value().code, expected);
    const Result<AttemptRecord> issued = engine.issue(request, adapter);
    CHECK(!issued.ok());
    CHECK_EQ(issued.code(), expected);
  };

  ControlRequest zero_epoch = request_of(s, "adv-epoch", ControlIntent::hold_setpoint,
                                         fan_percent(5'000), tick);
  zero_epoch.epoch = AuthorityEpoch::from(0);
  refused(zero_epoch, StatusCode::invalid_argument);

  ControlRequest zero_generation = request_of(s, "adv-gen", ControlIntent::hold_setpoint,
                                              fan_percent(5'000), tick);
  zero_generation.device_generation = DeviceGeneration::from(0);
  refused(zero_generation, StatusCode::invalid_argument);

  refused(request_of(s, "adv-setpoint", ControlIntent::hold_setpoint, std::nullopt, tick),
          StatusCode::invalid_argument);
  refused(request_of(s, "adv-release", ControlIntent::release_to_policy, fan_percent(5'000), tick),
          StatusCode::invalid_argument);

  ControlRequest stray_relationship =
      request_of(s, "adv-rel", ControlIntent::hold_setpoint, fan_percent(5'000), tick);
  stray_relationship.relationship = s.relationship;
  refused(stray_relationship, StatusCode::invalid_argument);

  ControlRequest missing_relationship =
      request_of(s, "adv-restore", ControlIntent::restore_pressure_relationship, fan_percent(5'000), tick);
  refused(missing_relationship, StatusCode::invalid_argument);

  // A request instant in the future of the logical clock.
  refused(request_of(s, "adv-future", ControlIntent::hold_setpoint, fan_percent(5'000),
                     LogicalTick::from(tick.value() + 1)),
          StatusCode::out_of_range);

  // A safety permit issued for another device.
  CHECK(engine.register_device(RegisterDeviceRequest{
            .device = AirflowDeviceId::parse("dev-other").value(),
            .generation = DeviceGeneration::from(1),
            .room = RoomId::parse("room-other").value(),
            .row = std::nullopt,
            .actor = s.actor,
            .requested_at = tick})
            .ok());
  CHECK(engine.add_safety_permit(SafetyPermit{.id = SafetyPermitId::parse("permit-other-device").value(),
                                              .issuer = s.issuer,
                                              .epoch = s.epoch,
                                              .device = AirflowDeviceId::parse("dev-other").value(),
                                              .issued_at = tick,
                                              .expires_at = std::nullopt,
                                              .reason = "another device"},
                                 s.actor, tick)
            .ok());
  ControlRequest foreign = request_of(s, "adv-permit", ControlIntent::emergency_purge,
                                      fan_percent(5'000), tick);
  foreign.safety_permit = SafetyPermitId::parse("permit-other-device").value();
  refused(foreign, StatusCode::identity_mismatch);

  // A relationship declared for another room.
  CHECK(engine.define_pressure_relationship(DefineRelationshipRequest{
            .id = PressureRelationshipId::parse("rel-other-room").value(),
            .room = RoomId::parse("room-other").value(),
            .controlled_space = SpaceRefId::parse("space-other-a").value(),
            .reference_space = SpaceRefId::parse("space-other-b").value(),
            .polarity = PressurePolarity::negative,
            .lower = Pressure::from_millipascals(-30'000),
            .upper = Pressure::from_millipascals(-10'000),
            .tolerance = Pressure::from_millipascals(2'000),
            .evidence_generation = s.evidence_generation,
            .expected_revision = std::nullopt,
            .actor = s.actor,
            .requested_at = tick})
            .ok());
  ControlRequest foreign_relationship =
      request_of(s, "adv-rel-room", ControlIntent::restore_pressure_relationship, fan_percent(5'000), tick);
  foreign_relationship.relationship = PressureRelationshipId::parse("rel-other-room").value();
  refused(foreign_relationship, StatusCode::identity_mismatch);

  // A device that was never registered.
  ControlRequest undeclared = request_of(s, "adv-device", ControlIntent::hold_setpoint,
                                         fan_percent(5'000), tick);
  undeclared.device = AirflowDeviceId::parse("dev-undeclared").value();
  refused(undeclared, StatusCode::not_found);

  CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});
  still_usable(engine, s);
}

AIRFLOW_TEST(boundary_quantities_are_exact) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  Result<Fixture> fixture = Fixture::open(ScenarioOptions{});
  REQUIRE(fixture.ok());
  AirflowControlEngine& engine = fixture.value().engine();
  const LogicalTick tick = engine.current_tick();

  // The largest setpoint and slew bound are representable; one past is not.
  CHECK(SetpointBasisPoints::create(SetpointBasisPoints::kFull).ok());
  CHECK_STATUS(SetpointBasisPoints::create(SetpointBasisPoints::kFull + 1), StatusCode::out_of_range);
  CHECK(SlewBasisPoints::create(SlewBasisPoints::kFull).ok());
  CHECK_STATUS(SlewBasisPoints::create(SlewBasisPoints::kFull + 1), StatusCode::out_of_range);
  CHECK_STATUS(SetpointBasisPoints::create(UINT32_MAX), StatusCode::out_of_range);

  // The largest representable airflow is accepted as evidence; one past is not.
  const std::int64_t largest = PhysicalBounds::max_abs_airflow_cubic_metres_per_hour;
  CHECK(is_representable(Airflow::from_cubic_metres_per_hour(largest)));
  CHECK(is_representable(Airflow::from_cubic_metres_per_hour(-largest)));
  CHECK(!is_representable(Airflow::from_cubic_metres_per_hour(largest + 1)));
  CHECK(!is_representable(Airflow::from_cubic_metres_per_hour(-largest - 1)));
  CHECK(is_representable(Pressure::from_millipascals(PhysicalBounds::max_abs_pressure_millipascals)));
  CHECK(!is_representable(Pressure::from_millipascals(PhysicalBounds::max_abs_pressure_millipascals + 1)));
  CHECK(is_representable(MilliCelsius::from_millidegrees(PhysicalBounds::max_abs_millidegrees_celsius)));
  CHECK(!is_representable(MilliCelsius::from_millidegrees(PhysicalBounds::max_abs_millidegrees_celsius + 1)));

  CHECK(engine.observe(ObservationDraft{.payload = AirflowReading{Airflow::from_cubic_metres_per_hour(largest)},
                                        .device = s.device,
                                        .relationship = std::nullopt,
                                        .point = SpaceRefId::parse("point-largest").value(),
                                        .source = s.source,
                                        .sequence = EvidenceSequence::from(1),
                                        .measured_at = tick,
                                        .device_generation = s.device_generation,
                                        .evidence_generation = s.evidence_generation,
                                        .quality = Quality::good},
                       tick)
            .ok());
  CHECK_EQ(engine.device(s.device).value().observed_airflow.value(),
           Airflow::from_cubic_metres_per_hour(largest));
  CHECK_STATUS(engine.observe(ObservationDraft{.payload = AirflowReading{Airflow::from_cubic_metres_per_hour(largest + 1)},
                                               .device = s.device,
                                               .relationship = std::nullopt,
                                               .point = SpaceRefId::parse("point-largest").value(),
                                               .source = s.source,
                                               .sequence = EvidenceSequence::from(2),
                                               .measured_at = tick,
                                               .device_generation = s.device_generation,
                                               .evidence_generation = s.evidence_generation,
                                               .quality = Quality::good},
                              tick),
               StatusCode::bounds_exceeded);

  // Arithmetic at the edge of the representable range fails rather than wrapping.
  CHECK_STATUS(Airflow::add(Airflow::from_cubic_metres_per_hour(std::numeric_limits<std::int64_t>::max()),
                            Airflow::from_cubic_metres_per_hour(1)),
               StatusCode::overflow);
  CHECK_STATUS(Airflow::subtract(Airflow::from_cubic_metres_per_hour(std::numeric_limits<std::int64_t>::min()),
                                 Airflow::from_cubic_metres_per_hour(1)),
               StatusCode::overflow);
  CHECK_STATUS(Airflow::scale(Airflow::from_cubic_metres_per_hour(1), 0, 0), StatusCode::invalid_argument);
  CHECK_STATUS(Pressure::from_millipascals(std::numeric_limits<std::int64_t>::min()).negate(),
               StatusCode::overflow);
  CHECK_STATUS(tick_add(LogicalTick::from(std::numeric_limits<std::uint64_t>::max()), 1),
               StatusCode::overflow);
  CHECK_STATUS(AttemptId::from(std::numeric_limits<std::uint64_t>::max()).next(), StatusCode::overflow);
  CHECK_STATUS(StateRevision::from(std::numeric_limits<std::uint64_t>::max()).next(), StatusCode::overflow);
  CHECK_STATUS(tick_age(LogicalTick::from(1), LogicalTick::from(2)), StatusCode::evidence_future);

  // The store's slot capacity bounds are exact.
  {
    EngineOptions too_small = base_options();
    too_small.store.slot_capacity_bytes = kMinSlotCapacityBytes - 1;
    TempDir directory("adversarial-capacity");
    CHECK_STATUS(AirflowControlEngine::open(directory.store_path(), OpenMode::open_or_create, too_small),
                 StatusCode::out_of_range);
    EngineOptions too_large = base_options();
    too_large.store.slot_capacity_bytes = kMaxSlotCapacityBytes + 1;
    CHECK_STATUS(AirflowControlEngine::open(directory.store_path(), OpenMode::open_or_create, too_large),
                 StatusCode::out_of_range);
    CHECK(AirflowControlEngine::open(directory.store_path(), OpenMode::open_or_create,
                                     base_options())
              .ok());
  }
  // Retainment bounds are exact too: a window of zero is not a window.
  {
    EngineOptions no_window = base_options();
    no_window.idempotency_window = 0;
    CHECK_STATUS(AirflowControlEngine::open_in_memory(no_window), StatusCode::out_of_range);
    EngineOptions no_audit = base_options();
    no_audit.audit_capacity = 0;
    CHECK_STATUS(AirflowControlEngine::open_in_memory(no_audit), StatusCode::out_of_range);
    EngineOptions many_sources = base_options();
    many_sources.evidence.max_relationship_sources = ModelBounds::max_sources_per_relationship + 1;
    CHECK_STATUS(AirflowControlEngine::open_in_memory(many_sources), StatusCode::out_of_range);
  }
  still_usable(engine, s);
}

AIRFLOW_TEST(hostile_store_paths_are_refused) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  TempDir directory("adversarial-store");
  const std::string root = directory.path();

  // A directory where the store file belongs.
  {
    const std::string path = root + "\\a-directory.afcstore";
    std::error_code error;
    std::filesystem::create_directories(path, error);
    CHECK(!error);
    expect_store_refusal(path, StatusCode::path_invalid, s);
  }
  // A path whose final component is a traversal.
  expect_store_refusal(root + "\\..", StatusCode::path_invalid, s);
  // A path that reaches the store through a traversal is resolved, not followed
  // blindly: the canonical form is the file inside this directory.
  {
    const std::string path = root + "\\nested\\..\\resolved.afcstore";
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_or_create, base_options());
    CHECK(opened.ok());
    if (opened.ok()) {
      CHECK(opened.value().close().ok());
    }
    CHECK(std::filesystem::exists(root + "\\resolved.afcstore"));
  }
#ifdef _WIN32
  // An alternate data stream is a different object, so it is refused outright.
  expect_store_refusal(root + "\\stream.afcstore:ads", StatusCode::path_invalid, s);
  // A reserved device name is not a file at all.
  expect_store_refusal(root + "\\NUL", StatusCode::path_invalid, s);
  expect_store_refusal(root + "\\CON.txt", StatusCode::path_invalid, s);
#endif
  // A directory where the lock file belongs: the lock cannot be taken.
  {
    const std::string path = root + "\\locked.afcstore";
    std::error_code error;
    std::filesystem::create_directories(path + ".lock", error);
    CHECK(!error);
    expect_store_refusal(path, StatusCode::store_io_error, s);
  }
  // A lock file that cannot be written.
  {
    const std::string path = root + "\\readonly-lock.afcstore";
    { std::ofstream lock(path + ".lock", std::ios::binary | std::ios::trunc); }
#ifdef _WIN32
    CHECK(SetFileAttributesA((path + ".lock").c_str(), FILE_ATTRIBUTE_READONLY) != 0);
#else
    std::error_code error;
    std::filesystem::permissions(path + ".lock",
                                 std::filesystem::perms::owner_read |
                                     std::filesystem::perms::group_read |
                                     std::filesystem::perms::others_read,
                                 std::filesystem::perm_options::replace, error);
    CHECK(!error);
#endif
    expect_store_refusal(path, StatusCode::store_io_error, s);
#ifdef _WIN32
    CHECK(SetFileAttributesA((path + ".lock").c_str(), FILE_ATTRIBUTE_NORMAL) != 0);
#endif
  }
  // The engine is still usable on a well formed path in the same directory.
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(root + "\\usable.afcstore", OpenMode::open_or_create, base_options());
    REQUIRE(opened.ok());
    CHECK(apply_scenario(opened.value(), s).ok());
    CHECK(opened.value().close().ok());
  }
}
