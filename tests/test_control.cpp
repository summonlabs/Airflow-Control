// Control semantics: the refusal precedence, the command-attempt boundary, the
// unresolved latch, definite refusals, and verification.
//
// The order in which the engine considers a control request is a machine
// contract: a caller branches on the status code it receives, so a multiply
// invalid request must always be refused for the same, documented reason, and
// repeating it must produce the identical code and message.

#include <cstdint>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include "test_harness.hpp"
#include "fixture.hpp"

using namespace airflow_control;
using namespace airflow_test;

namespace {

/// A control request for the fixture's device, with every field stated.
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

SetpointRequest airflow(std::int64_t cubic_metres_per_hour) {
  return SetpointRequest{SetpointAirflow{
      .airflow = Airflow::from_cubic_metres_per_hour(cubic_metres_per_hour)}};
}

/// The refusal of an evaluate() request, asserted twice: once as it is, and once
/// with the identical request repeated. A refusal that is not reproducible is not
/// a contract, so the code and the message must both be identical.
void expect_decision(AirflowControlEngine& engine, const ControlRequest& request,
                     StatusCode expected, const char* label) {
  const Result<Decision> first = engine.evaluate(request);
  CHECK(first.ok());
  CHECK_EQ(first.value().code, expected);
  const Result<Decision> second = engine.evaluate(request);
  CHECK(second.ok());
  CHECK_EQ(second.value().code, expected);
  CHECK_EQ(second.value().message, first.value().message);
  CHECK(first.value().eligible == (expected == StatusCode::ok));
  const bool repeated = first.value().message == second.value().message;
  CHECK(repeated);
  if (first.value().code != expected) {
    std::cout << "  " << label << ": expected " << to_string(expected) << " got "
              << to_string(first.value().code) << " (" << first.value().message << ")\n";
  }
}

/// The first trace entry whose outcome is not ok, which is the check that
/// produced the refusal.
std::string first_refusal(const Decision& decision) {
  for (const CheckTrace& entry : decision.trace) {
    if (entry.outcome != StatusCode::ok) {
      return entry.check;
    }
  }
  return std::string();
}

/// An adapter that counts calls and never refuses. It is the counter that proves
/// a failed precondition never reaches the vendor boundary.
class CountingAdapter final : public AirflowAdapter {
 public:
  [[nodiscard]] AdapterDescriptor describe() const override { return synthetic_descriptor(); }

  AdapterOutcome execute(const AdapterCommand& command) override {
    ++executes_;
    return AdapterOutcome{command.id(),
                          command.attempt(),
                          command.device(),
                          command.device_generation(),
                          disposition_,
                          AdapterSequence::from(executes_),
                          command.issued_at(),
                          "counted",
                          std::nullopt};
  }

  Result<ObservationDraft> read(const AdapterReadRequest&) override {
    ++reads_;
    return Status::failure(StatusCode::adapter_unavailable, "this adapter does not read");
  }

  void set_disposition(AdapterDisposition disposition) noexcept { disposition_ = disposition; }
  [[nodiscard]] std::uint64_t executes() const noexcept { return executes_; }
  [[nodiscard]] std::uint64_t reads() const noexcept { return reads_; }

 private:
  AdapterDisposition disposition_ = AdapterDisposition::accepted;
  std::uint64_t executes_ = 0;
  std::uint64_t reads_ = 0;
};

/// Opens an in-memory engine with the scenario applied, in the shape every case
/// below starts from.
Result<AirflowControlEngine> scenario_engine(const Scenario& scenario) {
  Result<AirflowControlEngine> engine = AirflowControlEngine::open_in_memory(base_options());
  if (!engine.ok()) {
    return engine.status();
  }
  const Status applied = apply_scenario(engine.value(), scenario);
  if (!applied.ok()) {
    return applied;
  }
  return engine;
}

}  // namespace

AIRFLOW_TEST(shape_precedence_is_deterministic) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  Result<AirflowControlEngine> opened = scenario_engine(s);
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value();
  const LogicalTick tick = engine.current_tick();
  CountingAdapter adapter;

  // 1 - shape, before identity: a zero epoch, a zero device generation, a
  // missing setpoint, a setpoint where none belongs, and a relationship named
  // for an intent that is not about pressure.
  ControlRequest zero_epoch = request_of(s, "shape-1", ControlIntent::hold_setpoint, fan_percent(5000), tick);
  zero_epoch.epoch = AuthorityEpoch::from(0);
  zero_epoch.device = AirflowDeviceId::parse("dev-absent").value();
  zero_epoch.setpoint = std::nullopt;
  expect_decision(engine, zero_epoch, StatusCode::invalid_argument, "zero epoch");

  ControlRequest zero_generation = request_of(s, "shape-2", ControlIntent::hold_setpoint,
                                              std::nullopt, tick);
  zero_generation.device_generation = DeviceGeneration::from(0);
  zero_generation.device = AirflowDeviceId::parse("dev-absent").value();
  expect_decision(engine, zero_generation, StatusCode::invalid_argument, "zero generation");

  ControlRequest missing_setpoint = request_of(s, "shape-3", ControlIntent::hold_setpoint,
                                               std::nullopt, tick);
  missing_setpoint.device_generation = DeviceGeneration::from(9);
  expect_decision(engine, missing_setpoint, StatusCode::invalid_argument, "missing setpoint");

  ControlRequest release_with_setpoint =
      request_of(s, "shape-4", ControlIntent::release_to_policy, fan_percent(5000), tick);
  expect_decision(engine, release_with_setpoint, StatusCode::invalid_argument,
                  "setpoint on release_to_policy");

  ControlRequest relationship_without_intent =
      request_of(s, "shape-5", ControlIntent::hold_setpoint, fan_percent(5000), tick);
  relationship_without_intent.relationship = s.relationship;
  expect_decision(engine, relationship_without_intent, StatusCode::invalid_argument,
                  "relationship named for a non-pressure intent");

  ControlRequest restore_without_relationship =
      request_of(s, "shape-6", ControlIntent::restore_pressure_relationship, fan_percent(5000), tick);
  expect_decision(engine, restore_without_relationship, StatusCode::invalid_argument,
                  "restore without a relationship");

  ControlRequest future = request_of(s, "shape-7", ControlIntent::hold_setpoint, fan_percent(5000),
                                     LogicalTick::from(tick.value() + 1));
  expect_decision(engine, future, StatusCode::out_of_range, "request instant in the future");

  // The shape checks all precede identity resolution: an unregistered device is
  // not reported until the request is well formed.
  ControlRequest unregistered = request_of(s, "shape-8", ControlIntent::hold_setpoint,
                                           fan_percent(5000), tick);
  unregistered.device = AirflowDeviceId::parse("dev-absent").value();
  expect_decision(engine, unregistered, StatusCode::not_found, "unregistered device");

  // 2 - idempotent replay precedes identity too.
  ControlRequest issued = request_of(s, "shape-9", ControlIntent::hold_setpoint, fan_percent(6000), tick);
  const Result<AttemptRecord> first = engine.issue(issued, adapter);
  REQUIRE(first.ok());
  CHECK_EQ(adapter.executes(), std::uint64_t{1});
  ControlRequest replay = issued;
  replay.device = AirflowDeviceId::parse("dev-absent").value();  // shape is still valid
  expect_decision(engine, replay, StatusCode::idempotency_conflict, "replay of a changed request");

  // Nothing above reached the adapter: the only call was the one issue() made.
  CHECK_EQ(adapter.executes(), std::uint64_t{1});
  CHECK_EQ(adapter.reads(), std::uint64_t{0});

  // The latch the first issue() set is resolved, so the generation check below is
  // the first precondition that fails.
  CHECK(engine.resolve_attempt(ResolveAttemptRequest{.attempt = first.value().id,
                                                     .target = AttemptState::resolved_without_effect,
                                                     .actor = s.actor,
                                                     .at = tick,
                                                     .reason = "cleared for the precedence probe"})
            .ok());

  // A failed precondition never reaches the adapter at all.
  ControlRequest refused = request_of(s, "shape-10", ControlIntent::hold_setpoint, fan_percent(5000), tick);
  refused.device_generation = DeviceGeneration::from(9);
  const Result<AttemptRecord> refused_issue = engine.issue(refused, adapter);
  CHECK(!refused_issue.ok());
  CHECK_EQ(refused_issue.code(), StatusCode::generation_mismatch);
  CHECK_EQ(adapter.executes(), std::uint64_t{1});
}

AIRFLOW_TEST(identity_lifecycle_and_attempt_precedence) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  Result<AirflowControlEngine> opened = scenario_engine(s);
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value();
  const LogicalTick tick = engine.current_tick();
  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, s);

  // 4 - identity binding: a relationship declared for another room.
  CHECK(engine.define_pressure_relationship(DefineRelationshipRequest{
            .id = PressureRelationshipId::parse("rel-other").value(),
            .room = RoomId::parse("room-other").value(),
            .controlled_space = SpaceRefId::parse("space-controlled-other").value(),
            .reference_space = SpaceRefId::parse("space-reference-other").value(),
            .polarity = PressurePolarity::negative,
            .lower = Pressure::from_millipascals(-30'000),
            .upper = Pressure::from_millipascals(-10'000),
            .tolerance = Pressure::from_millipascals(2'000),
            .evidence_generation = s.evidence_generation,
            .expected_revision = std::nullopt,
            .actor = s.actor,
            .requested_at = tick})
            .ok());
  ControlRequest foreign = request_of(s, "id-1", ControlIntent::restore_pressure_relationship,
                                      fan_percent(5000), tick);
  foreign.relationship = PressureRelationshipId::parse("rel-other").value();
  expect_decision(engine, foreign, StatusCode::identity_mismatch, "relationship in another room");

  ControlRequest undeclared = foreign;
  undeclared.relationship = PressureRelationshipId::parse("rel-absent").value();
  expect_decision(engine, undeclared, StatusCode::not_found, "undeclared relationship");

  // 5 - lifecycle gate precedes generation, revision, epoch, and authority.
  CHECK(engine.set_device_lifecycle(SetLifecycleRequest{
            .device = s.device,
            .generation = s.device_generation,
            .expected_revision = device_revision(engine, s.device).value(),
            .target = DeviceLifecycle::isolated,
            .epoch = s.epoch,
            .actor = s.actor,
            .requested_at = tick})
            .ok());
  ControlRequest forbidden = request_of(s, "life-1", ControlIntent::hold_setpoint, fan_percent(5000), tick);
  forbidden.device_generation = DeviceGeneration::from(9);
  forbidden.expected_revision = StateRevision::from(999);
  forbidden.epoch = AuthorityEpoch::from(7);
  expect_decision(engine, forbidden, StatusCode::lifecycle_forbidden, "isolated device");
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});

  // Return the device to service before the remaining checks.
  CHECK(engine.set_device_lifecycle(SetLifecycleRequest{
            .device = s.device,
            .generation = s.device_generation,
            .expected_revision = device_revision(engine, s.device).value(),
            .target = DeviceLifecycle::active,
            .epoch = s.epoch,
            .actor = s.actor,
            .requested_at = tick})
            .ok());

  // 8 - device generation precedes 9 - revision, which precedes 10 - epoch.
  ControlRequest generation = request_of(s, "gen-1", ControlIntent::hold_setpoint, fan_percent(5000), tick);
  generation.device_generation = DeviceGeneration::from(9);
  generation.expected_revision = StateRevision::from(999);
  generation.epoch = AuthorityEpoch::from(7);
  expect_decision(engine, generation, StatusCode::generation_mismatch, "wrong generation");

  ControlRequest revision = request_of(s, "rev-1", ControlIntent::hold_setpoint, fan_percent(5000), tick);
  revision.expected_revision = StateRevision::from(999);
  revision.epoch = AuthorityEpoch::from(7);
  expect_decision(engine, revision, StatusCode::revision_mismatch, "wrong revision");

  ControlRequest epoch = request_of(s, "epoch-1", ControlIntent::hold_setpoint, fan_percent(5000), tick);
  epoch.epoch = AuthorityEpoch::from(7);
  expect_decision(engine, epoch, StatusCode::epoch_stale, "unadopted epoch");

  ControlRequest current_revision = request_of(s, "rev-2", ControlIntent::hold_setpoint, fan_percent(5000), tick);
  current_revision.expected_revision = device_revision(engine, s.device).value();
  expect_decision(engine, current_revision, StatusCode::ok, "current revision");

  // 6 - the unresolved latch precedes the generation check.
  const Result<AttemptRecord> issued = engine.issue(
      request_of(s, "latch-1", ControlIntent::hold_setpoint, fan_percent(6000), tick), adapter);
  REQUIRE(issued.ok());
  CHECK_EQ(issued.value().state, AttemptState::acknowledged);
  CHECK(engine.device(s.device).value().unresolved_attempt.has_value());
  CHECK_EQ(*engine.device(s.device).value().unresolved_attempt, issued.value().id);

  ControlRequest blocked = request_of(s, "latch-2", ControlIntent::hold_setpoint, fan_percent(6000), tick);
  blocked.device_generation = DeviceGeneration::from(9);
  const Result<Decision> blocked_decision = engine.evaluate(blocked);
  CHECK(blocked_decision.ok());
  CHECK_EQ(blocked_decision.value().code, StatusCode::attempt_unresolved);
  CHECK_EQ(blocked_decision.value().message,
           blocked_decision.value().message);  // identical on repetition
  const Result<Decision> blocked_again = engine.evaluate(blocked);
  CHECK_EQ(blocked_again.value().message, blocked_decision.value().message);
  // The identity of the blocking attempt is reported.
  CHECK(blocked_decision.value().message.find(issued.value().id.to_string()) != std::string::npos);
  const Result<AttemptRecord> blocked_issue = engine.issue(blocked, adapter);
  CHECK(!blocked_issue.ok());
  CHECK_EQ(blocked_issue.code(), StatusCode::attempt_unresolved);
  CHECK(blocked_issue.message().find(issued.value().id.to_string()) != std::string::npos);
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{1});

  // 6 - supersession: only the exact unresolved attempt, at least as much
  // airflow, or a safety-directed request with a permit.
  ControlRequest lower = request_of(s, "sup-1", ControlIntent::hold_setpoint, fan_percent(5000), tick);
  lower.supersede = issued.value().id;
  expect_decision(engine, lower, StatusCode::supersession_unsupported, "supersession that reduces airflow");

  ControlRequest higher = request_of(s, "sup-2", ControlIntent::hold_setpoint, fan_percent(7000), tick);
  higher.supersede = issued.value().id;
  expect_decision(engine, higher, StatusCode::ok, "supersession that increases airflow");

  ControlRequest equal = request_of(s, "sup-3", ControlIntent::hold_setpoint, fan_percent(6000), tick);
  equal.supersede = issued.value().id;
  expect_decision(engine, equal, StatusCode::ok, "supersession with the same delivery");

  ControlRequest other_attempt = request_of(s, "sup-4", ControlIntent::hold_setpoint, fan_percent(7000), tick);
  other_attempt.supersede = AttemptId::from(4242);
  expect_decision(engine, other_attempt, StatusCode::not_found, "supersession of an unknown attempt");

  // The explicit supersession succeeds and marks the incumbent.
  const Result<AttemptRecord> superseding = engine.issue(higher, adapter);
  REQUIRE(superseding.ok());
  CHECK_EQ(engine.attempt(issued.value().id).value().state, AttemptState::superseded);
  CHECK_EQ(*engine.attempt(issued.value().id).value().superseded_by, superseding.value().id);
  CHECK_EQ(superseding.value().state, AttemptState::acknowledged);
  CHECK_EQ(engine.attempt(issued.value().id).value().supersedes.has_value(), false);
}

AIRFLOW_TEST(containment_pressure_obligation_precedence) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  Result<AirflowControlEngine> opened = scenario_engine(s);
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value();
  const LogicalTick tick = engine.current_tick();
  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, s);

  // 7 - containment. An optimization is blocked by every non-intact state; a
  // protective request is not blocked at all.
  ControlRequest optimization = request_of(s, "c-1", ControlIntent::lower_airflow, fan_percent(4000), tick);
  expect_decision(engine, optimization, StatusCode::pressure_unknown, "optimization with intact containment and no pressure proof");

  CHECK(engine.report_containment(ReportContainmentRequest{.element = s.containment,
                                                           .state = ContainmentState::breached,
                                                           .quality = Quality::good,
                                                           .source = s.source,
                                                           .sequence = EvidenceSequence::from(2),
                                                           .evidence_generation = s.evidence_generation,
                                                           .measured_at = tick})
            .ok());
  expect_decision(engine, optimization, StatusCode::containment_breached, "breached containment");

  ControlRequest protective = request_of(s, "c-2", ControlIntent::raise_airflow, fan_percent(4000), tick);
  expect_decision(engine, protective, StatusCode::ok, "protective request during a breach");
  ControlRequest protective_bad_generation =
      request_of(s, "c-3", ControlIntent::raise_airflow, fan_percent(4000), tick);
  protective_bad_generation.device_generation = DeviceGeneration::from(9);
  expect_decision(engine, protective_bad_generation, StatusCode::generation_mismatch,
                  "protective request past containment");

  CHECK(engine.report_containment(ReportContainmentRequest{.element = s.containment,
                                                           .state = ContainmentState::open_for_service,
                                                           .quality = Quality::good,
                                                           .source = s.source,
                                                           .sequence = EvidenceSequence::from(3),
                                                           .evidence_generation = s.evidence_generation,
                                                           .measured_at = tick})
            .ok());
  expect_decision(engine, optimization, StatusCode::containment_open, "deliberate service opening");

  CHECK(engine.report_containment(ReportContainmentRequest{.element = s.containment,
                                                           .state = ContainmentState::intact,
                                                           .quality = Quality::bad,
                                                           .source = s.source,
                                                           .sequence = EvidenceSequence::from(4),
                                                           .evidence_generation = s.evidence_generation,
                                                           .measured_at = tick})
            .ok());
  expect_decision(engine, optimization, StatusCode::containment_unknown,
                  "a report whose quality cannot establish containment");

  CHECK(engine.report_containment(ReportContainmentRequest{.element = s.containment,
                                                           .state = ContainmentState::intact,
                                                           .quality = Quality::good,
                                                           .source = s.source,
                                                           .sequence = EvidenceSequence::from(5),
                                                           .evidence_generation = s.evidence_generation,
                                                           .measured_at = tick})
            .ok());
  expect_decision(engine, optimization, StatusCode::pressure_unknown, "intact again, pressure still unknown");

  // A device whose room declares no containment element at all is unknown.
  {
    Result<AirflowControlEngine> bare = AirflowControlEngine::open_in_memory(base_options());
    REQUIRE(bare.ok());
    AirflowControlEngine& engine2 = bare.value();
    Result<Scenario> s2 = make_scenario("9", 1);
    REQUIRE(s2.ok());
    const Scenario other = s2.value();
    CHECK(engine2.adopt_epoch(other.epoch, other.actor, other.start_tick).ok());
    CHECK(engine2.register_device(RegisterDeviceRequest{.device = other.device,
                                                        .generation = other.device_generation,
                                                        .room = other.room,
                                                        .row = other.row,
                                                        .actor = other.actor,
                                                        .requested_at = engine2.current_tick()})
              .ok());
    CHECK(engine2.add_grant(PermissionGrant{.id = other.grant,
                                            .issuer = other.issuer,
                                            .epoch = other.epoch,
                                            .device = other.device,
                                            .device_generation = other.device_generation,
                                            .room = other.room,
                                            .actions = ActionSet::all(),
                                            .issued_at = engine2.current_tick(),
                                            .expires_at = std::nullopt,
                                            .revoked = false},
                            other.actor, engine2.current_tick())
              .ok());
    CHECK(engine2.set_device_lifecycle(SetLifecycleRequest{.device = other.device,
                                                           .generation = other.device_generation,
                                                           .expected_revision = StateRevision::from(1),
                                                           .target = DeviceLifecycle::active,
                                                           .epoch = other.epoch,
                                                           .actor = other.actor,
                                                           .requested_at = engine2.current_tick()})
              .ok());
    expect_decision(engine2,
                    request_of(other, "c-4", ControlIntent::trim_for_efficiency, fan_percent(4000),
                               engine2.current_tick()),
                    StatusCode::containment_unknown, "no containment element declared");
  }

  // 14 - pressure. The fixture's relationship is negative: -20.000 Pa is inside
  // the band, -50.000 Pa is outside it.
  CHECK(engine.observe(ObservationDraft{.payload = PressureReading{Pressure::from_millipascals(-20'000)},
                                        .device = s.device,
                                        .relationship = s.relationship,
                                        .point = SpaceRefId::parse("point-pressure-1").value(),
                                        .source = s.source,
                                        .sequence = EvidenceSequence::from(1),
                                        .measured_at = tick,
                                        .device_generation = s.device_generation,
                                        .evidence_generation = s.evidence_generation,
                                        .quality = Quality::good},
                       tick)
            .ok());
  expect_decision(engine, optimization, StatusCode::ok, "optimization with satisfied pressure");

  // Restore names one relationship and requires it to be proven; a protective
  // request that is not about pressure is not blocked by an unknown one.
  ControlRequest restore = request_of(s, "p-1", ControlIntent::restore_pressure_relationship,
                                      fan_percent(5000), tick);
  restore.relationship = s.relationship;
  expect_decision(engine, restore, StatusCode::ok, "restore with satisfied pressure");

  // 15 - obligations are enforced for optimization only.
  CHECK(engine.declare_obligation(DeclareObligationRequest{
            .id = ObligationId::parse("ob-1").value(),
            .scope = ObligationScope::room,
            .room = s.room,
            .row = std::nullopt,
            .rack = std::nullopt,
            .klass = ObligationClass::protected_obligation,
            .binding = ObligationBinding::device_sum,
            .metered_point = std::nullopt,
            .devices = {s.device},
            .minimum_airflow = Airflow::from_cubic_metres_per_hour(1'000),
            .target_airflow = Airflow::from_cubic_metres_per_hour(2'000),
            .source = s.source,
            .evidence_generation = s.evidence_generation,
            .expected_revision = std::nullopt,
            .actor = s.actor,
            .requested_at = tick})
            .ok());
  expect_decision(engine, optimization, StatusCode::obligation_unknown, "obligation with no current reading");
  expect_decision(engine, protective, StatusCode::ok, "protective request ignores obligations");

  CHECK(engine.observe(ObservationDraft{.payload = AirflowReading{Airflow::from_cubic_metres_per_hour(500)},
                                        .device = s.device,
                                        .relationship = std::nullopt,
                                        .point = SpaceRefId::parse("point-airflow-1").value(),
                                        .source = s.source,
                                        .sequence = EvidenceSequence::from(1),
                                        .measured_at = tick,
                                        .device_generation = s.device_generation,
                                        .evidence_generation = s.evidence_generation,
                                        .quality = Quality::good},
                       tick)
            .ok());
  expect_decision(engine, optimization, StatusCode::obligation_unsatisfied, "obligation below its minimum");

  // An advisory obligation is an optimization input that never blocks.
  CHECK(engine.declare_obligation(DeclareObligationRequest{
            .id = ObligationId::parse("ob-1").value(),
            .scope = ObligationScope::room,
            .room = s.room,
            .row = std::nullopt,
            .rack = std::nullopt,
            .klass = ObligationClass::advisory,
            .binding = ObligationBinding::device_sum,
            .metered_point = std::nullopt,
            .devices = {s.device},
            .minimum_airflow = Airflow::from_cubic_metres_per_hour(1'000),
            .target_airflow = Airflow::from_cubic_metres_per_hour(2'000),
            .source = s.source,
            .evidence_generation = s.evidence_generation,
            .expected_revision = std::nullopt,
            .actor = s.actor,
            .requested_at = tick})
            .ok());
  expect_decision(engine, optimization, StatusCode::ok, "an advisory obligation never blocks");

  // A reading outside the band makes the relationship violated, which blocks an
  // optimization but not a request that restores or increases airflow.
  CHECK(engine.observe(ObservationDraft{.payload = PressureReading{Pressure::from_millipascals(-50'000)},
                                        .device = s.device,
                                        .relationship = s.relationship,
                                        .point = SpaceRefId::parse("point-pressure-1").value(),
                                        .source = s.source,
                                        .sequence = EvidenceSequence::from(2),
                                        .measured_at = tick,
                                        .device_generation = s.device_generation,
                                        .evidence_generation = s.evidence_generation,
                                        .quality = Quality::good},
                       tick)
            .ok());
  expect_decision(engine, optimization, StatusCode::pressure_violated,
                  "violated pressure blocks optimization");
  expect_decision(engine, restore, StatusCode::ok,
                  "a violated relationship does not block a restore");
  expect_decision(engine, protective, StatusCode::ok, "protective request while pressure is violated");

  // A second, disagreeing source makes the relationship conflicted. Contradictory
  // evidence is never treated as safe, for any class of request.
  CHECK(engine.observe(ObservationDraft{.payload = PressureReading{Pressure::from_millipascals(-20'000)},
                                        .device = s.device,
                                        .relationship = s.relationship,
                                        .point = SpaceRefId::parse("point-pressure-2").value(),
                                        .source = SourceId::parse("source-second").value(),
                                        .sequence = EvidenceSequence::from(1),
                                        .measured_at = tick,
                                        .device_generation = s.device_generation,
                                        .evidence_generation = s.evidence_generation,
                                        .quality = Quality::good},
                       tick)
            .ok());
  const Result<RelationshipView> relationship = engine.relationship(s.relationship);
  REQUIRE(relationship.ok());
  CHECK_EQ(relationship.value().state, PressureState::conflicted);
  expect_decision(engine, optimization, StatusCode::pressure_conflicted, "conflicted pressure");
  expect_decision(engine, restore, StatusCode::pressure_conflicted,
                  "conflicted pressure blocks restore");
}

AIRFLOW_TEST(authority_and_interlock_precedence) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  Result<AirflowControlEngine> opened = scenario_engine(s);
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value();
  const LogicalTick tick = engine.current_tick();
  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, s);

  // 11 - permission, in its three flavours, precedes every evidence check.
  Result<Scenario> second = make_scenario("2", 1);
  REQUIRE(second.ok());
  const Scenario bare = second.value();
  CHECK(engine.adopt_epoch(AuthorityEpoch::from(2), s.actor, tick).ok());
  CHECK(engine.register_device(RegisterDeviceRequest{.device = bare.device,
                                                     .generation = bare.device_generation,
                                                     .room = bare.room,
                                                     .row = bare.row,
                                                     .actor = s.actor,
                                                     .requested_at = tick})
            .ok());
  const LogicalTick tick2 = engine.current_tick();
  ControlRequest bare_request = request_of(bare, "perm-1", ControlIntent::hold_setpoint,
                                           fan_percent(5000), tick2);
  bare_request.epoch = AuthorityEpoch::from(2);
  // The lifecycle gate precedes authority: a provisioned device is refused before
  // any grant is even considered.
  expect_decision(engine, bare_request, StatusCode::lifecycle_forbidden,
                  "a provisioned device is refused before authority is considered");

  CHECK(engine.add_grant(PermissionGrant{.id = bare.grant,
                                         .issuer = bare.issuer,
                                         .epoch = AuthorityEpoch::from(2),
                                         .device = bare.device,
                                         .device_generation = bare.device_generation,
                                         .room = bare.room,
                                         .actions = ActionSet::all(),
                                         .issued_at = tick2,
                                         .expires_at = std::nullopt,
                                         .revoked = false},
                           s.actor, tick2)
            .ok());
  CHECK(engine.set_device_lifecycle(SetLifecycleRequest{.device = bare.device,
                                                        .generation = bare.device_generation,
                                                        .expected_revision = StateRevision::from(1),
                                                        .target = DeviceLifecycle::active,
                                                        .epoch = AuthorityEpoch::from(2),
                                                        .actor = s.actor,
                                                        .requested_at = tick2})
            .ok());
  CHECK(engine.set_fan_policy(SetFanPolicyRequest{
            .device = bare.device,
            .generation = bare.device_generation,
            .expected_revision = StateRevision::from(2),
            .policy = FanPolicy{.id = bare.policy,
                                .generation = bare.policy_generation,
                                .envelope = OperatingEnvelope{
                                    .min_fan_percent = bare.min_fan_percent,
                                    .max_fan_percent = bare.max_fan_percent,
                                    .default_fan_percent = bare.default_fan_percent,
                                    .min_airflow = bare.min_airflow,
                                    .max_airflow = bare.max_airflow,
                                    .max_step = bare.max_step,
                                    .source = bare.source,
                                    .evidence_generation = bare.evidence_generation}},
            .actor = s.actor,
            .requested_at = tick2})
            .ok());
  // The device is now commissionable and control-ready, which is the starting
  // point for the permit, interlock, and envelope checks below.
  ControlRequest ready = request_of(bare, "perm-ready", ControlIntent::hold_setpoint, fan_percent(5000), tick2);
  ready.epoch = AuthorityEpoch::from(2);
  expect_decision(engine, ready, StatusCode::ok, "a fully granted device is eligible");

  // A second device carries the three permission outcomes. It is commissioned
  // with a narrow grant, which is then withdrawn.
  Result<Scenario> third_scenario = make_scenario("3", 1);
  REQUIRE(third_scenario.ok());
  const Scenario third = third_scenario.value();
  CHECK(engine.register_device(RegisterDeviceRequest{.device = third.device,
                                                     .generation = third.device_generation,
                                                     .room = third.room,
                                                     .row = third.row,
                                                     .actor = s.actor,
                                                     .requested_at = tick2})
            .ok());
  CHECK(engine.add_grant(PermissionGrant{.id = third.grant,
                                         .issuer = third.issuer,
                                         .epoch = AuthorityEpoch::from(2),
                                         .device = third.device,
                                         .device_generation = third.device_generation,
                                         .room = third.room,
                                         .actions = ActionSet::of({ControlAction::set_lifecycle,
                                                                   ControlAction::set_policy}),
                                         .issued_at = tick2,
                                         .expires_at = std::nullopt,
                                         .revoked = false},
                           s.actor, tick2)
            .ok());
  CHECK(engine.set_device_lifecycle(SetLifecycleRequest{.device = third.device,
                                                        .generation = third.device_generation,
                                                        .expected_revision = StateRevision::from(1),
                                                        .target = DeviceLifecycle::active,
                                                        .epoch = AuthorityEpoch::from(2),
                                                        .actor = s.actor,
                                                        .requested_at = tick2})
            .ok());
  CHECK(engine.set_fan_policy(SetFanPolicyRequest{
            .device = third.device,
            .generation = third.device_generation,
            .expected_revision = StateRevision::from(2),
            .policy = FanPolicy{.id = third.policy,
                                .generation = third.policy_generation,
                                .envelope = OperatingEnvelope{
                                    .min_fan_percent = third.min_fan_percent,
                                    .max_fan_percent = third.max_fan_percent,
                                    .default_fan_percent = third.default_fan_percent,
                                    .min_airflow = third.min_airflow,
                                    .max_airflow = third.max_airflow,
                                    .max_step = third.max_step,
                                    .source = third.source,
                                    .evidence_generation = third.evidence_generation}},
            .actor = s.actor,
            .requested_at = tick2})
            .ok());

  // Every grant this device holds is withdrawn: nothing issues the action.
  CHECK(engine.revoke_grant(third.grant, AuthorityEpoch::from(2), s.actor, tick2).ok());
  ControlRequest third_request = request_of(third, "perm-1", ControlIntent::hold_setpoint,
                                            fan_percent(5000), tick2);
  third_request.epoch = AuthorityEpoch::from(2);
  expect_decision(engine, third_request, StatusCode::permission_missing, "no grant issues the action");

  // A grant that carries the action but has expired.
  CHECK(engine.add_grant(PermissionGrant{.id = GrantId::parse("grant-expiring").value(),
                                         .issuer = third.issuer,
                                         .epoch = AuthorityEpoch::from(2),
                                         .device = third.device,
                                         .device_generation = third.device_generation,
                                         .room = third.room,
                                         .actions = ActionSet::of({ControlAction::apply_setpoint}),
                                         .issued_at = tick2,
                                         .expires_at = std::optional<LogicalTick>(LogicalTick::from(tick2.value() + 1)),
                                         .revoked = false},
                           s.actor, tick2)
            .ok());
  CHECK(engine.advance_tick(LogicalTick::from(tick2.value() + 4)).ok());
  const LogicalTick late = engine.current_tick();
  ControlRequest expired = request_of(third, "perm-2", ControlIntent::hold_setpoint, fan_percent(5000), late);
  expired.epoch = AuthorityEpoch::from(2);
  expect_decision(engine, expired, StatusCode::permission_stale, "expired grant");

  // A grant exists for the device but does not include the required action.
  CHECK(engine.revoke_grant(GrantId::parse("grant-expiring").value(), AuthorityEpoch::from(2), s.actor, late).ok());
  CHECK(engine.add_grant(PermissionGrant{.id = GrantId::parse("grant-wrong-action").value(),
                                         .issuer = third.issuer,
                                         .epoch = AuthorityEpoch::from(2),
                                         .device = third.device,
                                         .device_generation = third.device_generation,
                                         .room = third.room,
                                         .actions = ActionSet::of({ControlAction::set_policy}),
                                         .issued_at = late,
                                         .expires_at = std::nullopt,
                                         .revoked = false},
                           s.actor, late)
            .ok());
  expect_decision(engine, expired, StatusCode::permission_denied, "grant without the action");

  // The action is granted, but by an epoch that is no longer adopted.
  CHECK(engine.add_grant(PermissionGrant{.id = GrantId::parse("grant-old-epoch").value(),
                                         .issuer = third.issuer,
                                         .epoch = AuthorityEpoch::from(1),
                                         .device = third.device,
                                         .device_generation = third.device_generation,
                                         .room = third.room,
                                         .actions = ActionSet::of({ControlAction::apply_setpoint}),
                                         .issued_at = late,
                                         .expires_at = std::nullopt,
                                         .revoked = false},
                           s.actor, late)
            .ok());
  expect_decision(engine, expired, StatusCode::permission_stale, "grant from another epoch");

  // 12 - the safety permit precedes 13 - interlocks, and safety-directed
  // requests skip containment, pressure, and interlocks entirely.
  CHECK(engine.declare_interlock(DeclareInterlockRequest{.id = InterlockId::parse("il-open").value(),
                                                         .room = bare.room,
                                                         .row = bare.row,
                                                         .device = bare.device,
                                                         .klass = InterlockClass::protected_obligation,
                                                         .epoch = AuthorityEpoch::from(2),
                                                         .actor = s.actor,
                                                         .declared_at = late})
            .ok());
  ControlRequest purge = request_of(bare, "purge-1", ControlIntent::emergency_purge, fan_percent(5000), late);
  purge.epoch = AuthorityEpoch::from(2);
  expect_decision(engine, purge, StatusCode::safety_permit_missing, "purge without a permit");

  purge.safety_permit = SafetyPermitId::parse("permit-absent").value();
  expect_decision(engine, purge, StatusCode::safety_permit_missing, "purge naming an undeclared permit");

  CHECK(engine.add_safety_permit(SafetyPermit{.id = SafetyPermitId::parse("permit-other").value(),
                                              .issuer = bare.issuer,
                                              .epoch = AuthorityEpoch::from(2),
                                              .device = s.device,
                                              .issued_at = late,
                                              .expires_at = std::nullopt,
                                              .reason = "another device"},
                                 s.actor, late)
            .ok());
  purge.safety_permit = SafetyPermitId::parse("permit-other").value();
  expect_decision(engine, purge, StatusCode::identity_mismatch, "permit for another device");

  CHECK(engine.add_safety_permit(SafetyPermit{.id = SafetyPermitId::parse("permit-short").value(),
                                              .issuer = bare.issuer,
                                              .epoch = AuthorityEpoch::from(2),
                                              .device = bare.device,
                                              .issued_at = late,
                                              .expires_at = std::optional<LogicalTick>(LogicalTick::from(late.value() + 1)),
                                              .reason = "short lived"},
                                 s.actor, late)
            .ok());
  CHECK(engine.advance_tick(LogicalTick::from(late.value() + 3)).ok());
  ControlRequest stale_permit = request_of(bare, "purge-2", ControlIntent::emergency_purge, fan_percent(5000),
                                           engine.current_tick());
  stale_permit.epoch = AuthorityEpoch::from(2);
  stale_permit.safety_permit = SafetyPermitId::parse("permit-short").value();
  expect_decision(engine, stale_permit, StatusCode::safety_permit_stale, "expired permit");

  // A current permit authorises the purge even though a protected interlock is
  // unknown: safety-directed requests are not gated by interlocks.
  CHECK(engine.add_safety_permit(SafetyPermit{.id = SafetyPermitId::parse("permit-live").value(),
                                              .issuer = bare.issuer,
                                              .epoch = AuthorityEpoch::from(2),
                                              .device = bare.device,
                                              .issued_at = engine.current_tick(),
                                              .expires_at = std::nullopt,
                                              .reason = "smoke"},
                                 s.actor, engine.current_tick())
            .ok());
  ControlRequest permitted = request_of(bare, "purge-3", ControlIntent::emergency_purge, fan_percent(5000),
                                        engine.current_tick());
  permitted.epoch = AuthorityEpoch::from(2);
  permitted.safety_permit = SafetyPermitId::parse("permit-live").value();
  expect_decision(engine, permitted, StatusCode::ok, "purge with a current permit");

  // A non-safety request is refused while the protected interlock is unknown.
  ControlRequest held = request_of(bare, "interlock-1", ControlIntent::hold_setpoint, fan_percent(5000),
                                   engine.current_tick());
  held.epoch = AuthorityEpoch::from(2);
  expect_decision(engine, held, StatusCode::interlock_unknown, "unreported protected interlock");

  CHECK(engine.report_interlock(ReportInterlockRequest{.id = InterlockId::parse("il-open").value(),
                                                       .state = InterlockState::open,
                                                       .sequence = EvidenceSequence::from(1),
                                                       .epoch = AuthorityEpoch::from(2),
                                                       .reported_at = engine.current_tick()})
            .ok());
  expect_decision(engine, held, StatusCode::interlock_open, "open protected interlock");

  CHECK(engine.report_interlock(ReportInterlockRequest{.id = InterlockId::parse("il-open").value(),
                                                       .state = InterlockState::satisfied,
                                                       .sequence = EvidenceSequence::from(2),
                                                       .epoch = AuthorityEpoch::from(2),
                                                       .reported_at = engine.current_tick()})
            .ok());
  expect_decision(engine, held, StatusCode::ok, "satisfied protected interlock");

  // 16 - the envelope, and the slew bound measured from the policy default.
  ControlRequest outside = request_of(bare, "limit-1", ControlIntent::hold_setpoint, fan_percent(100), late);
  outside.epoch = AuthorityEpoch::from(2);
  expect_decision(engine, outside, StatusCode::limit_exceeded, "setpoint outside the envelope");
  ControlRequest slewing = request_of(bare, "limit-2", ControlIntent::hold_setpoint, fan_percent(7000), late);
  slewing.epoch = AuthorityEpoch::from(2);
  expect_decision(engine, slewing, StatusCode::limit_exceeded, "setpoint beyond the slew bound");

  // A device with no fan policy has no known envelope at all.
  CHECK(engine.register_device(RegisterDeviceRequest{.device = AirflowDeviceId::parse("dev-nopolicy").value(),
                                                     .generation = DeviceGeneration::from(1),
                                                     .room = RoomId::parse("room-nopolicy").value(),
                                                     .row = std::nullopt,
                                                     .actor = s.actor,
                                                     .requested_at = engine.current_tick()})
            .ok());
  CHECK(engine.add_grant(PermissionGrant{.id = GrantId::parse("grant-nopolicy").value(),
                                         .issuer = s.issuer,
                                         .epoch = AuthorityEpoch::from(2),
                                         .device = AirflowDeviceId::parse("dev-nopolicy").value(),
                                         .device_generation = DeviceGeneration::from(1),
                                         .room = RoomId::parse("room-nopolicy").value(),
                                         .actions = ActionSet::all(),
                                         .issued_at = engine.current_tick(),
                                         .expires_at = std::nullopt,
                                         .revoked = false},
                           s.actor, engine.current_tick())
            .ok());
  CHECK(engine.set_device_lifecycle(SetLifecycleRequest{.device = AirflowDeviceId::parse("dev-nopolicy").value(),
                                                        .generation = DeviceGeneration::from(1),
                                                        .expected_revision = StateRevision::from(1),
                                                        .target = DeviceLifecycle::active,
                                                        .epoch = AuthorityEpoch::from(2),
                                                        .actor = s.actor,
                                                        .requested_at = engine.current_tick()})
            .ok());
  const ControlRequest no_policy{.key = IdempotencyKey::parse("limit-3").value(),
                                 .device = AirflowDeviceId::parse("dev-nopolicy").value(),
                                 .device_generation = DeviceGeneration::from(1),
                                 .epoch = AuthorityEpoch::from(2),
                                 .expected_revision = std::nullopt,
                                 .intent = ControlIntent::hold_setpoint,
                                 .setpoint = fan_percent(5000),
                                 .actor = s.actor,
                                 .requested_at = engine.current_tick(),
                                 .safety_permit = std::nullopt,
                                 .supersede = std::nullopt,
                                 .relationship = std::nullopt};
  expect_decision(engine, no_policy, StatusCode::limit_unknown, "device without a fan policy");
}

AIRFLOW_TEST(acknowledgement_is_not_effect) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  Result<Fixture> fixture = Fixture::open(ScenarioOptions{});
  REQUIRE(fixture.ok());
  AirflowControlEngine& engine = fixture.value().engine();
  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, s);
  adapter.set_apply_on_execute(false);
  const LogicalTick tick = engine.current_tick();
  const std::optional<SyntheticPlantState> before = adapter.plant_state(s.device);
  REQUIRE(before.has_value());

  const Result<AttemptRecord> issued =
      engine.issue(request_of(s, "ack-1", ControlIntent::hold_setpoint, fan_percent(6000), tick), adapter);
  REQUIRE(issued.ok());
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{1});
  CHECK_EQ(issued.value().state, AttemptState::acknowledged);
  CHECK_EQ(issued.value().disposition, AdapterDisposition::accepted);
  CHECK_EQ(issued.value().resolved_at.has_value(), false);
  CHECK_EQ(issued.value().dispatched_at.has_value(), true);

  // Nothing moved: not the plant, not the effect, not the device's state.
  const std::optional<SyntheticPlantState> after = adapter.plant_state(s.device);
  REQUIRE(after.has_value());
  CHECK_EQ(after.value().fan_percent, before.value().fan_percent);
  CHECK_EQ(after.value().airflow, before.value().airflow);
  const Result<DeviceView> view = engine.device(s.device);
  REQUIRE(view.ok());
  CHECK_EQ(view.value().effect, EffectState::unverified);
  CHECK_EQ(view.value().has_fan_observation, false);
  CHECK(view.value().unresolved_attempt.has_value());
  CHECK_EQ(*view.value().unresolved_attempt, issued.value().id);

  // Verification without any accepted delivery reading cannot claim an effect.
  VerificationRequest verification{.attempt = issued.value().id,
                                   .adapter = nullptr,
                                   .source = s.source,
                                   .fan_point = SpaceRefId::parse("point-fan-1").value(),
                                   .pressure_point = SpaceRefId::parse("point-pressure-1").value(),
                                   .fan_sequence = EvidenceSequence::from(1),
                                   .pressure_sequence = EvidenceSequence::from(1),
                                   .at = tick,
                                   .relationship = std::nullopt};
  const Result<VerifiedEffect> effect = engine.verify(verification);
  REQUIRE(effect.ok());
  CHECK_EQ(effect.value().state, EffectState::indeterminate);
  CHECK(engine.device(s.device).value().effect != EffectState::effective);
  CHECK_EQ(engine.attempt(issued.value().id).value().state, AttemptState::effect_indeterminate);
}

AIRFLOW_TEST(definite_refusal_resolves_but_accepted_does_not) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  const AdapterDisposition definite[] = {AdapterDisposition::refused, AdapterDisposition::unavailable,
                                         AdapterDisposition::fault};
  const StatusCode expected[] = {StatusCode::adapter_refused, StatusCode::adapter_unavailable,
                                 StatusCode::adapter_fault};
  for (std::size_t index = 0; index < 3; ++index) {
    Result<Fixture> fixture = Fixture::open(ScenarioOptions{});
    REQUIRE(fixture.ok());
    AirflowControlEngine& engine = fixture.value().engine();
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    const LogicalTick tick = engine.current_tick();
    adapter.set_next_disposition(s.device, definite[index], "stated by the adapter");

    const Result<AttemptRecord> issued =
        engine.issue(request_of(s, "refuse-1", ControlIntent::hold_setpoint, fan_percent(6000), tick), adapter);
    REQUIRE(issued.ok());
    CHECK_EQ(issued.value().state,
             definite[index] == AdapterDisposition::refused
                 ? AttemptState::refused
                 : (definite[index] == AdapterDisposition::unavailable ? AttemptState::unavailable
                                                                       : AttemptState::faulted));
    CHECK_EQ(issued.value().disposition, definite[index]);
    CHECK(issued.value().resolved_at.has_value());
    CHECK_EQ(engine.device(s.device).value().unresolved_attempt.has_value(), false);
    CHECK_EQ(engine.device(s.device).value().effect, EffectState::unverified);

    // Verification of a definite refusal has no effect to establish and reports
    // the adapter's own status.
    VerificationRequest verification{.attempt = issued.value().id,
                                     .adapter = nullptr,
                                     .source = s.source,
                                     .fan_point = SpaceRefId::parse("point-fan-1").value(),
                                     .pressure_point = SpaceRefId::parse("point-pressure-1").value(),
                                     .fan_sequence = EvidenceSequence::from(1),
                                     .pressure_sequence = EvidenceSequence::from(1),
                                     .at = tick,
                                     .relationship = std::nullopt};
    const Result<VerifiedEffect> effect = engine.verify(verification);
    REQUIRE(effect.ok());
    CHECK_EQ(effect.value().state, EffectState::unverified);
    CHECK_EQ(effect.value().code, expected[index]);

    // A resolved attempt does not block the next command.
    adapter.clear_dispositions();
    const Result<Decision> next =
        engine.evaluate(request_of(s, "refuse-2", ControlIntent::hold_setpoint, fan_percent(6000), tick));
    CHECK(next.ok());
    CHECK_EQ(next.value().code, StatusCode::ok);
    const Result<AttemptRecord> second =
        engine.issue(request_of(s, "refuse-3", ControlIntent::hold_setpoint, fan_percent(6000), tick), adapter);
    CHECK(second.ok());
    CHECK_EQ(adapter.execute_calls(), std::uint64_t{2});

    // An attempt that resolves the same way again is refused: it is already
    // resolved.
    ResolveAttemptRequest resolve{.attempt = issued.value().id,
                                  .target = AttemptState::resolved_without_effect,
                                  .actor = s.actor,
                                  .at = tick,
                                  .reason = "already resolved"};
    const Status resolved = engine.resolve_attempt(resolve);
    CHECK(!resolved.ok());
    CHECK_EQ(resolved.code(), StatusCode::invalid_argument);
  }
}

AIRFLOW_TEST(verification_requires_a_fresh_bound_reading) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();

  // -- a bound, fresh reading of the commanded value establishes the effect.
  {
    Result<Fixture> fixture = Fixture::open(ScenarioOptions{});
    REQUIRE(fixture.ok());
    AirflowControlEngine& engine = fixture.value().engine();
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    const LogicalTick tick = engine.current_tick();
    const Result<AttemptRecord> issued =
        engine.issue(request_of(s, "verify-1", ControlIntent::hold_setpoint, fan_percent(6000), tick), adapter);
    REQUIRE(issued.ok());
    VerificationRequest verification{.attempt = issued.value().id,
                                     .adapter = &adapter,
                                     .source = s.source,
                                     .fan_point = SpaceRefId::parse("point-fan-1").value(),
                                     .pressure_point = SpaceRefId::parse("point-pressure-1").value(),
                                     .fan_sequence = EvidenceSequence::from(1),
                                     .pressure_sequence = EvidenceSequence::from(1),
                                     .at = tick,
                                     .relationship = std::nullopt};
    // Accepted at the same instant as the attempt: not strictly after it, so the
    // reading is not evidence about the command.
    const Result<VerifiedEffect> same_tick = engine.verify(verification);
    REQUIRE(same_tick.ok());
    CHECK_EQ(same_tick.value().state, EffectState::indeterminate);
    // An undecided verification records that the effect is undecided; it never
    // claims the effect was established.
    CHECK_EQ(engine.device(s.device).value().effect, EffectState::indeterminate);
    CHECK(engine.device(s.device).value().effect != EffectState::effective);
    CHECK_EQ(engine.attempt(issued.value().id).value().state, AttemptState::effect_indeterminate);
    CHECK(engine.device(s.device).value().unresolved_attempt.has_value());

    CHECK(engine.advance_tick(LogicalTick::from(tick.value() + 1)).ok());
    verification.at = engine.current_tick();
    verification.fan_sequence = EvidenceSequence::from(2);
    const Result<VerifiedEffect> later = engine.verify(verification);
    REQUIRE(later.ok());
    CHECK_EQ(later.value().state, EffectState::effective);
    CHECK_EQ(later.value().code, StatusCode::ok);
    CHECK(later.value().fan_observation.has_value());
    CHECK_EQ(later.value().observed_fan_percent.has_value(), true);
    CHECK_EQ(*later.value().observed_fan_percent, SetpointBasisPoints::create(6000).value());
    CHECK_EQ(engine.attempt(issued.value().id).value().state, AttemptState::effect_established);
    CHECK_EQ(engine.device(s.device).value().effect, EffectState::effective);
    CHECK_EQ(engine.device(s.device).value().unresolved_attempt.has_value(), false);

    // Re-verifying a resolved attempt reports the established effect rather than
    // re-deciding it.
    const Result<VerifiedEffect> again = engine.verify(verification);
    REQUIRE(again.ok());
    CHECK_EQ(again.value().state, EffectState::effective);
  }

  // -- a fresh reading that disagrees contradicts the effect.
  {
    Result<Fixture> fixture = Fixture::open(ScenarioOptions{});
    REQUIRE(fixture.ok());
    AirflowControlEngine& engine = fixture.value().engine();
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    adapter.set_apply_on_execute(false);
    const LogicalTick tick = engine.current_tick();
    const Result<AttemptRecord> issued =
        engine.issue(request_of(s, "verify-2", ControlIntent::hold_setpoint, fan_percent(6000), tick), adapter);
    REQUIRE(issued.ok());
    CHECK(engine.advance_tick(LogicalTick::from(tick.value() + 1)).ok());
    VerificationRequest verification{.attempt = issued.value().id,
                                     .adapter = &adapter,
                                     .source = s.source,
                                     .fan_point = SpaceRefId::parse("point-fan-1").value(),
                                     .pressure_point = SpaceRefId::parse("point-pressure-1").value(),
                                     .fan_sequence = EvidenceSequence::from(1),
                                     .pressure_sequence = EvidenceSequence::from(1),
                                     .at = engine.current_tick(),
                                     .relationship = std::nullopt};
    const Result<VerifiedEffect> contradicted = engine.verify(verification);
    REQUIRE(contradicted.ok());
    CHECK_EQ(contradicted.value().state, EffectState::contradicted);
    CHECK_EQ(*contradicted.value().observed_fan_percent, SetpointBasisPoints::create(5000).value());
    CHECK_EQ(engine.attempt(issued.value().id).value().state, AttemptState::effect_contradicted);
    CHECK_EQ(engine.device(s.device).value().effect, EffectState::contradicted);
    CHECK_EQ(engine.device(s.device).value().unresolved_attempt.has_value(), false);
  }

  // -- a stale reading is not proof.
  {
    Result<Fixture> fixture = Fixture::open(ScenarioOptions{});
    REQUIRE(fixture.ok());
    AirflowControlEngine& engine = fixture.value().engine();
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    const LogicalTick tick = engine.current_tick();
    const Result<AttemptRecord> issued =
        engine.issue(request_of(s, "verify-3", ControlIntent::hold_setpoint, fan_percent(6000), tick), adapter);
    REQUIRE(issued.ok());
    // A reading older than the configured freshness horizon, measured after the
    // command but too old to be current.
    adapter.set_measurement_backdate(2'000);
    CHECK(engine.advance_tick(LogicalTick::from(tick.value() + 2'500)).ok());
    VerificationRequest verification{.attempt = issued.value().id,
                                     .adapter = &adapter,
                                     .source = s.source,
                                     .fan_point = SpaceRefId::parse("point-fan-1").value(),
                                     .pressure_point = SpaceRefId::parse("point-pressure-1").value(),
                                     .fan_sequence = EvidenceSequence::from(1),
                                     .pressure_sequence = EvidenceSequence::from(1),
                                     .at = engine.current_tick(),
                                     .relationship = std::nullopt};
    const Result<VerifiedEffect> stale = engine.verify(verification);
    REQUIRE(stale.ok());
    CHECK_EQ(stale.value().state, EffectState::indeterminate);
    CHECK_EQ(engine.device(s.device).value().effect, EffectState::indeterminate);
    CHECK(engine.device(s.device).value().effect != EffectState::effective);
  }

  // -- a reading carrying the wrong evidence generation is not proof.
  {
    Result<Fixture> fixture = Fixture::open(ScenarioOptions{});
    REQUIRE(fixture.ok());
    AirflowControlEngine& engine = fixture.value().engine();
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    const LogicalTick tick = engine.current_tick();
    const Result<AttemptRecord> issued =
        engine.issue(request_of(s, "verify-4", ControlIntent::hold_setpoint, fan_percent(6000), tick), adapter);
    REQUIRE(issued.ok());
    adapter.set_evidence_generation_override(EvidenceGeneration::from(99));
    CHECK(engine.advance_tick(LogicalTick::from(tick.value() + 1)).ok());
    VerificationRequest verification{.attempt = issued.value().id,
                                     .adapter = &adapter,
                                     .source = s.source,
                                     .fan_point = SpaceRefId::parse("point-fan-1").value(),
                                     .pressure_point = SpaceRefId::parse("point-pressure-1").value(),
                                     .fan_sequence = EvidenceSequence::from(1),
                                     .pressure_sequence = EvidenceSequence::from(1),
                                     .at = engine.current_tick(),
                                     .relationship = std::nullopt};
    const Result<VerifiedEffect> wrong_generation = engine.verify(verification);
    REQUIRE(wrong_generation.ok());
    CHECK_EQ(wrong_generation.value().state, EffectState::indeterminate);
    CHECK_EQ(engine.device(s.device).value().effect, EffectState::indeterminate);
    CHECK(engine.device(s.device).value().effect != EffectState::effective);
  }

  // -- a reading for the wrong device generation is not even accepted.
  {
    Result<Fixture> fixture = Fixture::open(ScenarioOptions{});
    REQUIRE(fixture.ok());
    AirflowControlEngine& engine = fixture.value().engine();
    const LogicalTick tick = engine.current_tick();
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    const Result<AttemptRecord> issued =
        engine.issue(request_of(s, "verify-5", ControlIntent::hold_setpoint, fan_percent(6000), tick), adapter);
    REQUIRE(issued.ok());
    adapter.clear_dispositions();
    // A reading whose device generation is stale is refused at ingestion, so the
    // attempt has no delivery reading at all.
    CHECK(engine.advance_tick(LogicalTick::from(tick.value() + 1)).ok());
    const Result<Observation> observation = engine.observe(
        ObservationDraft{.payload = FanReading{SetpointBasisPoints::create(6000).value()},
                         .device = s.device,
                         .relationship = std::nullopt,
                         .point = SpaceRefId::parse("point-fan-1").value(),
                         .source = s.source,
                         .sequence = EvidenceSequence::from(1),
                         .measured_at = engine.current_tick(),
                         .device_generation = DeviceGeneration::from(9),
                         .evidence_generation = s.evidence_generation,
                         .quality = Quality::good},
        engine.current_tick());
    CHECK(!observation.ok());
    CHECK_EQ(observation.code(), StatusCode::generation_mismatch);
    VerificationRequest verification{.attempt = issued.value().id,
                                     .adapter = nullptr,
                                     .source = s.source,
                                     .fan_point = SpaceRefId::parse("point-fan-1").value(),
                                     .pressure_point = SpaceRefId::parse("point-pressure-1").value(),
                                     .fan_sequence = EvidenceSequence::from(1),
                                     .pressure_sequence = EvidenceSequence::from(1),
                                     .at = engine.current_tick(),
                                     .relationship = std::nullopt};
    const Result<VerifiedEffect> unknown = engine.verify(verification);
    REQUIRE(unknown.ok());
    CHECK_EQ(unknown.value().state, EffectState::indeterminate);
  }

  // -- verification refuses an instant that precedes the attempt or the clock.
  {
    Result<Fixture> fixture = Fixture::open(ScenarioOptions{});
    REQUIRE(fixture.ok());
    AirflowControlEngine& engine = fixture.value().engine();
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    const LogicalTick tick = engine.current_tick();
    const Result<AttemptRecord> issued =
        engine.issue(request_of(s, "verify-6", ControlIntent::hold_setpoint, fan_percent(6000), tick), adapter);
    REQUIRE(issued.ok());
    VerificationRequest verification{.attempt = issued.value().id,
                                     .adapter = nullptr,
                                     .source = s.source,
                                     .fan_point = SpaceRefId::parse("point-fan-1").value(),
                                     .pressure_point = SpaceRefId::parse("point-pressure-1").value(),
                                     .fan_sequence = EvidenceSequence::from(1),
                                     .pressure_sequence = EvidenceSequence::from(1),
                                     .at = tick,
                                     .relationship = std::nullopt};
    verification.at = LogicalTick::from(tick.value() + 5);
    CHECK_EQ(engine.verify(verification).code(), StatusCode::evidence_future);
    verification.at = tick;
    verification.attempt = AttemptId::from(9'999);
    CHECK_EQ(engine.verify(verification).code(), StatusCode::not_found);
  }
}

AIRFLOW_TEST(resolve_attempt_refuses_an_already_resolved_state) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  Result<Fixture> fixture = Fixture::open(ScenarioOptions{});
  REQUIRE(fixture.ok());
  AirflowControlEngine& engine = fixture.value().engine();
  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, s);
  const LogicalTick tick = engine.current_tick();

  const Result<AttemptRecord> issued =
      engine.issue(request_of(s, "resolve-1", ControlIntent::hold_setpoint, fan_percent(6000), tick), adapter);
  REQUIRE(issued.ok());
  // An attempt that is still unresolved can be resolved, and resolving it
  // releases the device latch.
  CHECK(engine.resolve_attempt(ResolveAttemptRequest{.attempt = issued.value().id,
                                                     .target = AttemptState::resolved_without_effect,
                                                     .actor = s.actor,
                                                     .at = tick,
                                                     .reason = "operator closed it"})
            .ok());
  const Result<AttemptRecord> resolved = engine.attempt(issued.value().id);
  REQUIRE(resolved.ok());
  CHECK_EQ(resolved.value().state, AttemptState::resolved_without_effect);
  CHECK_EQ(resolved.value().resolution_reason, std::string("operator closed it"));
  CHECK(resolved.value().resolved_at.has_value());
  CHECK_EQ(engine.device(s.device).value().unresolved_attempt.has_value(), false);

  // A second resolution of the same attempt is refused.
  const Status second = engine.resolve_attempt(ResolveAttemptRequest{.attempt = issued.value().id,
                                                                     .target = AttemptState::resolved_without_effect,
                                                                     .actor = s.actor,
                                                                     .at = tick,
                                                                     .reason = "again"});
  CHECK(!second.ok());
  CHECK_EQ(second.code(), StatusCode::invalid_argument);
  // An attempt cannot be resolved into a state that is not a resolution.
  const Status wrong_target = engine.resolve_attempt(ResolveAttemptRequest{.attempt = issued.value().id,
                                                                           .target = AttemptState::effect_established,
                                                                           .actor = s.actor,
                                                                           .at = tick,
                                                                           .reason = "not a resolution"});
  CHECK(!wrong_target.ok());
  CHECK_EQ(wrong_target.code(), StatusCode::invalid_argument);
  // An attempt the journal does not hold is not found.
  const Status missing = engine.resolve_attempt(ResolveAttemptRequest{.attempt = AttemptId::from(4'242),
                                                                      .target = AttemptState::resolved_without_effect,
                                                                      .actor = s.actor,
                                                                      .at = tick,
                                                                      .reason = "absent"});
  CHECK(!missing.ok());
  CHECK_EQ(missing.code(), StatusCode::not_found);
  // A resolution instant in the future is refused.
  const Result<AttemptRecord> pending =
      engine.issue(request_of(s, "resolve-2", ControlIntent::hold_setpoint, fan_percent(6000), tick), adapter);
  REQUIRE(pending.ok());
  const Status future = engine.resolve_attempt(ResolveAttemptRequest{.attempt = pending.value().id,
                                                                     .target = AttemptState::resolved_without_effect,
                                                                     .actor = s.actor,
                                                                     .at = LogicalTick::from(tick.value() + 1),
                                                                     .reason = "too soon"});
  CHECK(!future.ok());
  CHECK_EQ(future.code(), StatusCode::out_of_range);
  CHECK_EQ(engine.device(s.device).value().unresolved_attempt.has_value(), true);
}

AIRFLOW_TEST(evaluate_is_a_read_and_a_refusal_does_not_move_the_model) {
  TempDir directory("control");
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  Result<Fixture> fixture = Fixture::open(durable_options(directory.store_path()), s);
  REQUIRE(fixture.ok());
  AirflowControlEngine& engine = fixture.value().engine();
  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, s);
  const LogicalTick tick = engine.current_tick();

  const std::string before_state = engine.canonical_state();
  const std::string before_digest = engine.state_digest();

  // evaluate() on an eligible request changes nothing durable.
  const Result<Decision> eligible =
      engine.evaluate(request_of(s, "read-1", ControlIntent::hold_setpoint, fan_percent(6000), tick));
  REQUIRE(eligible.ok());
  CHECK_EQ(eligible.value().code, StatusCode::ok);
  CHECK_EQ(engine.canonical_state(), before_state);
  CHECK_EQ(engine.state_digest(), before_digest);

  // evaluate() on an ineligible request changes nothing either.
  ControlRequest invalid = request_of(s, "read-2", ControlIntent::hold_setpoint, fan_percent(6000), tick);
  invalid.device_generation = DeviceGeneration::from(9);
  const Result<Decision> ineligible = engine.evaluate(invalid);
  REQUIRE(ineligible.ok());
  CHECK_EQ(ineligible.value().code, StatusCode::generation_mismatch);
  CHECK_EQ(engine.canonical_state(), before_state);
  CHECK_EQ(engine.state_digest(), before_digest);

  // A request refused before the command-attempt boundary never reaches the
  // adapter and never moves the authoritative model.
  const Result<AttemptRecord> refused = engine.issue(invalid, adapter);
  CHECK(!refused.ok());
  CHECK_EQ(refused.code(), StatusCode::generation_mismatch);
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});
  CHECK_EQ(engine.canonical_state(), before_state);
  CHECK_EQ(engine.state_digest(), before_digest);

  // A request refused by an authority check behaves the same way.
  ControlRequest unpermitted = request_of(s, "read-3", ControlIntent::hold_setpoint, fan_percent(6000), tick);
  unpermitted.epoch = AuthorityEpoch::from(4);
  const Result<AttemptRecord> denied = engine.issue(unpermitted, adapter);
  CHECK(!denied.ok());
  CHECK_EQ(denied.code(), StatusCode::epoch_stale);
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});
  CHECK_EQ(engine.state_digest(), before_digest);

  // The engine is still usable and the accepted command still works.
  const Result<AttemptRecord> accepted =
      engine.issue(request_of(s, "read-4", ControlIntent::hold_setpoint, fan_percent(6000), tick), adapter);
  CHECK(accepted.ok());
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{1});
  CHECK_NE(engine.state_digest(), before_digest);
}
