#include "test_harness.hpp"

#include "fixture.hpp"

#include <cstdint>
#include <string>
#include <utility>

// Model-layer proof suite: authority. Grants, maintenance overrides, safety
// permits, and interlocks are driven through the public engine API on a real
// engine, because their meaning is the outcome of the validation order rather
// than of a predicate in isolation.

namespace {

using namespace airflow_control;  // NOLINT(google-build-using-namespace)
using airflow_test::base_options;
using airflow_test::device_revision;
using airflow_test::Fixture;
using airflow_test::Scenario;
using airflow_test::ScenarioOptions;

ScenarioOptions in_memory() {
  ScenarioOptions options;
  options.options = base_options();
  return options;
}

IdempotencyKey key(const char* text) { return IdempotencyKey::parse(text).value(); }

/// A hold-setpoint request at the policy default, which is inside the fixture's
/// envelope and requires no setpoint movement.
ControlRequest hold_request(const Scenario& scenario, LogicalTick at, const char* key_text) {
  return ControlRequest{
      .key = key(key_text),
      .device = scenario.device,
      .device_generation = scenario.device_generation,
      .epoch = scenario.epoch,
      .expected_revision = std::nullopt,
      .intent = ControlIntent::hold_setpoint,
      .setpoint = SetpointRequest{SetpointPercent{scenario.default_fan_percent}},
      .actor = scenario.actor,
      .requested_at = at,
      .safety_permit = std::nullopt,
      .supersede = std::nullopt,
      .relationship = std::nullopt,
  };
}

/// The code an evaluate() call refused with. evaluate() reports a refusal as a
/// Decision, so the code lives in the decision rather than in the Result.
StatusCode evaluation_code(const Result<Decision>& result) {
  return result.ok() ? result.value().code : result.code();
}

bool refused(const Result<Decision>& result) {
  return result.ok() && !result.value().eligible;
}

/// True when the decision's ordered trace contains this check with this
/// outcome.
bool trace_has(const Decision& decision, const std::string& check, StatusCode outcome) {
  for (const CheckTrace& entry : decision.trace) {
    if (entry.check == check && entry.outcome == outcome) {
      return true;
    }
  }
  return false;
}

/// The order in which evaluate() consults its checks, by the name each check
/// records in the trace. A trace is a subsequence of this order.
const char* const kCheckOrder[] = {
    "shape",       "idempotency", "identity",   "lifecycle",  "attempt",   "supersession",
    "containment", "generation",  "revision",   "epoch",      "permission", "safety_permit",
    "interlock",   "pressure",    "obligation", "envelope",   "slew",      "decision",
    "internal",
};
constexpr std::size_t kCheckOrderCount = sizeof(kCheckOrder) / sizeof(kCheckOrder[0]);

std::size_t check_rank(const std::string& check) {
  for (std::size_t index = 0; index < kCheckOrderCount; ++index) {
    if (check == kCheckOrder[index]) {
      return index;
    }
  }
  return kCheckOrderCount;
}

/// The position of the first entry recorded by this check, or the trace size
/// when the check did not report.
std::size_t trace_index(const Decision& decision, const std::string& check) {
  for (std::size_t index = 0; index < decision.trace.size(); ++index) {
    if (decision.trace[index].check == check) {
      return index;
    }
  }
  return decision.trace.size();
}

/// True when the trace reports its checks in the order the engine evaluates
/// them: a trace that reordered checks would make a refusal unexplainable.
bool trace_is_in_precedence_order(const Decision& decision) {
  bool started = false;
  std::size_t previous = 0;
  for (const CheckTrace& entry : decision.trace) {
    const std::size_t rank = check_rank(entry.check);
    if (started && rank < previous) {
      return false;
    }
    previous = rank;
    started = true;
  }
  return true;
}

/// A grant covering every action for the scenario's device in the given epoch.
PermissionGrant grant_for(const Scenario& scenario, const char* id, AuthorityEpoch epoch,
                          std::optional<LogicalTick> expires_at) {
  return PermissionGrant{
      .id = GrantId::parse(id).value(),
      .issuer = scenario.issuer,
      .epoch = epoch,
      .device = scenario.device,
      .device_generation = scenario.device_generation,
      .room = scenario.room,
      .actions = ActionSet::all(),
      .issued_at = scenario.start_tick,
      .expires_at = expires_at,
      .revoked = false,
  };
}

}  // namespace

AIRFLOW_TEST(permission_outcomes_are_distinguished_on_a_real_engine) {
  Result<Fixture> opened = Fixture::open(in_memory());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value().engine();
  const Scenario& scenario = opened.value().scenario();

  // The fixture's grant scopes every action to this device generation in this
  // epoch, so the baseline request is eligible.
  const Result<Decision> allowed =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-allowed"));
  REQUIRE(allowed.ok());
  CHECK(allowed.value().eligible);
  CHECK(trace_has(allowed.value(), "permission", StatusCode::ok));

  // Revoking it puts the device back to having no grant at all.
  REQUIRE(engine.revoke_grant(scenario.grant, scenario.epoch, scenario.actor, scenario.ready_tick)
              .ok());
  const Result<Decision> missing =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-missing"));
  CHECK(refused(missing));
  CHECK_EQ(evaluation_code(missing), StatusCode::permission_missing);
  CHECK(trace_has(missing.value(), "permission", StatusCode::permission_missing));
  CHECK_EQ(missing.value().message,
           std::string("no grant issues apply_setpoint for device ") + scenario.device.str());

  // A grant for the device that does not include the required action is a
  // denial, which is a different answer from having no grant.
  REQUIRE(engine
              .add_grant(PermissionGrant{
                             .id = GrantId::parse("grant-narrow").value(),
                             .issuer = scenario.issuer,
                             .epoch = scenario.epoch,
                             .device = scenario.device,
                             .device_generation = scenario.device_generation,
                             .room = scenario.room,
                             .actions = ActionSet::of({ControlAction::raise_airflow}),
                             .issued_at = scenario.ready_tick,
                             .expires_at = std::nullopt,
                             .revoked = false,
                         },
                         scenario.actor, scenario.ready_tick)
              .ok());
  const Result<Decision> denied =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-denied"));
  CHECK(refused(denied));
  CHECK_EQ(evaluation_code(denied), StatusCode::permission_denied);
  CHECK(trace_has(denied.value(), "permission", StatusCode::permission_denied));

  // The same grant does authorise the action it names.
  const ControlRequest raise{
      .key = key("key-raise"),
      .device = scenario.device,
      .device_generation = scenario.device_generation,
      .epoch = scenario.epoch,
      .expected_revision = std::nullopt,
      .intent = ControlIntent::raise_airflow,
      .setpoint = SetpointRequest{SetpointPercent{scenario.default_fan_percent}},
      .actor = scenario.actor,
      .requested_at = scenario.ready_tick,
      .safety_permit = std::nullopt,
      .supersede = std::nullopt,
      .relationship = std::nullopt,
  };
  const Result<Decision> raised = engine.evaluate(raise);
  CHECK(raised.value().eligible);
  CHECK_EQ(evaluation_code(raised), StatusCode::ok);

  // A grant from another epoch is stale: it was issued under authority that is
  // no longer adopted.
  REQUIRE(engine.revoke_grant(GrantId::parse("grant-narrow").value(), scenario.epoch, scenario.actor,
                              scenario.ready_tick)
              .ok());
  REQUIRE(engine
              .add_grant(grant_for(scenario, "grant-epoch", AuthorityEpoch::from(2), std::nullopt),
                         scenario.actor, scenario.ready_tick)
              .ok());
  const Result<Decision> stale =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-stale-epoch"));
  CHECK(refused(stale));
  CHECK_EQ(evaluation_code(stale), StatusCode::permission_stale);
  CHECK(trace_has(stale.value(), "permission", StatusCode::permission_stale));

  // A grant that expired before the request instant is stale too.
  REQUIRE(engine.revoke_grant(GrantId::parse("grant-epoch").value(), scenario.epoch, scenario.actor,
                              scenario.ready_tick)
              .ok());
  REQUIRE(engine
              .add_grant(grant_for(scenario, "grant-expired", scenario.epoch, scenario.start_tick),
                         scenario.actor, scenario.ready_tick)
              .ok());
  const Result<Decision> expired =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-expired"));
  CHECK(refused(expired));
  CHECK_EQ(evaluation_code(expired), StatusCode::permission_stale);

  // Expiry is inclusive: a grant that expires exactly at the request instant
  // still applies at that instant.
  REQUIRE(engine.revoke_grant(GrantId::parse("grant-expired").value(), scenario.epoch,
                              scenario.actor, scenario.ready_tick)
              .ok());
  REQUIRE(engine
              .add_grant(grant_for(scenario, "grant-edge", scenario.epoch, scenario.ready_tick),
                         scenario.actor, scenario.ready_tick)
              .ok());
  const Result<Decision> edge =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-edge"));
  CHECK(edge.value().eligible);
  CHECK_EQ(evaluation_code(edge), StatusCode::ok);

  // A grant bound to another device generation does not apply at all, so it is
  // not even a denial.
  REQUIRE(engine.revoke_grant(GrantId::parse("grant-edge").value(), scenario.epoch, scenario.actor,
                              scenario.ready_tick)
              .ok());
  REQUIRE(engine
              .add_grant(PermissionGrant{
                             .id = GrantId::parse("grant-next-generation").value(),
                             .issuer = scenario.issuer,
                             .epoch = scenario.epoch,
                             .device = scenario.device,
                             .device_generation = DeviceGeneration::from(2),
                             .room = scenario.room,
                             .actions = ActionSet::all(),
                             .issued_at = scenario.ready_tick,
                             .expires_at = std::nullopt,
                             .revoked = false,
                         },
                         scenario.actor, scenario.ready_tick)
              .ok());
  const Result<Decision> other_generation =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-other-generation"));
  CHECK(refused(other_generation));
  CHECK_EQ(evaluation_code(other_generation), StatusCode::permission_missing);

  // Neither does a grant for another device, or one scoped to another room.
  REQUIRE(engine
              .add_grant(PermissionGrant{
                             .id = GrantId::parse("grant-other-device").value(),
                             .issuer = scenario.issuer,
                             .epoch = scenario.epoch,
                             .device = AirflowDeviceId::parse("dev-other").value(),
                             .device_generation = std::nullopt,
                             .room = std::nullopt,
                             .actions = ActionSet::all(),
                             .issued_at = scenario.ready_tick,
                             .expires_at = std::nullopt,
                             .revoked = false,
                         },
                         scenario.actor, scenario.ready_tick)
              .ok());
  REQUIRE(engine
              .add_grant(PermissionGrant{
                             .id = GrantId::parse("grant-other-room").value(),
                             .issuer = scenario.issuer,
                             .epoch = scenario.epoch,
                             .device = std::nullopt,
                             .device_generation = std::nullopt,
                             .room = RoomId::parse("room-other").value(),
                             .actions = ActionSet::all(),
                             .issued_at = scenario.ready_tick,
                             .expires_at = std::nullopt,
                             .revoked = false,
                         },
                         scenario.actor, scenario.ready_tick)
              .ok());
  const Result<Decision> other_scope =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-other-scope"));
  CHECK(refused(other_scope));
  CHECK_EQ(evaluation_code(other_scope), StatusCode::permission_missing);

  // A room-scoped grant does apply to a device in that room, which is what
  // makes the previous answer a scoping decision rather than a blanket refusal.
  REQUIRE(engine
              .add_grant(PermissionGrant{
                             .id = GrantId::parse("grant-room").value(),
                             .issuer = scenario.issuer,
                             .epoch = scenario.epoch,
                             .device = std::nullopt,
                             .device_generation = std::nullopt,
                             .room = scenario.room,
                             .actions = ActionSet::all(),
                             .issued_at = scenario.ready_tick,
                             .expires_at = std::nullopt,
                             .revoked = false,
                         },
                         scenario.actor, scenario.ready_tick)
              .ok());
  const Result<Decision> room_scoped =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-room-scope"));
  CHECK(room_scoped.value().eligible);
  CHECK_EQ(evaluation_code(room_scoped), StatusCode::ok);
}

AIRFLOW_TEST(validation_precedence_is_deterministic) {
  Result<Fixture> opened = Fixture::open(in_memory());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value().engine();
  const Scenario& scenario = opened.value().scenario();

  // Remove the authority first, so every check below is reached with the
  // authority check failing as well: the code that comes back is then the
  // earliest failing check, not the only failing one.
  REQUIRE(engine.revoke_grant(scenario.grant, scenario.epoch, scenario.actor, scenario.ready_tick)
              .ok());

  // Shape is checked before everything.
  ControlRequest future = hold_request(scenario, scenario.ready_tick, "key-future");
  future.requested_at = LogicalTick::from(scenario.ready_tick.value() + 1);
  CHECK_EQ(evaluation_code(engine.evaluate(future)), StatusCode::out_of_range);

  ControlRequest zero_epoch = hold_request(scenario, scenario.ready_tick, "key-zero-epoch");
  zero_epoch.epoch = AuthorityEpoch::from(0);
  CHECK_EQ(evaluation_code(engine.evaluate(zero_epoch)), StatusCode::invalid_argument);

  ControlRequest zero_generation = hold_request(scenario, scenario.ready_tick, "key-zero-gen");
  zero_generation.device_generation = DeviceGeneration::from(0);
  CHECK_EQ(evaluation_code(engine.evaluate(zero_generation)), StatusCode::invalid_argument);

  ControlRequest no_setpoint = hold_request(scenario, scenario.ready_tick, "key-no-setpoint");
  no_setpoint.setpoint = std::nullopt;
  CHECK_EQ(evaluation_code(engine.evaluate(no_setpoint)), StatusCode::invalid_argument);

  ControlRequest setpoint_on_release =
      hold_request(scenario, scenario.ready_tick, "key-release-setpoint");
  setpoint_on_release.intent = ControlIntent::release_to_policy;
  CHECK_EQ(evaluation_code(engine.evaluate(setpoint_on_release)), StatusCode::invalid_argument);

  ControlRequest relationship_on_hold =
      hold_request(scenario, scenario.ready_tick, "key-relationship");
  relationship_on_hold.relationship = scenario.relationship;
  CHECK_EQ(evaluation_code(engine.evaluate(relationship_on_hold)), StatusCode::invalid_argument);

  ControlRequest restore_without_relationship =
      hold_request(scenario, scenario.ready_tick, "key-restore");
  restore_without_relationship.intent = ControlIntent::restore_pressure_relationship;
  CHECK_EQ(evaluation_code(engine.evaluate(restore_without_relationship)),
           StatusCode::invalid_argument);

  // Identity resolution comes next.
  ControlRequest unknown_device = hold_request(scenario, scenario.ready_tick, "key-unregistered");
  unknown_device.device = AirflowDeviceId::parse("dev-unregistered").value();
  const Result<Decision> not_found = engine.evaluate(unknown_device);
  CHECK_EQ(evaluation_code(not_found), StatusCode::not_found);
  CHECK(trace_has(not_found.value(), "identity", StatusCode::not_found));

  // Then the device generation, before the revision and the authority.
  ControlRequest wrong_generation =
      hold_request(scenario, scenario.ready_tick, "key-wrong-generation");
  wrong_generation.device_generation = DeviceGeneration::from(9);
  wrong_generation.expected_revision = StateRevision::from(99);
  const Result<Decision> generation = engine.evaluate(wrong_generation);
  CHECK_EQ(evaluation_code(generation), StatusCode::generation_mismatch);
  CHECK(trace_has(generation.value(), "generation", StatusCode::generation_mismatch));

  // Then the state revision, before the authority.
  ControlRequest wrong_revision =
      hold_request(scenario, scenario.ready_tick, "key-wrong-revision");
  wrong_revision.expected_revision = StateRevision::from(999);
  const Result<Decision> revision = engine.evaluate(wrong_revision);
  CHECK_EQ(evaluation_code(revision), StatusCode::revision_mismatch);
  CHECK(trace_has(revision.value(), "revision", StatusCode::revision_mismatch));

  // Then the authority epoch, before the permission.
  ControlRequest wrong_epoch = hold_request(scenario, scenario.ready_tick, "key-wrong-epoch");
  wrong_epoch.epoch = AuthorityEpoch::from(9);
  const Result<Decision> epoch = engine.evaluate(wrong_epoch);
  CHECK_EQ(evaluation_code(epoch), StatusCode::epoch_stale);
  CHECK(trace_has(epoch.value(), "epoch", StatusCode::epoch_stale));

  // Only then the permission.
  const Result<Decision> permission =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-permission-last"));
  CHECK_EQ(evaluation_code(permission), StatusCode::permission_missing);
  CHECK(trace_has(permission.value(), "permission", StatusCode::permission_missing));
  CHECK(!permission.value().eligible);
}

AIRFLOW_TEST(interlocks_fail_closed_and_are_epoch_bound) {
  Result<Fixture> opened = Fixture::open(in_memory());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value().engine();
  const Scenario& scenario = opened.value().scenario();

  const InterlockId second = InterlockId::parse("il-second").value();

  // Declared but never reported: the interlock is unknown, and an optimization
  // or protective request fails closed rather than assuming it is satisfied.
  REQUIRE(engine
              .declare_interlock(DeclareInterlockRequest{
                  .id = second,
                  .room = scenario.room,
                  .row = std::nullopt,
                  .device = std::nullopt,
                  .klass = InterlockClass::protected_obligation,
                  .epoch = scenario.epoch,
                  .actor = scenario.actor,
                  .declared_at = scenario.ready_tick,
              })
              .ok());
  const Result<Decision> unreported =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-unreported"));
  CHECK_EQ(evaluation_code(unreported), StatusCode::interlock_unknown);
  CHECK(trace_has(unreported.value(), "interlock", StatusCode::interlock_unknown));

  // A duplicate declaration is refused rather than silently replacing the
  // declaration the first report was attributed to.
  CHECK_STATUS(engine.declare_interlock(DeclareInterlockRequest{
                   .id = second,
                   .room = scenario.room,
                   .row = std::nullopt,
                   .device = std::nullopt,
                   .klass = InterlockClass::protected_obligation,
                   .epoch = scenario.epoch,
                   .actor = scenario.actor,
                   .declared_at = scenario.ready_tick,
               }),
               StatusCode::duplicate_identity);

  // A report for an interlock that was never declared is not found, so a report
  // can never create the obligation it claims to satisfy.
  CHECK_STATUS(engine.report_interlock(ReportInterlockRequest{
                   .id = InterlockId::parse("il-undeclared").value(),
                   .state = InterlockState::satisfied,
                   .sequence = EvidenceSequence::from(1),
                   .epoch = scenario.epoch,
                   .reported_at = scenario.ready_tick,
               }),
               StatusCode::not_found);

  // Reporting it satisfied clears the refusal.
  REQUIRE(engine
              .report_interlock(ReportInterlockRequest{
                  .id = second,
                  .state = InterlockState::satisfied,
                  .sequence = EvidenceSequence::from(1),
                  .epoch = scenario.epoch,
                  .reported_at = scenario.ready_tick,
              })
              .ok());
  const Result<Decision> satisfied =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-satisfied"));
  CHECK(satisfied.value().eligible);

  // A report that does not advance the sequence is out of order: a delayed
  // report must not overwrite a newer state.
  CHECK_STATUS(engine.report_interlock(ReportInterlockRequest{
                   .id = second,
                   .state = InterlockState::open,
                   .sequence = EvidenceSequence::from(1),
                   .epoch = scenario.epoch,
                   .reported_at = scenario.ready_tick,
               }),
               StatusCode::evidence_out_of_order);
  CHECK_STATUS(engine.report_interlock(ReportInterlockRequest{
                   .id = second,
                   .state = InterlockState::open,
                   .sequence = EvidenceSequence::from(0),
                   .epoch = scenario.epoch,
                   .reported_at = scenario.ready_tick,
               }),
               StatusCode::evidence_out_of_order);
  CHECK(satisfied.value().eligible);

  // An open protected interlock blocks control.
  REQUIRE(engine
              .report_interlock(ReportInterlockRequest{
                  .id = second,
                  .state = InterlockState::open,
                  .sequence = EvidenceSequence::from(2),
                  .epoch = scenario.epoch,
                  .reported_at = scenario.ready_tick,
              })
              .ok());
  const Result<Decision> open =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-open"));
  CHECK_EQ(evaluation_code(open), StatusCode::interlock_open);
  CHECK(trace_has(open.value(), "interlock", StatusCode::interlock_open));

  // A report from an epoch that is no longer adopted is not current: after the
  // epoch advances, both interlocks fail closed until each is reported again.
  REQUIRE(engine.adopt_epoch(AuthorityEpoch::from(2), scenario.actor, scenario.ready_tick).ok());
  REQUIRE(engine
              .add_grant(grant_for(scenario, "grant-epoch-two", AuthorityEpoch::from(2),
                                   std::nullopt),
                         scenario.actor, scenario.ready_tick)
              .ok());
  ControlRequest epoch_two = hold_request(scenario, scenario.ready_tick, "key-epoch-two");
  epoch_two.epoch = AuthorityEpoch::from(2);
  const Result<Decision> foreign_epoch = engine.evaluate(epoch_two);
  CHECK_EQ(evaluation_code(foreign_epoch), StatusCode::interlock_unknown);
  CHECK(trace_has(foreign_epoch.value(), "interlock", StatusCode::interlock_unknown));

  // Reporting one interlock in the new epoch is not enough while another is
  // still stamped with the old one.
  REQUIRE(engine
              .report_interlock(ReportInterlockRequest{
                  .id = second,
                  .state = InterlockState::satisfied,
                  .sequence = EvidenceSequence::from(3),
                  .epoch = AuthorityEpoch::from(2),
                  .reported_at = scenario.ready_tick,
              })
              .ok());
  const Result<Decision> partially_current = engine.evaluate(epoch_two);
  CHECK_EQ(evaluation_code(partially_current), StatusCode::interlock_unknown);

  // Once every protected interlock is current in the adopted epoch, control is
  // permitted again.
  REQUIRE(engine
              .report_interlock(ReportInterlockRequest{
                  .id = scenario.interlock,
                  .state = InterlockState::satisfied,
                  .sequence = EvidenceSequence::from(2),
                  .epoch = AuthorityEpoch::from(2),
                  .reported_at = scenario.ready_tick,
              })
              .ok());
  const Result<Decision> current = engine.evaluate(epoch_two);
  CHECK(current.value().eligible);
  CHECK_EQ(evaluation_code(current), StatusCode::ok);
}

AIRFLOW_TEST(authority_identities_are_unique_and_windows_are_ordered) {
  Result<Fixture> opened = Fixture::open(in_memory());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value().engine();
  const Scenario& scenario = opened.value().scenario();

  // Duplicate interlock, grant, override, and permit identities are refused.
  CHECK_STATUS(engine.declare_interlock(DeclareInterlockRequest{
                   .id = scenario.interlock,
                   .room = scenario.room,
                   .row = std::nullopt,
                   .device = std::nullopt,
                   .klass = InterlockClass::advisory,
                   .epoch = scenario.epoch,
                   .actor = scenario.actor,
                   .declared_at = scenario.ready_tick,
               }),
               StatusCode::duplicate_identity);
  CHECK_STATUS(engine.add_grant(grant_for(scenario, "grant-1", scenario.epoch, std::nullopt),
                                scenario.actor, scenario.ready_tick),
               StatusCode::duplicate_identity);

  const MaintenanceOverride override_entry{
      .id = OverrideId::parse("ov-1").value(),
      .device = scenario.device,
      .device_generation = scenario.device_generation,
      .epoch = scenario.epoch,
      .issued_at = scenario.ready_tick,
      .expires_at = LogicalTick::from(scenario.ready_tick.value() + 10),
      .reason = "service",
      .revoked = false,
  };
  REQUIRE(engine.add_maintenance_override(override_entry, scenario.actor, scenario.ready_tick).ok());
  CHECK_STATUS(engine.add_maintenance_override(override_entry, scenario.actor, scenario.ready_tick),
               StatusCode::duplicate_identity);

  const SafetyPermit permit{
      .id = SafetyPermitId::parse("permit-1").value(),
      .issuer = scenario.issuer,
      .epoch = scenario.epoch,
      .device = scenario.device,
      .issued_at = scenario.ready_tick,
      .expires_at = std::nullopt,
      .reason = "emergency ventilation",
  };
  REQUIRE(engine.add_safety_permit(permit, scenario.actor, scenario.ready_tick).ok());
  CHECK_STATUS(engine.add_safety_permit(permit, scenario.actor, scenario.ready_tick),
               StatusCode::duplicate_identity);

  // A window that closes before it opens is a shape fault, not a stale record:
  // there is no instant at which such an authority exists.
  MaintenanceOverride inverted = override_entry;
  inverted.id = OverrideId::parse("ov-inverted").value();
  inverted.issued_at = LogicalTick::from(scenario.ready_tick.value() + 5);
  inverted.expires_at = scenario.ready_tick;
  CHECK_STATUS(engine.add_maintenance_override(inverted, scenario.actor, scenario.ready_tick),
               StatusCode::invalid_argument);

  SafetyPermit inverted_permit = permit;
  inverted_permit.id = SafetyPermitId::parse("permit-inverted").value();
  inverted_permit.issued_at = LogicalTick::from(scenario.ready_tick.value() + 5);
  inverted_permit.expires_at = scenario.ready_tick;
  CHECK_STATUS(engine.add_safety_permit(inverted_permit, scenario.actor, scenario.ready_tick),
               StatusCode::invalid_argument);

  PermissionGrant inverted_grant = grant_for(scenario, "grant-inverted", scenario.epoch,
                                             scenario.start_tick);
  inverted_grant.issued_at = scenario.ready_tick;
  CHECK_STATUS(engine.add_grant(inverted_grant, scenario.actor, scenario.ready_tick),
               StatusCode::invalid_argument);

  // An authority that names no device is either refused as a shape fault or not
  // found, never silently widened to every device.
  MaintenanceOverride unknown_device = override_entry;
  unknown_device.id = OverrideId::parse("ov-unknown-device").value();
  unknown_device.device = AirflowDeviceId::parse("dev-unregistered").value();
  CHECK_STATUS(engine.add_maintenance_override(unknown_device, scenario.actor, scenario.ready_tick),
               StatusCode::not_found);

  SafetyPermit unknown_permit = permit;
  unknown_permit.id = SafetyPermitId::parse("permit-unknown-device").value();
  unknown_permit.device = AirflowDeviceId::parse("dev-unregistered").value();
  CHECK_STATUS(engine.add_safety_permit(unknown_permit, scenario.actor, scenario.ready_tick),
               StatusCode::not_found);

  PermissionGrant empty_grant = grant_for(scenario, "grant-empty", scenario.epoch, std::nullopt);
  empty_grant.actions = ActionSet::none();
  CHECK_STATUS(engine.add_grant(empty_grant, scenario.actor, scenario.ready_tick),
               StatusCode::invalid_argument);

  PermissionGrant generation_without_device =
      grant_for(scenario, "grant-generation-only", scenario.epoch, std::nullopt);
  generation_without_device.device = std::nullopt;
  CHECK_STATUS(engine.add_grant(generation_without_device, scenario.actor, scenario.ready_tick),
               StatusCode::invalid_argument);

  // Revoking something that was never declared is not found, and revoking twice
  // is idempotent rather than an error.
  CHECK_STATUS(engine.revoke_maintenance_override(OverrideId::parse("ov-absent").value(),
                                                  scenario.actor, scenario.ready_tick),
               StatusCode::not_found);
  CHECK(engine
            .revoke_maintenance_override(OverrideId::parse("ov-1").value(), scenario.actor,
                                         scenario.ready_tick)
            .ok());
  CHECK(engine
            .revoke_maintenance_override(OverrideId::parse("ov-1").value(), scenario.actor,
                                         scenario.ready_tick)
            .ok());

  // The permit and override records are retained and reported back unchanged.
  const std::vector<SafetyPermit> permits = engine.safety_permits();
  REQUIRE(permits.size() == 1);
  CHECK_EQ(permits.front().id, permit.id);
  CHECK_EQ(permits.front().epoch, scenario.epoch);
  CHECK(permits.front().device == scenario.device);
  const std::vector<MaintenanceOverride> overrides = engine.overrides();
  REQUIRE(overrides.size() == 1);
  CHECK(overrides.front().revoked);
  CHECK_EQ(overrides.front().id, override_entry.id);
}

AIRFLOW_TEST(maintenance_permission_needs_a_live_in_epoch_override) {
  Result<Fixture> opened = Fixture::open(in_memory());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value().engine();
  const Scenario& scenario = opened.value().scenario();

  const LogicalTick issued = scenario.ready_tick;
  const LogicalTick expiry = LogicalTick::from(scenario.ready_tick.value() + 10);

  // Entering maintenance without any override is refused.
  Result<StateRevision> revision = device_revision(engine, scenario.device);
  REQUIRE(revision.ok());
  CHECK_STATUS(engine.set_device_lifecycle(SetLifecycleRequest{
                   .device = scenario.device,
                   .generation = scenario.device_generation,
                   .expected_revision = revision.value(),
                   .target = DeviceLifecycle::maintenance,
                   .epoch = scenario.epoch,
                   .actor = scenario.actor,
                   .requested_at = scenario.ready_tick,
               }),
               StatusCode::lifecycle_forbidden);

  // An override for a different device generation does not authorise this one.
  REQUIRE(engine
              .add_maintenance_override(
                  MaintenanceOverride{
                      .id = OverrideId::parse("ov-wrong-generation").value(),
                      .device = scenario.device,
                      .device_generation = DeviceGeneration::from(2),
                      .epoch = scenario.epoch,
                      .issued_at = issued,
                      .expires_at = expiry,
                      .reason = "wrong generation",
                      .revoked = false,
                  },
                  scenario.actor, scenario.ready_tick)
              .ok());
  CHECK_STATUS(engine.set_device_lifecycle(SetLifecycleRequest{
                   .device = scenario.device,
                   .generation = scenario.device_generation,
                   .expected_revision = revision.value(),
                   .target = DeviceLifecycle::maintenance,
                   .epoch = scenario.epoch,
                   .actor = scenario.actor,
                   .requested_at = scenario.ready_tick,
               }),
               StatusCode::lifecycle_forbidden);

  // Nor does an override issued under another epoch.
  REQUIRE(engine
              .add_maintenance_override(
                  MaintenanceOverride{
                      .id = OverrideId::parse("ov-wrong-epoch").value(),
                      .device = scenario.device,
                      .device_generation = scenario.device_generation,
                      .epoch = AuthorityEpoch::from(2),
                      .issued_at = issued,
                      .expires_at = expiry,
                      .reason = "wrong epoch",
                      .revoked = false,
                  },
                  scenario.actor, scenario.ready_tick)
              .ok());
  CHECK_STATUS(engine.set_device_lifecycle(SetLifecycleRequest{
                   .device = scenario.device,
                   .generation = scenario.device_generation,
                   .expected_revision = revision.value(),
                   .target = DeviceLifecycle::maintenance,
                   .epoch = scenario.epoch,
                   .actor = scenario.actor,
                   .requested_at = scenario.ready_tick,
               }),
               StatusCode::lifecycle_forbidden);

  // A live, scoped, in-epoch override does.
  const OverrideId live = OverrideId::parse("ov-live").value();
  REQUIRE(engine
              .add_maintenance_override(
                  MaintenanceOverride{
                      .id = live,
                      .device = scenario.device,
                      .device_generation = scenario.device_generation,
                      .epoch = scenario.epoch,
                      .issued_at = issued,
                      .expires_at = expiry,
                      .reason = "service window",
                      .revoked = false,
                  },
                  scenario.actor, scenario.ready_tick)
              .ok());
  const Status entered = engine.set_device_lifecycle(SetLifecycleRequest{
      .device = scenario.device,
      .generation = scenario.device_generation,
      .expected_revision = revision.value(),
      .target = DeviceLifecycle::maintenance,
      .epoch = scenario.epoch,
      .actor = scenario.actor,
      .requested_at = scenario.ready_tick,
  });
  CHECK(entered.ok());
  const Result<DeviceView> view = engine.device(scenario.device);
  REQUIRE(view.ok());
  CHECK_EQ(view.value().lifecycle, DeviceLifecycle::maintenance);
  CHECK(!permits_control(view.value().lifecycle));

  // Control is permitted while the override is live.
  REQUIRE(engine.advance_tick(LogicalTick::from(scenario.ready_tick.value() + 11)).ok());
  const Result<Decision> within =
      engine.evaluate(hold_request(scenario, LogicalTick::from(scenario.ready_tick.value() + 5),
                                   "key-within-window"));
  CHECK(within.value().eligible);
  CHECK(trace_has(within.value(), "lifecycle", StatusCode::ok));

  // The window closes at its expiry instant inclusively, so the last instant it
  // covers is still permitted and the next one is not.
  const Result<Decision> at_expiry =
      engine.evaluate(hold_request(scenario, expiry, "key-at-expiry"));
  CHECK(at_expiry.value().eligible);
  const Result<Decision> past_expiry =
      engine.evaluate(hold_request(scenario, LogicalTick::from(expiry.value() + 1), "key-past"));
  CHECK_EQ(evaluation_code(past_expiry), StatusCode::lifecycle_forbidden);
  CHECK(trace_has(past_expiry.value(), "lifecycle", StatusCode::lifecycle_forbidden));

  // Revoking the override withdraws the permission immediately.
  REQUIRE(engine.revoke_maintenance_override(live, scenario.actor, scenario.ready_tick).ok());
  const Result<Decision> revoked =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-override-revoked"));
  CHECK_EQ(evaluation_code(revoked), StatusCode::lifecycle_forbidden);

  // Leaving maintenance needs no override; entering it again does.
  Result<StateRevision> in_maintenance = device_revision(engine, scenario.device);
  REQUIRE(in_maintenance.ok());
  REQUIRE(engine
              .set_device_lifecycle(SetLifecycleRequest{
                  .device = scenario.device,
                  .generation = scenario.device_generation,
                  .expected_revision = in_maintenance.value(),
                  .target = DeviceLifecycle::isolated,
                  .epoch = scenario.epoch,
                  .actor = scenario.actor,
                  .requested_at = scenario.ready_tick,
              })
              .ok());
  const Result<Decision> isolated =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-isolated"));
  CHECK_EQ(evaluation_code(isolated), StatusCode::lifecycle_forbidden);
}

AIRFLOW_TEST(refusal_trace_keeps_every_check_that_ran) {
  // A refusal must not lose the checks that produced it: the trace is the whole
  // explanation of the refusal, and issue() surfaces the same trace on the
  // Status it returns.
  Result<Fixture> opened = Fixture::open(in_memory());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value().engine();
  const Scenario& scenario = opened.value().scenario();

  // A successful decision carries the whole ordered evaluation, ending with the
  // decision point.
  const Result<Decision> eligible =
      engine.evaluate(hold_request(scenario, scenario.ready_tick, "key-trace-ok"));
  REQUIRE(eligible.ok());
  CHECK(eligible.value().eligible);
  CHECK(eligible.value().trace.size() > 5);
  CHECK(trace_has(eligible.value(), "shape", StatusCode::ok));
  CHECK(trace_has(eligible.value(), "permission", StatusCode::ok));
  CHECK(trace_has(eligible.value(), "lifecycle", StatusCode::ok));
  CHECK(trace_has(eligible.value(), "decision", StatusCode::ok));
  CHECK_EQ(eligible.value().trace.back().check, std::string("decision"));
  CHECK(trace_is_in_precedence_order(eligible.value()));

  // A refusal keeps every check that ran, in order. The request shape was valid,
  // so the trace still begins with the shape check reporting ok, and it ends with
  // the check that refused.
  REQUIRE(engine.revoke_grant(scenario.grant, scenario.epoch, scenario.actor, scenario.ready_tick)
              .ok());
  const ControlRequest request = hold_request(scenario, scenario.ready_tick, "key-trace-refusal");
  const Result<Decision> refusal = engine.evaluate(request);
  REQUIRE(refused(refusal));
  const Decision& decision = refusal.value();
  CHECK_EQ(decision.code, StatusCode::permission_missing);
  CHECK(!decision.trace.empty());
  CHECK_EQ(decision.trace.front().check, std::string("shape"));
  CHECK_EQ(decision.trace.front().outcome, StatusCode::ok);
  CHECK_EQ(decision.trace.back().check, std::string("permission"));
  CHECK_EQ(decision.trace.back().outcome, decision.code);
  CHECK_EQ(decision.trace.back().detail, decision.message);
  CHECK(trace_is_in_precedence_order(decision));

  // Every check that ran and had something to say is present, and the check that
  // refused is the last entry rather than a synthesised replacement for the
  // checks before it.
  CHECK(trace_has(decision, "identity", StatusCode::ok));
  CHECK(trace_has(decision, "lifecycle", StatusCode::ok));
  CHECK(trace_has(decision, "containment", StatusCode::ok));
  CHECK(trace_has(decision, "permission", StatusCode::permission_missing));
  CHECK_EQ(trace_index(decision, "permission"), decision.trace.size() - 1);
  CHECK(trace_index(decision, "shape") < trace_index(decision, "identity"));
  CHECK(trace_index(decision, "identity") < trace_index(decision, "lifecycle"));
  CHECK(trace_index(decision, "lifecycle") < trace_index(decision, "permission"));

  // No entry is reported twice: a check that recorded itself does not appear
  // again with the same outcome and detail.
  for (std::size_t left = 0; left < decision.trace.size(); ++left) {
    for (std::size_t right = left + 1; right < decision.trace.size(); ++right) {
      const bool duplicate = decision.trace[left].check == decision.trace[right].check &&
                             decision.trace[left].outcome == decision.trace[right].outcome &&
                             decision.trace[left].detail == decision.trace[right].detail;
      CHECK(!duplicate);
    }
  }

  // issue() reports the same trace on its Status, and a request that never
  // reached the adapter is never handed to one.
  SyntheticAirflowAdapter adapter(airflow_test::synthetic_descriptor());
  airflow_test::seed_synthetic_adapter(adapter, scenario);
  const Result<AttemptRecord> issued = engine.issue(request, adapter);
  CHECK(!issued.ok());
  CHECK_EQ(issued.code(), StatusCode::permission_missing);
  REQUIRE(issued.status().trace().size() == decision.trace.size());
  for (std::size_t index = 0; index < decision.trace.size(); ++index) {
    CHECK_EQ(issued.status().trace()[index].check, decision.trace[index].check);
    CHECK_EQ(issued.status().trace()[index].outcome, decision.trace[index].outcome);
    CHECK_EQ(issued.status().trace()[index].detail, decision.trace[index].detail);
  }
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});
  CHECK_EQ(adapter.read_calls(), std::uint64_t{0});
}
