// Property: a seeded randomised drive of the engine, adjudicated against an
// independent reference model of the control gate, with invariants asserted after
// every step.
//
// The reference is a predicate over the same inputs, written from the documented
// precedence order rather than from the engine's implementation: it decides
// eligibility itself, and every generated control request must produce exactly the
// code it predicts. The one fact it is handed rather than deriving is the identity
// of the attempt the engine reported as unresolved, which is an engine-allocated
// value; the reference only tracks whether the latch is set.

#include <cstdint>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "test_harness.hpp"
#include "fixture.hpp"

using namespace airflow_control;
using namespace airflow_test;

namespace {

/// The fixed seed. A failure prints it, and the same seed replays the run.
constexpr std::uint64_t kSeed = 0x5EED2024ull;

/// A deterministic generator: the same sequence on every platform and every
/// standard library.
class Rng {
 public:
  explicit Rng(std::uint64_t seed) : state_(seed == 0 ? 1 : seed) {}

  [[nodiscard]] std::uint64_t next() {
    state_ = state_ * 6364136223846793005ull + 1442695040888963407ull;
    return state_ >> 17;
  }
  [[nodiscard]] std::uint64_t below(std::uint64_t bound) { return bound == 0 ? 0 : next() % bound; }
  [[nodiscard]] bool chance(std::uint64_t percent) { return below(100) < percent; }

 private:
  std::uint64_t state_;
};

SetpointRequest percent_setpoint(std::uint32_t basis_points) {
  return SetpointRequest{SetpointPercent{.percent = SetpointBasisPoints::create(basis_points).value()}};
}

SetpointRequest airflow_setpoint(std::int64_t value) {
  return SetpointRequest{SetpointAirflow{.airflow = Airflow::from_cubic_metres_per_hour(value)}};
}

// ---------------------------------------------------------------------------
// The reference model of the gate
// ---------------------------------------------------------------------------

struct ReferenceGrant {
  std::uint64_t serial = 0;
  bool revoked = false;
  std::uint32_t mask = 0;
  std::uint64_t epoch = 1;
  bool has_expiry = false;
  std::uint64_t expiry = 0;
};

struct ReferencePermit {
  SafetyPermitId id;
  bool for_this_device = true;
  std::uint64_t epoch = 1;
  std::uint64_t issued = 0;
  bool has_expiry = false;
  std::uint64_t expiry = 0;
};

/// One reading, as the reference tracks it.
struct ReferenceReading {
  bool present = false;
  std::int64_t value = 0;
  std::uint64_t measured_at = 0;
};

/// The reference state. Every field is a fact the driver established through the
/// public API; decide() is a pure function of them.
struct ReferenceModel {
  AirflowDeviceId device = AirflowDeviceId::parse("dev-1").value();
  PressureRelationshipId relationship = PressureRelationshipId::parse("rel-1").value();
  SafetyPermitId foreign_permit = SafetyPermitId::parse("permit-other").value();

  std::uint64_t tick = 2;
  std::uint64_t epoch = 1;
  std::uint64_t generation = 1;
  std::uint64_t max_age = 1'000;
  DeviceLifecycle lifecycle = DeviceLifecycle::active;

  std::uint32_t min_percent = 2'000;
  std::uint32_t max_percent = 9'000;
  std::uint32_t default_percent = 5'000;
  std::int64_t min_airflow = 2'000;
  std::int64_t max_airflow = 9'000;
  std::uint32_t max_step = 1'000;

  ContainmentState containment = ContainmentState::intact;
  ReferenceReading pressure;
  std::int64_t band_lower = -30'000;
  std::int64_t band_upper = -10'000;

  bool has_obligation = true;
  bool obligation_advisory = false;
  std::int64_t obligation_minimum = 3'000;
  ReferenceReading airflow;

  bool interlock_protected = true;
  InterlockState interlock = InterlockState::satisfied;
  std::uint64_t interlock_epoch = 1;

  std::vector<ReferenceGrant> grants;
  std::vector<ReferencePermit> permits;

  bool latched = false;
  bool has_baseline = false;
  SetpointKind baseline_kind = SetpointKind::fan_percent;
  std::uint32_t baseline_percent = 5'000;

  [[nodiscard]] bool fresh(const ReferenceReading& reading, std::uint64_t now) const {
    if (!reading.present || reading.measured_at > now) {
      return false;
    }
    return now - reading.measured_at <= max_age;
  }

  [[nodiscard]] PressureState pressure_state(std::uint64_t now) const {
    if (!fresh(pressure, now)) {
      return PressureState::unknown;
    }
    return (pressure.value >= band_lower && pressure.value <= band_upper) ? PressureState::satisfied
                                                                        : PressureState::violated;
  }

  [[nodiscard]] ObligationState obligation_state(std::uint64_t now) const {
    if (!fresh(airflow, now)) {
      return ObligationState::unknown;
    }
    return airflow.value >= obligation_minimum ? ObligationState::satisfied
                                               : ObligationState::unsatisfied;
  }

  [[nodiscard]] StatusCode permission(ControlAction action, std::uint64_t at) const {
    bool any_for_device = false;
    bool any_action = false;
    for (const ReferenceGrant& grant : grants) {
      if (grant.revoked) {
        continue;
      }
      any_for_device = true;
      if ((grant.mask & (1u << static_cast<std::uint32_t>(action))) == 0u) {
        continue;
      }
      any_action = true;
      if (grant.epoch != epoch || (grant.has_expiry && at > grant.expiry)) {
        continue;
      }
      return StatusCode::ok;
    }
    if (any_for_device && any_action) {
      return StatusCode::permission_stale;
    }
    if (any_for_device) {
      return StatusCode::permission_denied;
    }
    return StatusCode::permission_missing;
  }

  [[nodiscard]] SetpointRequest resolve(const ControlRequest& request) const {
    if (request.setpoint.has_value()) {
      return *request.setpoint;
    }
    return SetpointRequest{
        SetpointPercent{.percent = SetpointBasisPoints::create(default_percent).value()}};
  }

  /// The documented precedence order, as an independent predicate.
  [[nodiscard]] StatusCode decide(const ControlRequest& request) const {
    const RequestClass klass = classify(request.intent);
    const bool safety_directed = klass == RequestClass::safety_directed;

    // 1 - shape.
    if (request.epoch.is_zero() || request.device_generation.is_zero()) {
      return StatusCode::invalid_argument;
    }
    if (changes_setpoint(request.intent) != request.setpoint.has_value()) {
      return StatusCode::invalid_argument;
    }
    if (request.relationship.has_value() != requires_pressure_proof(request.intent)) {
      return StatusCode::invalid_argument;
    }
    if (request.requested_at > LogicalTick::from(tick)) {
      return StatusCode::out_of_range;
    }
    const std::uint64_t at = request.requested_at.value();

    // 3 - identity, and 4 - identity binding.
    if (!(request.device == device)) {
      return StatusCode::not_found;
    }
    if (request.relationship.has_value() && !(*request.relationship == relationship)) {
      return StatusCode::not_found;
    }
    if (request.safety_permit.has_value() && *request.safety_permit == foreign_permit) {
      return StatusCode::identity_mismatch;
    }

    // 5 - lifecycle.
    if (!permits_control(lifecycle) && !requires_maintenance_override(lifecycle)) {
      return StatusCode::lifecycle_forbidden;
    }

    // 6 - the unresolved attempt.
    if (latched) {
      return StatusCode::attempt_unresolved;
    }

    // 7 - containment, for optimization only.
    if (klass == RequestClass::optimization && containment != ContainmentState::intact) {
      if (containment == ContainmentState::breached) {
        return StatusCode::containment_breached;
      }
      if (containment == ContainmentState::open_for_service) {
        return StatusCode::containment_open;
      }
      return StatusCode::containment_unknown;
    }

    // 8 - device generation, 9 - revision (never named here), 10 - epoch.
    if (request.device_generation.value() != generation) {
      return StatusCode::generation_mismatch;
    }
    if (request.epoch.value() != epoch) {
      return StatusCode::epoch_stale;
    }

    // 11 - permission.
    const StatusCode granted = permission(required_action(request.intent), at);
    if (granted != StatusCode::ok) {
      return granted;
    }

    // 12 - the safety permit.
    if (safety_directed) {
      if (!request.safety_permit.has_value()) {
        return StatusCode::safety_permit_missing;
      }
      const ReferencePermit* found = nullptr;
      for (const ReferencePermit& permit : permits) {
        if (permit.id == *request.safety_permit) {
          found = &permit;
          break;
        }
      }
      if (found == nullptr) {
        return StatusCode::safety_permit_missing;
      }
      if (found->epoch != epoch || at < found->issued ||
          (found->has_expiry && at > found->expiry)) {
        return StatusCode::safety_permit_stale;
      }
    }

    // 13 - protected interlocks, which safety-directed requests skip.
    if (!safety_directed && interlock_protected) {
      const InterlockState state = interlock_epoch == epoch ? interlock : InterlockState::unknown;
      if (state == InterlockState::open) {
        return StatusCode::interlock_open;
      }
      if (state != InterlockState::satisfied) {
        return StatusCode::interlock_unknown;
      }
    }

    // 14 - pressure.
    if (!safety_directed &&
        (requires_pressure_proof(request.intent) || klass == RequestClass::optimization)) {
      const PressureState state = pressure_state(at);
      if (state == PressureState::unknown) {
        return StatusCode::pressure_unknown;
      }
      if (state == PressureState::conflicted) {
        return StatusCode::pressure_conflicted;
      }
      if (state == PressureState::violated && klass == RequestClass::optimization) {
        return StatusCode::pressure_violated;
      }
    }

    // 15 - obligations, enforced for optimization only.
    if (has_obligation && klass == RequestClass::optimization && !obligation_advisory) {
      const ObligationState state = obligation_state(at);
      if (state != ObligationState::satisfied) {
        return state == ObligationState::unknown ? StatusCode::obligation_unknown
                                                 : StatusCode::obligation_unsatisfied;
      }
    }

    // 16 - the envelope, then the slew bound measured from the newest attempt.
    const SetpointRequest setpoint = resolve(request);
    if (const auto* percent = std::get_if<SetpointPercent>(&setpoint)) {
      const std::uint32_t basis_points = percent->percent.basis_points();
      if (basis_points < min_percent || basis_points > max_percent) {
        return StatusCode::limit_exceeded;
      }
      if (has_baseline && baseline_kind == SetpointKind::fan_percent) {
        const std::int64_t delta =
            static_cast<std::int64_t>(basis_points) - static_cast<std::int64_t>(baseline_percent);
        const std::int64_t magnitude = delta < 0 ? -delta : delta;
        if (magnitude > static_cast<std::int64_t>(max_step)) {
          return StatusCode::limit_exceeded;
        }
      }
    } else {
      const std::int64_t requested =
          std::get<SetpointAirflow>(setpoint).airflow.cubic_metres_per_hour();
      if (requested < min_airflow || requested > max_airflow) {
        return StatusCode::limit_exceeded;
      }
    }
    return StatusCode::ok;
  }
};

// ---------------------------------------------------------------------------
// The generated script
// ---------------------------------------------------------------------------

enum class StepKind : std::uint32_t {
  tick,
  observe_pressure,
  observe_airflow,
  report_containment,
  report_interlock,
  add_grant,
  revoke_grant,
  resolve,
  issue,
  evaluate,
};

struct Step {
  StepKind kind = StepKind::tick;
  std::uint64_t tick = 0;
  std::uint64_t serial = 0;   ///< the grant, where the step names one
  std::uint64_t sequence = 0;
  std::int64_t value = 0;
  std::uint32_t flavor = 0;
  std::uint32_t mask = 0;
  ControlIntent intent = ControlIntent::hold_setpoint;
  std::optional<SetpointRequest> setpoint;
  std::optional<PressureRelationshipId> relationship;
  std::optional<SafetyPermitId> permit;
  std::uint64_t epoch = 1;
  std::uint64_t generation = 1;
  AdapterDisposition disposition = AdapterDisposition::accepted;
};

/// True when the request is refused at the identity-binding step, which decides a
/// refusal before any check has recorded its own outcome.
[[nodiscard]] bool refuses_at_identity_binding(const Step& step, const Scenario& s) {
  if (step.permit.has_value() && *step.permit == SafetyPermitId::parse("permit-other").value()) {
    return true;
  }
  return step.relationship.has_value() && !(*step.relationship == s.relationship);
}

[[nodiscard]] std::string grant_name(std::uint64_t serial) {
  return "grant-" + std::to_string(serial);
}

/// Applies one step to the reference model. For a control step it returns the code
/// the engine must report; for every other step it returns nothing.
[[nodiscard]] std::optional<StatusCode> reference_advance(ReferenceModel& model, const Step& step,
                                                          const Scenario& s) {
  switch (step.kind) {
    case StepKind::tick:
      model.tick = step.tick;
      return std::nullopt;
    case StepKind::observe_pressure:
      model.pressure = ReferenceReading{true, step.value, step.tick};
      return std::nullopt;
    case StepKind::observe_airflow:
      model.airflow = ReferenceReading{true, step.value, step.tick};
      return std::nullopt;
    case StepKind::report_containment: {
      const ContainmentState states[] = {ContainmentState::intact, ContainmentState::open_for_service,
                                         ContainmentState::breached, ContainmentState::unknown};
      const ContainmentState state = states[step.flavor];
      model.containment =
          step.value == 1 ? ContainmentState::unknown : state;  // a poor-quality report
      return std::nullopt;
    }
    case StepKind::report_interlock: {
      const InterlockState states[] = {InterlockState::satisfied, InterlockState::open,
                                       InterlockState::unknown};
      model.interlock = states[step.flavor];
      model.interlock_epoch = model.epoch;
      return std::nullopt;
    }
    case StepKind::add_grant: {
      ReferenceGrant grant;
      grant.serial = step.serial;
      grant.mask = step.mask;
      grant.epoch = model.epoch;
      grant.has_expiry = step.value == 1;
      grant.expiry = step.tick + 1;
      model.grants.push_back(grant);
      return std::nullopt;
    }
    case StepKind::revoke_grant:
      for (ReferenceGrant& grant : model.grants) {
        if (grant.serial == step.serial) {
          grant.revoked = true;
        }
      }
      return std::nullopt;
    case StepKind::resolve:
      model.latched = false;
      return std::nullopt;
    case StepKind::issue:
    case StepKind::evaluate: {
      const ControlRequest request{
          .key = IdempotencyKey::parse("key-" + std::to_string(step.serial)).value(),
          .device = s.device,
          .device_generation = DeviceGeneration::from(step.generation),
          .epoch = AuthorityEpoch::from(step.epoch),
          .expected_revision = std::nullopt,
          .intent = step.intent,
          .setpoint = step.setpoint,
          .actor = s.actor,
          .requested_at = LogicalTick::from(step.tick),
          .safety_permit = step.permit,
          .supersede = std::nullopt,
          .relationship = step.relationship};
      const StatusCode expected = model.decide(request);
      if (step.kind == StepKind::issue && expected == StatusCode::ok) {
        const SetpointRequest resolved = model.resolve(request);
        model.has_baseline = true;
        model.baseline_kind = setpoint_kind(resolved);
        if (const auto* percent = std::get_if<SetpointPercent>(&resolved)) {
          model.baseline_percent = percent->percent.basis_points();
        }
        model.latched = step.disposition == AdapterDisposition::accepted;
      }
      return expected;
    }
  }
  return std::nullopt;
}

/// Generates the script from the seed. A scratch reference keeps the generated
/// values consistent with the state the script will establish.
[[nodiscard]] std::vector<Step> generate(const Scenario& s) {
  ReferenceModel model;
  model.device = s.device;
  model.relationship = s.relationship;
  model.grants.push_back(ReferenceGrant{0, false, ActionSet::all().mask(), 1, false, 0});
  model.permits.push_back(ReferencePermit{SafetyPermitId::parse("permit-live").value(), true, 1, 2,
                                           false, 0});
  model.permits.push_back(
      ReferencePermit{SafetyPermitId::parse("permit-other").value(), false, 1, 2, false, 0});

  Rng rng(kSeed);
  std::vector<Step> steps;
  // The fixture already reported containment and the interlock once, and already
  // issued one grant, so the generated sequences start after them.
  std::uint64_t sequences[5] = {1, 1, 2, 2, 100};  // pressure, airflow, containment, interlock, grant
  std::vector<std::uint64_t> live_grants;
  std::uint64_t replayed_key = 0;

  for (int index = 0; index < 260; ++index) {
    Step step;
    switch (rng.below(10)) {
      case 0: {
        step.kind = StepKind::tick;
        step.tick = model.tick + 1 + rng.below(5);
        break;
      }
      case 1: {
        step.kind = StepKind::observe_pressure;
        const std::uint64_t choice = rng.below(3);
        step.value = choice == 0 ? -20'000 : (choice == 1 ? -50'000 : -25'000);
        step.sequence = sequences[0]++;
        step.tick = model.tick;
        break;
      }
      case 2: {
        step.kind = StepKind::observe_airflow;
        const std::uint64_t choice = rng.below(3);
        step.value = choice == 0 ? 1'000 : (choice == 1 ? 4'000 : 8'000);
        step.sequence = sequences[1]++;
        step.tick = model.tick;
        break;
      }
      case 3: {
        step.kind = StepKind::report_containment;
        step.flavor = static_cast<std::uint32_t>(rng.below(4));
        step.value = rng.chance(25) ? 1 : 0;  // 1 means the report is not good evidence
        step.sequence = sequences[2]++;
        step.tick = model.tick;
        break;
      }
      case 4: {
        step.kind = StepKind::report_interlock;
        step.flavor = static_cast<std::uint32_t>(rng.below(3));
        step.sequence = sequences[3]++;
        step.tick = model.tick;
        break;
      }
      case 5: {
        step.kind = StepKind::add_grant;
        step.serial = sequences[4]++;
        const std::uint64_t choice = rng.below(5);
        const ControlAction action = choice == 0   ? ControlAction::apply_setpoint
                                     : choice == 1 ? ControlAction::lower_airflow
                                     : choice == 2 ? ControlAction::change_pressure_target
                                     : choice == 3 ? ControlAction::emergency_purge
                                                   : ControlAction::release_hold;
        step.mask = (choice == 4 || rng.chance(40))
                        ? ActionSet::all().mask()
                        : ActionSet::of({action, ControlAction::set_lifecycle,
                                         ControlAction::set_policy})
                              .mask();
        step.value = rng.chance(30) ? 1 : 0;  // 1 means the grant expires immediately
        step.tick = model.tick;
        live_grants.push_back(step.serial);
        break;
      }
      case 6: {
        if (live_grants.empty()) {
          step.kind = StepKind::tick;
          step.tick = model.tick + 1;
          break;
        }
        step.kind = StepKind::revoke_grant;
        const std::size_t which = static_cast<std::size_t>(rng.below(live_grants.size()));
        step.serial = live_grants[which];
        step.tick = model.tick;
        break;
      }
      case 7: {
        step.kind = StepKind::resolve;
        step.tick = model.tick;
        break;
      }
      default: {
        const bool issue = rng.chance(40);
        step.kind = issue ? StepKind::issue : StepKind::evaluate;
        const std::uint64_t intent_choice = rng.below(10);
        step.intent = intent_choice <= 1   ? ControlIntent::hold_setpoint
                      : intent_choice == 2 ? ControlIntent::raise_airflow
                      : intent_choice == 3 ? ControlIntent::lower_airflow
                      : intent_choice <= 5 ? ControlIntent::trim_for_efficiency
                      : intent_choice == 6 ? ControlIntent::restore_pressure_relationship
                      : intent_choice == 7 ? ControlIntent::release_to_policy
                                           : ControlIntent::emergency_purge;
        if (changes_setpoint(step.intent)) {
          if (rng.below(4) == 0) {
            step.setpoint = airflow_setpoint(rng.chance(30) ? 20'000 : 5'000);
          } else {
            const std::uint32_t choices[] = {4'000, 5'000, 6'000, 6'500, 100, 9'500};
            step.setpoint = percent_setpoint(choices[rng.below(6)]);
          }
        }
        if (step.intent == ControlIntent::restore_pressure_relationship) {
          step.relationship = rng.chance(15)
                                  ? PressureRelationshipId::parse("rel-absent").value()
                                  : s.relationship;
        }
        if (step.intent == ControlIntent::emergency_purge) {
          const std::uint64_t choice = rng.below(4);
          if (choice == 1) {
            step.permit = SafetyPermitId::parse("permit-live").value();
          } else if (choice == 2) {
            step.permit = SafetyPermitId::parse("permit-other").value();
          } else if (choice == 3) {
            step.permit = SafetyPermitId::parse("permit-absent").value();
          }
        }
        step.tick = model.tick;
        step.generation = rng.chance(10) ? 9 : 1;
        step.epoch = rng.chance(8) ? 2 : (rng.chance(4) ? 0 : 1);
        step.serial = replayed_key++;
        if (issue) {
          step.disposition = rng.chance(30) ? AdapterDisposition::refused
                                            : AdapterDisposition::accepted;
        }
        break;
      }
    }
    const std::optional<StatusCode> ignored = reference_advance(model, step, s);
    (void)ignored;
    steps.push_back(step);
  }
  return steps;
}

/// Everything both engines are given before the script runs.
Status prepare(AirflowControlEngine& engine, const Scenario& s) {
  Status status = apply_scenario(engine, s);
  if (!status.ok()) {
    return status;
  }
  status = engine.declare_obligation(DeclareObligationRequest{
      .id = ObligationId::parse("ob-1").value(),
      .scope = ObligationScope::room,
      .room = s.room,
      .row = std::nullopt,
      .rack = std::nullopt,
      .klass = ObligationClass::protected_obligation,
      .binding = ObligationBinding::device_sum,
      .metered_point = std::nullopt,
      .devices = {s.device},
      .minimum_airflow = Airflow::from_cubic_metres_per_hour(3'000),
      .target_airflow = Airflow::from_cubic_metres_per_hour(6'000),
      .source = s.source,
      .evidence_generation = s.evidence_generation,
      .expected_revision = std::nullopt,
      .actor = s.actor,
      .requested_at = engine.current_tick()});
  if (!status.ok()) {
    return status;
  }
  status = engine.add_safety_permit(SafetyPermit{.id = SafetyPermitId::parse("permit-live").value(),
                                                 .issuer = s.issuer,
                                                 .epoch = s.epoch,
                                                 .device = s.device,
                                                 .issued_at = s.ready_tick,
                                                 .expires_at = std::nullopt,
                                                 .reason = "property driver"},
                                    s.actor, engine.current_tick());
  if (!status.ok()) {
    return status;
  }
  Result<Scenario> other = make_scenario("2", 1);
  if (!other.ok()) {
    return other.status();
  }
  status = engine.register_device(RegisterDeviceRequest{.device = other.value().device,
                                                        .generation = other.value().device_generation,
                                                        .room = other.value().room,
                                                        .row = other.value().row,
                                                        .actor = s.actor,
                                                        .requested_at = engine.current_tick()});
  if (!status.ok()) {
    return status;
  }
  return engine.add_safety_permit(SafetyPermit{.id = SafetyPermitId::parse("permit-other").value(),
                                               .issuer = s.issuer,
                                               .epoch = s.epoch,
                                               .device = other.value().device,
                                               .issued_at = engine.current_tick(),
                                               .expires_at = std::nullopt,
                                               .reason = "another device"},
                                  s.actor, engine.current_tick());
}

}  // namespace

AIRFLOW_TEST(a_seeded_randomised_drive_matches_the_reference_gate) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  const std::vector<Step> steps = generate(s);
  REQUIRE(steps.size() > 200);

  Result<AirflowControlEngine> first = AirflowControlEngine::open_in_memory(base_options());
  Result<AirflowControlEngine> second = AirflowControlEngine::open_in_memory(base_options());
  REQUIRE(first.ok());
  REQUIRE(second.ok());
  AirflowControlEngine& engine = first.value();
  AirflowControlEngine& replay = second.value();
  REQUIRE(prepare(engine, s).ok());
  REQUIRE(prepare(replay, s).ok());

  ReferenceModel live;
  live.device = s.device;
  live.relationship = s.relationship;
  live.tick = engine.current_tick().value();
  live.epoch = engine.current_epoch().value();
  live.grants.push_back(ReferenceGrant{0, false, ActionSet::all().mask(), live.epoch, false, 0});
  live.permits.push_back(ReferencePermit{SafetyPermitId::parse("permit-live").value(), true, 1, 2,
                                          false, 0});
  live.permits.push_back(
      ReferencePermit{SafetyPermitId::parse("permit-other").value(), false, 1, 2, false, 0});

  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, s);
  SyntheticAirflowAdapter replay_adapter(synthetic_descriptor());
  seed_synthetic_adapter(replay_adapter, s);

  std::set<std::uint64_t> attempt_ids;
  std::set<std::uint64_t> observation_ids;
  std::uint64_t newest_attempt = 0;
  std::uint64_t newest_observation = 0;
  std::uint64_t latched_attempt = 0;
  std::uint64_t previous_tick = live.tick;
  std::size_t evaluates = 0;
  std::size_t issues = 0;
  std::size_t identity_binding_refusals = 0;
  std::map<std::uint32_t, std::size_t> codes_seen;

  const auto invariant = [&](std::size_t index) {
    for (const DeviceView& view : engine.devices()) {
      if (view.unresolved_attempt.has_value()) {
        const Result<AttemptRecord> record = engine.attempt(*view.unresolved_attempt);
        CHECK(record.ok());
        if (record.ok()) {
          CHECK(is_unresolved(record.value().state));
          CHECK_EQ(record.value().id, *view.unresolved_attempt);
        }
      }
    }
    CHECK(engine.current_tick().value() >= previous_tick);
    previous_tick = engine.current_tick().value();
    for (const Observation& observation : engine.observations(s.device)) {
      const std::uint64_t id = observation.id.value();
      if (observation_ids.find(id) == observation_ids.end()) {
        observation_ids.insert(id);
        CHECK(id > newest_observation);
        newest_observation = id;
      }
    }
    for (const AttemptView& view : engine.attempts()) {
      const std::uint64_t id = view.record.id.value();
      if (attempt_ids.find(id) == attempt_ids.end()) {
        attempt_ids.insert(id);
        CHECK(id > newest_attempt);
        newest_attempt = id;
      }
    }
    CHECK(engine.attempts().size() <= 512);
    (void)index;
  };

  const auto fail = [&](std::size_t index, const std::string& detail) {
    std::cout << "seed=" << kSeed << " step=" << index << ": " << detail << std::endl;
  };

  // Applies one step to one engine. The reference has already decided the control
  // steps, so the expectation is handed in rather than recomputed.
  const auto apply = [&](AirflowControlEngine& target, SyntheticAirflowAdapter& target_adapter,
                         const Step& step, std::size_t index,
                         const std::optional<StatusCode>& expected, bool checked) {
    switch (step.kind) {
      case StepKind::tick: {
        const Status status = target.advance_tick(LogicalTick::from(step.tick));
        if (checked) {
          CHECK(status.ok());
        }
        break;
      }
      case StepKind::observe_pressure: {
        const Result<Observation> observed =
            target.observe(ObservationDraft{
                               .payload = PressureReading{Pressure::from_millipascals(step.value)},
                               .device = s.device,
                               .relationship = s.relationship,
                               .point = SpaceRefId::parse("point-pressure-1").value(),
                               .source = s.source,
                               .sequence = EvidenceSequence::from(step.sequence),
                               .measured_at = LogicalTick::from(step.tick),
                               .device_generation = s.device_generation,
                               .evidence_generation = s.evidence_generation,
                               .quality = Quality::good},
                           LogicalTick::from(step.tick));
        if (checked) {
          if (!observed.ok()) {
            fail(index, std::string("observe_pressure ") + to_string(observed.code()) + " " + observed.message());
          }
          CHECK(observed.ok());
        }
        break;
      }
      case StepKind::observe_airflow: {
        const Result<Observation> observed =
            target.observe(ObservationDraft{
                               .payload = AirflowReading{Airflow::from_cubic_metres_per_hour(step.value)},
                               .device = s.device,
                               .relationship = std::nullopt,
                               .point = SpaceRefId::parse("point-airflow-1").value(),
                               .source = s.source,
                               .sequence = EvidenceSequence::from(step.sequence),
                               .measured_at = LogicalTick::from(step.tick),
                               .device_generation = s.device_generation,
                               .evidence_generation = s.evidence_generation,
                               .quality = Quality::good},
                           LogicalTick::from(step.tick));
        if (checked) {
          if (!observed.ok()) {
            fail(index, std::string("observe_airflow ") + to_string(observed.code()) + " " + observed.message());
          }
          CHECK(observed.ok());
        }
        break;
      }
      case StepKind::report_containment: {
        const ContainmentState states[] = {ContainmentState::intact, ContainmentState::open_for_service,
                                           ContainmentState::breached, ContainmentState::unknown};
        const Status reported = target.report_containment(ReportContainmentRequest{
            .element = s.containment,
            .state = states[step.flavor],
            .quality = step.value == 1 ? Quality::bad : Quality::good,
            .source = s.source,
            .sequence = EvidenceSequence::from(step.sequence),
            .evidence_generation = s.evidence_generation,
            .measured_at = LogicalTick::from(step.tick)});
        if (checked) {
          if (!reported.ok()) {
            fail(index, std::string("report_containment ") + to_string(reported.code()) + " " + reported.message());
          }
          CHECK(reported.ok());
        }
        break;
      }
      case StepKind::report_interlock: {
        const InterlockState states[] = {InterlockState::satisfied, InterlockState::open,
                                         InterlockState::unknown};
        const Status reported = target.report_interlock(ReportInterlockRequest{
            .id = s.interlock,
            .state = states[step.flavor],
            .sequence = EvidenceSequence::from(step.sequence),
            .epoch = AuthorityEpoch::from(live.epoch),
            .reported_at = LogicalTick::from(step.tick)});
        if (checked) {
          if (!reported.ok()) {
            fail(index, std::string("report_interlock ") + to_string(reported.code()) + " " + reported.message());
          }
          CHECK(reported.ok());
        }
        break;
      }
      case StepKind::add_grant: {
        const Status added = target.add_grant(
            PermissionGrant{.id = GrantId::parse(grant_name(step.serial)).value(),
                            .issuer = s.issuer,
                            .epoch = AuthorityEpoch::from(live.epoch),
                            .device = s.device,
                            .device_generation = s.device_generation,
                            .room = s.room,
                            .actions = ActionSet::from_mask(step.mask).value(),
                            .issued_at = LogicalTick::from(step.tick),
                            .expires_at = step.value == 1
                                              ? std::optional<LogicalTick>(LogicalTick::from(step.tick + 1))
                                              : std::nullopt,
                            .revoked = false},
            s.actor, LogicalTick::from(step.tick));
        if (checked) {
          if (!added.ok()) {
            fail(index, std::string("add_grant ") + to_string(added.code()) + " " + added.message());
          }
          CHECK(added.ok());
        }
        break;
      }
      case StepKind::revoke_grant: {
        const Status revoked =
            target.revoke_grant(GrantId::parse(grant_name(step.serial)).value(),
                                AuthorityEpoch::from(live.epoch), s.actor,
                                LogicalTick::from(step.tick));
        if (checked) {
          if (!revoked.ok()) {
            fail(index, std::string("revoke_grant ") + to_string(revoked.code()) + " " + revoked.message());
          }
          CHECK(revoked.ok());
        }
        break;
      }
      case StepKind::resolve:
        // Handled by the driver, which knows the identity of the attempt the
        // engine reported as unresolved.
        break;
      case StepKind::issue:
      case StepKind::evaluate: {
        const ControlRequest request{
            .key = IdempotencyKey::parse("key-" + std::to_string(step.serial)).value(),
            .device = s.device,
            .device_generation = DeviceGeneration::from(step.generation),
            .epoch = AuthorityEpoch::from(step.epoch),
            .expected_revision = std::nullopt,
            .intent = step.intent,
            .setpoint = step.setpoint,
            .actor = s.actor,
            .requested_at = LogicalTick::from(step.tick),
            .safety_permit = step.permit,
            .supersede = std::nullopt,
            .relationship = step.relationship};
        if (step.kind == StepKind::evaluate) {
          const Result<Decision> decision = target.evaluate(request);
          if (checked) {
            ++evaluates;
            CHECK(decision.ok());
            if (!decision.ok()) {
              fail(index, std::string("evaluate ") + to_string(decision.code()));
              break;
            }
            if (decision.value().code != *expected) {
              fail(index, std::string("evaluate expected ") + to_string(*expected) + " got " +
                              to_string(decision.value().code) + " (" + decision.value().message + ")");
            }
            CHECK_EQ(decision.value().code, *expected);
            CHECK_EQ(decision.value().eligible, *expected == StatusCode::ok);
            // The trace records every check that ran, so it is never empty, the
            // last entry carries the code the caller was given, and the refusing
            // check appears under its own name on every path, including the
            // identity-binding step.
            CHECK(!decision.value().trace.empty());
            bool recorded = false;
            for (const CheckTrace& entry : decision.value().trace) {
              if (entry.outcome == decision.value().code) {
                recorded = true;
              }
            }
            const bool identity_binding = refuses_at_identity_binding(step, s);
            if (identity_binding) {
              ++identity_binding_refusals;
            }
            ++codes_seen[static_cast<std::uint32_t>(decision.value().code)];
            if (!recorded) {
              fail(index, "the trace does not record " + std::string(to_string(decision.value().code)));
            }
            CHECK(recorded);
            CHECK_EQ(decision.value().trace.back().outcome, decision.value().code);
          }
        } else {
          if (step.disposition == AdapterDisposition::refused) {
            target_adapter.set_next_disposition(s.device, AdapterDisposition::refused, "declined");
          } else {
            target_adapter.clear_dispositions();
          }
          const Result<AttemptRecord> issued = target.issue(request, target_adapter);
          if (checked) {
            ++issues;
            if (*expected == StatusCode::ok) {
              CHECK(issued.ok());
              if (!issued.ok()) {
                fail(index, std::string("issue expected ok, got ") + to_string(issued.code()) + " " +
                                issued.message());
              } else {
                const AttemptState wanted = step.disposition == AdapterDisposition::refused
                                                ? AttemptState::refused
                                                : AttemptState::acknowledged;
                if (issued.value().state != wanted) {
                  fail(index, "issue state " + std::string(to_string(issued.value().state)) +
                                  std::string(" wanted ") + to_string(wanted));
                }
                CHECK_EQ(issued.value().state, wanted);
                if (step.disposition == AdapterDisposition::accepted) {
                  latched_attempt = issued.value().id.value();
                }
              }
            } else {
              CHECK(!issued.ok());
              if (issued.ok()) {
                fail(index, std::string("issue expected ") + to_string(*expected) + " but succeeded");
              } else {
                CHECK_EQ(issued.code(), *expected);
              }
            }
          }
        }
        break;
      }
    }
  };

  for (std::size_t index = 0; index < steps.size(); ++index) {
    const Step& step = steps[index];
    // A resolve step is the one place the driver needs an engine-allocated value:
    // the identity of the attempt the reference knows is latched. It is recorded
    // from what issue() returned, never predicted.
    std::optional<AttemptId> resolve_target;
    if (step.kind == StepKind::resolve && live.latched) {
      CHECK(latched_attempt != 0);
      resolve_target = AttemptId::from(latched_attempt);
    }
    // The reference decides the control steps and tracks the latch exactly once,
    // before either engine sees the step.
    const std::optional<StatusCode> expected = reference_advance(live, step, s);

    if (step.kind == StepKind::resolve) {
      if (resolve_target.has_value()) {
        const Status resolved = engine.resolve_attempt(
            ResolveAttemptRequest{.attempt = *resolve_target,
                                  .target = AttemptState::resolved_without_effect,
                                  .actor = s.actor,
                                  .at = LogicalTick::from(step.tick),
                                  .reason = "the randomised drive closed it"});
        if (!resolved.ok()) {
          fail(index, std::string("resolve ") + to_string(resolved.code()) + " " + resolved.message());
        }
        CHECK(resolved.ok());
        CHECK(replay
                  .resolve_attempt(ResolveAttemptRequest{.attempt = *resolve_target,
                                                         .target = AttemptState::resolved_without_effect,
                                                         .actor = s.actor,
                                                         .at = LogicalTick::from(step.tick),
                                                         .reason = "the randomised drive closed it"})
                  .ok());
        latched_attempt = 0;
      } else {
        // Nothing is latched, so the step is a no-op on both engines.
        CHECK(!engine.device(s.device).value().unresolved_attempt.has_value());
      }
    } else {
      apply(engine, adapter, step, index, expected, true);
      apply(replay, replay_adapter, step, index, expected, false);
    }
    // The same logical event sequence, replayed from scratch in a second engine,
    // renders the same state after every single step.
    CHECK_EQ(engine.canonical_state(), replay.canonical_state());
    CHECK_EQ(engine.state_digest(), replay.state_digest());
    invariant(index);
  }
  CHECK(evaluates > 20);
  CHECK(issues > 10);
  CHECK(attempt_ids.size() <= engine.attempts().size());
  // The drive must exercise a variety of outcomes, not one repeated answer.
  CHECK(codes_seen.size() >= 6);
  CHECK(codes_seen.find(static_cast<std::uint32_t>(StatusCode::ok)) != codes_seen.end());
  CHECK(codes_seen.find(static_cast<std::uint32_t>(StatusCode::limit_exceeded)) != codes_seen.end());
  CHECK(codes_seen.find(static_cast<std::uint32_t>(StatusCode::attempt_unresolved)) !=
        codes_seen.end());
  CHECK(identity_binding_refusals > 0);
  std::cout << "seed=" << kSeed << " steps=" << steps.size() << " evaluates=" << evaluates
            << " issues=" << issues << " attempts=" << engine.attempts().size()
            << " identity-binding-refusals=" << identity_binding_refusals << " distinct-codes="
            << codes_seen.size() << std::endl;
  for (const auto& entry : codes_seen) {
    std::cout << "  code " << to_string(static_cast<StatusCode>(entry.first)) << " x " << entry.second
              << std::endl;
  }
}
