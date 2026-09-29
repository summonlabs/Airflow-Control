// Idempotency: a retained key replays and never re-actuates; the same key with a
// different request is a conflict; eviction is observable; and the journal is
// never smaller than the window that names attempts in it.

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

/// True when two setpoint requests name the same delivery. The variant's own
/// comparison cannot be used: the alternative types do not define operator==.
[[nodiscard]] bool same_setpoint(const SetpointRequest& lhs, const SetpointRequest& rhs) {
  if (setpoint_kind(lhs) != setpoint_kind(rhs)) {
    return false;
  }
  if (const auto* percent = std::get_if<SetpointPercent>(&lhs)) {
    return percent->percent == std::get<SetpointPercent>(rhs).percent;
  }
  return std::get<SetpointAirflow>(lhs).airflow == std::get<SetpointAirflow>(rhs).airflow;
}

/// True when two retained attempts are identical in every field. AttemptRecord is
/// an aggregate without operator==, so the comparison is written out: a replay
/// that returned a nearly identical record would be a defect, and a comparison
/// that skipped a field could not see it.
[[nodiscard]] bool identical(const AttemptRecord& lhs, const AttemptRecord& rhs) {
  return lhs.id == rhs.id && lhs.ordinal == rhs.ordinal && lhs.key == rhs.key &&
         lhs.device == rhs.device && lhs.device_generation == rhs.device_generation &&
         lhs.epoch == rhs.epoch && lhs.planned_revision == rhs.planned_revision &&
         lhs.policy_generation == rhs.policy_generation &&
         lhs.evidence_generation == rhs.evidence_generation && lhs.intent == rhs.intent &&
         lhs.request_class == rhs.request_class && same_setpoint(lhs.setpoint, rhs.setpoint) &&
         lhs.actor == rhs.actor && lhs.accepted_at == rhs.accepted_at &&
         lhs.dispatched_at == rhs.dispatched_at && lhs.state == rhs.state &&
         lhs.command == rhs.command && lhs.adapter_sequence == rhs.adapter_sequence &&
         lhs.disposition == rhs.disposition && lhs.detail == rhs.detail &&
         lhs.safety_permit == rhs.safety_permit && lhs.supersedes == rhs.supersedes &&
         lhs.superseded_by == rhs.superseded_by && lhs.fan_observation == rhs.fan_observation &&
         lhs.pressure_observation == rhs.pressure_observation &&
         lhs.effect_sequence == rhs.effect_sequence && lhs.resolved_at == rhs.resolved_at &&
         lhs.resolution_reason == rhs.resolution_reason;
}

/// How many retained idempotency slots the canonical state reports.
[[nodiscard]] std::size_t retained_slots(const std::string& canonical) {
  std::size_t count = 0;
  std::size_t position = 0;
  while ((position = canonical.find("idempotency ", position)) != std::string::npos) {
    ++count;
    position += 12;
  }
  return count;
}

/// Every retained slot must name an attempt the journal still holds. A slot that
/// outlives its attempt would make a replay fail with internal_error instead of
/// returning the retained result.
[[nodiscard]] bool slots_resolve(const std::string& canonical) {
  std::size_t position = 0;
  while ((position = canonical.find("idempotency ", position)) != std::string::npos) {
    const std::size_t attempt_at = canonical.find("attempt=", position);
    if (attempt_at == std::string::npos) {
      return false;
    }
    const std::size_t start = attempt_at + 8;
    const std::size_t end = canonical.find(' ', start);
    const std::string id = canonical.substr(start, end - start);
    if (canonical.find("attempt " + id + " ordinal=") == std::string::npos) {
      return false;
    }
    position = end;
  }
  return true;
}

}  // namespace

AIRFLOW_TEST(a_replay_is_a_read_and_never_re_actuates) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  Result<AirflowControlEngine> opened = AirflowControlEngine::open_in_memory(base_options());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value();
  CHECK(apply_scenario(engine, s).ok());
  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, s);
  const LogicalTick tick = engine.current_tick();

  const ControlRequest request =
      request_of(s, "key-1", ControlIntent::hold_setpoint, fan_percent(6000), tick);
  const Result<AttemptRecord> first = engine.issue(request, adapter);
  REQUIRE(first.ok());
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{1});
  CHECK_EQ(adapter.accepted_commands(), std::uint64_t{1});
  const std::optional<SyntheticPlantState> plant = adapter.plant_state(s.device);
  REQUIRE(plant.has_value());
  CHECK_EQ(plant.value().fan_percent, SetpointBasisPoints::create(6000).value());
  const std::string digest = engine.state_digest();

  // evaluate() sees the replay and reports the retained attempt.
  const Result<Decision> decision = engine.evaluate(request);
  REQUIRE(decision.ok());
  CHECK_EQ(decision.value().code, StatusCode::ok);
  CHECK(decision.value().eligible);
  CHECK(decision.value().replayed);
  CHECK(decision.value().replayed_attempt.has_value());
  CHECK_EQ(*decision.value().replayed_attempt, first.value().id);
  CHECK(decision.value().resolved_setpoint.has_value());
  CHECK(same_setpoint(*decision.value().resolved_setpoint, first.value().setpoint));

  // issue() returns the retained record, identical in every field.
  const Result<AttemptRecord> replay = engine.issue(request, adapter);
  REQUIRE(replay.ok());
  CHECK(identical(replay.value(), first.value()));
  CHECK(identical(replay.value(), engine.attempt(first.value().id).value()));
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{1});
  CHECK_EQ(adapter.read_calls(), std::uint64_t{0});
  CHECK_EQ(adapter.accepted_commands(), std::uint64_t{1});
  CHECK_EQ(adapter.applied_commands(), std::uint64_t{1});
  const std::optional<SyntheticPlantState> after = adapter.plant_state(s.device);
  REQUIRE(after.has_value());
  CHECK_EQ(after.value().fan_percent, plant.value().fan_percent);
  CHECK_EQ(after.value().airflow, plant.value().airflow);
  CHECK_EQ(engine.state_digest(), digest);
  CHECK_EQ(engine.attempts().size(), std::size_t{1});

  // The replay is not a new attempt, so the latch still names the first one.
  const Result<AttemptRecord> other =
      engine.issue(request_of(s, "key-2", ControlIntent::hold_setpoint, fan_percent(7000), tick), adapter);
  CHECK(!other.ok());
  CHECK_EQ(other.code(), StatusCode::attempt_unresolved);
  CHECK(other.message().find(first.value().id.to_string()) != std::string::npos);
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{1});
}

AIRFLOW_TEST(a_different_request_under_the_same_key_is_a_conflict) {
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  Result<AirflowControlEngine> opened = AirflowControlEngine::open_in_memory(base_options());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value();
  CHECK(apply_scenario(engine, s).ok());
  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, s);
  const LogicalTick tick = engine.current_tick();

  const ControlRequest base = request_of(s, "shared-key", ControlIntent::hold_setpoint,
                                         fan_percent(5000), tick);
  const Result<AttemptRecord> issued = engine.issue(base, adapter);
  REQUIRE(issued.ok());
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{1});
  const std::string digest = engine.state_digest();

  const auto conflict = [&](const char* label, const ControlRequest& changed) {
    const Result<Decision> decision = engine.evaluate(changed);
    CHECK(decision.ok());
    CHECK_EQ(decision.value().code, StatusCode::idempotency_conflict);
    CHECK(!decision.value().eligible);
    CHECK(!decision.value().replayed);
    const Result<AttemptRecord> attempt = engine.issue(changed, adapter);
    CHECK(!attempt.ok());
    CHECK_EQ(attempt.code(), StatusCode::idempotency_conflict);
    CHECK(attempt.message().find("shared-key") != std::string::npos);
    if (decision.value().code != StatusCode::idempotency_conflict) {
      std::cout << "  " << label << " was not a conflict: " << decision.value().message << "\n";
    }
  };

  { ControlRequest changed = base; changed.device_generation = DeviceGeneration::from(2); conflict("device generation", changed); }
  { ControlRequest changed = base; changed.epoch = AuthorityEpoch::from(2); conflict("epoch", changed); }
  { ControlRequest changed = base; changed.expected_revision = StateRevision::from(1); conflict("revision", changed); }
  { ControlRequest changed = base; changed.intent = ControlIntent::raise_airflow; conflict("intent", changed); }
  { ControlRequest changed = base; changed.setpoint = fan_percent(6000); conflict("setpoint", changed); }
  { ControlRequest changed = base; changed.actor = ActorId::parse("actor-other").value(); conflict("actor", changed); }
  { ControlRequest changed = base; changed.requested_at = LogicalTick::from(tick.value() - 1); conflict("requested_at", changed); }
  { ControlRequest changed = base; changed.safety_permit = SafetyPermitId::parse("permit-1").value(); conflict("safety permit", changed); }
  { ControlRequest changed = base; changed.supersede = AttemptId::from(1); conflict("supersede", changed); }

  // Not one of the conflicts reached the adapter, and none of them moved state.
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{1});
  CHECK_EQ(engine.state_digest(), digest);
  CHECK_EQ(engine.attempts().size(), std::size_t{1});

  // The conflict is symmetric: issue the original again and it still replays.
  const Result<AttemptRecord> replay = engine.issue(base, adapter);
  REQUIRE(replay.ok());
  CHECK(identical(replay.value(), issued.value()));
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{1});
}

AIRFLOW_TEST(a_replay_survives_close_and_reopen) {
  TempDir directory("idempotency");
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  const std::string path = directory.store_path();
  AttemptId retained = AttemptId::from(0);
  std::string canonical;
  std::uint64_t generation_after_close = 0;
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_or_create, base_options());
    REQUIRE(opened.ok());
    AirflowControlEngine& engine = opened.value();
    CHECK(apply_scenario(engine, s).ok());
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    const Result<AttemptRecord> issued =
        engine.issue(request_of(s, "durable-key", ControlIntent::hold_setpoint, fan_percent(6000),
                                engine.current_tick()),
                     adapter);
    REQUIRE(issued.ok());
    CHECK_EQ(issued.value().state, AttemptState::acknowledged);
    CHECK_EQ(adapter.execute_calls(), std::uint64_t{1});
    retained = issued.value().id;
    for (const AttemptView& view : engine.attempts()) {
      if (view.record.id == retained) {
        canonical = engine.canonical_state();
      }
    }
    CHECK(engine.close().ok());
  }
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
    REQUIRE(opened.ok());
    AirflowControlEngine& engine = opened.value();
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    const ControlRequest request =
        request_of(s, "durable-key", ControlIntent::hold_setpoint, fan_percent(6000),
                   engine.current_tick());
    const StoreGeneration generation = engine.store_audit().generation;
    const Result<AttemptRecord> replay = engine.issue(request, adapter);
    REQUIRE(replay.ok());
    CHECK_EQ(replay.value().id, retained);
    CHECK_EQ(replay.value().state, AttemptState::acknowledged);
    CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});
    CHECK_EQ(adapter.read_calls(), std::uint64_t{0});
    // A replay performs no durable mutation: not even a publication.
    CHECK_EQ(engine.store_audit().generation, generation);
    CHECK_EQ(engine.attempts().size(), std::size_t{1});
    // A new key is a new request, and the recovered latch refuses it.
    const Result<AttemptRecord> fresh =
        engine.issue(request_of(s, "durable-key-2", ControlIntent::hold_setpoint, fan_percent(6000),
                                engine.current_tick()),
                     adapter);
    CHECK(!fresh.ok());
    CHECK_EQ(fresh.code(), StatusCode::attempt_unresolved);
    CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});
    generation_after_close = engine.store_audit().generation.value();
    CHECK(engine.close().ok());
  }
  CHECK(generation_after_close != 0);
  CHECK(!canonical.empty());
}

AIRFLOW_TEST(eviction_makes_the_oldest_key_a_new_request_again) {
  // The effective window is never below the model's own retention, which starts
  // at 256, so that is the smallest window this test can observe.
  EngineOptions options = base_options();
  options.idempotency_window = 256;
  options.attempt_journal_capacity = 512;
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  Result<AirflowControlEngine> opened = AirflowControlEngine::open_in_memory(options);
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value();
  CHECK(apply_scenario(engine, s).ok());
  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, s);
  adapter.set_persistent_disposition(s.device, AdapterDisposition::refused, "not now");

  const int count = 300;
  AttemptId first_attempt = AttemptId::from(0);
  AttemptId last_attempt = AttemptId::from(0);
  for (int index = 0; index < count; ++index) {
    const ControlRequest request = request_of(s, "bulk-" + std::to_string(index),
                                              ControlIntent::hold_setpoint, fan_percent(5000),
                                              engine.current_tick());
    const Result<AttemptRecord> issued = engine.issue(request, adapter);
    if (!issued.ok()) {
      std::cout << "bulk issue " << index << " failed: " << to_string(issued.code()) << " "
                << issued.message() << "\n";
      CHECK(false);
      break;
    }
    // A definite refusal resolves the attempt, so the next one is admissible.
    if (index == 0) {
      first_attempt = issued.value().id;
    }
    last_attempt = issued.value().id;
  }
  CHECK_EQ(adapter.execute_calls(), static_cast<std::uint64_t>(count));
  const std::string canonical = engine.canonical_state();
  CHECK_EQ(retained_slots(canonical), std::size_t{256});
  CHECK(slots_resolve(canonical));

  // The oldest key left the window, so it is a fresh request again.
  const std::uint64_t before = adapter.execute_calls();
  const ControlRequest oldest = request_of(s, "bulk-0", ControlIntent::hold_setpoint,
                                           fan_percent(5000), engine.current_tick());
  const Result<AttemptRecord> again = engine.issue(oldest, adapter);
  REQUIRE(again.ok());
  CHECK_EQ(adapter.execute_calls(), before + 1);
  CHECK(again.value().id != first_attempt);
  CHECK_EQ(again.value().key, IdempotencyKey::parse("bulk-0").value());

  // The newest key is still retained: it replays and does not re-actuate.
  const ControlRequest newest = request_of(s, "bulk-" + std::to_string(count - 1),
                                           ControlIntent::hold_setpoint, fan_percent(5000),
                                           engine.current_tick());
  const std::uint64_t before_replay = adapter.execute_calls();
  const Result<AttemptRecord> replay = engine.issue(newest, adapter);
  REQUIRE(replay.ok());
  CHECK_EQ(replay.value().id, last_attempt);
  CHECK_EQ(adapter.execute_calls(), before_replay);
  CHECK_EQ(retained_slots(engine.canonical_state()), std::size_t{256});
  CHECK(slots_resolve(engine.canonical_state()));
}

AIRFLOW_TEST(the_journal_is_never_smaller_than_the_window) {
  EngineOptions options = base_options();
  options.idempotency_window = 4096;
  options.attempt_journal_capacity = 1;  // raised to at least the window
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  Result<AirflowControlEngine> opened = AirflowControlEngine::open_in_memory(options);
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value();
  CHECK(apply_scenario(engine, s).ok());
  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, s);
  adapter.set_persistent_disposition(s.device, AdapterDisposition::refused, "not now");

  const int count = 64;
  std::vector<AttemptId> attempts;
  attempts.reserve(static_cast<std::size_t>(count));
  for (int index = 0; index < count; ++index) {
    const Result<AttemptRecord> issued =
        engine.issue(request_of(s, "window-" + std::to_string(index), ControlIntent::hold_setpoint,
                                fan_percent(5000), engine.current_tick()),
                     adapter);
    REQUIRE(issued.ok());
    attempts.push_back(issued.value().id);
  }
  const std::string canonical = engine.canonical_state();
  // The configured journal capacity of one was raised to the window: a retained
  // key must always name an attempt the journal still holds.
  CHECK(canonical.find("idempotency-window=4096") != std::string::npos);
  CHECK(canonical.find("attempt-journal=4096") != std::string::npos);
  CHECK(slots_resolve(canonical));
  CHECK_EQ(retained_slots(canonical), static_cast<std::size_t>(count));

  const std::uint64_t calls = adapter.execute_calls();
  for (int index = 0; index < count; ++index) {
    const Result<AttemptRecord> replay =
        engine.issue(request_of(s, "window-" + std::to_string(index), ControlIntent::hold_setpoint,
                                fan_percent(5000), engine.current_tick()),
                     adapter);
    REQUIRE(replay.ok());
    CHECK_EQ(replay.value().id, attempts[static_cast<std::size_t>(index)]);
  }
  CHECK_EQ(adapter.execute_calls(), calls);
  CHECK_EQ(engine.attempts().size(), static_cast<std::size_t>(count));
}
