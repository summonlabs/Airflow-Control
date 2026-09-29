// Recovery: what a restart restores, what it refuses to trust, and what it must
// never re-send.

#include <cstdint>
#include <iostream>
#include <optional>
#include <stdexcept>
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

/// An adapter that dies inside execute(). The durable record was published before
/// the call, so what it leaves behind is exactly what a process that dies at the
/// command-attempt boundary leaves behind.
class DyingAdapter final : public AirflowAdapter {
 public:
  [[nodiscard]] AdapterDescriptor describe() const override { return synthetic_descriptor(); }
  AdapterOutcome execute(const AdapterCommand&) override {
    throw std::runtime_error("the adapter did not return");
  }
  Result<ObservationDraft> read(const AdapterReadRequest&) override {
    return Status::failure(StatusCode::adapter_unavailable, "this adapter does not read");
  }
};

}  // namespace

AIRFLOW_TEST(recovered_evidence_is_not_fresh_and_proof_fails_closed) {
  TempDir directory("recovery-freshness");
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  const std::string path = directory.store_path();
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_or_create, base_options());
    REQUIRE(opened.ok());
    AirflowControlEngine& engine = opened.value();
    CHECK(apply_scenario(engine, s).ok());
    const LogicalTick tick = engine.current_tick();
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
    CHECK(engine.observe(ObservationDraft{.payload = FanReading{SetpointBasisPoints::create(5'000).value()},
                                           .device = s.device,
                                           .relationship = std::nullopt,
                                           .point = SpaceRefId::parse("point-fan-1").value(),
                                           .source = s.source,
                                           .sequence = EvidenceSequence::from(1),
                                           .measured_at = tick,
                                           .device_generation = s.device_generation,
                                           .evidence_generation = s.evidence_generation,
                                           .quality = Quality::good},
                         tick)
              .ok());
    // Proof holds while the readings are current.
    const Result<Decision> before =
        engine.evaluate(request_of(s, "recovery-1", ControlIntent::lower_airflow, fan_percent(4'000), tick));
    REQUIRE(before.ok());
    CHECK_EQ(before.value().code, StatusCode::ok);
    CHECK(engine.close().ok());
  }
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
    REQUIRE(opened.ok());
    AirflowControlEngine& engine = opened.value();
    const LogicalTick tick = engine.current_tick();

    // Every restored observation is marked recovered, and none of them is fresh.
    const std::vector<Observation> observations = engine.observations(s.device);
    CHECK(!observations.empty());
    for (const Observation& observation : observations) {
      CHECK(observation.recovered);
    }
    const Result<DeviceView> device = engine.device(s.device);
    REQUIRE(device.ok());
    CHECK(device.value().has_fan_observation);
    CHECK(device.value().has_airflow_observation == false);
    CHECK_EQ(device.value().fan_freshness, FreshnessVerdict::recovered);
    const Result<RelationshipView> relationship = engine.relationship(s.relationship);
    REQUIRE(relationship.ok());
    CHECK_EQ(relationship.value().state, PressureState::unknown);
    CHECK_EQ(relationship.value().contributing_sources, std::size_t{0});

    // A transition that requires pressure proof fails closed until a current
    // reading arrives.
    const ControlRequest optimization =
        request_of(s, "recovery-2", ControlIntent::lower_airflow, fan_percent(4'000), tick);
    const Result<Decision> closed =
        engine.evaluate(optimization);
    REQUIRE(closed.ok());
    CHECK_EQ(closed.value().code, StatusCode::pressure_unknown);
    CHECK(!closed.value().eligible);
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    const Result<AttemptRecord> refused = engine.issue(optimization, adapter);
    CHECK(!refused.ok());
    CHECK_EQ(refused.code(), StatusCode::pressure_unknown);
    CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});

    // A current reading is what restores the proof.
    CHECK(engine.observe(ObservationDraft{.payload = PressureReading{Pressure::from_millipascals(-20'000)},
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
    const Result<RelationshipView> restored = engine.relationship(s.relationship);
    REQUIRE(restored.ok());
    CHECK_EQ(restored.value().state, PressureState::satisfied);
    const Result<Decision> eligible = engine.evaluate(optimization);
    REQUIRE(eligible.ok());
    CHECK_EQ(eligible.value().code, StatusCode::ok);
    CHECK(engine.close().ok());
  }
}

AIRFLOW_TEST(a_dispatched_attempt_is_never_re_sent) {
  TempDir directory("recovery-dispatched");
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  const std::string path = directory.store_path();
  AttemptId dispatched = AttemptId::from(0);
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_or_create, base_options());
    REQUIRE(opened.ok());
    AirflowControlEngine& engine = opened.value();
    CHECK(apply_scenario(engine, s).ok());
    DyingAdapter dying;
    bool threw = false;
    try {
      const Result<AttemptRecord> issued =
          engine.issue(request_of(s, "crash-key", ControlIntent::hold_setpoint, fan_percent(6'000),
                                  engine.current_tick()),
                       dying);
      CHECK(!issued.ok());
    } catch (const std::exception&) {
      threw = true;
    }
    CHECK(threw);
    const std::vector<AttemptView> attempts = engine.attempts();
    REQUIRE(attempts.size() == 1);
    dispatched = attempts[0].record.id;
    // The durable publication happened before the adapter was called, and it is
    // already visible through the store.
    CHECK_EQ(attempts[0].record.state, AttemptState::dispatched);
    CHECK(engine.device(s.device).value().unresolved_attempt.has_value());
    // The publication happened before the adapter was called, so the durable
    // store is already at the generation that carries the dispatched attempt.
    CHECK(engine.store_audit().generation.value() > 0);
    CHECK(engine.close().ok());
  }
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
    REQUIRE(opened.ok());
    AirflowControlEngine& engine = opened.value();
    const std::vector<AttemptView> attempts = engine.attempts();
    REQUIRE(attempts.size() == 1);
    CHECK_EQ(attempts[0].record.id, dispatched);
    CHECK_EQ(attempts[0].record.state, AttemptState::recovery_required);
    CHECK_EQ(attempts[0].record.detail.find("not re-sent") != std::string::npos, true);
    const Result<DeviceView> device = engine.device(s.device);
    REQUIRE(device.ok());
    CHECK(device.value().unresolved_attempt.has_value());
    CHECK_EQ(*device.value().unresolved_attempt, dispatched);

    // The adapter is a fresh object: the proof is that it is never called during
    // recovery or afterwards by any retry.
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});
    const Result<Decision> blocked =
        engine.evaluate(request_of(s, "crash-key-2", ControlIntent::hold_setpoint, fan_percent(6'000),
                                   engine.current_tick()));
    REQUIRE(blocked.ok());
    CHECK_EQ(blocked.value().code, StatusCode::attempt_unresolved);
    const Result<AttemptRecord> refused =
        engine.issue(request_of(s, "crash-key-2", ControlIntent::hold_setpoint, fan_percent(6'000),
                                engine.current_tick()),
                     adapter);
    CHECK(!refused.ok());
    CHECK_EQ(refused.code(), StatusCode::attempt_unresolved);
    CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});
    // Only an explicit resolution of the recovered attempt releases the device.
    CHECK(engine.resolve_attempt(ResolveAttemptRequest{.attempt = dispatched,
                                                       .target = AttemptState::resolved_without_effect,
                                                       .actor = s.actor,
                                                       .at = engine.current_tick(),
                                                       .reason = "checked at the panel"})
              .ok());
    const Result<AttemptRecord> next =
        engine.issue(request_of(s, "crash-key-3", ControlIntent::hold_setpoint, fan_percent(6'000),
                                engine.current_tick()),
                     adapter);
    CHECK(next.ok());
    CHECK_EQ(adapter.execute_calls(), std::uint64_t{1});
    CHECK(engine.close().ok());
  }
}

AIRFLOW_TEST(a_same_key_retry_after_a_restart_replays) {
  TempDir directory("recovery-replay");
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  const std::string path = directory.store_path();
  AttemptId retained = AttemptId::from(0);
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_or_create, base_options());
    REQUIRE(opened.ok());
    AirflowControlEngine& engine = opened.value();
    CHECK(apply_scenario(engine, s).ok());
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    // An acknowledged command leaves an unresolved attempt and a retained key.
    const Result<AttemptRecord> issued =
        engine.issue(request_of(s, "retry-key", ControlIntent::hold_setpoint, fan_percent(6'000),
                                engine.current_tick()),
                     adapter);
    REQUIRE(issued.ok());
    CHECK_EQ(issued.value().state, AttemptState::acknowledged);
    retained = issued.value().id;
    CHECK_EQ(adapter.execute_calls(), std::uint64_t{1});
    CHECK(engine.close().ok());
  }
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
    REQUIRE(opened.ok());
    AirflowControlEngine& engine = opened.value();
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    const ControlRequest retry =
        request_of(s, "retry-key", ControlIntent::hold_setpoint, fan_percent(6'000),
                   engine.current_tick());
    const StoreGeneration generation = engine.store_audit().generation;
    const Result<AttemptRecord> replay = engine.issue(retry, adapter);
    REQUIRE(replay.ok());
    CHECK_EQ(replay.value().id, retained);
    CHECK_EQ(replay.value().state, AttemptState::acknowledged);
    CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});
    CHECK_EQ(engine.store_audit().generation, generation);
    // A new key is a new request, and the recovered latch refuses it.
    const Result<AttemptRecord> fresh =
        engine.issue(request_of(s, "retry-key-2", ControlIntent::hold_setpoint, fan_percent(6'000),
                                engine.current_tick()),
                     adapter);
    CHECK(!fresh.ok());
    CHECK_EQ(fresh.code(), StatusCode::attempt_unresolved);
    CHECK(fresh.message().find(retained.to_string()) != std::string::npos);
    CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});
    CHECK(engine.close().ok());
  }
}

AIRFLOW_TEST(a_resolved_attempt_stays_resolved_across_a_restart) {
  TempDir directory("recovery-resolved");
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  const std::string path = directory.store_path();
  AttemptId refused_id = AttemptId::from(0);
  AttemptId resolved_id = AttemptId::from(0);
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_or_create, base_options());
    REQUIRE(opened.ok());
    AirflowControlEngine& engine = opened.value();
    CHECK(apply_scenario(engine, s).ok());
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    adapter.set_next_disposition(s.device, AdapterDisposition::refused, "declined");
    const Result<AttemptRecord> refused =
        engine.issue(request_of(s, "resolved-key-1", ControlIntent::hold_setpoint, fan_percent(6'000),
                                engine.current_tick()),
                     adapter);
    REQUIRE(refused.ok());
    refused_id = refused.value().id;
    CHECK_EQ(refused.value().state, AttemptState::refused);
    const Result<AttemptRecord> acknowledged =
        engine.issue(request_of(s, "resolved-key-2", ControlIntent::hold_setpoint, fan_percent(6'000),
                                engine.current_tick()),
                     adapter);
    REQUIRE(acknowledged.ok());
    resolved_id = acknowledged.value().id;
    CHECK_EQ(acknowledged.value().state, AttemptState::acknowledged);
    CHECK(engine.resolve_attempt(ResolveAttemptRequest{.attempt = resolved_id,
                                                       .target = AttemptState::resolved_without_effect,
                                                       .actor = s.actor,
                                                       .at = engine.current_tick(),
                                                       .reason = "operator closed it"})
              .ok());
    CHECK(engine.close().ok());
  }
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
    REQUIRE(opened.ok());
    AirflowControlEngine& engine = opened.value();
    const Result<AttemptRecord> refused = engine.attempt(refused_id);
    REQUIRE(refused.ok());
    CHECK_EQ(refused.value().state, AttemptState::refused);
    CHECK(refused.value().resolved_at.has_value());
    const Result<AttemptRecord> resolved = engine.attempt(resolved_id);
    REQUIRE(resolved.ok());
    CHECK_EQ(resolved.value().state, AttemptState::resolved_without_effect);
    CHECK_EQ(resolved.value().resolution_reason, std::string("operator closed it"));
    // Neither attempt blocks the device, so a new command is admissible.
    const Result<DeviceView> device = engine.device(s.device);
    REQUIRE(device.ok());
    CHECK(!device.value().unresolved_attempt.has_value());
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    const Result<AttemptRecord> fresh =
        engine.issue(request_of(s, "resolved-key-3", ControlIntent::hold_setpoint, fan_percent(6'000),
                                engine.current_tick()),
                     adapter);
    CHECK(fresh.ok());
    CHECK_EQ(adapter.execute_calls(), std::uint64_t{1});
    CHECK(engine.close().ok());
  }
}

AIRFLOW_TEST(the_generation_advances_and_the_fence_refuses_an_older_store) {
  TempDir directory("recovery-fence");
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  const std::string path = directory.store_path();
  // The persisted generation is read through the store itself: an engine open
  // publishes its own recovery generation, so its audit is a later fact.
  const auto persisted_generation = [&]() {
    Result<DurableStore> store = DurableStore::open(path, OpenMode::open_existing, StoreOptions{});
    if (!store.ok()) {
      CHECK(false);
      return std::uint64_t{0};
    }
    const std::uint64_t generation = store.value().generation().value();
    CHECK_EQ(store.value().audit().publications, std::uint64_t{0});
    CHECK(store.value().close().ok());
    return generation;
  };

  std::vector<std::uint64_t> generations;
  for (int cycle = 0; cycle < 4; ++cycle) {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_or_create, base_options());
    REQUIRE(opened.ok());
    if (cycle == 0) {
      CHECK(apply_scenario(opened.value(), s).ok());
    }
    CHECK(opened.value().advance_tick(LogicalTick::from(opened.value().current_tick().value() + 1)).ok());
    CHECK(opened.value().close().ok());
    generations.push_back(persisted_generation());
  }
  REQUIRE(generations.size() == 4);
  for (std::size_t index = 1; index < generations.size(); ++index) {
    if (!(generations[index] > generations[index - 1])) {
      std::cout << "  generation " << index << " = " << generations[index] << " after "
                << generations[index - 1] << std::endl;
    }
    CHECK(generations[index] > generations[index - 1]);
  }
  const std::uint64_t current = generations.back();

  // A store at the generation the caller last saw is adopted.
  {
    EngineOptions options = base_options();
    options.store.fence_min_generation = true;
    options.store.min_generation = StoreGeneration::from(current);
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_existing, options);
    CHECK(opened.ok());
    if (opened.ok()) {
      CHECK(opened.value().device(s.device).ok());
      CHECK(opened.value().close().ok());
    }
  }
  const std::uint64_t after = persisted_generation();
  CHECK(after > current);

  // A store older than the required minimum is refused, and the refusal does not
  // damage it.
  {
    EngineOptions options = base_options();
    options.store.fence_min_generation = true;
    options.store.min_generation = StoreGeneration::from(after + 1);
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_existing, options);
    CHECK(!opened.ok());
    CHECK_EQ(opened.code(), StatusCode::rollback_detected);
  }
  CHECK_EQ(persisted_generation(), after);
  // The refused open released the lock, so the store is still usable.
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
    CHECK(opened.ok());
    if (opened.ok()) {
      CHECK(opened.value().device(s.device).ok());
      CHECK(opened.value().close().ok());
    }
  }
}
