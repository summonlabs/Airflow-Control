#include "test_harness.hpp"

#include "fixture.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

// Model-layer proof suite: the vendor boundary.
//
// verify_outcome_echo and the disposition predicates are pure and are checked
// directly; the synthetic plant is driven through the engine so that the
// counters exercised are the ones a real adapter call produces.

namespace {

using namespace airflow_control;  // NOLINT(google-build-using-namespace)
using airflow_test::Fixture;
using airflow_test::Scenario;
using airflow_test::ScenarioOptions;
using airflow_test::seed_synthetic_adapter;
using airflow_test::synthetic_descriptor;

ScenarioOptions in_memory() {
  ScenarioOptions options;
  options.options = airflow_test::base_options();
  return options;
}

IdempotencyKey key(const char* text) { return IdempotencyKey::parse(text).value(); }

ControlRequest setpoint_request(const Scenario& scenario, LogicalTick at, const char* key_text,
                                std::uint32_t basis_points) {
  return ControlRequest{
      .key = key(key_text),
      .device = scenario.device,
      .device_generation = scenario.device_generation,
      .epoch = scenario.epoch,
      .expected_revision = std::nullopt,
      .intent = ControlIntent::hold_setpoint,
      .setpoint = SetpointRequest{SetpointPercent{SetpointBasisPoints::create(basis_points).value()}},
      .actor = scenario.actor,
      .requested_at = at,
      .safety_permit = std::nullopt,
      .supersede = std::nullopt,
      .relationship = std::nullopt,
  };
}

Status resolve_now(AirflowControlEngine& engine, const Scenario& scenario, AttemptId attempt) {
  return engine.resolve_attempt(ResolveAttemptRequest{
      .attempt = attempt,
      .target = AttemptState::resolved_without_effect,
      .actor = scenario.actor,
      .at = scenario.ready_tick,
      .reason = "resolved by the adapter suite",
  });
}

AdapterReadRequest read_request(const Scenario& scenario, ObservationKind kind,
                                std::optional<PressureRelationshipId> relationship,
                                const SourceId& source, LogicalTick at, std::uint64_t sequence) {
  return AdapterReadRequest{
      .device = scenario.device,
      .device_generation = scenario.device_generation,
      .kind = kind,
      .relationship = std::move(relationship),
      .point = scenario.controlled_space,
      .source = source,
      .sequence = EvidenceSequence::from(sequence),
      .at = at,
      .evidence_generation = scenario.evidence_generation,
  };
}

std::uint32_t fan_percent_of(const ObservationDraft& draft) {
  return std::get<FanReading>(draft.payload).percent.basis_points();
}

Pressure differential_of(const ObservationDraft& draft) {
  return std::get<PressureReading>(draft.payload).differential;
}

/// An adapter that records the command it was handed and answers with a
/// scripted disposition. It is the only way to obtain an AdapterCommand outside
/// the engine, because the authorization that constructs one is private.
class RecordingAdapter final : public AirflowAdapter {
 public:
  [[nodiscard]] AdapterDescriptor describe() const override {
    AdapterDescriptor descriptor;
    descriptor.vendor = "airflow-control-tests";
    descriptor.model = "recording";
    descriptor.protocol = "in-process";
    descriptor.capabilities = 0;
    descriptor.synthetic = false;
    return descriptor;
  }

  AdapterOutcome execute(const AdapterCommand& command) override {
    last_command = command;
    ++execute_calls;
    if (observed_engine != nullptr) {
      const Result<AttemptRecord> at_dispatch = observed_engine->attempt(command.attempt());
      if (at_dispatch.ok()) {
        state_at_dispatch = at_dispatch.value().state;
      }
    }
    return AdapterOutcome{command.id(),
                          command.attempt(),
                          command.device(),
                          command.device_generation(),
                          disposition,
                          AdapterSequence::from(1),
                          command.issued_at(),
                          std::string(),
                          std::nullopt};
  }

  Result<ObservationDraft> read(const AdapterReadRequest& request) override {
    (void)request;
    return Status::failure(StatusCode::not_found, "the recording adapter does not read");
  }

  const AirflowControlEngine* observed_engine = nullptr;
  std::optional<AdapterCommand> last_command;
  /// The state of the attempt as the adapter could see it while it was being
  /// called: this is the command-attempt boundary observed from outside.
  std::optional<AttemptState> state_at_dispatch;
  AdapterDisposition disposition = AdapterDisposition::accepted;
  std::uint64_t execute_calls = 0;
};

}  // namespace

AIRFLOW_TEST(outcome_echo_accepts_a_match_and_fences_every_mismatch) {
  Result<Fixture> opened = Fixture::open(in_memory());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value().engine();
  const Scenario& scenario = opened.value().scenario();

  RecordingAdapter adapter;
  adapter.observed_engine = &engine;
  const Result<AttemptRecord> issued =
      engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-echo", 5000), adapter);
  REQUIRE(issued.ok());
  REQUIRE(adapter.last_command.has_value());
  const AdapterCommand& command = *adapter.last_command;

  // The attempt record was already published, in state dispatched, while the
  // adapter was being called: acknowledgement is not effect, and the durable
  // record of the command precedes the command itself.
  REQUIRE(adapter.state_at_dispatch.has_value());
  CHECK_EQ(adapter.state_at_dispatch.value(), AttemptState::dispatched);
  CHECK_EQ(adapter.execute_calls, std::uint64_t{1});
  CHECK_EQ(issued.value().state, AttemptState::acknowledged);
  CHECK_EQ(issued.value().command, command.id());
  CHECK_EQ(issued.value().ordinal, AttemptOrdinal::from(1));

  AdapterOutcome matching{command.id(),
                          command.attempt(),
                          command.device(),
                          command.device_generation(),
                          AdapterDisposition::accepted,
                          AdapterSequence::from(1),
                          command.issued_at(),
                          std::string(),
                          std::nullopt};
  CHECK(verify_outcome_echo(command, matching).ok());
  CHECK_EQ(verify_outcome_echo(command, matching).code(), StatusCode::ok);

  // The acknowledgement instant is bound below by the issue instant, inclusive.
  matching.acknowledged_at = command.issued_at();
  CHECK(verify_outcome_echo(command, matching).ok());
  matching.acknowledged_at = LogicalTick::from(command.issued_at().value() + 5);
  CHECK(verify_outcome_echo(command, matching).ok());
  matching.acknowledged_at = LogicalTick::from(command.issued_at().value() - 1);
  CHECK_EQ(verify_outcome_echo(command, matching).code(), StatusCode::adapter_fenced);

  // Each field of the echo is checked on its own, so a stale answer cannot be
  // attributed to a command it was not given.
  AdapterOutcome wrong = matching;
  wrong.acknowledged_at = command.issued_at();
  wrong.command = CommandId::from(command.id().value() + 1);
  CHECK_EQ(verify_outcome_echo(command, wrong).code(), StatusCode::adapter_fenced);

  wrong = matching;
  wrong.attempt = AttemptId::from(command.attempt().value() + 1);
  CHECK_EQ(verify_outcome_echo(command, wrong).code(), StatusCode::adapter_fenced);

  wrong = matching;
  wrong.device = AirflowDeviceId::parse("dev-other").value();
  CHECK_EQ(verify_outcome_echo(command, wrong).code(), StatusCode::adapter_fenced);

  wrong = matching;
  wrong.device_generation = DeviceGeneration::from(command.device_generation().value() + 1);
  CHECK_EQ(verify_outcome_echo(command, wrong).code(), StatusCode::adapter_fenced);

  // A fenced answer names the command it actually answered.
  wrong = matching;
  wrong.command = CommandId::from(command.id().value() + 7);
  const Status fenced = verify_outcome_echo(command, wrong);
  CHECK(!fenced.ok());
  CHECK(fenced.message().find(CommandId::from(command.id().value() + 7).to_string()) !=
        std::string::npos);
  CHECK(fenced.message().find(command.id().to_string()) != std::string::npos);
}

AIRFLOW_TEST(a_fenced_echo_is_discarded_by_the_engine) {
  Result<Fixture> opened = Fixture::open(in_memory());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value().engine();
  const Scenario& scenario = opened.value().scenario();

  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, scenario);
  adapter.set_fence_echo(true);

  const Result<AttemptRecord> issued =
      engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-fenced", 5000), adapter);
  REQUIRE(issued.ok());

  // The adapter answered, and its answer named a different command, so the
  // engine discarded the disposition as well as the sequence and recorded an
  // indeterminate outcome. The attempt stays unresolved.
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{1});
  CHECK_EQ(adapter.accepted_commands(), std::uint64_t{1});
  CHECK_EQ(issued.value().disposition, AdapterDisposition::indeterminate);
  CHECK_EQ(issued.value().state, AttemptState::indeterminate);
  CHECK_EQ(issued.value().adapter_sequence, AdapterSequence::from(0));
  CHECK(issued.value().detail.rfind("adapter answer was fenced:", 0) == 0);
  CHECK(!issued.value().resolved_at.has_value());
  CHECK(is_unresolved(issued.value().state));

  const Result<DeviceView> view = engine.device(scenario.device);
  REQUIRE(view.ok());
  REQUIRE(view.value().unresolved_attempt.has_value());
  CHECK_EQ(view.value().unresolved_attempt.value(), issued.value().id);
}

AIRFLOW_TEST(is_definite_refusal_is_exact) {
  const AdapterDisposition dispositions[] = {
      AdapterDisposition::accepted, AdapterDisposition::refused, AdapterDisposition::unavailable,
      AdapterDisposition::fault, AdapterDisposition::indeterminate,
  };
  for (const AdapterDisposition disposition : dispositions) {
    const bool definite = disposition == AdapterDisposition::refused ||
                          disposition == AdapterDisposition::unavailable ||
                          disposition == AdapterDisposition::fault;
    CHECK_EQ(is_definite_refusal(disposition), definite);
    const std::string_view rendered = to_string(disposition);
    CHECK(!rendered.empty());
    const std::optional<AdapterDisposition> parsed = parse_adapter_disposition(rendered);
    REQUIRE(parsed.has_value());
    CHECK(parsed.value() == disposition);
  }
  CHECK(!is_definite_refusal(AdapterDisposition::accepted));
  CHECK(!is_definite_refusal(AdapterDisposition::indeterminate));
  for (std::size_t left = 0; left < 5; ++left) {
    for (std::size_t right = left + 1; right < 5; ++right) {
      CHECK(std::string_view(to_string(dispositions[left])) !=
            std::string_view(to_string(dispositions[right])));
    }
  }
  CHECK(!parse_adapter_disposition("Accepted").has_value());
  CHECK(!parse_adapter_disposition("").has_value());
}

AIRFLOW_TEST(descriptors_and_capabilities_round_trip) {
  const AdapterDescriptor fixture_descriptor = synthetic_descriptor();
  SyntheticAirflowAdapter adapter(fixture_descriptor);
  const AdapterDescriptor described = adapter.describe();
  CHECK_EQ(described.vendor, std::string("Airflow Control"));
  CHECK_EQ(described.model, std::string("synthetic-test-plant"));
  CHECK_EQ(described.protocol, std::string("in-process"));
  CHECK_EQ(described.capabilities, fixture_descriptor.capabilities);
  CHECK(described.synthetic);

  // The synthetic adapter never reports synthetic=false, whatever the caller
  // constructed it with: every surface that prints its results must be able to
  // say that it drives no hardware.
  AdapterDescriptor lying = fixture_descriptor;
  lying.synthetic = false;
  SyntheticAirflowAdapter forced(std::move(lying));
  CHECK(forced.describe().synthetic);
  CHECK_EQ(forced.describe().capabilities, fixture_descriptor.capabilities);

  const AdapterCapability declared[] = {
      AdapterCapability::set_fan_percent, AdapterCapability::set_airflow,
      AdapterCapability::read_pressure,   AdapterCapability::read_airflow,
      AdapterCapability::read_fan_percent};
  for (const AdapterCapability capability : declared) {
    CHECK(declares(fixture_descriptor, capability));
  }
  CHECK(!declares(fixture_descriptor, AdapterCapability::observe_containment));

  AdapterDescriptor silent = fixture_descriptor;
  silent.capabilities = 0;
  for (const AdapterCapability capability : declared) {
    CHECK(!declares(silent, capability));
  }
  CHECK(!declares(silent, AdapterCapability::observe_containment));

  AdapterDescriptor everything = fixture_descriptor;
  everything.capabilities |= static_cast<std::uint32_t>(AdapterCapability::observe_containment);
  CHECK(declares(everything, AdapterCapability::observe_containment));

  // The rendered capability list is ordered by the declared table and names
  // every bit the build understands.
  CHECK_EQ(describe_capabilities(fixture_descriptor.capabilities),
           std::string("set_fan_percent,set_airflow,read_pressure,read_airflow,read_fan_percent"));
  CHECK_EQ(describe_capabilities(everything.capabilities),
           std::string(
               "set_fan_percent,set_airflow,read_pressure,read_airflow,read_fan_percent,"
               "observe_containment"));
  CHECK_EQ(describe_capabilities(0), std::string("none"));
  CHECK_EQ(describe_capabilities(static_cast<std::uint32_t>(AdapterCapability::read_fan_percent) |
                                 static_cast<std::uint32_t>(AdapterCapability::set_fan_percent)),
           std::string("set_fan_percent,read_fan_percent"));
  // A bit this build does not define is not invented as a capability name.
  CHECK_EQ(describe_capabilities(1u << 20), std::string("none"));
}

AIRFLOW_TEST(a_removed_device_fails_reads_and_refuses_commands) {
  Result<Fixture> opened = Fixture::open(in_memory());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value().engine();
  const Scenario& scenario = opened.value().scenario();

  // Capture a real, engine-authorised command for this device.
  RecordingAdapter recording;
  const Result<AttemptRecord> captured =
      engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-capture", 5000), recording);
  REQUIRE(captured.ok());
  REQUIRE(recording.last_command.has_value());
  const AdapterCommand command = *recording.last_command;

  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, scenario);
  CHECK(adapter.has_device(scenario.device));

  // A read for a device the plant does not hold is unavailable rather than zero.
  SyntheticAirflowAdapter empty(synthetic_descriptor());
  CHECK(!empty.has_device(scenario.device));
  const Result<ObservationDraft> absent_read =
      empty.read(read_request(scenario, ObservationKind::fan_setpoint, std::nullopt, scenario.source,
                              scenario.ready_tick, 1));
  CHECK_STATUS(absent_read, StatusCode::adapter_unavailable);
  CHECK_EQ(empty.read_calls(), std::uint64_t{1});

  // A read that names a generation the plant does not hold is a generation
  // mismatch, which is a different answer from an absent device.
  const Result<ObservationDraft> wrong_generation = adapter.read(AdapterReadRequest{
      .device = scenario.device,
      .device_generation = DeviceGeneration::from(2),
      .kind = ObservationKind::fan_setpoint,
      .relationship = std::nullopt,
      .point = scenario.controlled_space,
      .source = scenario.source,
      .sequence = EvidenceSequence::from(1),
      .at = scenario.ready_tick,
      .evidence_generation = scenario.evidence_generation,
  });
  CHECK_STATUS(wrong_generation, StatusCode::generation_mismatch);

  // The command is accepted while the device is present, then refused once it
  // is gone.
  const AdapterOutcome accepted = adapter.execute(command);
  CHECK_EQ(accepted.disposition, AdapterDisposition::accepted);
  CHECK_EQ(adapter.accepted_commands(), std::uint64_t{1});
  CHECK_EQ(adapter.refused_commands(), std::uint64_t{0});

  adapter.remove_device(scenario.device);
  CHECK(!adapter.has_device(scenario.device));
  CHECK(!adapter.plant_state(scenario.device).has_value());

  const AdapterOutcome refused = adapter.execute(command);
  CHECK_EQ(refused.disposition, AdapterDisposition::unavailable);
  CHECK_EQ(refused.command, command.id());
  CHECK_EQ(refused.attempt, command.attempt());
  CHECK_EQ(adapter.refused_commands(), std::uint64_t{1});
  CHECK_EQ(adapter.accepted_commands(), std::uint64_t{1});
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{2});

  const Result<ObservationDraft> removed_read =
      adapter.read(read_request(scenario, ObservationKind::fan_setpoint, std::nullopt,
                               scenario.source, scenario.ready_tick, 2));
  CHECK_STATUS(removed_read, StatusCode::adapter_unavailable);
}

AIRFLOW_TEST(a_removed_device_refuses_the_engine_command) {
  Result<Fixture> opened = Fixture::open(in_memory());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value().engine();
  const Scenario& scenario = opened.value().scenario();

  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, scenario);
  adapter.remove_device(scenario.device);

  const Result<AttemptRecord> issued =
      engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-absent", 5000), adapter);
  REQUIRE(issued.ok());
  CHECK_EQ(issued.value().disposition, AdapterDisposition::unavailable);
  CHECK_EQ(issued.value().state, AttemptState::unavailable);
  CHECK(is_definite(issued.value().state));
  CHECK(issued.value().resolved_at.has_value());

  // A definite refusal resolves the attempt immediately, so the device is not
  // left latched and the next request is evaluated rather than blocked.
  const Result<DeviceView> view = engine.device(scenario.device);
  REQUIRE(view.ok());
  CHECK(!view.value().unresolved_attempt.has_value());

  const Result<AttemptRecord> again =
      engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-absent-again", 5000), adapter);
  REQUIRE(again.ok());
  CHECK_EQ(again.value().state, AttemptState::unavailable);
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{2});
  CHECK_EQ(adapter.refused_commands(), std::uint64_t{2});
  CHECK_EQ(adapter.accepted_commands(), std::uint64_t{0});
}

AIRFLOW_TEST(armed_dispositions_are_consumed_once_and_persistent_ones_are_not) {
  Result<Fixture> opened = Fixture::open(in_memory());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value().engine();
  const Scenario& scenario = opened.value().scenario();

  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, scenario);

  // An armed next-disposition applies to exactly one execute().
  adapter.set_next_disposition(scenario.device, AdapterDisposition::refused, "armed once");
  const Result<AttemptRecord> first =
      engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-arm-1", 5000), adapter);
  REQUIRE(first.ok());
  CHECK_EQ(first.value().disposition, AdapterDisposition::refused);
  CHECK_EQ(first.value().state, AttemptState::refused);
  CHECK_EQ(first.value().detail, std::string("armed once"));

  const Result<AttemptRecord> second =
      engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-arm-2", 5000), adapter);
  REQUIRE(second.ok());
  CHECK_EQ(second.value().disposition, AdapterDisposition::accepted);
  CHECK_EQ(second.value().state, AttemptState::acknowledged);
  REQUIRE(resolve_now(engine, scenario, second.value().id).ok());

  // A persistent disposition survives every execute() until it is cleared.
  adapter.set_persistent_disposition(scenario.device, AdapterDisposition::fault, "persistent");
  const Result<AttemptRecord> third =
      engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-persistent-1", 5000), adapter);
  REQUIRE(third.ok());
  CHECK_EQ(third.value().disposition, AdapterDisposition::fault);
  CHECK_EQ(third.value().state, AttemptState::faulted);
  CHECK_EQ(third.value().detail, std::string("persistent"));

  const Result<AttemptRecord> fourth =
      engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-persistent-2", 5000), adapter);
  REQUIRE(fourth.ok());
  CHECK_EQ(fourth.value().disposition, AdapterDisposition::fault);

  adapter.clear_dispositions();
  const Result<AttemptRecord> fifth =
      engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-cleared", 5000), adapter);
  REQUIRE(fifth.ok());
  CHECK_EQ(fifth.value().disposition, AdapterDisposition::accepted);

  CHECK_EQ(adapter.execute_calls(), std::uint64_t{5});
  CHECK_EQ(adapter.refused_commands(), std::uint64_t{3});
  CHECK_EQ(adapter.accepted_commands(), std::uint64_t{2});
}

AIRFLOW_TEST(observation_delay_holds_the_pre_command_value) {
  // The delay is expressed in reads: the plant reports its pre-command value for
  // exactly read_count reads, and the pending command becomes visible on the read
  // after them. A delay of zero applies the command during execute().
  for (const std::uint32_t delay : {0u, 1u, 2u, 3u}) {
    Result<Fixture> opened = Fixture::open(in_memory());
    REQUIRE(opened.ok());
    AirflowControlEngine& engine = opened.value().engine();
    const Scenario& scenario = opened.value().scenario();

    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, scenario);
    adapter.set_observation_delay(delay);

    const std::uint32_t commanded = scenario.default_fan_percent.basis_points() + 1000;
    const Result<AttemptRecord> issued =
        engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-delay", commanded), adapter);
    REQUIRE(issued.ok());
    CHECK_EQ(issued.value().state, AttemptState::acknowledged);

    for (std::uint32_t read = 1; read <= delay + 1; ++read) {
      const Result<ObservationDraft> draft =
          adapter.read(read_request(scenario, ObservationKind::fan_setpoint, std::nullopt,
                                    scenario.source, scenario.ready_tick, read));
      REQUIRE(draft.ok());
      const bool before_commit = read <= delay;
      const std::uint32_t expected =
          before_commit ? scenario.default_fan_percent.basis_points() : commanded;
      CHECK_EQ(fan_percent_of(draft.value()), expected);
    }
    const std::optional<SyntheticPlantState> state = adapter.plant_state(scenario.device);
    REQUIRE(state.has_value());
    CHECK_EQ(state.value().fan_percent.basis_points(), commanded);
    CHECK_EQ(adapter.read_calls(), static_cast<std::uint64_t>(delay) + 1);
  }
}

AIRFLOW_TEST(read_failure_applies_to_every_read_until_cleared) {
  Result<Fixture> opened = Fixture::open(in_memory());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value().engine();
  const Scenario& scenario = opened.value().scenario();
  (void)engine;

  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, scenario);

  adapter.set_read_failure(StatusCode::adapter_fault, "injected fault");
  for (std::uint64_t attempt = 1; attempt <= 3; ++attempt) {
    const Result<ObservationDraft> failed =
        adapter.read(read_request(scenario, ObservationKind::fan_setpoint, std::nullopt,
                                  scenario.source, scenario.ready_tick, attempt));
    CHECK_STATUS(failed, StatusCode::adapter_fault);
    CHECK_EQ(failed.message(), std::string("injected fault"));
  }
  CHECK_EQ(adapter.read_calls(), std::uint64_t{3});

  // The failure is persistent, not a one-shot, and only clear_read_failure
  // removes it.
  adapter.clear_read_failure();
  const Result<ObservationDraft> recovered =
      adapter.read(read_request(scenario, ObservationKind::airflow, std::nullopt, scenario.source,
                                scenario.ready_tick, 4));
  REQUIRE(recovered.ok());
  CHECK_EQ(std::get<AirflowReading>(recovered.value().payload).value.cubic_metres_per_hour(),
           std::int64_t{5000});
  CHECK_EQ(adapter.read_calls(), std::uint64_t{4});

  // Any code may be injected, including one that is not an adapter code.
  adapter.set_read_failure(StatusCode::evidence_unknown, "no instrument");
  const Result<ObservationDraft> injected =
      adapter.read(read_request(scenario, ObservationKind::pressure, scenario.relationship,
                                scenario.source, scenario.ready_tick, 5));
  CHECK_STATUS(injected, StatusCode::evidence_unknown);
  CHECK_EQ(adapter.read_calls(), std::uint64_t{5});
}

AIRFLOW_TEST(a_secondary_pressure_source_is_returned_only_for_its_source) {
  Result<Fixture> opened = Fixture::open(in_memory());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value().engine();
  const Scenario& scenario = opened.value().scenario();
  (void)engine;

  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, scenario);

  const SourceId secondary = SourceId::parse("source-secondary").value();
  const Pressure secondary_value = Pressure::from_millipascals(-5000);
  adapter.set_secondary_pressure(scenario.relationship, secondary, secondary_value,
                                 Quality::suspect);

  // The declared source still sees the plant's own differential.
  const Result<ObservationDraft> primary =
      adapter.read(read_request(scenario, ObservationKind::pressure, scenario.relationship,
                                scenario.source, scenario.ready_tick, 1));
  REQUIRE(primary.ok());
  CHECK_EQ(differential_of(primary.value()), scenario.plant_differential);
  CHECK_EQ(primary.value().quality, Quality::good);
  CHECK(primary.value().source == scenario.source);
  CHECK(primary.value().relationship.has_value());
  CHECK(primary.value().relationship.value() == scenario.relationship);

  // The secondary source is returned only for the source it was declared for.
  const Result<ObservationDraft> second =
      adapter.read(read_request(scenario, ObservationKind::pressure, scenario.relationship,
                                secondary, scenario.ready_tick, 2));
  REQUIRE(second.ok());
  CHECK_EQ(differential_of(second.value()), secondary_value);
  CHECK_EQ(second.value().quality, Quality::suspect);
  CHECK(second.value().source == secondary);

  // A different relationship, or an unnamed one, is not a match.
  const PressureRelationshipId other = PressureRelationshipId::parse("rel-other").value();
  const Result<ObservationDraft> other_relationship =
      adapter.read(read_request(scenario, ObservationKind::pressure, other, secondary,
                                scenario.ready_tick, 3));
  REQUIRE(other_relationship.ok());
  CHECK_EQ(differential_of(other_relationship.value()), scenario.plant_differential);
  CHECK_EQ(other_relationship.value().quality, Quality::good);

  // A secondary for one source never answers a read for another source.
  const SourceId third = SourceId::parse("source-third").value();
  const Result<ObservationDraft> other_source =
      adapter.read(read_request(scenario, ObservationKind::pressure, scenario.relationship, third,
                                scenario.ready_tick, 4));
  REQUIRE(other_source.ok());
  CHECK_EQ(differential_of(other_source.value()), scenario.plant_differential);

  adapter.clear_secondary_pressure();
  const Result<ObservationDraft> cleared =
      adapter.read(read_request(scenario, ObservationKind::pressure, scenario.relationship,
                                secondary, scenario.ready_tick, 5));
  REQUIRE(cleared.ok());
  CHECK_EQ(differential_of(cleared.value()), scenario.plant_differential);
  CHECK_EQ(cleared.value().quality, Quality::good);
  CHECK_EQ(adapter.read_calls(), std::uint64_t{5});
}

AIRFLOW_TEST(adapter_counters_are_exact) {
  Result<Fixture> opened = Fixture::open(in_memory());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value().engine();
  const Scenario& scenario = opened.value().scenario();

  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, scenario);

  CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});
  CHECK_EQ(adapter.read_calls(), std::uint64_t{0});
  CHECK_EQ(adapter.accepted_commands(), std::uint64_t{0});
  CHECK_EQ(adapter.applied_commands(), std::uint64_t{0});
  CHECK_EQ(adapter.refused_commands(), std::uint64_t{0});

  // 1: accepted and applied immediately.
  const Result<AttemptRecord> first =
      engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-count-1", 5000), adapter);
  REQUIRE(first.ok());
  CHECK_EQ(first.value().state, AttemptState::acknowledged);
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{1});
  CHECK_EQ(adapter.accepted_commands(), std::uint64_t{1});
  CHECK_EQ(adapter.applied_commands(), std::uint64_t{1});
  REQUIRE(resolve_now(engine, scenario, first.value().id).ok());

  // 2: refused.
  adapter.set_next_disposition(scenario.device, AdapterDisposition::refused, "counter refusal");
  const Result<AttemptRecord> second =
      engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-count-2", 5000), adapter);
  REQUIRE(second.ok());
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{2});
  CHECK_EQ(adapter.refused_commands(), std::uint64_t{1});
  CHECK_EQ(adapter.accepted_commands(), std::uint64_t{1});
  CHECK_EQ(adapter.applied_commands(), std::uint64_t{1});

  // 3: acknowledged without effect: the plant does not move.
  adapter.set_apply_on_execute(false);
  CHECK(!adapter.apply_on_execute());
  const Result<AttemptRecord> third =
      engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-count-3", 6000), adapter);
  REQUIRE(third.ok());
  CHECK_EQ(third.value().state, AttemptState::acknowledged);
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{3});
  CHECK_EQ(adapter.accepted_commands(), std::uint64_t{2});
  CHECK_EQ(adapter.applied_commands(), std::uint64_t{1});
  adapter.apply_pending();
  CHECK_EQ(adapter.applied_commands(), std::uint64_t{1});
  REQUIRE(resolve_now(engine, scenario, third.value().id).ok());

  // 4: delayed application. The two delayed reads report the pre-command value and
  // do not commit; the read after them commits and is the one that counts applied.
  adapter.set_apply_on_execute(true);
  adapter.set_observation_delay(2);
  const Result<AttemptRecord> fourth =
      engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-count-4", 6000), adapter);
  REQUIRE(fourth.ok());
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{4});
  CHECK_EQ(adapter.accepted_commands(), std::uint64_t{3});
  CHECK_EQ(adapter.applied_commands(), std::uint64_t{1});
  REQUIRE(adapter
              .read(read_request(scenario, ObservationKind::fan_setpoint, std::nullopt,
                                 scenario.source, scenario.ready_tick, 1))
              .ok());
  CHECK_EQ(adapter.read_calls(), std::uint64_t{1});
  CHECK_EQ(adapter.applied_commands(), std::uint64_t{1});
  REQUIRE(adapter
              .read(read_request(scenario, ObservationKind::fan_setpoint, std::nullopt,
                                 scenario.source, scenario.ready_tick, 2))
              .ok());
  CHECK_EQ(adapter.read_calls(), std::uint64_t{2});
  CHECK_EQ(adapter.applied_commands(), std::uint64_t{1});
  REQUIRE(adapter
              .read(read_request(scenario, ObservationKind::fan_setpoint, std::nullopt,
                                 scenario.source, scenario.ready_tick, 3))
              .ok());
  CHECK_EQ(adapter.read_calls(), std::uint64_t{3});
  CHECK_EQ(adapter.applied_commands(), std::uint64_t{2});
  REQUIRE(resolve_now(engine, scenario, fourth.value().id).ok());

  // 5: the same delay, committed explicitly instead of by a read.
  adapter.set_observation_delay(1);
  const Result<AttemptRecord> fifth =
      engine.issue(setpoint_request(scenario, scenario.ready_tick, "key-count-5", 6000), adapter);
  REQUIRE(fifth.ok());
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{5});
  CHECK_EQ(adapter.accepted_commands(), std::uint64_t{4});
  CHECK_EQ(adapter.applied_commands(), std::uint64_t{2});
  adapter.apply_pending();
  CHECK_EQ(adapter.applied_commands(), std::uint64_t{3});
  CHECK_EQ(adapter.refused_commands(), std::uint64_t{1});
  CHECK_EQ(adapter.read_calls(), std::uint64_t{3});
}
