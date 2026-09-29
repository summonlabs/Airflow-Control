// Example: an acknowledgement that produces no effect.
//
// The adapter is configured with set_apply_on_execute(false): it answers every
// command with accepted and changes nothing in the simulated plant. The runtime
// must record that answer as an acknowledgement, must never claim an effect
// from it, and must let fresh evidence be what contradicts it.
//
// Everything here is SYNTHETIC: the adapter is a deterministic simulator that
// drives no hardware.

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>

#include "airflow_control/engine.hpp"
#include "airflow_control/synthetic_adapter.hpp"

using namespace airflow_control;

namespace {

int failures = 0;

void expect(bool condition, const std::string& description) {
  if (!condition) {
    std::cout << "EXPECTATION FAILED: " << description << "\n";
    failures += 1;
  } else {
    std::cout << "ok: " << description << "\n";
  }
}

const AirflowDeviceId kDevice = AirflowDeviceId::parse("room-b-fan-1").value();
const RoomId kRoom = RoomId::parse("room-b").value();
const ActorId kActor = ActorId::parse("example-operator").value();
const SourceId kField = SourceId::parse("field-instrumentation").value();

EngineOptions example_options() {
  EngineOptions options;
  options.evidence.max_age_ticks = LogicalTick::from(1000);
  options.audit_capacity = 128;
  options.idempotency_window = 32;
  options.attempt_journal_capacity = 64;
  return options;
}

std::string store_path(const std::string& directory) {
  std::error_code error;
  std::filesystem::create_directories(directory, error);
  return directory + "/airflow-ack-without-effect.airflowstore";
}

Status build_model(AirflowControlEngine& engine) {
  Status status = engine.adopt_epoch(AuthorityEpoch::from(1), kActor, LogicalTick::from(0));
  if (!status.ok()) {
    return status;
  }
  status = engine.register_device(RegisterDeviceRequest{
      .device = kDevice,
      .generation = DeviceGeneration::from(1),
      .room = kRoom,
      .row = std::nullopt,
      .actor = kActor,
      .requested_at = LogicalTick::from(0),
  });
  if (!status.ok()) {
    return status;
  }
  status = engine.add_grant(
      PermissionGrant{
          .id = GrantId::parse("room-b-grant").value(),
          .issuer = SourceId::parse("authority-airflow").value(),
          .epoch = AuthorityEpoch::from(1),
          .device = kDevice,
          .device_generation = DeviceGeneration::from(1),
          .room = kRoom,
          .actions = ActionSet::all(),
          .issued_at = LogicalTick::from(0),
          .expires_at = std::nullopt,
          .revoked = false,
      },
      kActor, LogicalTick::from(0));
  if (!status.ok()) {
    return status;
  }
  status = engine.set_device_lifecycle(SetLifecycleRequest{
      .device = kDevice,
      .generation = DeviceGeneration::from(1),
      .expected_revision = StateRevision::from(1),
      .target = DeviceLifecycle::active,
      .epoch = AuthorityEpoch::from(1),
      .actor = kActor,
      .requested_at = LogicalTick::from(0),
  });
  if (!status.ok()) {
    return status;
  }
  return engine.set_fan_policy(SetFanPolicyRequest{
      .device = kDevice,
      .generation = DeviceGeneration::from(1),
      .expected_revision = StateRevision::from(2),
      .policy =
          FanPolicy{
              .id = PolicyId::parse("room-b-policy").value(),
              .generation = PolicyGeneration::from(1),
              .envelope =
                  OperatingEnvelope{
                      .min_fan_percent = SetpointBasisPoints::create(0).value(),
                      .max_fan_percent = SetpointBasisPoints::create(10000).value(),
                      .default_fan_percent = SetpointBasisPoints::create(5000).value(),
                      .min_airflow = Airflow::from_cubic_metres_per_hour(0),
                      .max_airflow = Airflow::from_cubic_metres_per_hour(100000),
                      .max_step = SlewBasisPoints::create(10000).value(),
                      .source = SourceId::parse("authority-airflow").value(),
                      .evidence_generation = EvidenceGeneration::from(1),
                  },
          },
      .actor = kActor,
      .requested_at = LogicalTick::from(0),
  });
}

AdapterDescriptor synthetic_descriptor() {
  AdapterDescriptor descriptor;
  descriptor.vendor = "Airflow Control";
  descriptor.model = "synthetic-acknowledge-only";
  descriptor.protocol = "in-process";
  descriptor.capabilities = static_cast<std::uint32_t>(AdapterCapability::set_fan_percent) |
                            static_cast<std::uint32_t>(AdapterCapability::read_fan_percent);
  descriptor.synthetic = true;
  return descriptor;
}

}  // namespace

int main(int argc, char** argv) {
  const std::string directory = argc > 1 ? argv[1] : "example-ack-without-effect";
  const std::string store = store_path(directory);
  std::remove(store.c_str());
  std::remove((store + ".lock").c_str());
  std::cout << "store: " << store << "\n";
  std::cout << "adapter=SYNTHETIC (no hardware is driven)\n";

  auto opened = AirflowControlEngine::open(store, OpenMode::create_new, example_options());
  if (!opened.ok()) {
    std::cout << "open failed: " << opened.status().to_string() << "\n";
    return 2;
  }
  AirflowControlEngine& engine = opened.value();
  const Status built = build_model(engine);
  if (!built.ok()) {
    std::cout << "setup failed: " << built.to_string() << "\n";
    return 2;
  }

  // The plant sits at 50.00 percent and the field has already said so.
  if (!engine.advance_tick(LogicalTick::from(1)).ok()) {
    return 2;
  }
  const auto seeded = engine.observe(
      ObservationDraft{
          .payload = FanReading{SetpointBasisPoints::create(5000).value()},
          .device = kDevice,
          .relationship = std::nullopt,
          .point = SpaceRefId::parse("room-b-fan-point").value(),
          .source = kField,
          .sequence = EvidenceSequence::from(1),
          .measured_at = LogicalTick::from(1),
          .device_generation = DeviceGeneration::from(1),
          .evidence_generation = EvidenceGeneration::from(1),
          .quality = Quality::good,
      },
      LogicalTick::from(1));
  if (!seeded.ok()) {
    std::cout << "initial observation refused: " << seeded.status().to_string() << "\n";
    return 2;
  }

  AdapterDescriptor descriptor = synthetic_descriptor();
  SyntheticAirflowAdapter adapter(descriptor);
  adapter.seed_device(kDevice, DeviceGeneration::from(1),
                      SyntheticPlantState{
                          .fan_percent = SetpointBasisPoints::create(5000).value(),
                          .airflow = Airflow::from_cubic_metres_per_hour(5000),
                          .differential = Pressure::from_millipascals(-6000),
                      });
  adapter.set_apply_on_execute(false);
  std::cout << "adapter apply-on-execute=" << (adapter.apply_on_execute() ? "true" : "false")
            << "\n";

  if (!engine.advance_tick(LogicalTick::from(2)).ok()) {
    return 2;
  }
  const ControlRequest request{
      .key = IdempotencyKey::parse("raise-room-b-1").value(),
      .device = kDevice,
      .device_generation = DeviceGeneration::from(1),
      .epoch = AuthorityEpoch::from(1),
      .expected_revision = std::nullopt,
      .intent = ControlIntent::raise_airflow,
      .setpoint = SetpointPercent{SetpointBasisPoints::create(7000).value()},
      .actor = kActor,
      .requested_at = LogicalTick::from(2),
      .safety_permit = std::nullopt,
      .supersede = std::nullopt,
      .relationship = std::nullopt,
  };
  const auto issued = engine.issue(request, adapter);
  if (!issued.ok()) {
    std::cout << "issue failed: " << issued.status().to_string() << "\n";
    return 2;
  }
  std::cout << "attempt " << issued.value().id.to_string()
            << " disposition=" << to_string(issued.value().disposition)
            << " attempt-state=" << to_string(issued.value().state)
            << " commanded=" << to_string(issued.value().setpoint) << "\n";

  expect(issued.value().disposition == AdapterDisposition::accepted,
         "the AdapterOutcome is recorded as an acknowledgement: disposition accepted");
  expect(issued.value().state == AttemptState::acknowledged,
         "the durable attempt state is acknowledged, not established");
  expect(!issued.value().effect_sequence.has_value(),
         "no effect sequence was assigned by the acknowledgement");
  expect(adapter.applied_commands() == 0,
         "the simulated plant applied no command, because the adapter was told not to apply");
  const auto plant = adapter.plant_state(kDevice);
  expect(plant.has_value() &&
             plant->fan_percent == SetpointBasisPoints::create(5000).value(),
         "the simulated plant still reports the pre-command 50.00 percent");

  const auto view = engine.device(kDevice);
  expect(view.ok() && view.value().effect == EffectState::unverified,
         "no effect is claimed from the acknowledgement: the device effect is still unverified");
  expect(view.ok() && view.value().unresolved_attempt.has_value() &&
             *view.value().unresolved_attempt == issued.value().id,
         "the acknowledged attempt is the device's unresolved latch: no effect is claimed, so "
         "further control is blocked until the attempt is verified or resolved");
  const auto live = engine.attempt(issued.value().id);
  expect(live.ok() && live.value().state != AttemptState::effect_established,
         "the attempt was never treated as effective");

  // Fresh evidence is what contradicts the acknowledgement.
  if (!engine.advance_tick(LogicalTick::from(3)).ok()) {
    return 2;
  }
  const auto verified = engine.verify(VerificationRequest{
      .attempt = issued.value().id,
      .adapter = &adapter,
      .source = kField,
      .fan_point = SpaceRefId::parse("room-b-fan-point").value(),
      .pressure_point = SpaceRefId::parse("room-b-pressure-point").value(),
      .fan_sequence = EvidenceSequence::from(2),
      .pressure_sequence = EvidenceSequence::from(1),
      .at = LogicalTick::from(3),
      .relationship = std::nullopt,
  });
  if (!verified.ok()) {
    std::cout << "verify failed: " << verified.status().to_string() << "\n";
    return 2;
  }
  std::cout << "verification effect=" << to_string(verified.value().state)
            << " observed-fan-percent="
            << (verified.value().observed_fan_percent.has_value()
                    ? verified.value().observed_fan_percent->to_string()
                    : std::string("-"))
            << " detail=" << verified.value().message << "\n";
  expect(verified.value().state == EffectState::contradicted,
         "fresh evidence after the acknowledgement establishes that the commanded setpoint was "
         "not delivered");
  expect(verified.value().observed_fan_percent.has_value() &&
             verified.value().observed_fan_percent->basis_points() == 5000,
         "the fresh observation reports the unchanged 50.00 percent");
  const auto closed_record = engine.attempt(issued.value().id);
  expect(closed_record.ok() &&
             closed_record.value().state == AttemptState::effect_contradicted,
         "the attempt is closed as effect_contradicted");

  const Status closed = engine.close();
  expect(closed.ok(), "the durable store closed cleanly");
  if (failures != 0) {
    std::cout << failures << " expectation(s) failed\n";
    return 1;
  }
  std::cout << "an acknowledgement was never mistaken for an effect\n";
  return 0;
}
