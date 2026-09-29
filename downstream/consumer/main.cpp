// Out-of-tree consumer of the installed Airflow Control package.
//
// This program is not part of the Airflow Control build tree. It is configured
// and built separately against an install prefix with
// find_package(AirflowControl 1.0 REQUIRED), and it exercises the installed
// headers and library through a real lifecycle: open a durable store, adopt an
// epoch, register a device, set the lifecycle and a fan policy, define a pressure
// relationship and a containment element, declare and satisfy a protected
// interlock, add a grant, accept an initial observation, evaluate and issue a
// raise_airflow command through a SyntheticAirflowAdapter, verify the effect from
// a fresh observation, close, reopen, and prove that the attempt journal and the
// verified effect survived.
//
// The adapter is SYNTHETIC: it is a deterministic simulator that drives no
// hardware.

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <string>

#include <airflow_control/engine.hpp>
#include <airflow_control/synthetic_adapter.hpp>
#include <airflow_control/version.hpp>

using namespace airflow_control;

namespace {

const AirflowDeviceId kDevice = AirflowDeviceId::parse("consumer-fan-1").value();
const RoomId kRoom = RoomId::parse("consumer-room").value();
const PressureRelationshipId kRelationship =
    PressureRelationshipId::parse("consumer-pressure").value();
const ContainmentId kAisle = ContainmentId::parse("consumer-aisle").value();
const InterlockId kInterlock = InterlockId::parse("consumer-safe").value();
const ActorId kActor = ActorId::parse("consumer-operator").value();
const SourceId kField = SourceId::parse("consumer-field").value();

int fail(const std::string& step, const Status& status) {
  std::cout << step << " failed: " << status.to_string() << "\n";
  return 1;
}

EngineOptions consumer_options() {
  EngineOptions options;
  options.evidence.max_age_ticks = LogicalTick::from(1000);
  options.audit_capacity = 128;
  options.idempotency_window = 32;
  options.attempt_journal_capacity = 64;
  return options;
}

AdapterDescriptor synthetic_descriptor() {
  AdapterDescriptor descriptor;
  descriptor.vendor = "Airflow Control";
  descriptor.model = "synthetic-consumer-plant";
  descriptor.protocol = "in-process";
  descriptor.capabilities = static_cast<std::uint32_t>(AdapterCapability::set_fan_percent) |
                            static_cast<std::uint32_t>(AdapterCapability::read_fan_percent);
  descriptor.synthetic = true;
  return descriptor;
}

}  // namespace

int main(int argc, char** argv) {
  const std::string directory = argc > 1 ? argv[1] : "consumer-store";
  std::error_code error;
  std::filesystem::create_directories(directory, error);
  const std::string store = directory + "/airflow-consumer.airflowstore";
  std::remove(store.c_str());
  std::remove((store + ".lock").c_str());

  std::cout << "Airflow Control " << kVersionString << " consumer\n";
  std::cout << "store: " << store << "\n";
  std::cout << "adapter=SYNTHETIC (no hardware is driven)\n";

  std::uint64_t attempts_after_reopen = 0;
  {
    auto opened =
        AirflowControlEngine::open(store, OpenMode::create_new, consumer_options());
    if (!opened.ok()) {
      return fail("open", opened.status());
    }
    AirflowControlEngine& engine = opened.value();

    Status status = engine.adopt_epoch(AuthorityEpoch::from(1), kActor, LogicalTick::from(0));
    if (!status.ok()) {
      return fail("adopt_epoch", status);
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
      return fail("register_device", status);
    }
    // The grant exists before the device is commissioned, because commissioning
    // a device is itself an authority-bearing act.
    status = engine.add_grant(
        PermissionGrant{
            .id = GrantId::parse("consumer-grant").value(),
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
      return fail("add_grant", status);
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
      return fail("set_device_lifecycle", status);
    }
    status = engine.set_fan_policy(SetFanPolicyRequest{
        .device = kDevice,
        .generation = DeviceGeneration::from(1),
        .expected_revision = StateRevision::from(2),
        .policy =
            FanPolicy{
                .id = PolicyId::parse("consumer-policy").value(),
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
    if (!status.ok()) {
      return fail("set_fan_policy", status);
    }
    status = engine.define_pressure_relationship(DefineRelationshipRequest{
        .id = kRelationship,
        .room = kRoom,
        .controlled_space = SpaceRefId::parse("consumer-controlled-space").value(),
        .reference_space = SpaceRefId::parse("consumer-reference-space").value(),
        .polarity = PressurePolarity::positive,
        .lower = Pressure::from_millipascals(1000),
        .upper = Pressure::from_millipascals(5000),
        .tolerance = Pressure::from_millipascals(200),
        .evidence_generation = EvidenceGeneration::from(1),
        .expected_revision = std::nullopt,
        .actor = kActor,
        .requested_at = LogicalTick::from(0),
    });
    if (!status.ok()) {
      return fail("define_pressure_relationship", status);
    }
    status = engine.define_containment_element(DefineContainmentRequest{
        .id = kAisle,
        .room = kRoom,
        .row = std::nullopt,
        .kind = ContainmentKind::aisle_containment,
        .expected_revision = std::nullopt,
        .actor = kActor,
        .requested_at = LogicalTick::from(0),
    });
    if (!status.ok()) {
      return fail("define_containment_element", status);
    }
    if (!engine.advance_tick(LogicalTick::from(1)).ok()) {
      std::cout << "advance_tick failed\n";
      return 1;
    }
    status = engine.report_containment(ReportContainmentRequest{
        .element = kAisle,
        .state = ContainmentState::intact,
        .quality = Quality::good,
        .source = kField,
        .sequence = EvidenceSequence::from(1),
        .evidence_generation = EvidenceGeneration::from(1),
        .measured_at = LogicalTick::from(1),
    });
    if (!status.ok()) {
      return fail("report_containment", status);
    }
    status = engine.declare_interlock(DeclareInterlockRequest{
        .id = kInterlock,
        .room = kRoom,
        .row = std::nullopt,
        .device = kDevice,
        .klass = InterlockClass::protected_obligation,
        .epoch = AuthorityEpoch::from(1),
        .actor = kActor,
        .declared_at = LogicalTick::from(1),
    });
    if (!status.ok()) {
      return fail("declare_interlock", status);
    }
    status = engine.report_interlock(ReportInterlockRequest{
        .id = kInterlock,
        .state = InterlockState::satisfied,
        .sequence = EvidenceSequence::from(1),
        .epoch = AuthorityEpoch::from(1),
        .reported_at = LogicalTick::from(1),
    });
    if (!status.ok()) {
      return fail("report_interlock", status);
    }

    // The initial observation: what the field says before the command.
    if (!engine.advance_tick(LogicalTick::from(2)).ok()) {
      std::cout << "advance_tick failed\n";
      return 1;
    }
    const auto initial = engine.observe(
        ObservationDraft{
            .payload = FanReading{SetpointBasisPoints::create(5000).value()},
            .device = kDevice,
            .relationship = std::nullopt,
            .point = SpaceRefId::parse("consumer-fan-point").value(),
            .source = kField,
            .sequence = EvidenceSequence::from(1),
            .measured_at = LogicalTick::from(2),
            .device_generation = DeviceGeneration::from(1),
            .evidence_generation = EvidenceGeneration::from(1),
            .quality = Quality::good,
        },
        LogicalTick::from(2));
    if (!initial.ok()) {
      return fail("observe", initial.status());
    }

    AdapterDescriptor descriptor = synthetic_descriptor();
    SyntheticAirflowAdapter adapter(descriptor);
    adapter.seed_device(kDevice, DeviceGeneration::from(1),
                        SyntheticPlantState{
                            .fan_percent = SetpointBasisPoints::create(5000).value(),
                            .airflow = Airflow::from_cubic_metres_per_hour(5000),
                            .differential = Pressure::from_millipascals(2000),
                        });

    const ControlRequest request{
        .key = IdempotencyKey::parse("consumer-raise-1").value(),
        .device = kDevice,
        .device_generation = DeviceGeneration::from(1),
        .epoch = AuthorityEpoch::from(1),
        .expected_revision = std::nullopt,
        .intent = ControlIntent::raise_airflow,
        .setpoint = SetpointPercent{SetpointBasisPoints::create(6000).value()},
        .actor = kActor,
        .requested_at = LogicalTick::from(2),
        .safety_permit = std::nullopt,
        .supersede = std::nullopt,
        .relationship = std::nullopt,
    };
    const auto decision = engine.evaluate(request);
    if (!decision.ok()) {
      return fail("evaluate", decision.status());
    }
    if (!decision.value().eligible) {
      std::cout << "evaluate refused: " << to_string(decision.value().code) << ": "
                << decision.value().message << "\n";
      return 1;
    }
    const auto issued = engine.issue(request, adapter);
    if (!issued.ok()) {
      return fail("issue", issued.status());
    }
    std::cout << "attempt=" << issued.value().id.to_string()
              << " disposition=" << to_string(issued.value().disposition)
              << " attempt-state=" << to_string(issued.value().state) << "\n";

    // Verification against a fresh synthetic observation.
    if (!engine.advance_tick(LogicalTick::from(4)).ok()) {
      std::cout << "advance_tick failed\n";
      return 1;
    }
    const auto verified = engine.verify(VerificationRequest{
        .attempt = issued.value().id,
        .adapter = &adapter,
        .source = kField,
        .fan_point = SpaceRefId::parse("consumer-fan-point").value(),
        .pressure_point = SpaceRefId::parse("consumer-pressure-point").value(),
        .fan_sequence = EvidenceSequence::from(2),
        .pressure_sequence = EvidenceSequence::from(1),
        .at = LogicalTick::from(4),
        .relationship = std::nullopt,
    });
    if (!verified.ok()) {
      return fail("verify", verified.status());
    }
    std::cout << "effect=" << to_string(verified.value().state)
              << " detail=" << verified.value().message << "\n";
    if (verified.value().state != EffectState::effective) {
      std::cout << "the effect was not established\n";
      return 1;
    }
    const auto before_close = engine.device(kDevice);
    if (!before_close.ok() || before_close.value().effect != EffectState::effective) {
      std::cout << "the device effect was not recorded\n";
      return 1;
    }
    const Status closed = engine.close();
    if (!closed.ok()) {
      return fail("close", closed);
    }
  }

  auto reopened = AirflowControlEngine::open(store, OpenMode::open_existing, consumer_options());
  if (!reopened.ok()) {
    return fail("reopen", reopened.status());
  }
  AirflowControlEngine& engine = reopened.value();
  const std::vector<AttemptView> attempts = engine.attempts();
  attempts_after_reopen = attempts.size();
  const auto view = engine.device(kDevice);
  const bool journal_survived =
      attempts.size() == 1 && attempts.front().record.state == AttemptState::effect_established;
  const bool effect_survived =
      view.ok() && view.value().effect == EffectState::effective &&
      !view.value().unresolved_attempt.has_value();
  std::cout << "after reopen: attempts=" << attempts_after_reopen
            << " attempt-state="
            << (attempts.empty() ? std::string("-")
                                 : std::string(to_string(attempts.front().record.state)))
            << " effect=" << (view.ok() ? std::string(to_string(view.value().effect))
                                        : std::string("-"))
            << " unresolved="
            << (view.ok() && view.value().unresolved_attempt.has_value() ? "true" : "false")
            << "\n";
  const Status closed = engine.close();
  if (!closed.ok()) {
    return fail("close", closed);
  }
  if (!journal_survived) {
    std::cout << "the attempt journal did not survive the reopen\n";
    return 1;
  }
  if (!effect_survived) {
    std::cout << "the verified effect did not survive the reopen\n";
    return 1;
  }
  std::cout << "the installed package preserved the attempt journal and the verified effect\n";
  std::cout << "adapter=SYNTHETIC (no hardware is driven)\n";
  return 0;
}
