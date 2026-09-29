// Example: containment precedence over optimization.
//
// With containment intact, an optimization intent is eligible. Once the same
// room's containment is reported breached, the identical optimization request is
// refused with containment_breached -- while a protective raise_airflow is still
// eligible, and a safety-directed emergency_purge is refused with
// safety_permit_missing until the authority that owns emergency policy adds an
// explicit permit.
//
// Everything here is SYNTHETIC: the containment reports are synthetic inputs and
// no adapter is used at all in this example.

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>

#include "airflow_control/engine.hpp"

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

const AirflowDeviceId kDevice = AirflowDeviceId::parse("room-c-fan-1").value();
const RoomId kRoom = RoomId::parse("room-c").value();
const ContainmentId kAisle = ContainmentId::parse("room-c-aisle").value();
const SafetyPermitId kPurgePermit = SafetyPermitId::parse("room-c-purge-permit").value();
const ActorId kActor = ActorId::parse("example-operator").value();
const SourceId kPanel = SourceId::parse("containment-panel").value();

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
  return directory + "/airflow-containment-precedence.airflowstore";
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
          .id = GrantId::parse("room-c-grant").value(),
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
  status = engine.set_fan_policy(SetFanPolicyRequest{
      .device = kDevice,
      .generation = DeviceGeneration::from(1),
      .expected_revision = StateRevision::from(2),
      .policy =
          FanPolicy{
              .id = PolicyId::parse("room-c-policy").value(),
              .generation = PolicyGeneration::from(1),
              .envelope =
                  OperatingEnvelope{
                      .min_fan_percent = SetpointBasisPoints::create(0).value(),
                      .max_fan_percent = SetpointBasisPoints::create(10000).value(),
                      .default_fan_percent = SetpointBasisPoints::create(6000).value(),
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
    return status;
  }
  return engine.define_containment_element(DefineContainmentRequest{
      .id = kAisle,
      .room = kRoom,
      .row = std::nullopt,
      .kind = ContainmentKind::aisle_containment,
      .expected_revision = std::nullopt,
      .actor = kActor,
      .requested_at = LogicalTick::from(0),
  });
}

ControlRequest request_for(const char* key, ControlIntent intent, SetpointBasisPoints setpoint,
                           LogicalTick at, std::optional<SafetyPermitId> permit) {
  return ControlRequest{
      .key = IdempotencyKey::parse(key).value(),
      .device = kDevice,
      .device_generation = DeviceGeneration::from(1),
      .epoch = AuthorityEpoch::from(1),
      .expected_revision = std::nullopt,
      .intent = intent,
      .setpoint = SetpointPercent{setpoint},
      .actor = kActor,
      .requested_at = at,
      .safety_permit = permit,
      .supersede = std::nullopt,
      .relationship = std::nullopt,
  };
}

/// The refusal rendered exactly as the machine contract states it.
std::string rendered(const Decision& decision) {
  return std::string(to_string(decision.code)) + ": " + decision.message;
}

}  // namespace

int main(int argc, char** argv) {
  const std::string directory = argc > 1 ? argv[1] : "example-containment-precedence";
  const std::string store = store_path(directory);
  std::remove(store.c_str());
  std::remove((store + ".lock").c_str());
  std::cout << "store: " << store << "\n";
  std::cout << "adapter=SYNTHETIC (no hardware is driven); this example issues no command\n";

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

  const LogicalTick now = LogicalTick::from(2);
  if (!engine.advance_tick(now).ok()) {
    return 2;
  }
  const Status intact = engine.report_containment(ReportContainmentRequest{
      .element = kAisle,
      .state = ContainmentState::intact,
      .quality = Quality::good,
      .source = kPanel,
      .sequence = EvidenceSequence::from(1),
      .evidence_generation = EvidenceGeneration::from(1),
      .measured_at = now,
  });
  if (!intact.ok()) {
    std::cout << "containment report failed: " << intact.to_string() << "\n";
    return 2;
  }

  // The optimization request that is used unchanged throughout this example.
  const ControlRequest optimize = request_for(
      "trim-room-c-1", ControlIntent::trim_for_efficiency,
      SetpointBasisPoints::create(5500).value(), now, std::nullopt);
  const auto eligible = engine.evaluate(optimize);
  if (!eligible.ok()) {
    std::cout << "evaluate failed: " << eligible.status().to_string() << "\n";
    return 2;
  }
  std::cout << "step 1 containment=intact optimization-eligible="
            << (eligible.value().eligible ? "true" : "false")
            << " request-class=" << to_string(eligible.value().request_class) << "\n";
  expect(eligible.value().eligible,
         "with containment intact the trim_for_efficiency optimization request is eligible");
  expect(eligible.value().request_class == RequestClass::optimization,
         "the intent is classified as an optimization by the runtime, not by the caller");

  // The same room's containment is now reported breached.
  const Status breach = engine.report_containment(ReportContainmentRequest{
      .element = kAisle,
      .state = ContainmentState::breached,
      .quality = Quality::good,
      .source = kPanel,
      .sequence = EvidenceSequence::from(2),
      .evidence_generation = EvidenceGeneration::from(1),
      .measured_at = now,
  });
  if (!breach.ok()) {
    std::cout << "breach report failed: " << breach.to_string() << "\n";
    return 2;
  }
  const auto refused = engine.evaluate(optimize);
  if (!refused.ok()) {
    std::cout << "evaluate failed: " << refused.status().to_string() << "\n";
    return 2;
  }
  const std::string first_refusal = rendered(refused.value());
  std::cout << "step 2 containment=breached optimization-eligible="
            << (refused.value().eligible ? "true" : "false")
            << " refusal=" << first_refusal << "\n";
  expect(!refused.value().eligible && refused.value().code == StatusCode::containment_breached,
         "the identical optimization request is now refused with containment_breached");
  expect(std::string(to_string(refused.value().code)) == "containment_breached",
         "the refusal token is byte-for-byte containment_breached");

  // A protective request is not blocked by a known breach.
  const ControlRequest protect = request_for(
      "raise-room-c-1", ControlIntent::raise_airflow,
      SetpointBasisPoints::create(6500).value(), now, std::nullopt);
  const auto protective = engine.evaluate(protect);
  if (!protective.ok()) {
    std::cout << "evaluate failed: " << protective.status().to_string() << "\n";
    return 2;
  }
  std::cout << "step 3 containment=breached protective-eligible="
            << (protective.value().eligible ? "true" : "false")
            << " request-class=" << to_string(protective.value().request_class) << "\n";
  expect(protective.value().eligible,
         "a protective raise_airflow is still eligible while containment is known breached");

  // A safety-directed request needs an explicit permit, even though nothing
  // else blocks it.
  const ControlRequest purge = request_for(
      "purge-room-c-1", ControlIntent::emergency_purge,
      SetpointBasisPoints::create(9000).value(), now, kPurgePermit);
  const auto unpermitted = engine.evaluate(purge);
  if (!unpermitted.ok()) {
    std::cout << "evaluate failed: " << unpermitted.status().to_string() << "\n";
    return 2;
  }
  std::cout << "step 4 purge-eligible=" << (unpermitted.value().eligible ? "true" : "false")
            << " refusal=" << rendered(unpermitted.value()) << "\n";
  expect(!unpermitted.value().eligible &&
             unpermitted.value().code == StatusCode::safety_permit_missing,
         "the safety-directed emergency_purge is refused with safety_permit_missing");

  const Status permitted = engine.add_safety_permit(
      SafetyPermit{
          .id = kPurgePermit,
          .issuer = SourceId::parse("authority-emergency-policy").value(),
          .epoch = AuthorityEpoch::from(1),
          .device = kDevice,
          .issued_at = now,
          .expires_at = LogicalTick::from(10),
          .reason = "example permit: smoke clearance directed by the emergency authority",
      },
      kActor, now);
  if (!permitted.ok()) {
    std::cout << "safety permit refused: " << permitted.to_string() << "\n";
    return 2;
  }
  const auto safety = engine.evaluate(purge);
  if (!safety.ok()) {
    std::cout << "evaluate failed: " << safety.status().to_string() << "\n";
    return 2;
  }
  std::cout << "step 5 purge-eligible=" << (safety.value().eligible ? "true" : "false")
            << "\n";
  expect(safety.value().eligible,
         "the same emergency_purge request is eligible once the explicit permit exists");

  // The optimization refusal is unchanged by the permit and byte-identical to
  // the first one.
  const auto repeated = engine.evaluate(optimize);
  if (!repeated.ok()) {
    std::cout << "evaluate failed: " << repeated.status().to_string() << "\n";
    return 2;
  }
  const std::string second_refusal = rendered(repeated.value());
  std::cout << "step 6 refusal=" << second_refusal << "\n";
  std::cout << "step 6 byte-identical=" << (first_refusal == second_refusal ? "true" : "false")
            << " code-identical="
            << (refused.value().code == repeated.value().code ? "true" : "false")
            << "\n";
  expect(first_refusal == second_refusal,
         "repeating the optimization request refuses it with a byte-identical code and message");
  expect(repeated.value().code == StatusCode::containment_breached,
         "the repeated refusal is still containment_breached");

  const Status closed = engine.close();
  expect(closed.ok(), "the durable store closed cleanly");
  if (failures != 0) {
    std::cout << failures << " expectation(s) failed\n";
    return 1;
  }
  std::cout << "containment precedence held: optimization blocked, protection permitted, "
               "emergency permit required\n";
  return 0;
}
