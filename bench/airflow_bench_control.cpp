// Benchmark of the completed control operation.
//
// One timed operation is one whole accepted control command through a durable
// store, with nothing that belongs to the operation left outside the
// measurement:
//   * request validation and precondition evaluation;
//   * the durable publication of the attempt record, which is the
//     command-attempt boundary;
//   * the adapter call;
//   * the durable publication of the adapter outcome;
//   * verification against a fresh synthetic observation;
//   * the durable publication of the verification.
// The tick advances that the two instants require are inside the loop iteration
// as well. The durability cost is included: no flush is excluded, and the
// numbers below are end-to-end operation throughput, never submission latency.
//
// Labeling: the store path measured here is a REAL file and device I/O. The
// adapter is SYNTHETIC: it is a deterministic simulator that drives no hardware.
// No result below is hardware evidence.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <locale>
#include <string>
#include <vector>

#include "airflow_control/engine.hpp"
#include "airflow_control/synthetic_adapter.hpp"

using namespace airflow_control;

namespace {

constexpr std::uint64_t kDefaultOperations = 200;
constexpr std::uint64_t kDefaultDevices = 4;
constexpr std::uint64_t kDefaultRepetitions = 5;

/// The engine configuration the runs below are measured under. It is printed
/// with the results, because a throughput figure without its configuration is
/// not a measurement.
///
/// The runtime treats a caller's retention as a floor rather than a ceiling: a
/// store reopened with a smaller window keeps the replay guarantee it was
/// written under. These values are therefore set to the values the runtime
/// itself retains a fresh store under, and every run verifies that the effective
/// retention in canonical_state() is exactly the configuration printed here.
constexpr std::size_t kAuditCapacity = 256;
constexpr std::size_t kIdempotencyWindow = 256;
constexpr std::size_t kAttemptJournalCapacity = 512;
constexpr std::uint64_t kSlotCapacityBytes = 262144;

/// Every device is seeded at this setpoint, and every command moves it.
constexpr std::uint32_t kLowSetpoint = 4000;
constexpr std::uint32_t kHighSetpoint = 7000;

const ActorId kActor = ActorId::parse("bench-operator").value();
const RoomId kRoom = RoomId::parse("bench-room").value();
const SourceId kField = SourceId::parse("bench-field").value();

struct Workload {
  std::uint64_t operations{kDefaultOperations};
  std::uint64_t devices{kDefaultDevices};
  std::uint64_t repetitions{kDefaultRepetitions};
  std::string store{"airflow-bench-control.airflowstore"};
};

EngineOptions bench_options() {
  EngineOptions options;
  options.evidence.max_age_ticks = LogicalTick::from(1000000000ULL);
  options.audit_capacity = kAuditCapacity;
  options.idempotency_window = kIdempotencyWindow;
  options.attempt_journal_capacity = kAttemptJournalCapacity;
  options.store.slot_capacity_bytes = kSlotCapacityBytes;
  return options;
}

Result<std::uint64_t> parse_count(const std::string& text) {
  if (text.empty()) {
    return Status::failure(StatusCode::invalid_argument, "an empty count");
  }
  std::uint64_t value = 0;
  for (const char digit : text) {
    if (digit < '0' || digit > '9') {
      return Status::failure(StatusCode::invalid_argument, "not a count: " + text);
    }
    value = value * 10 + static_cast<std::uint64_t>(digit - '0');
  }
  return value;
}

Result<Workload> parse_workload(int argc, char** argv) {
  Workload workload;
  for (int index = 1; index < argc; ++index) {
    const std::string argument = argv[index];
    const bool needs_value =
        argument == "--operations" || argument == "--devices" || argument == "--repetitions" ||
        argument == "--store";
    if (!needs_value) {
      return Status::failure(StatusCode::invalid_argument, "unknown option " + argument);
    }
    if (index + 1 >= argc) {
      return Status::failure(StatusCode::invalid_argument, argument + " needs a value");
    }
    const std::string value = argv[++index];
    if (argument == "--store") {
      workload.store = value;
      continue;
    }
    const Result<std::uint64_t> count = parse_count(value);
    if (!count.ok()) {
      return count.status();
    }
    if (argument == "--operations") {
      workload.operations = count.value();
    } else if (argument == "--devices") {
      workload.devices = count.value();
    } else {
      workload.repetitions = count.value();
    }
  }
  if (workload.operations == 0 || workload.devices == 0 || workload.repetitions == 0) {
    return Status::failure(StatusCode::invalid_argument, "counts must be positive");
  }
  if (workload.devices > ModelBounds::max_devices) {
    return Status::failure(StatusCode::out_of_range,
                           "devices must not exceed the structural maximum of " +
                               std::to_string(ModelBounds::max_devices));
  }
  return workload;
}

AdapterDescriptor synthetic_descriptor() {
  AdapterDescriptor descriptor;
  descriptor.vendor = "Airflow Control";
  descriptor.model = "synthetic-bench-plant";
  descriptor.protocol = "in-process";
  descriptor.capabilities = static_cast<std::uint32_t>(AdapterCapability::set_fan_percent) |
                            static_cast<std::uint32_t>(AdapterCapability::read_fan_percent);
  descriptor.synthetic = true;
  return descriptor;
}

AirflowDeviceId device_id(std::uint64_t index) {
  return AirflowDeviceId::parse("bench-fan-" + std::to_string(index)).value();
}

/// What one repetition measured.
struct RunResult {
  std::uint64_t operations{0};
  double seconds{0.0};
  std::uint64_t publications{0};
  std::uint64_t attempts{0};
  std::size_t canonical_bytes{0};
  bool verified{false};
};

/// Builds the workload's model and runs the timed loop.
Result<RunResult> run_once(const Workload& workload, std::uint64_t repetition) {
  RunResult result;
  const std::string store = workload.store + "." + std::to_string(repetition);
  std::remove(store.c_str());
  std::remove((store + ".lock").c_str());

  auto opened = AirflowControlEngine::open(store, OpenMode::create_new, bench_options());
  if (!opened.ok()) {
    return opened.status();
  }
  AirflowControlEngine& engine = opened.value();
  Status status = engine.adopt_epoch(AuthorityEpoch::from(1), kActor, LogicalTick::from(0));
  if (!status.ok()) {
    return status;
  }

  std::vector<AirflowDeviceId> devices;
  devices.reserve(static_cast<std::size_t>(workload.devices));
  for (std::uint64_t index = 0; index < workload.devices; ++index) {
    const AirflowDeviceId device = device_id(index);
    devices.push_back(device);
    status = engine.register_device(RegisterDeviceRequest{
        .device = device,
        .generation = DeviceGeneration::from(1),
        .room = kRoom,
        .row = std::nullopt,
        .actor = kActor,
        .requested_at = LogicalTick::from(0),
    });
    if (!status.ok()) {
      return status;
    }
  }

  // One room-scoped grant covers every device in the room, and it is recorded
  // before the devices are commissioned because commissioning is itself an
  // authority-bearing act.
  status = engine.add_grant(
      PermissionGrant{
          .id = GrantId::parse("bench-grant").value(),
          .issuer = SourceId::parse("bench-authority").value(),
          .epoch = AuthorityEpoch::from(1),
          .device = std::nullopt,
          .device_generation = std::nullopt,
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

  AdapterDescriptor descriptor = synthetic_descriptor();
  SyntheticAirflowAdapter adapter(descriptor);
  for (const AirflowDeviceId& device : devices) {
    status = engine.set_device_lifecycle(SetLifecycleRequest{
        .device = device,
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
        .device = device,
        .generation = DeviceGeneration::from(1),
        .expected_revision = StateRevision::from(2),
        .policy =
            FanPolicy{
                .id = PolicyId::parse("bench-policy").value(),
                .generation = PolicyGeneration::from(1),
                .envelope =
                    OperatingEnvelope{
                        .min_fan_percent = SetpointBasisPoints::create(0).value(),
                        .max_fan_percent = SetpointBasisPoints::create(10000).value(),
                        .default_fan_percent = SetpointBasisPoints::create(kLowSetpoint).value(),
                        .min_airflow = Airflow::from_cubic_metres_per_hour(0),
                        .max_airflow = Airflow::from_cubic_metres_per_hour(100000),
                        .max_step = SlewBasisPoints::create(10000).value(),
                        .source = SourceId::parse("bench-authority").value(),
                        .evidence_generation = EvidenceGeneration::from(1),
                    },
            },
        .actor = kActor,
        .requested_at = LogicalTick::from(0),
    });
    if (!status.ok()) {
      return status;
    }
    adapter.seed_device(device, DeviceGeneration::from(1),
                        SyntheticPlantState{
                            .fan_percent = SetpointBasisPoints::create(kLowSetpoint).value(),
                            .airflow = Airflow::from_cubic_metres_per_hour(
                                static_cast<std::int64_t>(kLowSetpoint)),
                            .differential = Pressure::from_millipascals(-6000),
                        });
  }

  // One evidence sequence per device: a reading that is not strictly newer than
  // the one retained for its device is refused, which is correct and would end
  // the run rather than quietly measure something else.
  std::vector<std::uint64_t> sequences(devices.size(), 0);
  const std::uint64_t before_publications = engine.store_audit().publications;
  std::uint64_t completed = 0;
  const auto started = std::chrono::steady_clock::now();
  for (std::uint64_t index = 0; index < workload.operations; ++index) {
    const std::size_t slot = static_cast<std::size_t>(index % workload.devices);
    const AirflowDeviceId device = devices[slot];
    const std::uint32_t setpoint = (index % 2) == 0 ? kHighSetpoint : kLowSetpoint;
    const LogicalTick request_at = LogicalTick::from(2 + 2 * index);
    const LogicalTick verify_at = LogicalTick::from(request_at.value() + 1);
    if (!engine.advance_tick(request_at).ok()) {
      return Status::failure(StatusCode::internal_error, "the clock did not advance");
    }
    const ControlRequest request{
        .key = IdempotencyKey::parse("bench-op-" + std::to_string(index)).value(),
        .device = device,
        .device_generation = DeviceGeneration::from(1),
        .epoch = AuthorityEpoch::from(1),
        .expected_revision = std::nullopt,
        .intent = ControlIntent::raise_airflow,
        .setpoint = SetpointPercent{SetpointBasisPoints::create(setpoint).value()},
        .actor = kActor,
        .requested_at = request_at,
        .safety_permit = std::nullopt,
        .supersede = std::nullopt,
        .relationship = std::nullopt,
    };
    const auto issued = engine.issue(request, adapter);
    if (!issued.ok()) {
      return issued.status();
    }
    if (!engine.advance_tick(verify_at).ok()) {
      return Status::failure(StatusCode::internal_error, "the clock did not advance");
    }
    sequences[slot] += 1;
    const auto verified = engine.verify(VerificationRequest{
        .attempt = issued.value().id,
        .adapter = &adapter,
        .source = kField,
        .fan_point = SpaceRefId::parse("bench-fan-point").value(),
        .pressure_point = SpaceRefId::parse("bench-pressure-point").value(),
        .fan_sequence = EvidenceSequence::from(sequences[slot]),
        .pressure_sequence = EvidenceSequence::from(sequences[slot]),
        .at = verify_at,
        .relationship = std::nullopt,
    });
    if (!verified.ok()) {
      return verified.status();
    }
    if (verified.value().state != EffectState::effective) {
      return Status::failure(StatusCode::internal_error,
                             "a bench operation did not reach a verified effect");
    }
    completed += 1;
  }
  const auto finished = std::chrono::steady_clock::now();

  const std::string canonical = engine.canonical_state();
  // The configuration printed with the results is the configuration the run was
  // actually measured under, not merely the one that was requested.
  const std::string retention = "retention idempotency-window=" +
                                std::to_string(kIdempotencyWindow) +
                                " attempt-journal=" + std::to_string(kAttemptJournalCapacity) +
                                " audit-capacity=" + std::to_string(kAuditCapacity);
  const bool retention_matches = canonical.find(retention) != std::string::npos;
  result.operations = completed;
  result.seconds = std::chrono::duration<double>(finished - started).count();
  result.publications = engine.store_audit().publications - before_publications;
  result.attempts = engine.attempts().size();
  result.canonical_bytes = canonical.size();

  // The state is verified before anything is reported: a throughput figure for a
  // run that did not produce the command history it claims measures nothing.
  const std::uint64_t expected_attempts =
      std::min<std::uint64_t>(workload.operations, kAttemptJournalCapacity);
  result.verified = !canonical.empty() && result.attempts == expected_attempts &&
                    completed == workload.operations && retention_matches;

  const Status closed = engine.close();
  if (!closed.ok()) {
    return closed;
  }
  std::remove(store.c_str());
  std::remove((store + ".lock").c_str());
  return result;
}

}  // namespace

int main(int argc, char** argv) {
  std::cout.imbue(std::locale::classic());
  const Result<Workload> workload = parse_workload(argc, argv);
  if (!workload.ok()) {
    std::cout << "workload error: " << workload.status().to_string() << "\n";
    return 2;
  }
  std::cout << "workload operations=" << workload.value().operations
            << " devices=" << workload.value().devices
            << " repetitions=" << workload.value().repetitions << "\n";
  std::cout << "configuration audit-capacity=" << kAuditCapacity
            << " idempotency-window=" << kIdempotencyWindow
            << " attempt-journal=" << kAttemptJournalCapacity
            << " slot-capacity-bytes=" << kSlotCapacityBytes << "\n";
  std::cout << "label store=REAL adapter=SYNTHETIC (no hardware is driven)\n";

  std::vector<double> throughputs;
  bool verified = true;
  for (std::uint64_t repetition = 0; repetition < workload.value().repetitions; ++repetition) {
    const Result<RunResult> run = run_once(workload.value(), repetition);
    if (!run.ok()) {
      std::cout << "run failed: " << run.status().to_string() << "\n";
      return 1;
    }
    const double throughput = run.value().seconds > 0.0
                                  ? static_cast<double>(run.value().operations) /
                                        run.value().seconds
                                  : 0.0;
    throughputs.push_back(throughput);
    verified = verified && run.value().verified;
    std::cout << std::setprecision(6);
    std::cout << "run=" << repetition << " completed=" << run.value().operations
              << " seconds=" << run.value().seconds
              << " operations-per-second=" << throughput
              << " publications=" << run.value().publications
              << " state-verified=" << (run.value().verified ? "true" : "false") << "\n";
  }
  if (!verified) {
    std::cout << "state verification failed: the reported runs did not produce the command "
                 "history they claim\n";
    return 1;
  }
  std::sort(throughputs.begin(), throughputs.end());
  double total = 0.0;
  for (const double value : throughputs) {
    total += value;
  }
  const double mean =
      throughputs.empty() ? 0.0 : total / static_cast<double>(throughputs.size());
  const double median = throughputs.empty() ? 0.0 : throughputs[throughputs.size() / 2];
  std::cout << std::setprecision(6);
  std::cout << "summary statistic=median-and-mean median-operations-per-second=" << median
            << " mean-operations-per-second=" << mean << "\n";
  return 0;
}
