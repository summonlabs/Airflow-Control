// The probe is a real program started as an independent operating-system
// process by the crash, fencing, and multiprocess tests. It either performs one
// named scenario and reports what it did on stdout, or terminates itself at a
// chosen durable stage so that a test can observe what a process death leaves
// behind.
//
// It links the library and nothing else, so it repeats the scenario that
// airflow_test::apply_scenario builds rather than calling it. The two have to
// agree: a store written by one is read by the other.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "airflow_control/engine.hpp"
#include "airflow_control/synthetic_adapter.hpp"
#include "detail/crash_point.hpp"
#include "detail/path.hpp"
#include "detail/process.hpp"
#include "detail/store_file.hpp"

using namespace airflow_control;

namespace {

namespace afc_detail = ::airflow_control::detail;
using afc_detail::CrashPoint;

/// Engine bounds. These must agree with airflow_test::base_options() in the test
/// support library: a probe process and a test process describe the same engine,
/// and durable state written by one is read back under the same retention by the
/// other.
EngineOptions probe_options() {
  EngineOptions options;
  options.evidence.max_age_ticks = LogicalTick::from(1000);
  options.evidence.max_relationship_sources = 4;
  options.idempotency_window = 16;
  options.audit_capacity = 64;
  options.attempt_journal_capacity = 64;
  return options;
}

/// Every value the probe's scenario is built from.
///
/// It is an aggregate because make_ids() builds it in one expression, and no
/// member is default-constructed: a probe that ran with a zero where a policy
/// value belongs would exercise a scenario nobody declared.
struct ProbeIds {
  AirflowDeviceId device;
  DeviceGeneration device_generation;
  RoomId room;
  RowId row;
  PressureRelationshipId relationship;
  ContainmentId containment;
  InterlockId interlock;
  GrantId grant;
  PolicyId policy;
  SourceId source;
  SourceId issuer;
  ActorId actor;
  SpaceRefId controlled_space;
  SpaceRefId reference_space;

  AuthorityEpoch epoch;
  EvidenceGeneration evidence_generation;
  PolicyGeneration policy_generation;

  SetpointBasisPoints min_fan_percent;
  SetpointBasisPoints max_fan_percent;
  SetpointBasisPoints default_fan_percent;
  Airflow min_airflow;
  Airflow max_airflow;
  SlewBasisPoints max_step;

  Pressure band_lower;
  Pressure band_upper;
  Pressure band_tolerance;
  Pressure plant_differential;
  Airflow airflow_per_basis_point;
};

/// Composes one identifier from a fixed prefix.
template <typename Id>
Result<Id> compose(const char* prefix, const std::string& suffix) {
  return Id::parse(std::string(prefix) + suffix);
}

Result<ProbeIds> make_ids() {
  Result<AirflowDeviceId> device = compose<AirflowDeviceId>("dev-", "1");
  if (!device.ok()) {
    return device.status();
  }
  Result<RoomId> room = compose<RoomId>("room-", "1");
  if (!room.ok()) {
    return room.status();
  }
  Result<RowId> row = compose<RowId>("row-", "1");
  if (!row.ok()) {
    return row.status();
  }
  Result<PressureRelationshipId> relationship = compose<PressureRelationshipId>("rel-", "1");
  if (!relationship.ok()) {
    return relationship.status();
  }
  Result<ContainmentId> containment = compose<ContainmentId>("cont-", "1");
  if (!containment.ok()) {
    return containment.status();
  }
  Result<InterlockId> interlock = compose<InterlockId>("il-", "1");
  if (!interlock.ok()) {
    return interlock.status();
  }
  Result<GrantId> grant = compose<GrantId>("grant-", "1");
  if (!grant.ok()) {
    return grant.status();
  }
  Result<PolicyId> policy = compose<PolicyId>("policy-", "1");
  if (!policy.ok()) {
    return policy.status();
  }
  Result<SourceId> source = compose<SourceId>("source-", "1");
  if (!source.ok()) {
    return source.status();
  }
  Result<SourceId> issuer = compose<SourceId>("issuer-", "1");
  if (!issuer.ok()) {
    return issuer.status();
  }
  Result<ActorId> actor = compose<ActorId>("actor-", "1");
  if (!actor.ok()) {
    return actor.status();
  }
  Result<SpaceRefId> controlled = compose<SpaceRefId>("space-controlled-", "1");
  if (!controlled.ok()) {
    return controlled.status();
  }
  Result<SpaceRefId> reference = compose<SpaceRefId>("space-reference-", "1");
  if (!reference.ok()) {
    return reference.status();
  }
  Result<SetpointBasisPoints> min_fan = SetpointBasisPoints::create(2000);
  if (!min_fan.ok()) {
    return min_fan.status();
  }
  Result<SetpointBasisPoints> max_fan = SetpointBasisPoints::create(9000);
  if (!max_fan.ok()) {
    return max_fan.status();
  }
  Result<SetpointBasisPoints> default_fan = SetpointBasisPoints::create(5000);
  if (!default_fan.ok()) {
    return default_fan.status();
  }
  Result<SlewBasisPoints> max_step = SlewBasisPoints::create(1000);
  if (!max_step.ok()) {
    return max_step.status();
  }
  return ProbeIds{
      .device = std::move(device).value(),
      .device_generation = DeviceGeneration::from(1),
      .room = std::move(room).value(),
      .row = std::move(row).value(),
      .relationship = std::move(relationship).value(),
      .containment = std::move(containment).value(),
      .interlock = std::move(interlock).value(),
      .grant = std::move(grant).value(),
      .policy = std::move(policy).value(),
      .source = std::move(source).value(),
      .issuer = std::move(issuer).value(),
      .actor = std::move(actor).value(),
      .controlled_space = std::move(controlled).value(),
      .reference_space = std::move(reference).value(),
      .epoch = AuthorityEpoch::from(1),
      .evidence_generation = EvidenceGeneration::from(1),
      .policy_generation = PolicyGeneration::from(1),
      .min_fan_percent = min_fan.value(),
      .max_fan_percent = max_fan.value(),
      .default_fan_percent = default_fan.value(),
      .min_airflow = Airflow::from_cubic_metres_per_hour(2000),
      .max_airflow = Airflow::from_cubic_metres_per_hour(9000),
      .max_step = max_step.value(),
      .band_lower = Pressure::from_millipascals(-30'000),
      .band_upper = Pressure::from_millipascals(-10'000),
      .band_tolerance = Pressure::from_millipascals(2'000),
      .plant_differential = Pressure::from_millipascals(-20'000),
      .airflow_per_basis_point = Airflow::from_cubic_metres_per_hour(1),
  };
}

int fail(const Status& status) {
  std::cout << "error=" << status.to_string() << "\n";
  std::cout.flush();
  return 1;
}

int usage() {
  std::cout << "usage: airflow_probe <scenario> <store> [extra...]\n"
               "  register <store>\n"
               "  crash-after-slot-write <store> <tick>\n"
               "  crash-after-slot-flush <store> <tick>\n"
               "  crash-after-head-write <store> <tick>\n"
               "  crash-in-adapter <store> <tick>\n"
               "  hold-lock <store> <milliseconds>\n"
               "  forge-head <store> <generation>\n"
               "  inspect <store>\n";
  std::cout.flush();
  return 2;
}

/// Parses a non-negative decimal argument exactly. A scenario argument that was
/// silently replaced by a default would make the probe test something the caller
/// did not ask for, so malformed input is refused.
Result<std::uint64_t> parse_number(const std::string& text, const char* what) {
  if (text.empty()) {
    return Status::failure(StatusCode::invalid_argument,
                           std::string("no ") + what + " was supplied");
  }
  std::uint64_t value = 0;
  for (const char character : text) {
    if (character < '0' || character > '9') {
      return Status::failure(StatusCode::invalid_argument,
                             std::string(what) + " must be a decimal number, not \"" + text + "\"");
    }
    const std::uint64_t digit = static_cast<std::uint64_t>(character - '0');
    if (value > (std::numeric_limits<std::uint64_t>::max() - digit) / 10) {
      return Status::failure(StatusCode::overflow,
                             std::string(what) + " does not fit in 64 bits");
    }
    value = value * 10 + digit;
  }
  return value;
}

/// Adopts the probe's epoch. An epoch already in force is not a failure: the
/// probe is run against stores that an earlier probe process seeded.
Status ensure_epoch(AirflowControlEngine& engine, const ProbeIds& ids, LogicalTick at) {
  const Status adopted = engine.adopt_epoch(ids.epoch, ids.actor, at);
  if (!adopted.ok() && adopted.code() != StatusCode::epoch_stale) {
    return adopted;
  }
  return Status::success();
}

/// Registers the device. Tolerates the state a previous probe process
/// established, so one store can be seeded once and used by many scenarios.
Status ensure_device(AirflowControlEngine& engine, const ProbeIds& ids, LogicalTick at) {
  const Status status = engine.register_device(RegisterDeviceRequest{
      .device = ids.device,
      .generation = ids.device_generation,
      .room = ids.room,
      .row = ids.row,
      .actor = ids.actor,
      .requested_at = at,
  });
  if (!status.ok() && status.code() != StatusCode::duplicate_identity) {
    return status;
  }
  return Status::success();
}

/// Brings the device to active and installs its fan policy.
///
/// This runs after the grant: commissioning a device is itself an
/// authority-bearing act, so the permission to perform it must already exist.
Status ensure_activation(AirflowControlEngine& engine, const ProbeIds& ids, LogicalTick at) {
  Result<DeviceView> view = engine.device(ids.device);
  if (!view.ok()) {
    return view.status();
  }
  if (view.value().lifecycle != DeviceLifecycle::active) {
    Status status = engine.set_device_lifecycle(SetLifecycleRequest{
        .device = ids.device,
        .generation = ids.device_generation,
        .expected_revision = view.value().revision,
        .target = DeviceLifecycle::active,
        .epoch = ids.epoch,
        .actor = ids.actor,
        .requested_at = at,
    });
    if (!status.ok()) {
      return status;
    }
    view = engine.device(ids.device);
    if (!view.ok()) {
      return view.status();
    }
  }
  if (view.value().policy_id.has_value()) {
    return Status::success();
  }
  return engine.set_fan_policy(SetFanPolicyRequest{
      .device = ids.device,
      .generation = ids.device_generation,
      .expected_revision = view.value().revision,
      .policy =
          FanPolicy{
              .id = ids.policy,
              .generation = ids.policy_generation,
              .envelope =
                  OperatingEnvelope{
                      .min_fan_percent = ids.min_fan_percent,
                      .max_fan_percent = ids.max_fan_percent,
                      .default_fan_percent = ids.default_fan_percent,
                      .min_airflow = ids.min_airflow,
                      .max_airflow = ids.max_airflow,
                      .max_step = ids.max_step,
                      .source = ids.source,
                      .evidence_generation = ids.evidence_generation,
                  },
          },
      .actor = ids.actor,
      .requested_at = at,
  });
}

Status ensure_relationship(AirflowControlEngine& engine, const ProbeIds& ids, LogicalTick at) {
  const Status status = engine.define_pressure_relationship(DefineRelationshipRequest{
      .id = ids.relationship,
      .room = ids.room,
      .controlled_space = ids.controlled_space,
      .reference_space = ids.reference_space,
      .polarity = PressurePolarity::negative,
      .lower = ids.band_lower,
      .upper = ids.band_upper,
      .tolerance = ids.band_tolerance,
      .evidence_generation = ids.evidence_generation,
      .expected_revision = std::nullopt,
      .actor = ids.actor,
      .requested_at = at,
  });
  if (!status.ok() && status.code() != StatusCode::duplicate_identity) {
    return status;
  }
  return Status::success();
}

Status ensure_containment(AirflowControlEngine& engine, const ProbeIds& ids, LogicalTick at) {
  Status status = engine.define_containment_element(DefineContainmentRequest{
      .id = ids.containment,
      .room = ids.room,
      .row = ids.row,
      .kind = ContainmentKind::aisle_containment,
      .expected_revision = std::nullopt,
      .actor = ids.actor,
      .requested_at = at,
  });
  if (!status.ok() && status.code() != StatusCode::duplicate_identity) {
    return status;
  }
  for (const ContainmentView& view : engine.containment()) {
    if (view.id == ids.containment && view.has_report && view.state == ContainmentState::intact) {
      return Status::success();
    }
  }
  return engine.report_containment(ReportContainmentRequest{
      .element = ids.containment,
      .state = ContainmentState::intact,
      .quality = Quality::good,
      .source = ids.source,
      .sequence = EvidenceSequence::from(1),
      .evidence_generation = ids.evidence_generation,
      .measured_at = at,
  });
}

Status ensure_interlock(AirflowControlEngine& engine, const ProbeIds& ids, LogicalTick at) {
  Status status = engine.declare_interlock(DeclareInterlockRequest{
      .id = ids.interlock,
      .room = ids.room,
      .row = ids.row,
      .device = ids.device,
      .klass = InterlockClass::protected_obligation,
      .epoch = ids.epoch,
      .actor = ids.actor,
      .declared_at = at,
  });
  if (!status.ok() && status.code() != StatusCode::duplicate_identity) {
    return status;
  }
  for (const InterlockView& view : engine.interlocks()) {
    if (view.id == ids.interlock && view.has_report && view.state == InterlockState::satisfied) {
      return Status::success();
    }
  }
  return engine.report_interlock(ReportInterlockRequest{
      .id = ids.interlock,
      .state = InterlockState::satisfied,
      .sequence = EvidenceSequence::from(1),
      .epoch = ids.epoch,
      .reported_at = at,
  });
}

Status ensure_grant(AirflowControlEngine& engine, const ProbeIds& ids, LogicalTick at) {
  for (const PermissionGrant& grant : engine.grants()) {
    if (grant.id == ids.grant) {
      return Status::success();
    }
  }
  return engine.add_grant(
      PermissionGrant{
          .id = ids.grant,
          .issuer = ids.issuer,
          .epoch = ids.epoch,
          .device = ids.device,
          .device_generation = ids.device_generation,
          .room = ids.room,
          .actions = ActionSet::all(),
          .issued_at = at,
          .expires_at = std::nullopt,
          .revoked = false,
      },
      ids.actor, at);
}

/// Records fresh proof of the device and of the pressure relationship.
///
/// A reading restored from durable state is never fresh, so a reopened store
/// holds evidence it cannot act on: the probe records new readings at the tick it
/// is running at. Their sequences are derived from that tick, so a later run of
/// the probe is never older than an earlier one for the same source.
Status ensure_observations(AirflowControlEngine& engine, const ProbeIds& ids, LogicalTick at) {
  const std::uint64_t base = at.value() * 4;
  const Airflow airflow = Airflow::from_cubic_metres_per_hour(
      static_cast<std::int64_t>(ids.default_fan_percent.basis_points()) *
      ids.airflow_per_basis_point.cubic_metres_per_hour());

  const Result<Observation> fan = engine.observe(
      ObservationDraft{
          .payload = FanReading{.percent = ids.default_fan_percent},
          .device = ids.device,
          .relationship = std::nullopt,
          .point = ids.controlled_space,
          .source = ids.source,
          .sequence = EvidenceSequence::from(base + 1),
          .measured_at = at,
          .device_generation = ids.device_generation,
          .evidence_generation = ids.evidence_generation,
          .quality = Quality::good,
      },
      at);
  if (!fan.ok()) {
    return fan.status();
  }
  const Result<Observation> measured = engine.observe(
      ObservationDraft{
          .payload = AirflowReading{.value = airflow},
          .device = ids.device,
          .relationship = std::nullopt,
          .point = ids.controlled_space,
          .source = ids.source,
          .sequence = EvidenceSequence::from(base + 2),
          .measured_at = at,
          .device_generation = ids.device_generation,
          .evidence_generation = ids.evidence_generation,
          .quality = Quality::good,
      },
      at);
  if (!measured.ok()) {
    return measured.status();
  }
  const Result<Observation> differential = engine.observe(
      ObservationDraft{
          .payload = PressureReading{.differential = ids.plant_differential},
          .device = ids.device,
          .relationship = ids.relationship,
          .point = ids.controlled_space,
          .source = ids.source,
          .sequence = EvidenceSequence::from(base + 3),
          .measured_at = at,
          .device_generation = ids.device_generation,
          .evidence_generation = ids.evidence_generation,
          .quality = Quality::good,
      },
      at);
  if (!differential.ok()) {
    return differential.status();
  }
  return Status::success();
}

/// The whole scenario: epoch, device, policy, relationship, containment,
/// interlock, grant, and fresh proof of the plant.
Status ensure_scenario(AirflowControlEngine& engine, const ProbeIds& ids, LogicalTick at) {
  Status status = ensure_epoch(engine, ids, at);
  if (!status.ok()) {
    return status;
  }
  status = ensure_device(engine, ids, at);
  if (!status.ok()) {
    return status;
  }
  status = ensure_grant(engine, ids, at);
  if (!status.ok()) {
    return status;
  }
  status = ensure_activation(engine, ids, at);
  if (!status.ok()) {
    return status;
  }
  status = ensure_relationship(engine, ids, at);
  if (!status.ok()) {
    return status;
  }
  status = ensure_containment(engine, ids, at);
  if (!status.ok()) {
    return status;
  }
  status = ensure_interlock(engine, ids, at);
  if (!status.ok()) {
    return status;
  }
  return ensure_observations(engine, ids, at);
}

/// The airflow the plant reports at a setpoint, on the scenario's declared
/// one-cubic-metre-per-basis-point curve. Integer arithmetic only.
Airflow plant_airflow(const ProbeIds& ids, SetpointBasisPoints percent) {
  return Airflow::from_cubic_metres_per_hour(
      static_cast<std::int64_t>(percent.basis_points()) *
      ids.airflow_per_basis_point.cubic_metres_per_hour());
}

/// Puts the synthetic plant where the scenario's policy says it is: at the policy
/// default, delivering the airflow that setpoint produces, and holding a
/// differential inside the relationship's band.
void seed_plant(SyntheticAirflowAdapter& adapter, const ProbeIds& ids) {
  adapter.set_airflow_per_basis_point(ids.airflow_per_basis_point.cubic_metres_per_hour());
  adapter.seed_device(ids.device, ids.device_generation,
                      SyntheticPlantState{
                          .fan_percent = ids.default_fan_percent,
                          .airflow = plant_airflow(ids, ids.default_fan_percent),
                          .differential = ids.plant_differential,
                      });
  adapter.set_read_quality(Quality::good);
}

/// A synthetic plant that dies at the command-attempt boundary.
///
/// The engine writes and durably publishes the attempt record before it hands the
/// command to an adapter, so ending the process inside execute() is a death
/// strictly after that boundary: a command may have reached the device, and no
/// outcome was recorded. Nothing is printed first, because a process that dies
/// here has to leave stdout exactly as it found it.
class DyingAdapter final : public AirflowAdapter {
 public:
  explicit DyingAdapter(AdapterDescriptor descriptor) : synthetic_(std::move(descriptor)) {}

  [[nodiscard]] AdapterDescriptor describe() const override { return synthetic_.describe(); }

  AdapterOutcome execute(const AdapterCommand& command) override {
    (void)command;
    // The crash exit code the store's own crash points use, so a test tells a
    // death at the command boundary and a death at a durable stage apart by
    // nothing but the scenario it asked for.
    std::_Exit(afc_detail::kCrashExitCode);
  }

  [[nodiscard]] Result<ObservationDraft> read(const AdapterReadRequest& request) override {
    return synthetic_.read(request);
  }

  [[nodiscard]] SyntheticAirflowAdapter& plant() noexcept { return synthetic_; }

 private:
  SyntheticAirflowAdapter synthetic_;
};

AdapterDescriptor probe_descriptor() {
  AdapterDescriptor descriptor;
  descriptor.vendor = "Airflow Control";
  descriptor.model = "synthetic-probe-plant";
  descriptor.protocol = "in-process";
  descriptor.capabilities = static_cast<std::uint32_t>(AdapterCapability::set_fan_percent) |
                            static_cast<std::uint32_t>(AdapterCapability::set_airflow) |
                            static_cast<std::uint32_t>(AdapterCapability::read_pressure) |
                            static_cast<std::uint32_t>(AdapterCapability::read_airflow) |
                            static_cast<std::uint32_t>(AdapterCapability::read_fan_percent);
  descriptor.synthetic = true;
  return descriptor;
}

/// The newest committed head of a store, and the slot it was read from.
struct CommittedHead {
  unsigned slot = 0;
  afc_detail::SlotHead head;
};

/// The newest head a reader would adopt: the valid one with the largest
/// generation. A slot that was never written reads as invalid rather than as an
/// error, so this is also how an empty store is detected.
Result<CommittedHead> newest_head(const afc_detail::StoreFile& file) {
  CommittedHead best;
  bool found = false;
  for (unsigned slot = 0; slot < afc_detail::kSlotCount; ++slot) {
    Result<afc_detail::SlotHead> head = file.read_head(slot);
    if (!head.ok()) {
      return head.status();
    }
    if (!head.value().valid) {
      continue;
    }
    if (!found || head.value().generation > best.head.generation) {
      best.slot = slot;
      best.head = head.value();
      found = true;
    }
  }
  if (!found) {
    return Status::failure(StatusCode::store_missing,
                           "the store holds no committed generation to forge from");
  }
  return best;
}

/// Writes a head record for a generation the engine never saw.
///
/// The store file is opened directly rather than through the engine: the engine
/// would adopt the head on open and refuse to publish a generation it never
/// committed, and creating that condition from the outside is the whole point of
/// this scenario. The payload is the one the committed head already describes, so
/// the forged generation names real state rather than inventing any; only the
/// generation and the writer are new, and the committed head is left untouched in
/// its own slot.
int forge_head(const std::string& store, std::uint64_t generation) {
  Result<std::string> canonical = afc_detail::canonical_store_path(store);
  if (!canonical.ok()) {
    return fail(canonical.status());
  }
  Result<afc_detail::StoreFile> file = afc_detail::StoreFile::open(canonical.value());
  if (!file.ok()) {
    return fail(file.status());
  }
  Result<CommittedHead> committed = newest_head(file.value());
  if (!committed.ok()) {
    return fail(committed.status());
  }
  if (generation <= committed.value().head.generation) {
    return fail(Status::failure(
        StatusCode::invalid_argument,
        "the forged generation must be ahead of the committed generation " +
            std::to_string(committed.value().head.generation) +
            ", because a reader adopts the newest valid head and a lower one "
            "would change nothing"));
  }
  Result<std::vector<std::uint8_t>> payload = file.value().read_payload(committed.value().head);
  if (!payload.ok()) {
    return fail(payload.status());
  }
  const unsigned target = (committed.value().slot + 1U) % afc_detail::kSlotCount;
  // An incarnation this store has never seen: a head that named the writer that
  // is already committed would describe the publication it is replacing.
  const std::uint64_t incarnation = committed.value().head.writer_incarnation + 1000;
  Status status = file.value().write_slot(target, generation, payload.value(), incarnation);
  if (!status.ok()) {
    return fail(status);
  }
  status = file.value().flush();
  if (!status.ok()) {
    return fail(status);
  }
  std::cout << "forged-generation=" << generation << "\n";
  std::cout << "committed-generation=" << committed.value().head.generation << "\n";
  std::cout << "slot=" << target << "\n";
  std::cout << "payload-bytes=" << payload.value().size() << "\n";
  std::cout.flush();
  return 0;
}

/// Opens the store, arms one durable stage, advances the logical clock once, and
/// dies at that stage. The clock is durable state, so the advance is exactly one
/// durable mutation.
int crash_at(CrashPoint point, const std::string& store, std::uint64_t tick) {
  Result<AirflowControlEngine> opened =
      AirflowControlEngine::open(store, OpenMode::open_or_create, probe_options());
  if (!opened.ok()) {
    return fail(opened.status());
  }
  AirflowControlEngine& engine = opened.value();
  const LogicalTick target = LogicalTick::from(tick);
  if (engine.current_tick() >= target) {
    return fail(Status::failure(StatusCode::invalid_argument,
                                "the tick to advance to must be ahead of the current tick " +
                                    std::to_string(engine.current_tick().value()) +
                                    ", or nothing is published and no crash point is reached"));
  }
  afc_detail::arm_crash_point(point);
  const Status advanced = engine.advance_tick(target);
  if (!advanced.ok()) {
    afc_detail::arm_crash_point(CrashPoint::none);
    return fail(advanced);
  }
  // Reaching this line means the process survived a stage it was armed to die
  // at. That is a failure the caller has to see, never a scenario that quietly
  // did something else.
  afc_detail::arm_crash_point(CrashPoint::none);
  std::cout << "unexpected-return\n";
  std::cout.flush();
  return 3;
}

/// Completes the whole scenario, then issues one control command through an
/// adapter that ends the process in execute().
int crash_in_adapter(const std::string& store, std::uint64_t tick) {
  Result<ProbeIds> ids = make_ids();
  if (!ids.ok()) {
    return fail(ids.status());
  }
  Result<AirflowControlEngine> opened =
      AirflowControlEngine::open(store, OpenMode::open_or_create, probe_options());
  if (!opened.ok()) {
    return fail(opened.status());
  }
  AirflowControlEngine& engine = opened.value();
  Status status = ensure_scenario(engine, ids.value(), LogicalTick::from(1));
  if (!status.ok()) {
    return fail(status);
  }
  const LogicalTick target = LogicalTick::from(tick);
  if (engine.current_tick() < target) {
    status = engine.advance_tick(target);
    if (!status.ok()) {
      return fail(status);
    }
  }
  // Nothing durable is armed: this process dies because the adapter it drove
  // ended it, not because a durable stage fired.
  afc_detail::arm_crash_point(CrashPoint::none);

  Result<IdempotencyKey> key = IdempotencyKey::parse("probe-command-" + std::to_string(tick));
  if (!key.ok()) {
    return fail(key.status());
  }
  DyingAdapter adapter(probe_descriptor());
  seed_plant(adapter.plant(), ids.value());

  const ControlRequest request{
      .key = key.value(),
      .device = ids.value().device,
      .device_generation = ids.value().device_generation,
      .epoch = ids.value().epoch,
      .expected_revision = std::nullopt,
      .intent = ControlIntent::hold_setpoint,
      .setpoint = SetpointRequest{SetpointPercent{.percent = ids.value().default_fan_percent}},
      .actor = ids.value().actor,
      .requested_at = engine.current_tick(),
      .safety_permit = std::nullopt,
      .supersede = std::nullopt,
  };
  const Result<AttemptRecord> issued = engine.issue(request, adapter);
  if (!issued.ok()) {
    // The command was refused before it reached the adapter, so the process is
    // still alive and the caller has to be told why.
    return fail(issued.status());
  }
  std::cout << "unexpected-return\n";
  std::cout.flush();
  return 3;
}

/// Registers the probe's device and exits. The store is created when it does not
/// exist, so a test can seed one store and run every other scenario against it.
int register_device(const std::string& store) {
  Result<ProbeIds> ids = make_ids();
  if (!ids.ok()) {
    return fail(ids.status());
  }
  Result<AirflowControlEngine> opened =
      AirflowControlEngine::open(store, OpenMode::open_or_create, probe_options());
  if (!opened.ok()) {
    return fail(opened.status());
  }
  AirflowControlEngine& engine = opened.value();
  const LogicalTick at = LogicalTick::from(1);
  Status status = ensure_epoch(engine, ids.value(), at);
  if (!status.ok()) {
    return fail(status);
  }
  if (engine.current_tick() < at) {
    status = engine.advance_tick(at);
    if (!status.ok()) {
      return fail(status);
    }
  }
  status = engine.register_device(RegisterDeviceRequest{
      .device = ids.value().device,
      .generation = ids.value().device_generation,
      .room = ids.value().room,
      .row = ids.value().row,
      .actor = ids.value().actor,
      .requested_at = at,
  });
  if (!status.ok() && status.code() != StatusCode::duplicate_identity) {
    return fail(status);
  }
  const Result<DeviceView> view = engine.device(ids.value().device);
  if (!view.ok()) {
    return fail(view.status());
  }
  std::cout << "device=" << ids.value().device.str() << "\n";
  std::cout << "generation=" << view.value().generation.value() << "\n";
  std::cout << "revision=" << view.value().revision.value() << "\n";
  std::cout << "tick=" << engine.current_tick().value() << "\n";
  std::cout.flush();
  const Status closed = engine.close();
  if (!closed.ok()) {
    return fail(closed);
  }
  return 0;
}

/// Opens the store and holds its write authority for the requested interval, so a
/// test can prove that a second writer is refused and that a killed holder leaves
/// no residue.
int hold_lock(const std::string& store, std::uint64_t milliseconds) {
  if (milliseconds > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
    return fail(Status::failure(StatusCode::invalid_argument,
                                "the hold interval does not fit a signed millisecond count"));
  }
  Result<AirflowControlEngine> opened =
      AirflowControlEngine::open(store, OpenMode::open_or_create, probe_options());
  if (!opened.ok()) {
    return fail(opened.status());
  }
  std::cout << "holding=" << store << "\n";
  std::cout.flush();
  std::this_thread::sleep_for(
      std::chrono::milliseconds(static_cast<std::int64_t>(milliseconds)));
  const Status closed = opened.value().close();
  if (!closed.ok()) {
    return fail(closed);
  }
  return 0;
}

/// Opens the store and prints the authoritative model as deterministic text, so a
/// test can compare what two processes see.
int inspect(const std::string& store) {
  Result<AirflowControlEngine> opened =
      AirflowControlEngine::open(store, OpenMode::open_existing, probe_options());
  if (!opened.ok()) {
    return fail(opened.status());
  }
  AirflowControlEngine& engine = opened.value();
  const std::string canonical = engine.canonical_state();
  std::cout << canonical;
  if (canonical.empty() || canonical.back() != '\n') {
    std::cout << "\n";
  }
  std::cout << "state-digest=" << engine.state_digest() << "\n";
  std::cout.flush();
  const Status closed = engine.close();
  if (!closed.ok()) {
    return fail(closed);
  }
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 3) {
    return usage();
  }
  const std::string scenario = argv[1];
  const std::string store = argv[2];

  if (scenario == "register") {
    return register_device(store);
  }
  if (scenario == "inspect") {
    return inspect(store);
  }
  if (scenario == "hold-lock") {
    if (argc < 4) {
      return usage();
    }
    Result<std::uint64_t> milliseconds = parse_number(argv[3], "hold interval");
    if (!milliseconds.ok()) {
      return fail(milliseconds.status());
    }
    return hold_lock(store, milliseconds.value());
  }
  if (scenario == "forge-head") {
    if (argc < 4) {
      return usage();
    }
    Result<std::uint64_t> generation = parse_number(argv[3], "generation");
    if (!generation.ok()) {
      return fail(generation.status());
    }
    return forge_head(store, generation.value());
  }
  if (scenario == "crash-in-adapter") {
    if (argc < 4) {
      return usage();
    }
    Result<std::uint64_t> tick = parse_number(argv[3], "tick");
    if (!tick.ok()) {
      return fail(tick.status());
    }
    return crash_in_adapter(store, tick.value());
  }
  if (scenario == "crash-after-slot-write" || scenario == "crash-after-slot-flush" ||
      scenario == "crash-after-head-write") {
    if (argc < 4) {
      return usage();
    }
    Result<std::uint64_t> tick = parse_number(argv[3], "tick");
    if (!tick.ok()) {
      return fail(tick.status());
    }
    const CrashPoint point = scenario == "crash-after-slot-write" ? CrashPoint::after_slot_write
                             : scenario == "crash-after-slot-flush"
                                 ? CrashPoint::after_slot_flush
                                 : CrashPoint::after_head_write;
    return crash_at(point, store, tick.value());
  }
  return usage();
}
