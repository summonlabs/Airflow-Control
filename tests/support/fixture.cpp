#include "fixture.hpp"

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <utility>

#include "detail/path.hpp"
#include "detail/process.hpp"

namespace airflow_test {
namespace {

/// Composes one fixture identifier from a fixed prefix and the scenario suffix.
template <typename Id>
Result<Id> compose(const char* prefix, const std::string& suffix) {
  return Id::parse(std::string(prefix) + suffix);
}

/// The airflow the synthetic plant reports at a setpoint, on the scenario's
/// declared one-cubic-metre-per-basis-point curve.
///
/// Integer arithmetic throughout: the fixture never introduces a floating-point
/// value into a quantity a decision is taken against.
Airflow plant_airflow(const Scenario& scenario, SetpointBasisPoints percent) {
  return Airflow::from_cubic_metres_per_hour(
      static_cast<std::int64_t>(percent.basis_points()) *
      scenario.airflow_per_basis_point.cubic_metres_per_hour());
}

OperatingEnvelope operating_envelope(const Scenario& scenario) {
  return OperatingEnvelope{
      .min_fan_percent = scenario.min_fan_percent,
      .max_fan_percent = scenario.max_fan_percent,
      .default_fan_percent = scenario.default_fan_percent,
      .min_airflow = scenario.min_airflow,
      .max_airflow = scenario.max_airflow,
      .max_step = scenario.max_step,
      .source = scenario.source,
      .evidence_generation = scenario.evidence_generation,
  };
}

}  // namespace

Result<Scenario> make_scenario(const std::string& suffix, std::uint64_t start_tick) {
  Result<AirflowDeviceId> device = compose<AirflowDeviceId>("dev-", suffix);
  if (!device.ok()) {
    return device.status();
  }
  Result<RoomId> room = compose<RoomId>("room-", suffix);
  if (!room.ok()) {
    return room.status();
  }
  Result<RowId> row = compose<RowId>("row-", suffix);
  if (!row.ok()) {
    return row.status();
  }
  Result<PressureRelationshipId> relationship = compose<PressureRelationshipId>("rel-", suffix);
  if (!relationship.ok()) {
    return relationship.status();
  }
  Result<ContainmentId> containment = compose<ContainmentId>("cont-", suffix);
  if (!containment.ok()) {
    return containment.status();
  }
  Result<InterlockId> interlock = compose<InterlockId>("il-", suffix);
  if (!interlock.ok()) {
    return interlock.status();
  }
  Result<GrantId> grant = compose<GrantId>("grant-", suffix);
  if (!grant.ok()) {
    return grant.status();
  }
  Result<PolicyId> policy = compose<PolicyId>("policy-", suffix);
  if (!policy.ok()) {
    return policy.status();
  }
  Result<SourceId> source = compose<SourceId>("source-", suffix);
  if (!source.ok()) {
    return source.status();
  }
  Result<SourceId> issuer = compose<SourceId>("issuer-", suffix);
  if (!issuer.ok()) {
    return issuer.status();
  }
  Result<ActorId> actor = compose<ActorId>("actor-", suffix);
  if (!actor.ok()) {
    return actor.status();
  }
  Result<SpaceRefId> controlled = compose<SpaceRefId>("space-controlled-", suffix);
  if (!controlled.ok()) {
    return controlled.status();
  }
  Result<SpaceRefId> reference = compose<SpaceRefId>("space-reference-", suffix);
  if (!reference.ok()) {
    return reference.status();
  }

  // The setpoint and slew bounds are validated by the library, so the fixture
  // asks for them through the same door a caller does instead of asserting that
  // its own literals are in range.
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
  Result<LogicalTick> ready_tick = tick_add(LogicalTick::from(start_tick), 1);
  if (!ready_tick.ok()) {
    return ready_tick.status();
  }

  return Scenario{
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
      .start_tick = LogicalTick::from(start_tick),
      .ready_tick = ready_tick.value(),
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

EngineOptions base_options() {
  EngineOptions options;
  options.evidence.max_age_ticks = LogicalTick::from(1000);
  options.evidence.max_relationship_sources = 4;
  options.idempotency_window = 16;
  options.audit_capacity = 64;
  // At least the idempotency window, so a retained key always names an attempt
  // the journal still holds.
  options.attempt_journal_capacity = 64;
  return options;
}

ScenarioOptions durable_options(const std::string& store_path) {
  ScenarioOptions options;
  options.store_path = store_path;
  options.durable = true;
  options.options = base_options();
  return options;
}

Result<StateRevision> device_revision(const AirflowControlEngine& engine,
                                      const AirflowDeviceId& device) {
  const Result<DeviceView> view = engine.device(device);
  if (!view.ok()) {
    return view.status();
  }
  return view.value().revision;
}

Status apply_scenario(AirflowControlEngine& engine, const Scenario& scenario) {
  Status status = engine.adopt_epoch(scenario.epoch, scenario.actor, scenario.start_tick);
  if (!status.ok()) {
    return status;
  }
  // The clock is durable state, so advancing it is what makes everything below
  // committed rather than merely in memory.
  status = engine.advance_tick(scenario.ready_tick);
  if (!status.ok()) {
    return status;
  }

  status = engine.register_device(RegisterDeviceRequest{
      .device = scenario.device,
      .generation = scenario.device_generation,
      .room = scenario.room,
      .row = scenario.row,
      .actor = scenario.actor,
      .requested_at = scenario.ready_tick,
  });
  if (!status.ok()) {
    return status;
  }

  Result<StateRevision> revision = device_revision(engine, scenario.device);
  if (!revision.ok()) {
    return revision.status();
  }

  // The grant comes first: commissioning a device is itself an authority-bearing
  // act, so the permission to perform it must already exist.
  // Every control action, scoped to this device generation in this epoch. The
  // grant does not expire: a test that wants an expired grant records its own.
  status = engine.add_grant(
      PermissionGrant{
          .id = scenario.grant,
          .issuer = scenario.issuer,
          .epoch = scenario.epoch,
          .device = scenario.device,
          .device_generation = scenario.device_generation,
          .room = scenario.room,
          .actions = ActionSet::all(),
          .issued_at = scenario.ready_tick,
          .expires_at = std::nullopt,
          .revoked = false,
      },
      scenario.actor, scenario.ready_tick);
  if (!status.ok()) {
    return status;
  }

  // A device is provisioned when it is registered, and a fan policy may only be
  // set on a device that may be controlled, so the lifecycle comes next.
  status = engine.set_device_lifecycle(SetLifecycleRequest{
      .device = scenario.device,
      .generation = scenario.device_generation,
      .expected_revision = revision.value(),
      .target = DeviceLifecycle::active,
      .epoch = scenario.epoch,
      .actor = scenario.actor,
      .requested_at = scenario.ready_tick,
  });
  if (!status.ok()) {
    return status;
  }

  revision = device_revision(engine, scenario.device);
  if (!revision.ok()) {
    return revision.status();
  }

  status = engine.set_fan_policy(SetFanPolicyRequest{
      .device = scenario.device,
      .generation = scenario.device_generation,
      .expected_revision = revision.value(),
      .policy =
          FanPolicy{
              .id = scenario.policy,
              .generation = scenario.policy_generation,
              .envelope = operating_envelope(scenario),
          },
      .actor = scenario.actor,
      .requested_at = scenario.ready_tick,
  });
  if (!status.ok()) {
    return status;
  }

  status = engine.define_pressure_relationship(DefineRelationshipRequest{
      .id = scenario.relationship,
      .room = scenario.room,
      .controlled_space = scenario.controlled_space,
      .reference_space = scenario.reference_space,
      .polarity = PressurePolarity::negative,
      .lower = scenario.band_lower,
      .upper = scenario.band_upper,
      .tolerance = scenario.band_tolerance,
      .evidence_generation = scenario.evidence_generation,
      .expected_revision = std::nullopt,
      .actor = scenario.actor,
      .requested_at = scenario.ready_tick,
  });
  if (!status.ok()) {
    return status;
  }

  status = engine.define_containment_element(DefineContainmentRequest{
      .id = scenario.containment,
      .room = scenario.room,
      .row = scenario.row,
      .kind = ContainmentKind::aisle_containment,
      .expected_revision = std::nullopt,
      .actor = scenario.actor,
      .requested_at = scenario.ready_tick,
  });
  if (!status.ok()) {
    return status;
  }

  status = engine.report_containment(ReportContainmentRequest{
      .element = scenario.containment,
      .state = ContainmentState::intact,
      .quality = Quality::good,
      .source = scenario.source,
      .sequence = EvidenceSequence::from(1),
      .evidence_generation = scenario.evidence_generation,
      .measured_at = scenario.ready_tick,
  });
  if (!status.ok()) {
    return status;
  }

  status = engine.declare_interlock(DeclareInterlockRequest{
      .id = scenario.interlock,
      .room = scenario.room,
      .row = scenario.row,
      .device = scenario.device,
      .klass = InterlockClass::protected_obligation,
      .epoch = scenario.epoch,
      .actor = scenario.actor,
      .declared_at = scenario.ready_tick,
  });
  if (!status.ok()) {
    return status;
  }

  status = engine.report_interlock(ReportInterlockRequest{
      .id = scenario.interlock,
      .state = InterlockState::satisfied,
      .sequence = EvidenceSequence::from(1),
      .epoch = scenario.epoch,
      .reported_at = scenario.ready_tick,
  });
  if (!status.ok()) {
    return status;
  }

  return Status::success();
}

AdapterDescriptor synthetic_descriptor() {
  AdapterDescriptor descriptor;
  descriptor.vendor = "Airflow Control";
  descriptor.model = "synthetic-test-plant";
  descriptor.protocol = "in-process";
  descriptor.capabilities = static_cast<std::uint32_t>(AdapterCapability::set_fan_percent) |
                            static_cast<std::uint32_t>(AdapterCapability::set_airflow) |
                            static_cast<std::uint32_t>(AdapterCapability::read_pressure) |
                            static_cast<std::uint32_t>(AdapterCapability::read_airflow) |
                            static_cast<std::uint32_t>(AdapterCapability::read_fan_percent);
  descriptor.synthetic = true;
  return descriptor;
}

void seed_synthetic_adapter(SyntheticAirflowAdapter& adapter, const Scenario& scenario) {
  adapter.set_airflow_per_basis_point(scenario.airflow_per_basis_point.cubic_metres_per_hour());
  adapter.seed_device(scenario.device, scenario.device_generation,
                      SyntheticPlantState{
                          .fan_percent = scenario.default_fan_percent,
                          .airflow = plant_airflow(scenario, scenario.default_fan_percent),
                          .differential = scenario.plant_differential,
                      });
  adapter.set_read_quality(Quality::good);
}

Fixture::Fixture(AirflowControlEngine engine, Scenario scenario)
    : engine_(std::move(engine)), scenario_(std::move(scenario)) {}

Fixture::Fixture(Fixture&& other) noexcept
    : engine_(std::move(other.engine_)), scenario_(std::move(other.scenario_)) {}

Fixture& Fixture::operator=(Fixture&& other) noexcept {
  if (this != &other) {
    if (engine_.is_open()) {
      (void)engine_.close();
    }
    engine_ = std::move(other.engine_);
    scenario_ = std::move(other.scenario_);
  }
  return *this;
}

Fixture::~Fixture() {
  // Closing here is defence against a test that forgets: a durable store keeps
  // its operating-system lock until it is closed, and a leaked lock makes the
  // next test fail for a reason that has nothing to do with what it tests. The
  // status is deliberately dropped, because a test that needs it calls close().
  if (engine_.is_open()) {
    (void)engine_.close();
  }
}

Result<Fixture> Fixture::open(const ScenarioOptions& options) {
  Result<Scenario> scenario = make_scenario();
  if (!scenario.ok()) {
    return scenario.status();
  }
  return open(options, scenario.value());
}

Result<Fixture> Fixture::open(const ScenarioOptions& options, const Scenario& scenario) {
  Result<AirflowControlEngine> engine =
      options.durable && !options.store_path.empty()
          ? AirflowControlEngine::open(options.store_path, OpenMode::open_or_create,
                                       options.options)
          : AirflowControlEngine::open_in_memory(options.options);
  if (!engine.ok()) {
    return engine.status();
  }
  const Status applied = apply_scenario(engine.value(), scenario);
  if (!applied.ok()) {
    // The store is released before the failure is reported: a fixture that could
    // not be built must not leave a locked store behind for the next case.
    (void)engine.value().close();
    return applied;
  }
  return Fixture(std::move(engine).value(), scenario);
}

Status Fixture::close() { return engine_.close(); }

TempDir::TempDir(const std::string& name) {
  path_ = "scratch/" + name + "-" + std::to_string(::airflow_control::detail::current_process_id());
  (void)::airflow_control::detail::ensure_parent_directory(path_ + "/placeholder");
}

TempDir::~TempDir() {
  // The scratch tree is removed through the operating system, bounded to the
  // directory this object created.
  std::string command;
#ifdef _WIN32
  command = "cmd /c rmdir /s /q \"" + path_ + "\" >nul 2>&1";
#else
  command = "rm -rf \"" + path_ + "\"";
#endif
  (void)std::system(command.c_str());
}

Status TempDir::file(const std::string& leaf, std::string& out) const {
  if (leaf.empty()) {
    return Status::failure(StatusCode::invalid_argument, "a scratch file needs a name");
  }
  if (leaf.find('/') != std::string::npos || leaf.find('\\') != std::string::npos ||
      leaf == "." || leaf == "..") {
    return Status::failure(StatusCode::path_invalid,
                           "a scratch file name must not contain a path separator or be a "
                           "traversal component");
  }
  out = path_ + "/" + leaf;
  return Status::success();
}

std::string TempDir::store_path() const { return path_ + "/site.airflowstore"; }

}  // namespace airflow_test
