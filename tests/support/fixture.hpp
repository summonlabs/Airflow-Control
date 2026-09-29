#pragma once

// Deterministic test fixtures: one complete, valid airflow scenario applied
// through the public engine API, and a scratch directory that refuses to touch
// anything outside the directory it created.
//
// Nothing here reaches into the library's internals. A scenario is built the way
// a real caller would build one, so a test that drives this fixture is driving
// the public contract.

#include <cstdint>
#include <string>

#include "airflow_control/engine.hpp"
#include "airflow_control/status.hpp"
#include "airflow_control/synthetic_adapter.hpp"

namespace airflow_test {

// The fixture names library values by their library names, so that a reader of a
// test sees the same type names the library uses.
using namespace ::airflow_control;  // NOLINT(google-build-using-namespace)

/// How a fixture engine is opened.
struct ScenarioOptions {
  /// Path of the durable store. Empty means the store path is unused.
  std::string store_path;
  /// When true and a store path is set, the engine is durable. Otherwise the
  /// engine holds no durable state and nothing survives close().
  bool durable = false;
  /// Engine bounds. base_options() supplies the values the suite expects.
  EngineOptions options;
};

/// The complete description of one scenario.
///
/// Every field is a real value: none of these types has a default constructor,
/// so a scenario is never half-initialised and a test never reads a zero that no
/// part of the scenario declared. It is an aggregate because make_scenario()
/// builds it in one expression, and the field names are the documentation.
struct Scenario {
  AirflowDeviceId device;                  ///< "dev-<suffix>"
  DeviceGeneration device_generation;      ///< 1
  RoomId room;                             ///< "room-<suffix>"
  RowId row;                               ///< "row-<suffix>"
  PressureRelationshipId relationship;     ///< "rel-<suffix>"
  ContainmentId containment;               ///< "cont-<suffix>"
  InterlockId interlock;                   ///< "il-<suffix>"
  GrantId grant;                           ///< "grant-<suffix>"
  PolicyId policy;                         ///< "policy-<suffix>"
  SourceId source;                         ///< "source-<suffix>"
  SourceId issuer;                         ///< "issuer-<suffix>"
  ActorId actor;                           ///< "actor-<suffix>"
  SpaceRefId controlled_space;             ///< "space-controlled-<suffix>"
  SpaceRefId reference_space;              ///< "space-reference-<suffix>"

  AuthorityEpoch epoch;                    ///< 1
  EvidenceGeneration evidence_generation;  ///< 1
  PolicyGeneration policy_generation;      ///< 1
  LogicalTick start_tick;                  ///< the tick the epoch is adopted at
  LogicalTick ready_tick;                  ///< start_tick + 1, when the model is built

  // The fan policy's safe operating envelope: 20% .. 90%, default 50%, and the
  // airflow the default delivers on the synthetic plant's curve.
  SetpointBasisPoints min_fan_percent;
  SetpointBasisPoints max_fan_percent;
  SetpointBasisPoints default_fan_percent;
  Airflow min_airflow;
  Airflow max_airflow;
  SlewBasisPoints max_step;

  // A negative-pressure relationship: the controlled space is held below its
  // reference space, so the band and its polarity are both below zero.
  Pressure band_lower;
  Pressure band_upper;
  Pressure band_tolerance;
  /// The differential the synthetic plant holds: inside the band.
  Pressure plant_differential;
  /// The synthetic plant's airflow per basis point, stated rather than assumed.
  Airflow airflow_per_basis_point;
};

/// Builds the scenario. Fails only when the identifiers this fixture composes
/// from its own literals are not valid identifiers, which is a defect in the
/// fixture rather than in a caller; the status names the identifier that failed.
[[nodiscard]] Result<Scenario> make_scenario(const std::string& suffix = "1",
                                             std::uint64_t start_tick = 1);

/// The engine bounds the suite uses. A probe process that has to agree with a
/// fixture on durable state uses the same values, stated once here.
[[nodiscard]] EngineOptions base_options();

/// A durable scenario in a caller-supplied store path.
[[nodiscard]] ScenarioOptions durable_options(const std::string& store_path);

/// Applies the scenario to an engine: adopts the epoch, advances the tick,
/// registers the device, brings it to active, sets its fan policy, defines a
/// negative-pressure relationship, defines and reports a containment element
/// intact, declares and satisfies a protected interlock, and records a grant
/// covering every control action for this device generation in this epoch.
[[nodiscard]] Status apply_scenario(AirflowControlEngine& engine, const Scenario& scenario);

/// The device revision the engine currently holds. Read back rather than
/// assumed, so no test and no fixture hard-codes a revision the engine chose.
[[nodiscard]] Result<StateRevision> device_revision(const AirflowControlEngine& engine,
                                                    const AirflowDeviceId& device);

/// A descriptor for the synthetic plant the fixture describes. synthetic is
/// true, because this adapter drives no hardware and every surface that reports
/// its results must say so.
[[nodiscard]] AdapterDescriptor synthetic_descriptor();

/// Seeds a synthetic adapter with the plant the scenario's policy describes: the
/// device sitting at the policy default, delivering the airflow that setpoint
/// produces on the adapter's airflow-per-basis-point curve, and holding a
/// differential inside the relationship's declared band.
void seed_synthetic_adapter(SyntheticAirflowAdapter& adapter, const Scenario& scenario);

/// An engine with the scenario already applied, and the ids it was built from.
class Fixture {
 public:
  Fixture() = delete;
  Fixture(const Fixture&) = delete;
  Fixture& operator=(const Fixture&) = delete;
  Fixture(Fixture&& other) noexcept;
  Fixture& operator=(Fixture&& other) noexcept;
  /// Closes the engine so a test that forgets to close cannot leak the store
  /// lock; a test that needs the status calls close() itself.
  ~Fixture();

  /// Opens the engine and applies make_scenario()'s default scenario.
  [[nodiscard]] static Result<Fixture> open(const ScenarioOptions& options);
  /// Opens the engine and applies the supplied scenario.
  [[nodiscard]] static Result<Fixture> open(const ScenarioOptions& options,
                                            const Scenario& scenario);

  [[nodiscard]] AirflowControlEngine& engine() noexcept { return engine_; }
  [[nodiscard]] const AirflowControlEngine& engine() const noexcept { return engine_; }
  [[nodiscard]] const Scenario& scenario() const noexcept { return scenario_; }

  Status close();

 private:
  Fixture(AirflowControlEngine engine, Scenario scenario);

  AirflowControlEngine engine_;
  Scenario scenario_;
};

/// A scratch directory under the test binary directory.
///
/// The name carries the process id, so two test binaries running at once never
/// share one, and the destructor removes only the directory this object created.
class TempDir {
 public:
  explicit TempDir(const std::string& name);
  ~TempDir();
  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  [[nodiscard]] const std::string& path() const noexcept { return path_; }
  /// A path of one file inside this directory. Refuses separator characters and
  /// traversal, so a test cannot escape its own scratch directory.
  [[nodiscard]] Status file(const std::string& leaf, std::string& out) const;
  [[nodiscard]] std::string store_path() const;

 private:
  std::string path_;
};

}  // namespace airflow_test
