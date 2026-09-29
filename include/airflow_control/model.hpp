#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "airflow_control/ids.hpp"
#include "airflow_control/status.hpp"
#include "airflow_control/units.hpp"

namespace airflow_control {

/// Structural bounds on the durable model.
///
/// These are resource bounds, not policy. They exist so that an adversarial or
/// corrupt store is refused before anything is allocated from a declared size.
struct ModelBounds {
  static constexpr std::size_t max_devices = 1024;
  static constexpr std::size_t max_rooms = 256;
  static constexpr std::size_t max_rows = 1024;
  static constexpr std::size_t max_racks = 4096;
  static constexpr std::size_t max_pressure_relationships = 512;
  static constexpr std::size_t max_containment_elements = 2048;
  static constexpr std::size_t max_obligations = 2048;
  static constexpr std::size_t max_interlocks = 1024;
  static constexpr std::size_t max_grants = 1024;
  static constexpr std::size_t max_overrides = 512;
  static constexpr std::size_t max_safety_permits = 512;
  static constexpr std::size_t max_observations = 8192;
  static constexpr std::size_t max_attempts = 4096;
  static constexpr std::size_t max_audit_capacity = 4096;
  static constexpr std::size_t max_idempotency_window = 4096;
  static constexpr std::size_t max_devices_per_obligation = 256;
  static constexpr std::size_t max_bound_devices = 1024;
  static constexpr std::size_t max_sources_per_relationship = 16;
  static constexpr std::size_t max_recent_effects = 2048;
};

// ---------------------------------------------------------------------------
// Pressure relationships
// ---------------------------------------------------------------------------

/// The direction of a controlled space relative to its reference space.
///
/// Modeled explicitly rather than as free-form text: a pressure relationship is
/// a typed safety property, and a target or an observation that does not have
/// the declared direction is a violation, not a spelling difference.
enum class PressurePolarity : std::uint32_t {
  positive = 0,  ///< controlled space above the reference space
  negative = 1,  ///< controlled space below the reference space
  neutral = 2,   ///< controlled space nominally equal to the reference space
};

[[nodiscard]] const char* to_string(PressurePolarity polarity) noexcept;
[[nodiscard]] std::optional<PressurePolarity> parse_pressure_polarity(std::string_view token) noexcept;

/// The polarity a measured differential actually exhibits.
[[nodiscard]] PressurePolarity classify(Pressure differential) noexcept;

/// The closed differential-pressure band a relationship must be held inside.
///
/// A band carries the polarity it is valid for, because a band that does not
/// agree with its declared polarity would silently accept the wrong physical
/// condition.
class PressureBand {
 public:
  PressureBand() = delete;

  /// Validates and constructs a band.
  ///
  /// Refused with pressure_band_invalid when the interval is inverted, when the
  /// tolerance is negative or oversized, or when the interval contradicts the
  /// declared polarity: a positive relationship must lie strictly above zero, a
  /// negative one strictly below, and a neutral one must straddle zero within
  /// the structural neutral bound.
  [[nodiscard]] static Result<PressureBand> create(PressurePolarity polarity, Pressure lower,
                                                   Pressure upper, Pressure tolerance) noexcept;

  [[nodiscard]] PressurePolarity polarity() const noexcept { return polarity_; }
  [[nodiscard]] Pressure lower() const noexcept { return lower_; }
  [[nodiscard]] Pressure upper() const noexcept { return upper_; }
  [[nodiscard]] Pressure tolerance() const noexcept { return tolerance_; }

  /// True when the differential lies inside the closed band.
  [[nodiscard]] bool contains(Pressure differential) const noexcept;
  /// True when the differential exhibits the declared polarity.
  [[nodiscard]] bool sign_matches(Pressure differential) const noexcept;
  /// True when both containment and polarity hold.
  [[nodiscard]] bool satisfied_by(Pressure differential) const noexcept;

  friend bool operator==(const PressureBand& lhs, const PressureBand& rhs) noexcept = default;

  [[nodiscard]] std::string to_string() const;

 private:
  PressureBand(PressurePolarity polarity, Pressure lower, Pressure upper, Pressure tolerance) noexcept
      : polarity_(polarity), lower_(lower), upper_(upper), tolerance_(tolerance) {}

  PressurePolarity polarity_;
  Pressure lower_;
  Pressure upper_;
  Pressure tolerance_;
};

/// A declared pressure relationship between a controlled space and a reference
/// space.
///
/// The spaces themselves are opaque references owned by the facility topology
/// layer; this runtime never resolves, walks, or re-derives topology. It owns
/// the relationship's polarity, its band, and the evidence about it.
struct PressureRelationship {
  PressureRelationshipId id;
  RoomId room;
  SpaceRefId controlled_space;
  SpaceRefId reference_space;
  PressureBand band;
  EvidenceGeneration evidence_generation;
  StateRevision revision;
};

// ---------------------------------------------------------------------------
// Containment
// ---------------------------------------------------------------------------

/// Containment state relevant to airflow control.
enum class ContainmentState : std::uint32_t {
  intact = 0,
  open_for_service = 1,
  breached = 2,
  unknown = 3,
};

/// Safety precedence of a containment state, lowest risk first.
///
/// This ordering is a policy decision and is deliberately not the numeric order
/// of the enumeration: breached outranks unknown, which outranks a deliberate
/// service opening, which outranks intact. A caller must not derive precedence
/// from the enum value.
[[nodiscard]] std::uint32_t containment_rank(ContainmentState state) noexcept;

/// True when the state is known at all.
[[nodiscard]] bool is_known(ContainmentState state) noexcept;

[[nodiscard]] const char* to_string(ContainmentState state) noexcept;
[[nodiscard]] std::optional<ContainmentState> parse_containment_state(std::string_view token) noexcept;

/// The kind of containment element. This is a closed set; the vendor name of a
/// panel is not a containment kind.
enum class ContainmentKind : std::uint32_t {
  aisle_containment = 0,
  rack_chimney = 1,
  blanking_panel = 2,
  subfloor_barrier = 3,
  door = 4,
  other = 5,
};

[[nodiscard]] const char* to_string(ContainmentKind kind) noexcept;
[[nodiscard]] std::optional<ContainmentKind> parse_containment_kind(std::string_view token) noexcept;

/// A containment element in a room, optionally scoped to one row.
struct ContainmentElement {
  ContainmentId id;
  RoomId room;
  std::optional<RowId> row;
  ContainmentKind kind;
  StateRevision revision;
};

// ---------------------------------------------------------------------------
// Airflow obligations
// ---------------------------------------------------------------------------

enum class ObligationScope : std::uint32_t {
  room = 0,
  row = 1,
  rack = 2,
};

/// A protected obligation blocks a request that would violate it. An advisory
/// obligation is an optimization input and never blocks a protective request.
enum class ObligationClass : std::uint32_t {
  protected_obligation = 0,
  advisory = 1,
};

/// How an obligation is evaluated against evidence.
enum class ObligationBinding : std::uint32_t {
  /// A single metered point owned by the facility instrumentation layer.
  metered_scope = 0,
  /// The sum of the fresh airflow readings of the bound devices.
  device_sum = 1,
};

[[nodiscard]] const char* to_string(ObligationScope scope) noexcept;
[[nodiscard]] const char* to_string(ObligationClass klass) noexcept;
[[nodiscard]] const char* to_string(ObligationBinding binding) noexcept;
[[nodiscard]] std::optional<ObligationScope> parse_obligation_scope(std::string_view token) noexcept;
[[nodiscard]] std::optional<ObligationClass> parse_obligation_class(std::string_view token) noexcept;
[[nodiscard]] std::optional<ObligationBinding> parse_obligation_binding(std::string_view token) noexcept;

/// A minimum airflow the owning authority requires at one scope.
///
/// This runtime does not derive the minimum: it consumes the minimum, together
/// with the provenance of the authority that owns it, and enforces it.
struct AirflowObligation {
  ObligationId id;
  ObligationScope scope = ObligationScope::room;
  RoomId room;
  std::optional<RowId> row;
  std::optional<RackId> rack;
  ObligationClass klass = ObligationClass::protected_obligation;
  ObligationBinding binding = ObligationBinding::device_sum;
  std::optional<SpaceRefId> metered_point;
  std::vector<AirflowDeviceId> devices;
  Airflow minimum_airflow;
  Airflow target_airflow;
  SourceId source;
  EvidenceGeneration evidence_generation;
  StateRevision revision;
};

// ---------------------------------------------------------------------------
// Fan policy and safe operating envelope
// ---------------------------------------------------------------------------

/// The safe operating envelope of one airflow device.
///
/// Every field is consumed from the authority that owns it, with provenance.
/// Nothing here is derived from facility topology, cooling capacity, or a
/// thermal model; those belong to other systems.
struct OperatingEnvelope {
  SetpointBasisPoints min_fan_percent;
  SetpointBasisPoints max_fan_percent;
  SetpointBasisPoints default_fan_percent;
  Airflow min_airflow;
  Airflow max_airflow;
  SlewBasisPoints max_step;
  SourceId source;
  EvidenceGeneration evidence_generation;
};

/// Validates an envelope. Refused with limit_invalid when the setpoint or
/// airflow interval is inverted, when the default lies outside it, or when a
/// value is not structurally representable.
[[nodiscard]] Status validate_envelope(const OperatingEnvelope& envelope);

/// A fan/setpoint policy: the envelope plus the policy identity and generation
/// it was published under.
struct FanPolicy {
  PolicyId id;
  PolicyGeneration generation;
  OperatingEnvelope envelope;
};

// ---------------------------------------------------------------------------
// Control intents
// ---------------------------------------------------------------------------

/// What a caller is asking the airflow to do.
enum class ControlIntent : std::uint32_t {
  hold_setpoint = 0,
  raise_airflow = 1,
  lower_airflow = 2,
  restore_pressure_relationship = 3,
  rebalance_rows = 4,
  trim_for_efficiency = 5,
  emergency_purge = 6,
  release_to_policy = 7,
};

/// The class a request belongs to. The class decides which safety conditions
/// apply, and it is a pure function of the intent: a caller cannot promote an
/// optimization into a protective request by asserting a class.
enum class RequestClass : std::uint32_t {
  /// Reduces delivered airflow or redistributes it. Blocked by any containment
  /// state other than intact and by any unknown pressure evidence.
  optimization = 0,
  /// Maintains, increases, or restores airflow. Permitted while containment is
  /// known, including a known breach, but never while containment is unknown.
  protective = 1,
  /// Emergency ventilation directed by the authority that owns emergency
  /// policy. Requires an explicit, in-epoch, unexpired safety permit.
  safety_directed = 2,
};

[[nodiscard]] const char* to_string(ControlIntent intent) noexcept;
[[nodiscard]] const char* to_string(RequestClass klass) noexcept;
[[nodiscard]] std::optional<ControlIntent> parse_control_intent(std::string_view token) noexcept;

/// The class of an intent. Pure and total.
[[nodiscard]] RequestClass classify(ControlIntent intent) noexcept;

/// True when the intent may only be attempted against proven pressure evidence.
[[nodiscard]] bool requires_pressure_proof(ControlIntent intent) noexcept;
/// True when the intent may only be attempted against proven containment state.
[[nodiscard]] bool requires_containment_proof(ControlIntent intent) noexcept;
/// True when the intent requires an explicit safety permit.
[[nodiscard]] bool requires_safety_permit(ControlIntent intent) noexcept;
/// True when the intent changes the commanded setpoint at all.
[[nodiscard]] bool changes_setpoint(ControlIntent intent) noexcept;

// ---------------------------------------------------------------------------
// Setpoint requests
// ---------------------------------------------------------------------------

struct SetpointPercent {
  SetpointBasisPoints percent;
};

struct SetpointAirflow {
  Airflow airflow;
};

/// A requested setpoint, as exactly one of the two supported kinds.
///
/// A std::variant rather than a struct with two fields and a tag: there is no
/// representable state in which a caller asked for both, or for neither, and no
/// accessor can silently return a zero for the kind that was not requested.
using SetpointRequest = std::variant<SetpointPercent, SetpointAirflow>;

enum class SetpointKind : std::uint32_t {
  fan_percent = 0,
  airflow = 1,
};

[[nodiscard]] SetpointKind setpoint_kind(const SetpointRequest& request) noexcept;
[[nodiscard]] const char* to_string(SetpointKind kind) noexcept;
[[nodiscard]] std::string to_string(const SetpointRequest& request);
[[nodiscard]] std::optional<SetpointKind> parse_setpoint_kind(std::string_view token) noexcept;

/// The setpoint a policy supplies for an intent that does not name one.
[[nodiscard]] SetpointRequest policy_default_setpoint(const FanPolicy& policy);

}  // namespace airflow_control
