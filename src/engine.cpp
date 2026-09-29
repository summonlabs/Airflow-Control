#include "airflow_control/engine.hpp"

#include <algorithm>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "detail/codec.hpp"
#include "detail/records.hpp"
#include "detail/serialization.hpp"
#include "detail/store_file.hpp"

namespace airflow_control {
namespace {

// The durable records live in detail; the engine names them without the
// qualifier because every one of them is an internal representation of a public
// concept.
using detail::ContainmentRecord;
using detail::DeviceRecord;
using detail::ObligationRecord;
using detail::RelationshipRecord;

/// Reduces untrusted diagnostic text to something the durable format accepts.
///
/// Valid text is kept exactly as it is. Text that is invalid, over-long, or
/// carries control characters is reduced to printable ASCII, because a
/// diagnostic an adapter supplied must never be able to make the store
/// undecodable.
[[nodiscard]] std::string sanitize_text(const std::string& text, std::size_t max_length) {
  Result<std::string> valid = validate_text(text, max_length);
  if (valid.ok()) {
    return std::move(valid).value();
  }
  std::string reduced;
  for (const char value : text) {
    if (reduced.size() >= max_length) {
      break;
    }
    const auto byte = static_cast<unsigned char>(value);
    if (byte >= 0x20u && byte <= 0x7Eu) {
      reduced.push_back(value);
    }
  }
  if (reduced.empty()) {
    reduced = "(text rejected)";
  }
  return reduced;
}

[[nodiscard]] std::int64_t absolute_difference(std::int64_t lhs, std::int64_t rhs) noexcept {
  const std::int64_t difference = lhs >= rhs ? lhs - rhs : rhs - lhs;
  return difference;
}

[[nodiscard]] Result<std::int64_t> checked_absolute_difference(std::int64_t lhs,
                                                               std::int64_t rhs) noexcept {
  if (lhs >= rhs) {
    if (rhs < 0 && lhs > INT64_MAX + rhs) {
      return Status::failure(StatusCode::overflow, "difference overflow");
    }
    return lhs - rhs;
  }
  if (lhs < 0 && rhs > INT64_MAX + lhs) {
    return Status::failure(StatusCode::overflow, "difference overflow");
  }
  return rhs - lhs;
}

}  // namespace

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

struct AirflowControlEngine::Impl {
  EngineOptions options;
  bool opened = false;
  bool durable = false;
  DurableStore store;
  detail::ModelState model = detail::empty_model();
  IncarnationId incarnation = IncarnationId::from(0);

  // -- bookkeeping ---------------------------------------------------------

  void audit(AuditKind kind, const std::string& subject, const std::string& detail) {
    const AuditSequence sequence = model.next_audit;
    Result<AuditSequence> advanced = model.next_audit.next();
    if (advanced.ok()) {
      model.next_audit = advanced.value();
    }
    model.audit.push_back(AuditEntry{sequence, model.tick, kind, sanitize_text(subject, kMaxTextLength),
                                     sanitize_text(detail, kMaxTextLength)});
    while (model.audit.size() > model.audit_capacity) {
      model.audit.erase(model.audit.begin());
      ++model.audit_dropped;
    }
  }

  [[nodiscard]] Status publish() {
    if (!durable) {
      return Status::success();
    }
    std::vector<std::uint8_t> payload;
    Status encoded = detail::encode_model(model, payload);
    if (!encoded.ok()) {
      return encoded;
    }
    return store.publish(payload, incarnation);
  }

  [[nodiscard]] DeviceRecord* device_mut(const AirflowDeviceId& id) {
    return detail::find_device(model, id);
  }
  [[nodiscard]] const DeviceRecord* device(const AirflowDeviceId& id) const {
    return detail::find_device(model, id);
  }

  /// Records a refusal on the decision that has been accumulating checks.
  ///
  /// The trace is what makes a refusal explainable, so a refusal must never
  /// discard the checks that already ran. When a specific check has not already
  /// recorded itself, the refusal records the decision point under the name the
  /// caller supplied.
  [[nodiscard]] static Result<Decision> refuse(Decision& decision, const ControlRequest& request,
                                               const char* check, StatusCode code,
                                               const std::string& message) {
    decision.eligible = false;
    decision.code = code;
    decision.message = message;
    decision.request_class = classify(request.intent);
    // Record the refusal unless the check that refused already recorded this
    // exact outcome. That keeps the trace a complete account of what ran: the
    // last entry always carries the code the caller was given.
    if (decision.trace.empty() || decision.trace.back().outcome != code) {
      decision.trace.push_back(CheckTrace{check, code, message});
    }
    return decision;
  }

  // -- scope resolution ----------------------------------------------------

  /// The most dangerous containment state among the elements that govern this
  /// device, and the identity that produced it.
  struct ContainmentSummary {
    ContainmentState worst = ContainmentState::unknown;
    std::optional<ContainmentId> worst_id;
    std::size_t considered = 0;
    bool declared = false;
  };

  [[nodiscard]] ContainmentSummary containment_for(const DeviceRecord& target) const {
    ContainmentSummary summary;
    for (const detail::ContainmentRecord& record : model.containment) {
      if (!(record.element.room == target.room)) {
        continue;
      }
      if (record.element.row.has_value() && target.row.has_value() &&
          !(*record.element.row == *target.row)) {
        continue;
      }
      if (record.element.row.has_value() && !target.row.has_value()) {
        // A row-scoped element does not govern a device with no declared row.
        continue;
      }
      ++summary.considered;
      summary.declared = true;
      if (!summary.worst_id.has_value() ||
          containment_rank(record.state) > containment_rank(summary.worst) ||
          (containment_rank(record.state) == containment_rank(summary.worst) &&
           record.element.id < *summary.worst_id)) {
        summary.worst = record.state;
        summary.worst_id = record.element.id;
      }
    }
    if (!summary.declared) {
      // Absence of a declaration is not evidence that containment is intact.
      summary.worst = ContainmentState::unknown;
    }
    return summary;
  }

  [[nodiscard]] std::vector<const detail::ObligationRecord*> obligations_for(
      const DeviceRecord& target) const {
    std::vector<const detail::ObligationRecord*> scoped;
    for (const detail::ObligationRecord& record : model.obligations) {
      if (!(record.obligation.room == target.room)) {
        continue;
      }
      if (record.obligation.row.has_value() && target.row.has_value() &&
          !(*record.obligation.row == *target.row)) {
        continue;
      }
      if (record.obligation.row.has_value() && !target.row.has_value()) {
        continue;
      }
      scoped.push_back(&record);
    }
    return scoped;
  }

  [[nodiscard]] std::vector<const Interlock*> interlocks_for(const DeviceRecord& target) const {
    std::vector<const Interlock*> scoped;
    for (const Interlock& interlock : model.interlocks) {
      if (!(interlock.room == target.room)) {
        continue;
      }
      if (interlock.row.has_value()) {
        if (!target.row.has_value() || !(*interlock.row == *target.row)) {
          continue;
        }
      }
      if (interlock.device.has_value() && !(*interlock.device == target.id)) {
        continue;
      }
      scoped.push_back(&interlock);
    }
    return scoped;
  }

  [[nodiscard]] std::vector<const detail::RelationshipRecord*> relationships_for(
      const RoomId& room) const {
    std::vector<const detail::RelationshipRecord*> scoped;
    for (const detail::RelationshipRecord& record : model.relationships) {
      if (record.relationship.room == room) {
        scoped.push_back(&record);
      }
    }
    return scoped;
  }

  // -- evidence adjudication ----------------------------------------------

  [[nodiscard]] FreshnessRequirements freshness_for(const DeviceRecord& target,
                                                    LogicalTick now) const {
    return FreshnessRequirements{now, options.evidence.max_age_ticks, target.generation,
                                 policy_evidence_generation(target), false};
  }

  [[nodiscard]] static EvidenceGeneration policy_evidence_generation(const DeviceRecord& target) {
    if (target.policy.has_value()) {
      return target.policy->envelope.evidence_generation;
    }
    return EvidenceGeneration::from(0);
  }

  /// Distance from the closed band, zero when inside.
  [[nodiscard]] static std::int64_t band_distance(const PressureBand& band, Pressure value) noexcept {
    if (band.contains(value)) {
      return 0;
    }
    const std::int64_t lower = band.lower().millipascals();
    const std::int64_t upper = band.upper().millipascals();
    const std::int64_t point = value.millipascals();
    const std::int64_t to_lower = absolute_difference(point, lower);
    const std::int64_t to_upper = absolute_difference(point, upper);
    return std::min(to_lower, to_upper);
  }

  [[nodiscard]] PressureState adjudicate(const detail::RelationshipRecord& record,
                                         const FreshnessRequirements& requirements,
                                         const EvidenceBinding& binding,
                                         std::optional<Pressure>& representative,
                                         std::size_t& contributing,
                                         std::size_t& conflicting) const {
    std::vector<const Observation*> fresh;
    for (const Observation& observation : record.evidence) {
      if (is_proof(assess_freshness(observation, requirements)) && is_bound(observation, binding)) {
        fresh.push_back(&observation);
      }
    }
    contributing = fresh.size();
    conflicting = 0;
    representative.reset();
    if (fresh.empty()) {
      return PressureState::unknown;
    }

    const PressureBand& band = record.relationship.band;
    bool disagrees = false;
    for (std::size_t outer = 0; outer < fresh.size() && !disagrees; ++outer) {
      for (std::size_t inner = outer + 1; inner < fresh.size(); ++inner) {
        const Pressure lhs = std::get<PressureReading>(fresh[outer]->draft.payload).differential;
        const Pressure rhs = std::get<PressureReading>(fresh[inner]->draft.payload).differential;
        if (band.satisfied_by(lhs) != band.satisfied_by(rhs)) {
          disagrees = true;
          break;
        }
        const std::int64_t difference = absolute_difference(lhs.millipascals(), rhs.millipascals());
        if (difference > band.tolerance().millipascals()) {
          disagrees = true;
          break;
        }
      }
    }

    // The reading closest to a band edge decides, in both directions: the best
    // case when the band holds, the least-bad case when it does not.
    const Observation* chosen = fresh.front();
    std::int64_t chosen_distance = band_distance(
        band, std::get<PressureReading>(chosen->draft.payload).differential);
    for (const Observation* candidate : fresh) {
      const std::int64_t distance =
          band_distance(band, std::get<PressureReading>(candidate->draft.payload).differential);
      if (distance < chosen_distance ||
          (distance == chosen_distance && candidate->draft.source < chosen->draft.source)) {
        chosen = candidate;
        chosen_distance = distance;
      }
    }
    representative = std::get<PressureReading>(chosen->draft.payload).differential;

    if (disagrees) {
      conflicting = fresh.size();
      return PressureState::conflicted;
    }
    return band.satisfied_by(*representative) ? PressureState::satisfied : PressureState::violated;
  }

  /// The airflow an obligation currently observes, or nothing when the
  /// evidence needed to evaluate it is not available.
  [[nodiscard]] Result<std::optional<Airflow>> obligation_airflow(
      const detail::ObligationRecord& record, const FreshnessRequirements& requirements) const {
    const AirflowObligation& obligation = record.obligation;
    if (obligation.binding == ObligationBinding::metered_scope) {
      if (!record.metered_observation.has_value()) {
        return std::optional<Airflow>{};
      }
      // A metered reading is stamped with the obligation's own evidence
      // generation, which is published by the authority that owns the
      // obligation, not by the device policy.
      FreshnessRequirements metered = requirements;
      metered.evidence_generation = obligation.evidence_generation;
      const Observation& observation = *record.metered_observation;
      if (!is_proof(assess_freshness(observation, metered))) {
        return std::optional<Airflow>{};
      }
      return std::optional<Airflow>{std::get<AirflowReading>(observation.draft.payload).value};
    }

    Airflow total = Airflow::from_cubic_metres_per_hour(0);
    for (const AirflowDeviceId& bound : obligation.devices) {
      const DeviceRecord* bound_device = device(bound);
      if (bound_device == nullptr || !bound_device->airflow_observation.has_value()) {
        return std::optional<Airflow>{};
      }
      const Observation& observation = *bound_device->airflow_observation;
      FreshnessRequirements bound_requirements = requirements;
      bound_requirements.device_generation = bound_device->generation;
      bound_requirements.evidence_generation = policy_evidence_generation(*bound_device);
      if (!is_proof(assess_freshness(observation, bound_requirements))) {
        // A bound device with no current reading makes the aggregate unknown,
        // never zero.
        return std::optional<Airflow>{};
      }
      Result<Airflow> sum =
          Airflow::add(total, std::get<AirflowReading>(observation.draft.payload).value);
      if (!sum.ok()) {
        return sum.status();
      }
      total = sum.value();
    }
    return std::optional<Airflow>{total};
  }

  [[nodiscard]] ObligationState obligation_state(const detail::ObligationRecord& record,
                                                 const FreshnessRequirements& requirements,
                                                 std::optional<Airflow>& observed) const {
    Result<std::optional<Airflow>> airflow = obligation_airflow(record, requirements);
    if (!airflow.ok()) {
      observed.reset();
      return ObligationState::unknown;
    }
    observed = airflow.value();
    if (!observed.has_value()) {
      return ObligationState::unknown;
    }
    return *observed >= record.obligation.minimum_airflow ? ObligationState::satisfied
                                                          : ObligationState::unsatisfied;
  }

  // -- authority -----------------------------------------------------------

  [[nodiscard]] bool grant_grants(const PermissionGrant& grant, const DeviceRecord& target,
                                  AuthorityEpoch epoch, LogicalTick at, ControlAction action) const {
    if (grant.revoked) {
      return false;
    }
    if (grant.epoch != epoch) {
      return false;
    }
    if (grant.expires_at.has_value() && at > *grant.expires_at) {
      return false;
    }
    if (grant.device.has_value() && !(*grant.device == target.id)) {
      return false;
    }
    if (grant.device_generation.has_value() && *grant.device_generation != target.generation) {
      return false;
    }
    if (grant.room.has_value() && !(*grant.room == target.room)) {
      return false;
    }
    return grant.actions.contains(action);
  }

  [[nodiscard]] bool any_grant_for_device(const DeviceRecord& target) const {
    for (const PermissionGrant& grant : model.grants) {
      if (grant.revoked) {
        continue;
      }
      if (grant.device.has_value() && !(*grant.device == target.id)) {
        continue;
      }
      if (grant.device_generation.has_value() && *grant.device_generation != target.generation) {
        continue;
      }
      if (grant.room.has_value() && !(*grant.room == target.room)) {
        continue;
      }
      return true;
    }
    return false;
  }

  [[nodiscard]] bool has_live_maintenance_override(const DeviceRecord& target, AuthorityEpoch epoch,
                                                   LogicalTick at) const {
    for (const MaintenanceOverride& entry : model.overrides) {
      if (entry.revoked) {
        continue;
      }
      if (!(entry.device == target.id)) {
        continue;
      }
      if (entry.device_generation != target.generation) {
        continue;
      }
      if (entry.epoch != epoch) {
        continue;
      }
      if (at < entry.issued_at || at > entry.expires_at) {
        continue;
      }
      return true;
    }
    return false;
  }

  [[nodiscard]] const SafetyPermit* find_permit(const SafetyPermitId& id) const {
    for (const SafetyPermit& permit : model.permits) {
      if (permit.id == id) {
        return &permit;
      }
    }
    return nullptr;
  }

  // -- evidence ingestion --------------------------------------------------

  [[nodiscard]] Result<Decision> evaluate_impl(const ControlRequest& request);
  [[nodiscard]] Result<SetpointRequest> resolve_setpoint(const DeviceRecord& target,
                                                         const ControlRequest& request) const;
  [[nodiscard]] std::optional<SetpointRequest> baseline_setpoint(const DeviceRecord& target) const;
  [[nodiscard]] Result<AttemptRecord> issue_impl(const ControlRequest& request,
                                                 AirflowAdapter& adapter);
  [[nodiscard]] Result<VerifiedEffect> verify_impl(const VerificationRequest& request);

  /// The outcome of the permission check, without presentation.
  [[nodiscard]] StatusCode permission_code(const DeviceRecord& target, AuthorityEpoch epoch,
                                           LogicalTick at, ControlAction action,
                                           std::string& detail) const;

  Status recover();
  Status adopt_epoch(AuthorityEpoch epoch, const ActorId& actor, LogicalTick at);
  Status advance_tick(LogicalTick tick);
  Status register_device(const RegisterDeviceRequest& request);
  Status set_device_lifecycle(const SetLifecycleRequest& request);
  Status set_fan_policy(const SetFanPolicyRequest& request);
  Status define_pressure_relationship(const DefineRelationshipRequest& request);
  Status define_containment_element(const DefineContainmentRequest& request);
  Status report_containment(const ReportContainmentRequest& request);
  Status declare_obligation(const DeclareObligationRequest& request);
  Status declare_interlock(const DeclareInterlockRequest& request);
  Status report_interlock(const ReportInterlockRequest& request);
  Status add_grant(const PermissionGrant& grant, const ActorId& actor, LogicalTick at);
  Status revoke_grant(const GrantId& grant, AuthorityEpoch epoch, const ActorId& actor, LogicalTick at);
  Status add_maintenance_override(const MaintenanceOverride& entry, const ActorId& actor,
                                  LogicalTick at);
  Status revoke_maintenance_override(const OverrideId& id, const ActorId& actor, LogicalTick at);
  Status add_safety_permit(const SafetyPermit& permit, const ActorId& actor, LogicalTick at);
  Status resolve_attempt(const ResolveAttemptRequest& request);

  [[nodiscard]] std::vector<DeviceView> device_views() const;
  [[nodiscard]] Result<DeviceView> device_view(const AirflowDeviceId& id) const;
  [[nodiscard]] std::vector<RelationshipView> relationship_views() const;
  [[nodiscard]] Result<RelationshipView> relationship_view(const PressureRelationshipId& id) const;
  [[nodiscard]] std::vector<ContainmentView> containment_views() const;
  [[nodiscard]] std::vector<ObligationView> obligation_views() const;
  [[nodiscard]] std::vector<InterlockView> interlock_views() const;
  [[nodiscard]] std::vector<AttemptView> attempt_views() const;
  [[nodiscard]] Result<AttemptRecord> attempt_view(AttemptId id) const;
  [[nodiscard]] std::vector<Observation> observations_for(const AirflowDeviceId& id) const;
  [[nodiscard]] HistoryView history_view(std::size_t limit) const;
  [[nodiscard]] std::string canonical_state() const;
  [[nodiscard]] std::string state_digest() const;

  [[nodiscard]] Result<Observation> ingest(const ObservationDraft& draft, LogicalTick accepted_at,
                                           bool from_adapter) {
    if (accepted_at > model.tick) {
      return Status::failure(StatusCode::evidence_future,
                             "acceptance instant " + accepted_at.to_string() +
                                 " is later than the logical clock " + model.tick.to_string());
    }
    if (draft.measured_at > accepted_at) {
      return Status::failure(StatusCode::evidence_future,
                             "a reading measured at " + draft.measured_at.to_string() +
                                 " cannot be accepted at " + accepted_at.to_string());
    }

    DeviceRecord* target = device_mut(draft.device);
    if (target == nullptr) {
      return Status::failure(StatusCode::not_found,
                             "device " + draft.device.str() + " is not registered");
    }
    if (draft.device_generation != target->generation) {
      return Status::failure(StatusCode::generation_mismatch,
                             "reading names device generation " +
                                 draft.device_generation.to_string() + " but the device is at " +
                                 target->generation.to_string());
    }
    const ObservationKind kind = observation_kind(draft.payload);
    if (kind == ObservationKind::pressure) {
      if (!is_representable(std::get<PressureReading>(draft.payload).differential)) {
        return Status::failure(StatusCode::bounds_exceeded,
                               "pressure reading is outside the representable range");
      }
    } else if (kind == ObservationKind::airflow) {
      if (!is_representable(std::get<AirflowReading>(draft.payload).value)) {
        return Status::failure(StatusCode::bounds_exceeded,
                               "airflow reading is outside the representable range");
      }
    }
    if (kind == ObservationKind::pressure) {
      if (!draft.relationship.has_value()) {
        return Status::failure(StatusCode::invalid_argument,
                               "a pressure reading must name the relationship it measures");
      }
      detail::RelationshipRecord* record =
          detail::find_relationship(model, *draft.relationship);
      if (record == nullptr) {
        return Status::failure(StatusCode::not_found, "pressure relationship " +
                                                          draft.relationship->str() +
                                                          " is not declared");
      }
      if (record->relationship.evidence_generation != draft.evidence_generation) {
        return Status::failure(
            StatusCode::evidence_generation_mismatch,
            "reading carries evidence generation " + draft.evidence_generation.to_string() +
                " but the relationship is at " +
                record->relationship.evidence_generation.to_string());
      }
      for (const Observation& existing : record->evidence) {
        if (!(existing.draft.source == draft.source)) {
          continue;
        }
        if (draft.sequence <= existing.draft.sequence) {
          return Status::failure(StatusCode::evidence_out_of_order,
                                 "reading sequence " + draft.sequence.to_string() +
                                     " is not after the retained sequence " +
                                     existing.draft.sequence.to_string() + " from the same source");
        }
      }
      bool replaced = false;
      for (Observation& existing : record->evidence) {
        if (existing.draft.source == draft.source) {
          existing = Observation{model.next_observation, draft, accepted_at, false};
          replaced = true;
          break;
        }
      }
      if (!replaced) {
        if (record->evidence.size() >= options.evidence.max_relationship_sources) {
          // Evict the source with the oldest retained sequence; the bound is
          // reported through the audit ring rather than hidden.
          auto oldest = std::min_element(
              record->evidence.begin(), record->evidence.end(),
              [](const Observation& lhs, const Observation& rhs) {
                return lhs.draft.sequence < rhs.draft.sequence;
              });
          if (oldest != record->evidence.end()) {
            record->evidence.erase(oldest);
          }
        }
        record->evidence.push_back(Observation{model.next_observation, draft, accepted_at, false});
      }
      std::sort(record->evidence.begin(), record->evidence.end(),
                [](const Observation& lhs, const Observation& rhs) {
                  return lhs.draft.source < rhs.draft.source;
                });
      const ObservationId assigned = model.next_observation;
      Result<ObservationId> advanced = model.next_observation.next();
      if (advanced.ok()) {
        model.next_observation = advanced.value();
      }
      for (const Observation& stored : record->evidence) {
        if (stored.id == assigned) {
          return stored;
        }
      }
      return Status::failure(StatusCode::internal_error, "observation was not retained");
    }

    if (draft.relationship.has_value()) {
      return Status::failure(StatusCode::invalid_argument,
                             "only a pressure reading may name a pressure relationship");
    }

    const Observation observation{model.next_observation, draft, accepted_at, false};

    if (kind == ObservationKind::fan_setpoint) {
      if (target->fan_observation.has_value() &&
          draft.sequence <= target->fan_observation->draft.sequence) {
        return Status::failure(StatusCode::evidence_out_of_order,
                               "fan reading sequence " + draft.sequence.to_string() +
                                   " is not after the retained sequence " +
                                   target->fan_observation->draft.sequence.to_string());
      }
      target->fan_observation = observation;
    } else {
      if (target->airflow_observation.has_value() &&
          draft.sequence <= target->airflow_observation->draft.sequence) {
        return Status::failure(StatusCode::evidence_out_of_order,
                               "airflow reading sequence " + draft.sequence.to_string() +
                                   " is not after the retained sequence " +
                                   target->airflow_observation->draft.sequence.to_string());
      }
      target->airflow_observation = observation;

      for (detail::ObligationRecord& record : model.obligations) {
        if (record.obligation.binding != ObligationBinding::metered_scope) {
          continue;
        }
        if (!record.obligation.metered_point.has_value()) {
          continue;
        }
        if (!(*record.obligation.metered_point == draft.point)) {
          continue;
        }
        if (record.metered_observation.has_value() &&
            draft.sequence <= record.metered_observation->draft.sequence) {
          return Status::failure(StatusCode::evidence_out_of_order,
                                 "metered reading sequence " + draft.sequence.to_string() +
                                     " is not after the retained sequence " +
                                     record.metered_observation->draft.sequence.to_string());
        }
        record.metered_observation = observation;
      }
    }

    Result<ObservationId> advanced = model.next_observation.next();
    if (advanced.ok()) {
      model.next_observation = advanced.value();
    }
    (void)from_adapter;
    return observation;
  }
};

namespace {

/// A fingerprint of everything a control request says except its idempotency
/// key. A retry that repeats the request replays; the same key with a different
/// request is a conflict, and this is what makes that distinction decidable.
[[nodiscard]] std::uint64_t request_fingerprint(const ControlRequest& request) {
  detail::ByteWriter writer;
  writer.text(request.device.str());
  writer.u64(request.device_generation.value());
  writer.u64(request.epoch.value());
  writer.optional(request.expected_revision,
                  [&writer](StateRevision value) { writer.u64(value.value()); });
  writer.u32(static_cast<std::uint32_t>(request.intent));
  writer.optional(request.setpoint, [&writer](const SetpointRequest& value) {
    if (const auto* percent = std::get_if<SetpointPercent>(&value)) {
      writer.u8(0);
      writer.u32(percent->percent.basis_points());
    } else {
      writer.u8(1);
      writer.i64(std::get<SetpointAirflow>(value).airflow.cubic_metres_per_hour());
    }
  });
  writer.text(request.actor.str());
  writer.u64(request.requested_at.value());
  writer.optional(request.safety_permit,
                  [&writer](const SafetyPermitId& value) { writer.text(value.str()); });
  writer.optional(request.supersede,
                  [&writer](AttemptId value) { writer.u64(value.value()); });
  writer.optional(request.relationship,
                  [&writer](const PressureRelationshipId& value) { writer.text(value.str()); });
  return detail::fnv1a64(writer.data().data(), writer.size());
}

/// What one issue operation changed, so that a failed durable publication can
/// be undone exactly. Publication failure is the only reason to roll back, and
/// the rollback is bounded by the journal eviction policy.
struct IssueUndo {
  bool appended = false;
  std::optional<AttemptId> unresolved_before;
  std::optional<AttemptRecord> superseded_before;
  AttemptId next_attempt_before = AttemptId::from(0);
  CommandId next_command_before = CommandId::from(0);
  std::vector<AttemptRecord> evicted_attempts;
  std::vector<detail::IdempotencySlot> evicted_slots;
};

}  // namespace

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

Result<Decision> AirflowControlEngine::Impl::evaluate_impl(const ControlRequest& request) {
  Decision decision;
  decision.request_class = classify(request.intent);

  // 1 - request shape.
  if (request.epoch.is_zero()) {
    return refuse(decision, request, "shape", StatusCode::invalid_argument,
                  "a control request must carry an adopted authority epoch");
  }
  if (request.device_generation.is_zero()) {
    return refuse(decision, request, "shape", StatusCode::invalid_argument,
                  "a control request must carry a device generation");
  }
  if (changes_setpoint(request.intent) && !request.setpoint.has_value()) {
    return refuse(decision, request, "shape", StatusCode::invalid_argument,
                  std::string("intent ") + to_string(request.intent) + " must name a setpoint");
  }
  if (!changes_setpoint(request.intent) && request.setpoint.has_value()) {
    return refuse(decision, request, "shape", StatusCode::invalid_argument,
                  std::string("intent ") + to_string(request.intent) +
                      " does not name a setpoint");
  }
  if (requires_pressure_proof(request.intent) && !request.relationship.has_value()) {
    return refuse(decision, request, "shape", StatusCode::invalid_argument,
                  "restore_pressure_relationship must name the relationship it restores");
  }
  if (request.relationship.has_value() && !requires_pressure_proof(request.intent)) {
    return refuse(decision, request, "shape", StatusCode::invalid_argument,
                  std::string("intent ") + to_string(request.intent) +
                      " is not about a pressure relationship");
  }
  if (request.requested_at > model.tick) {
    return refuse(decision, request, "shape", StatusCode::out_of_range,
                  "request instant " + request.requested_at.to_string() +
                      " is later than the logical clock " + model.tick.to_string());
  }
  decision.trace.push_back(CheckTrace{"shape", StatusCode::ok, "request shape is valid"});

  // 2 - idempotent replay.
  const std::uint64_t fingerprint = request_fingerprint(request);
  if (const detail::IdempotencySlot* slot = detail::find_idempotency(model, request.key)) {
    if (slot->fingerprint != fingerprint) {
      decision.trace.push_back(CheckTrace{"idempotency", StatusCode::idempotency_conflict,
                                          "key " + request.key.str() +
                                              " was used for a different request"});
      return refuse(decision, request, "idempotency", StatusCode::idempotency_conflict,
                    "idempotency key " + request.key.str() +
                        " is retained for a different request");
    }
    decision.replayed = true;
    decision.replayed_attempt = slot->attempt;
    decision.eligible = slot->result == StatusCode::ok;
    decision.code = slot->result;
    decision.trace.push_back(CheckTrace{"idempotency", StatusCode::ok,
                                        "retained result replayed from attempt " +
                                            slot->attempt.to_string()});
    if (const AttemptRecord* retained = detail::find_attempt(model, slot->attempt)) {
      decision.planned_revision = retained->planned_revision;
      decision.device_generation = retained->device_generation;
      decision.epoch = retained->epoch;
      decision.policy_generation = retained->policy_generation;
      decision.evidence_generation = retained->evidence_generation;
      decision.resolved_setpoint = retained->setpoint;
      if (!decision.eligible) {
        decision.message = "the retained attempt was refused";
      }
    }
    return decision;
  }

  // 3 - identity resolution and 4 - identity binding.
  DeviceRecord* raw_target = device_mut(request.device);
  if (raw_target == nullptr) {
    decision.trace.push_back(CheckTrace{"identity", StatusCode::not_found, "device is not registered"});
    return refuse(decision, request, "identity", StatusCode::not_found,
                  "device " + request.device.str() + " is not registered");
  }
  const DeviceRecord& target = *raw_target;
  if (request.supersede.has_value()) {
    const AttemptRecord* superseded = detail::find_attempt(model, *request.supersede);
    if (superseded == nullptr) {
      return refuse(decision, request, "identity", StatusCode::not_found,
                    "attempt " + request.supersede->to_string() + " is not in the journal");
    }
    if (!(superseded->device == target.id)) {
      return refuse(decision, request, "identity", StatusCode::identity_mismatch,
                    "attempt " + request.supersede->to_string() +
                        " belongs to a different device");
    }
  }
  if (request.safety_permit.has_value()) {
    const SafetyPermit* permit = find_permit(*request.safety_permit);
    if (permit != nullptr && !(permit->device == target.id)) {
      return refuse(decision, request, "identity", StatusCode::identity_mismatch,
                    "safety permit " + request.safety_permit->str() +
                        " belongs to a different device");
    }
  }
  if (request.relationship.has_value()) {
    const detail::RelationshipRecord* record =
        detail::find_relationship(model, *request.relationship);
    if (record == nullptr) {
      return refuse(decision, request, "identity", StatusCode::not_found,
                    "pressure relationship " + request.relationship->str() + " is not declared");
    }
    if (!(record->relationship.room == target.room)) {
      return refuse(decision, request, "identity", StatusCode::identity_mismatch,
                    "pressure relationship " + request.relationship->str() +
                        " is declared for a different room");
    }
  }
  decision.trace.push_back(CheckTrace{"identity", StatusCode::ok, "identity resolved"});

  // 5 - lifecycle gate.
  const bool override_live =
      has_live_maintenance_override(target, request.epoch, request.requested_at);
  if (!permits_control(target.lifecycle) &&
      !(requires_maintenance_override(target.lifecycle) && override_live)) {
    const std::string detail =
        std::string("device lifecycle is ") + to_string(target.lifecycle);
    decision.trace.push_back(CheckTrace{"lifecycle", StatusCode::lifecycle_forbidden, detail});
    return refuse(decision, request, "lifecycle", StatusCode::lifecycle_forbidden, detail);
  }
  decision.trace.push_back(
      CheckTrace{"lifecycle", StatusCode::ok, std::string("lifecycle ") + to_string(target.lifecycle)});

  // 6 - unresolved attempt and supersession.
  if (target.unresolved_attempt.has_value()) {
    const AttemptId unresolved = *target.unresolved_attempt;
    if (!request.supersede.has_value() || !(*request.supersede == unresolved)) {
      decision.unresolved_attempt = unresolved;
      decision.trace.push_back(CheckTrace{"attempt", StatusCode::attempt_unresolved,
                                          "attempt " + unresolved.to_string() +
                                              " has no established effect"});
      return refuse(decision, request, "attempt", StatusCode::attempt_unresolved,
                    "attempt " + unresolved.to_string() +
                        " is unresolved and must be verified, resolved, or explicitly superseded");
    }
    const AttemptRecord* incumbent = detail::find_attempt(model, unresolved);
    if (incumbent == nullptr) {
      return refuse(decision, request, "internal", StatusCode::internal_error,
                    "the device names an unresolved attempt the journal does not hold");
    }
    Result<SetpointRequest> candidate = resolve_setpoint(target, request);
    if (!candidate.ok()) {
      return refuse(decision, request, "envelope", candidate.code(), candidate.message());
    }
    const DeliveryComparison comparison = compare_delivery(candidate.value(), incumbent->setpoint);
    if (!supersession_permitted(decision.request_class, request.safety_permit.has_value(), comparison)) {
      decision.trace.push_back(CheckTrace{"supersession", StatusCode::supersession_unsupported,
                                          "supersession of attempt " + unresolved.to_string() +
                                              " is not permitted for this request"});
      return refuse(decision, request, "supersession", StatusCode::supersession_unsupported,
                    "attempt " + unresolved.to_string() +
                        " may not be superseded by this request: it would not deliver at least as "
                        "much airflow, and it carries no emergency permit");
    }
    decision.trace.push_back(CheckTrace{"supersession", StatusCode::ok,
                                        "attempt " + unresolved.to_string() +
                                            " may be superseded explicitly"});
  } else if (request.supersede.has_value()) {
    return refuse(decision, request, "identity", StatusCode::identity_mismatch,
                  "attempt " + request.supersede->to_string() +
                      " is not the unresolved attempt of this device");
  }

  // 7 - containment precedence.
  const ContainmentSummary containment = containment_for(target);
  decision.trace.push_back(CheckTrace{"containment", StatusCode::ok,
                                      std::string("containment is ") + to_string(containment.worst) +
                                          " across " + std::to_string(containment.considered) +
                                          " element(s)"});
  if (requires_containment_proof(request.intent) && containment.worst != ContainmentState::intact) {
    StatusCode code = StatusCode::containment_unknown;
    if (containment.worst == ContainmentState::breached) {
      code = StatusCode::containment_breached;
    } else if (containment.worst == ContainmentState::open_for_service) {
      code = StatusCode::containment_open;
    }
    const std::string detail =
        std::string("containment is ") + to_string(containment.worst) +
        (containment.worst_id.has_value() ? " at " + containment.worst_id->str() : "") +
        "; an optimization request requires intact containment";
    decision.trace.push_back(CheckTrace{"containment", code, detail});
    return refuse(decision, request, "decision", code, detail);
  }

  // 8 - device generation.
  if (target.generation != request.device_generation) {
    const std::string detail = "request names device generation " +
                               request.device_generation.to_string() + " but the device is at " +
                               target.generation.to_string();
    decision.trace.push_back(CheckTrace{"generation", StatusCode::generation_mismatch, detail});
    return refuse(decision, request, "generation", StatusCode::generation_mismatch, detail);
  }
  decision.device_generation = target.generation;

  // 9 - state revision.
  if (request.expected_revision.has_value() && *request.expected_revision != target.revision) {
    const std::string detail = "request expects revision " +
                               request.expected_revision->to_string() + " but the device is at " +
                               target.revision.to_string();
    decision.trace.push_back(CheckTrace{"revision", StatusCode::revision_mismatch, detail});
    return refuse(decision, request, "revision", StatusCode::revision_mismatch, detail);
  }
  decision.planned_revision = target.revision;

  // 10 - authority epoch.
  if (model.epoch.is_zero() || request.epoch != model.epoch) {
    const std::string detail = "request epoch " + request.epoch.to_string() +
                               " is not the adopted epoch " + model.epoch.to_string();
    decision.trace.push_back(CheckTrace{"epoch", StatusCode::epoch_stale, detail});
    return refuse(decision, request, "epoch", StatusCode::epoch_stale, detail);
  }
  decision.epoch = model.epoch;

  // 11 - permission.
  const ControlAction action = required_action(request.intent);
  std::string permission_detail;
  const StatusCode permission =
      permission_code(target, request.epoch, request.requested_at, action, permission_detail);
  if (permission != StatusCode::ok) {
    decision.trace.push_back(CheckTrace{"permission", permission, permission_detail});
    return refuse(decision, request, "decision", permission, permission_detail);
  }
  decision.trace.push_back(CheckTrace{"permission", StatusCode::ok,
                                      std::string("action ") + to_string(action) + " is granted"});

  // 12 - safety permit.
  if (requires_safety_permit(request.intent)) {
    if (!request.safety_permit.has_value()) {
      const std::string detail = std::string("intent ") + to_string(request.intent) +
                                 " requires an explicit safety permit";
      decision.trace.push_back(CheckTrace{"safety_permit", StatusCode::safety_permit_missing, detail});
      return refuse(decision, request, "safety_permit", StatusCode::safety_permit_missing, detail);
    }
    const SafetyPermit* permit = find_permit(*request.safety_permit);
    if (permit == nullptr) {
      const std::string detail =
          "safety permit " + request.safety_permit->str() + " is not declared";
      decision.trace.push_back(CheckTrace{"safety_permit", StatusCode::safety_permit_missing, detail});
      return refuse(decision, request, "safety_permit", StatusCode::safety_permit_missing, detail);
    }
    if (permit->epoch != request.epoch ||
        (permit->expires_at.has_value() && request.requested_at > *permit->expires_at) ||
        request.requested_at < permit->issued_at) {
      const std::string detail =
          "safety permit " + request.safety_permit->str() + " is not current at " +
          request.requested_at.to_string();
      decision.trace.push_back(CheckTrace{"safety_permit", StatusCode::safety_permit_stale, detail});
      return refuse(decision, request, "safety_permit", StatusCode::safety_permit_stale, detail);
    }
    decision.trace.push_back(CheckTrace{"safety_permit", StatusCode::ok, "safety permit is current"});
  }

  // 13 - interlocks.
  const bool safety_directed = decision.request_class == RequestClass::safety_directed;
  if (!safety_directed) {
    for (const Interlock* interlock : interlocks_for(target)) {
      if (interlock->klass != InterlockClass::protected_obligation) {
        continue;
      }
      InterlockState state = interlock->state;
      if (!interlock->reported_at.has_value() || interlock->epoch != model.epoch) {
        // An interlock that was declared but never reported, or whose last
        // report came from another epoch, is not current. It fails closed as
        // unknown rather than passing as satisfied.
        state = InterlockState::unknown;
      }
      if (state == InterlockState::satisfied) {
        continue;
      }
      StatusCode code = state == InterlockState::open ? StatusCode::interlock_open
                                                      : StatusCode::interlock_unknown;
      const std::string detail = "protected interlock " + interlock->id.str() + " is " +
                                 to_string(state);
      decision.trace.push_back(CheckTrace{"interlock", code, detail});
      return refuse(decision, request, "decision", code, detail);
    }
    decision.trace.push_back(CheckTrace{"interlock", StatusCode::ok, "protected interlocks hold"});
  }

  // 14 - evidence currency.
  // Freshness is judged at the instant the request was planned for, so a
  // decision is a function of the request and the state, never of when the
  // caller happened to ask.
  const FreshnessRequirements requirements =
      FreshnessRequirements{request.requested_at, options.evidence.max_age_ticks, target.generation,
                            policy_evidence_generation(target), false};
  const std::vector<const detail::RelationshipRecord*> room_relationships =
      relationships_for(target.room);
  if (!safety_directed) {
    std::vector<const detail::RelationshipRecord*> required;
    if (requires_pressure_proof(request.intent)) {
      required.push_back(detail::find_relationship(model, *request.relationship));
    } else if (decision.request_class == RequestClass::optimization) {
      required = room_relationships;
    }
    for (const detail::RelationshipRecord* record : required) {
      if (record == nullptr) {
        continue;
      }
      // Pressure evidence is stamped with the relationship's own evidence
      // generation, not with the device policy's: the two are published by
      // different authorities and are not interchangeable.
      FreshnessRequirements relationship_requirements = requirements;
      relationship_requirements.evidence_generation = record->relationship.evidence_generation;
      std::optional<Pressure> representative;
      std::size_t contributing = 0;
      std::size_t conflicting = 0;
      const PressureState state =
          adjudicate(*record, relationship_requirements, EvidenceBinding{}, representative,
                     contributing, conflicting);
      const std::string detail = "pressure relationship " + record->relationship.id.str() + " is " +
                                 to_string(state) + " from " + std::to_string(contributing) +
                                 " current source(s)";
      decision.trace.push_back(CheckTrace{"pressure", StatusCode::ok, detail});
      if (state == PressureState::unknown) {
        return refuse(decision, request, "pressure", StatusCode::pressure_unknown,
                      detail + "; a request that requires pressure proof fails closed while the "
                               "evidence is unknown or stale");
      }
      if (state == PressureState::conflicted) {
        decision.trace.push_back(CheckTrace{"pressure", StatusCode::pressure_conflicted, detail});
        return refuse(decision, request, "pressure", StatusCode::pressure_conflicted,
                      detail + "; contradictory sensor evidence is not treated as safe");
      }
      if (decision.request_class == RequestClass::optimization &&
          state == PressureState::violated) {
        decision.trace.push_back(CheckTrace{"pressure", StatusCode::pressure_violated, detail});
        return refuse(decision, request, "pressure", StatusCode::pressure_violated,
                      detail + "; an optimization request may not proceed while a pressure "
                               "obligation in the room is violated");
      }
    }
  }

  // 15 - obligations.
  const auto scoped_obligations = obligations_for(target);
  for (const detail::ObligationRecord* record : scoped_obligations) {
    std::optional<Airflow> observed;
    const ObligationState state = obligation_state(*record, requirements, observed);
    const std::string detail =
        "obligation " + record->obligation.id.str() + " is " + to_string(state) +
        (observed.has_value() ? " at " + observed->to_string() : " with no current reading");
    if (decision.request_class != RequestClass::optimization) {
      decision.trace.push_back(CheckTrace{"obligation", StatusCode::ok, detail});
      continue;
    }
    if (record->obligation.klass != ObligationClass::protected_obligation) {
      decision.trace.push_back(CheckTrace{"obligation", StatusCode::ok,
                                          detail + "; the obligation is advisory"});
      continue;
    }
    if (state == ObligationState::satisfied) {
      decision.trace.push_back(CheckTrace{"obligation", StatusCode::ok, detail});
      continue;
    }
    const StatusCode code = state == ObligationState::unknown ? StatusCode::obligation_unknown
                                                              : StatusCode::obligation_unsatisfied;
    decision.trace.push_back(CheckTrace{"obligation", code, detail});
    return refuse(decision, request, "decision", code, detail);
  }

  // 16 - envelope and limits.
  if (!target.policy.has_value()) {
    const std::string detail = "device " + target.id.str() +
                               " has no fan policy, so no safe operating envelope is known";
    decision.trace.push_back(CheckTrace{"envelope", StatusCode::limit_unknown, detail});
    return refuse(decision, request, "envelope", StatusCode::limit_unknown, detail);
  }
  const OperatingEnvelope& envelope = target.policy->envelope;
  Result<SetpointRequest> resolved = resolve_setpoint(target, request);
  if (!resolved.ok()) {
    decision.trace.push_back(CheckTrace{"envelope", resolved.code(), resolved.message()});
    return refuse(decision, request, "decision", resolved.code(), resolved.message());
  }
  decision.resolved_setpoint = resolved.value();
  decision.policy_generation = target.policy->generation;
  decision.evidence_generation = envelope.evidence_generation;

  if (const auto* percent = std::get_if<SetpointPercent>(&*decision.resolved_setpoint)) {
    if (percent->percent < envelope.min_fan_percent || percent->percent > envelope.max_fan_percent) {
      const std::string detail = "requested fan setpoint " + percent->percent.to_string() +
                                 " lies outside the envelope " +
                                 envelope.min_fan_percent.to_string() + " .. " +
                                 envelope.max_fan_percent.to_string();
      decision.trace.push_back(CheckTrace{"envelope", StatusCode::limit_exceeded, detail});
      return refuse(decision, request, "envelope", StatusCode::limit_exceeded, detail);
    }
  } else {
    const Airflow requested = std::get<SetpointAirflow>(*decision.resolved_setpoint).airflow;
    if (requested < envelope.min_airflow || requested > envelope.max_airflow) {
      const std::string detail = "requested airflow " + requested.to_string() +
                                 " lies outside the envelope " + envelope.min_airflow.to_string() +
                                 " .. " + envelope.max_airflow.to_string();
      decision.trace.push_back(CheckTrace{"envelope", StatusCode::limit_exceeded, detail});
      return refuse(decision, request, "envelope", StatusCode::limit_exceeded, detail);
    }
  }

  // The slew bound is enforced on fan-percent setpoints. Relating an airflow
  // request to a fan position needs the device's fan curve, which this runtime
  // does not own, so an airflow request is bounded by the envelope alone.
  const auto* candidate_percent = std::get_if<SetpointPercent>(&*decision.resolved_setpoint);
  const std::optional<SetpointRequest> baseline = baseline_setpoint(target);
  if (candidate_percent != nullptr && baseline.has_value()) {
    const auto* baseline_percent = std::get_if<SetpointPercent>(&*baseline);
    if (baseline_percent != nullptr) {
      const std::int64_t delta = absolute_difference(
          static_cast<std::int64_t>(candidate_percent->percent.basis_points()),
          static_cast<std::int64_t>(baseline_percent->percent.basis_points()));
      if (delta > static_cast<std::int64_t>(envelope.max_step.basis_points())) {
        const std::string detail = "setpoint change of " + std::to_string(delta) +
                                   " basis points exceeds the slew bound " +
                                   envelope.max_step.to_string();
        decision.trace.push_back(CheckTrace{"slew", StatusCode::limit_exceeded, detail});
        return refuse(decision, request, "envelope", StatusCode::limit_exceeded, detail);
      }
      decision.trace.push_back(CheckTrace{"slew", StatusCode::ok,
                                          "setpoint change of " + std::to_string(delta) +
                                              " basis points is within the slew bound"});
    } else {
      decision.trace.push_back(
          CheckTrace{"slew", StatusCode::ok,
                     "slew is not enforced between setpoints of different kinds"});
    }
  }

  decision.eligible = true;
  decision.code = StatusCode::ok;
  decision.message = "the transition may be attempted";
  decision.current_effect = target.effect;
  decision.unresolved_attempt = target.unresolved_attempt;
  decision.trace.push_back(CheckTrace{"decision", StatusCode::ok, decision.message});
  return decision;
}

Result<SetpointRequest> AirflowControlEngine::Impl::resolve_setpoint(
    const DeviceRecord& target, const ControlRequest& request) const {
  if (request.setpoint.has_value()) {
    return *request.setpoint;
  }
  if (!target.policy.has_value()) {
    return Status::failure(StatusCode::limit_unknown,
                           "release_to_policy needs a fan policy and the device has none");
  }
  return policy_default_setpoint(*target.policy);
}

std::optional<SetpointRequest> AirflowControlEngine::Impl::baseline_setpoint(
    const DeviceRecord& target) const {
  const AttemptRecord* newest = nullptr;
  for (const AttemptRecord& attempt : model.attempts) {
    if (!(attempt.device == target.id)) {
      continue;
    }
    if (newest == nullptr || attempt.id > newest->id) {
      newest = &attempt;
    }
  }
  if (newest != nullptr) {
    return newest->setpoint;
  }
  if (target.policy.has_value()) {
    return policy_default_setpoint(*target.policy);
  }
  return std::nullopt;
}

namespace {

[[nodiscard]] AttemptState state_for_disposition(AdapterDisposition disposition) noexcept {
  switch (disposition) {
    case AdapterDisposition::accepted:
      return AttemptState::acknowledged;
    case AdapterDisposition::refused:
      return AttemptState::refused;
    case AdapterDisposition::unavailable:
      return AttemptState::unavailable;
    case AdapterDisposition::fault:
      return AttemptState::faulted;
    case AdapterDisposition::indeterminate:
      return AttemptState::indeterminate;
  }
  return AttemptState::indeterminate;
}

[[nodiscard]] StatusCode code_for_disposition(AdapterDisposition disposition) noexcept {
  switch (disposition) {
    case AdapterDisposition::accepted:
      return StatusCode::ok;
    case AdapterDisposition::refused:
      return StatusCode::adapter_refused;
    case AdapterDisposition::unavailable:
      return StatusCode::adapter_unavailable;
    case AdapterDisposition::fault:
      return StatusCode::adapter_fault;
    case AdapterDisposition::indeterminate:
      return StatusCode::adapter_indeterminate;
  }
  return StatusCode::adapter_indeterminate;
}

}  // namespace

// ---------------------------------------------------------------------------
// Issue
// ---------------------------------------------------------------------------

Result<AttemptRecord> AirflowControlEngine::Impl::issue_impl(const ControlRequest& request,
                                                             AirflowAdapter& adapter) {
  Result<Decision> evaluated = evaluate_impl(request);
  if (!evaluated.ok()) {
    return evaluated.status();
  }
  Decision decision = std::move(evaluated).value();
  if (!decision.eligible) {
    audit(AuditKind::attempt_rejected, request.key.str(),
          std::string(to_string(decision.code)) + ": " + decision.message);
    Status published = publish();
    Status status = Status::failure(decision.code, decision.message);
    status.trace() = decision.trace;
    if (!published.ok()) {
      status.note("publication", "the refusal could not be journalled: " + published.message());
    }
    return status;
  }
  if (decision.replayed) {
    // A replay is a read of a retained result. It re-actuates nothing and
    // changes nothing, so it publishes nothing either.
    const AttemptRecord* retained = detail::find_attempt(model, *decision.replayed_attempt);
    if (retained == nullptr) {
      return Status::failure(StatusCode::internal_error,
                             "a retained idempotency slot names an attempt the journal lacks");
    }
    return *retained;
  }

  DeviceRecord* target = device_mut(request.device);
  if (target == nullptr) {
    return Status::failure(StatusCode::internal_error, "the device vanished between checks");
  }

  const AttemptId attempt_id = model.next_attempt;
  const CommandId command_id = model.next_command;
  const std::uint64_t fingerprint = request_fingerprint(request);

  IssueUndo undo;
  undo.next_attempt_before = model.next_attempt;
  undo.next_command_before = model.next_command;
  undo.unresolved_before = target->unresolved_attempt;

  AttemptOrdinal ordinal = AttemptOrdinal::from(1);
  for (const AttemptRecord& existing : model.attempts) {
    if (existing.device == target->id) {
      ordinal = AttemptOrdinal::from(ordinal.value() + 1);
    }
  }

  if (request.supersede.has_value()) {
    AttemptRecord* incumbent = detail::find_attempt(model, *request.supersede);
    if (incumbent == nullptr) {
      return Status::failure(StatusCode::internal_error, "the superseded attempt vanished");
    }
    undo.superseded_before = *incumbent;
    incumbent->state = AttemptState::superseded;
    incumbent->superseded_by = attempt_id;
    incumbent->resolved_at = request.requested_at;
    incumbent->resolution_reason = sanitize_text(
        "superseded by attempt " + attempt_id.to_string(), kMaxTextLength);
  }

  // Evict before appending so that the journal never exceeds its bound, and
  // evict the retained idempotency slots of the attempts that leave with it:
  // a retained key must always name an attempt the journal still holds.
  while (model.attempts.size() + 1 > model.attempt_journal_capacity) {
    undo.evicted_attempts.push_back(model.attempts.front());
    model.attempts.erase(model.attempts.begin());
  }
  for (const AttemptRecord& evicted : undo.evicted_attempts) {
    for (auto slot = model.idempotency.begin(); slot != model.idempotency.end();) {
      if (slot->attempt == evicted.id) {
        undo.evicted_slots.push_back(*slot);
        slot = model.idempotency.erase(slot);
      } else {
        ++slot;
      }
    }
  }

  const AttemptRecord record{.id = attempt_id,
                             .ordinal = ordinal,
                             .key = request.key,
                             .device = target->id,
                             .device_generation = target->generation,
                             .epoch = request.epoch,
                             .planned_revision = decision.planned_revision,
                             .policy_generation = decision.policy_generation,
                             .evidence_generation = decision.evidence_generation,
                             .intent = request.intent,
                             .request_class = decision.request_class,
                             .setpoint = *decision.resolved_setpoint,
                             .actor = request.actor,
                             .accepted_at = request.requested_at,
                             .dispatched_at = request.requested_at,
                             .state = AttemptState::dispatched,
                             .command = command_id,
                             .adapter_sequence = AdapterSequence::from(0),
                             .disposition = AdapterDisposition::indeterminate,
                             .detail = std::string(),
                             .safety_permit = request.safety_permit,
                             .supersedes = request.supersede,
                             .superseded_by = std::nullopt,
                             .fan_observation = std::nullopt,
                             .pressure_observation = std::nullopt,
                             .effect_sequence = std::nullopt,
                             .resolved_at = std::nullopt,
                             .resolution_reason = std::string()};
  model.attempts.push_back(record);
  undo.appended = true;
  target->unresolved_attempt = attempt_id;
  Result<AttemptId> advanced_attempt = attempt_id.next();
  if (advanced_attempt.ok()) {
    model.next_attempt = advanced_attempt.value();
  }
  Result<CommandId> advanced_command = command_id.next();
  if (advanced_command.ok()) {
    model.next_command = advanced_command.value();
  }
  // The idempotency key is retained at the same boundary as the attempt, so a
  // lost response replays the durable record instead of becoming a new request.
  // A key that became durable only after the adapter answered would leave the
  // window in which a retry is indistinguishable from a fresh command.
  {
    detail::IdempotencySlot slot{request.key, attempt_id, fingerprint, StatusCode::ok};
    bool replaced = false;
    for (detail::IdempotencySlot& existing : model.idempotency) {
      if (existing.key == request.key) {
        existing = slot;
        replaced = true;
        break;
      }
    }
    if (!replaced) {
      model.idempotency.push_back(slot);
    }
    while (model.idempotency.size() > model.idempotency_window) {
      auto oldest = std::min_element(
          model.idempotency.begin(), model.idempotency.end(),
          [](const detail::IdempotencySlot& lhs, const detail::IdempotencySlot& rhs) {
            return lhs.attempt < rhs.attempt;
          });
      if (oldest == model.idempotency.end()) {
        break;
      }
      model.idempotency.erase(oldest);
    }
  }

  audit(AuditKind::attempt_accepted, request.key.str(),
        "attempt " + attempt_id.to_string() + " dispatched to the adapter");

  // The command-attempt boundary: the durable record that says a command may
  // reach the device, and the key that makes a retry a replay, are committed
  // before the command leaves for the adapter.
  Status published = publish();
  if (!published.ok()) {
    if (undo.appended && !model.attempts.empty() && model.attempts.back().id == attempt_id) {
      model.attempts.pop_back();
    }
    if (undo.superseded_before.has_value()) {
      AttemptRecord* incumbent = detail::find_attempt(model, undo.superseded_before->id);
      if (incumbent != nullptr) {
        *incumbent = *undo.superseded_before;
      }
    }
    for (auto entry = undo.evicted_attempts.rbegin(); entry != undo.evicted_attempts.rend(); ++entry) {
      model.attempts.insert(model.attempts.begin(), *entry);
    }
    for (const detail::IdempotencySlot& slot : undo.evicted_slots) {
      model.idempotency.push_back(slot);
    }
    for (auto slot = model.idempotency.begin(); slot != model.idempotency.end();) {
      if (slot->key == request.key && slot->attempt == attempt_id) {
        slot = model.idempotency.erase(slot);
      } else {
        ++slot;
      }
    }
    target->unresolved_attempt = undo.unresolved_before;
    model.next_attempt = undo.next_attempt_before;
    model.next_command = undo.next_command_before;
    audit(AuditKind::request_refused, request.key.str(),
          "the command-attempt boundary could not be committed: " + published.message());
    return published;
  }

  const ActuationAuthorization authorization = AirflowControlEngine::make_authorization(
      command_id, attempt_id, target->id, target->generation, request.epoch,
      decision.planned_revision, decision.policy_generation, decision.evidence_generation,
      request.requested_at);
  const AdapterCommand command{authorization, request.intent, decision.request_class,
                               *decision.resolved_setpoint};

  const AdapterOutcome outcome = adapter.execute(command);
  const Status echo = verify_outcome_echo(command, outcome);
  AdapterDisposition disposition = outcome.disposition;
  AdapterSequence sequence = outcome.sequence;
  std::string detail = outcome.detail;
  if (!echo.ok()) {
    disposition = AdapterDisposition::indeterminate;
    sequence = AdapterSequence::from(0);
    detail = "adapter answer was fenced: " + echo.message();
  }

  AttemptRecord* stored = detail::find_attempt(model, attempt_id);
  if (stored == nullptr) {
    return Status::failure(StatusCode::internal_error, "the attempt record vanished");
  }
  const AttemptRecord snapshot = *stored;
  const std::optional<AttemptId> unresolved_before_outcome = target->unresolved_attempt;

  stored->state = state_for_disposition(disposition);
  stored->disposition = disposition;
  stored->adapter_sequence = sequence;
  stored->detail = sanitize_text(detail, kMaxTextLength);
  if (is_definite(stored->state)) {
    // The adapter stated a definite outcome, so the requested condition is
    // repaired rather than left claimed.
    stored->resolved_at = request.requested_at;
    stored->resolution_reason = sanitize_text(
        std::string("the adapter stated ") + to_string(disposition), kMaxTextLength);
    target->unresolved_attempt.reset();
  }
  audit(AuditKind::adapter_outcome, request.key.str(),
        std::string("attempt ") + attempt_id.to_string() + " answered " + to_string(disposition) +
            (echo.ok() ? "" : " (fenced)"));

  if (disposition == AdapterDisposition::accepted) {
    if (outcome.reading.has_value()) {
      Result<Observation> ingested = ingest(*outcome.reading, request.requested_at, true);
      if (!ingested.ok()) {
        audit(AuditKind::observation_rejected, request.key.str(),
              "the reading an adapter returned with its answer was refused: " +
                  ingested.message());
      }
    }
  }

  Status outcome_published = publish();
  if (!outcome_published.ok()) {
    *stored = snapshot;
    target->unresolved_attempt = unresolved_before_outcome;
    audit(AuditKind::request_refused, request.key.str(),
          "the adapter outcome could not be committed: " + outcome_published.message());
    return outcome_published;
  }

  const AttemptRecord* result = detail::find_attempt(model, attempt_id);
  if (result == nullptr) {
    return Status::failure(StatusCode::internal_error, "the attempt record vanished");
  }
  return *result;
}

// ---------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------

Result<VerifiedEffect> AirflowControlEngine::Impl::verify_impl(const VerificationRequest& request) {
  AttemptRecord* attempt = detail::find_attempt(model, request.attempt);
  if (attempt == nullptr) {
    return Status::failure(StatusCode::not_found,
                           "attempt " + request.attempt.to_string() + " is not in the journal");
  }
  if (request.at > model.tick) {
    return Status::failure(StatusCode::evidence_future,
                           "verification instant " + request.at.to_string() +
                               " is later than the logical clock " + model.tick.to_string());
  }
  if (request.at < attempt->accepted_at) {
    return Status::failure(StatusCode::out_of_range,
                           "verification instant " + request.at.to_string() +
                               " precedes the attempt accepted at " +
                               attempt->accepted_at.to_string());
  }

  VerifiedEffect effect{};
  effect.attempt = attempt->id;
  effect.verified_at = request.at;
  effect.code = StatusCode::ok;

  if (is_definite(attempt->state)) {
    effect.state = EffectState::unverified;
    effect.code = code_for_disposition(attempt->disposition);
    effect.message = std::string("the adapter stated ") + to_string(attempt->disposition) +
                     "; there is no effect to establish";
    return effect;
  }
  if (attempt->state == AttemptState::superseded ||
      attempt->state == AttemptState::resolved_without_effect) {
    effect.state = EffectState::unverified;
    effect.message = std::string("the attempt was closed as ") + to_string(attempt->state) +
                     " without an established effect";
    return effect;
  }
  if (attempt->state == AttemptState::effect_established ||
      attempt->state == AttemptState::effect_contradicted) {
    effect.state = attempt->state == AttemptState::effect_established ? EffectState::effective
                                                                     : EffectState::contradicted;
    effect.fan_observation = attempt->fan_observation;
    effect.pressure_observation = attempt->pressure_observation;
    if (attempt->effect_sequence.has_value()) {
      effect.sequence = *attempt->effect_sequence;
    }
    if (attempt->resolved_at.has_value()) {
      effect.verified_at = *attempt->resolved_at;
    }
    effect.message = "the effect was already established";
    return effect;
  }

  DeviceRecord* target = device_mut(attempt->device);
  if (target == nullptr) {
    return Status::failure(StatusCode::not_found,
                           "device " + attempt->device.str() + " is not registered");
  }

  const FreshnessRequirements requirements =
      FreshnessRequirements{request.at, options.evidence.max_age_ticks, target->generation,
                            policy_evidence_generation(*target), false};
  const LogicalTick dispatched =
      attempt->dispatched_at.has_value() ? *attempt->dispatched_at : attempt->accepted_at;
  const EvidenceBinding binding{dispatched, attempt->accepted_at};

  if (request.adapter != nullptr) {
    const AdapterReadRequest read{.device = target->id,
                                  .device_generation = target->generation,
                                  .kind = setpoint_kind(attempt->setpoint) == SetpointKind::fan_percent
                                              ? ObservationKind::fan_setpoint
                                              : ObservationKind::airflow,
                                  .relationship = std::nullopt,
                                  .point = request.fan_point,
                                  .source = request.source,
                                  .sequence = request.fan_sequence,
                                  .at = request.at,
                                  .evidence_generation = requirements.evidence_generation};
    Result<ObservationDraft> draft = request.adapter->read(read);
    if (!draft.ok()) {
      effect.trace.push_back(CheckTrace{"read", draft.code(), draft.message()});
    } else {
      Result<Observation> ingested = ingest(draft.value(), request.at, true);
      if (!ingested.ok()) {
        effect.trace.push_back(CheckTrace{"read", ingested.code(), ingested.message()});
      }
    }
  }

  bool delivery_effective = false;
  bool delivery_contradicted = false;
  const bool want_fan = setpoint_kind(attempt->setpoint) == SetpointKind::fan_percent;
  const std::optional<Observation>& delivery =
      want_fan ? target->fan_observation : target->airflow_observation;
  if (!delivery.has_value()) {
    effect.trace.push_back(CheckTrace{"delivery", StatusCode::evidence_unknown,
                                      "no delivery reading has been accepted for this device"});
  } else {
    const FreshnessVerdict verdict = assess_freshness(*delivery, requirements);
    if (!is_proof(verdict)) {
      effect.trace.push_back(CheckTrace{"delivery", freshness_status(verdict),
                                        std::string("delivery evidence is ") + to_string(verdict)});
    } else if (!is_bound(*delivery, binding)) {
      effect.trace.push_back(
          CheckTrace{"delivery", StatusCode::evidence_stale,
                     "the delivery reading predates the command, so it is not evidence about it"});
    } else if (want_fan) {
      const auto* observed = std::get_if<FanReading>(&delivery->draft.payload);
      const auto* commanded = std::get_if<SetpointPercent>(&attempt->setpoint);
      if (observed == nullptr || commanded == nullptr) {
        effect.trace.push_back(CheckTrace{"delivery", StatusCode::internal_error,
                                          "the delivery reading has the wrong kind"});
      } else {
        const std::int64_t delta =
            absolute_difference(static_cast<std::int64_t>(observed->percent.basis_points()),
                                static_cast<std::int64_t>(commanded->percent.basis_points()));
        effect.observed_fan_percent = observed->percent;
        effect.fan_observation = delivery->id;
        delivery_effective =
            delta <= static_cast<std::int64_t>(options.evidence.setpoint_tolerance_basis_points);
        delivery_contradicted = !delivery_effective;
        effect.trace.push_back(CheckTrace{
            "delivery", delivery_effective ? StatusCode::ok : StatusCode::ok,
            "observed fan setpoint " + observed->percent.to_string() + " against commanded " +
                commanded->percent.to_string()});
      }
    } else {
      const auto* observed = std::get_if<AirflowReading>(&delivery->draft.payload);
      const auto* commanded = std::get_if<SetpointAirflow>(&attempt->setpoint);
      if (observed == nullptr || commanded == nullptr) {
        effect.trace.push_back(CheckTrace{"delivery", StatusCode::internal_error,
                                          "the delivery reading has the wrong kind"});
      } else {
        const std::int64_t delta = absolute_difference(observed->value.cubic_metres_per_hour(),
                                                       commanded->airflow.cubic_metres_per_hour());
        effect.observed_airflow = observed->value;
        effect.fan_observation = delivery->id;
        delivery_effective =
            delta <= options.evidence.airflow_tolerance_cubic_metres_per_hour;
        delivery_contradicted = !delivery_effective;
        effect.trace.push_back(CheckTrace{
            "delivery", StatusCode::ok,
            "observed airflow " + observed->value.to_string() + " against commanded " +
                commanded->airflow.to_string()});
      }
    }
  }

  bool pressure_effective = false;
  bool pressure_contradicted = false;
  const bool needs_pressure = requires_pressure_proof(attempt->intent);
  if (needs_pressure) {
    const detail::RelationshipRecord* record =
        request.relationship.has_value() ? detail::find_relationship(model, *request.relationship)
                                         : nullptr;
    // The relationship's own evidence generation governs pressure evidence.
    FreshnessRequirements relationship_requirements = requirements;
    if (record != nullptr) {
      relationship_requirements.evidence_generation = record->relationship.evidence_generation;
    }
    if (record != nullptr && request.adapter != nullptr) {
      const AdapterReadRequest read{.device = target->id,
                                    .device_generation = target->generation,
                                    .kind = ObservationKind::pressure,
                                    .relationship = request.relationship,
                                    .point = request.pressure_point,
                                    .source = request.source,
                                    .sequence = request.pressure_sequence,
                                    .at = request.at,
                                    .evidence_generation = relationship_requirements.evidence_generation};
      Result<ObservationDraft> draft = request.adapter->read(read);
      if (!draft.ok()) {
        effect.trace.push_back(CheckTrace{"pressure_read", draft.code(), draft.message()});
      } else {
        Result<Observation> ingested = ingest(draft.value(), request.at, true);
        if (!ingested.ok()) {
          effect.trace.push_back(
              CheckTrace{"pressure_read", ingested.code(), ingested.message()});
        }
      }
    }
    if (record == nullptr) {
      effect.trace.push_back(CheckTrace{"pressure", StatusCode::evidence_unknown,
                                        "no pressure relationship was named for verification"});
    } else {
      std::optional<Pressure> representative;
      std::size_t contributing = 0;
      std::size_t conflicting = 0;
      const PressureState state = adjudicate(*record, relationship_requirements, binding,
                                             representative, contributing, conflicting);
      effect.observed_differential = representative;
      if (state == PressureState::satisfied) {
        pressure_effective = true;
        for (const Observation& observation : record->evidence) {
          if (is_proof(assess_freshness(observation, relationship_requirements)) &&
              is_bound(observation, binding)) {
            effect.pressure_observation = observation.id;
            break;
          }
        }
        effect.trace.push_back(CheckTrace{"pressure", StatusCode::ok,
                                          "the pressure relationship is satisfied by " +
                                              std::to_string(contributing) +
                                              " post-command source(s)"});
      } else if (state == PressureState::violated) {
        pressure_contradicted = true;
        effect.trace.push_back(CheckTrace{"pressure", StatusCode::pressure_violated,
                                          "the pressure relationship is still violated by " +
                                              std::to_string(contributing) +
                                              " post-command source(s)"});
      } else {
        effect.trace.push_back(CheckTrace{
            "pressure",
            state == PressureState::conflicted ? StatusCode::pressure_conflicted
                                               : StatusCode::pressure_unknown,
            std::string("the pressure relationship is ") + to_string(state) +
                " after the command"});
      }
    }
  }

  EffectState resulting = EffectState::indeterminate;
  if (delivery_contradicted || pressure_contradicted) {
    resulting = EffectState::contradicted;
  } else if (delivery_effective && (!needs_pressure || pressure_effective)) {
    resulting = EffectState::effective;
  }
  effect.state = resulting;
  effect.sequence = model.next_effect;
  switch (resulting) {
    case EffectState::effective:
      effect.message = "fresh evidence establishes the requested condition";
      break;
    case EffectState::contradicted:
      effect.message = "fresh evidence establishes a different condition";
      break;
    case EffectState::indeterminate:
      effect.message = "the evidence available does not decide the effect";
      break;
    case EffectState::unverified:
      effect.message = "the effect was not verified";
      break;
  }

  const AttemptRecord attempt_snapshot = *attempt;
  const std::optional<AttemptId> unresolved_before = target->unresolved_attempt;
  const EffectState effect_before = target->effect;
  const EffectSequence sequence_before = model.next_effect;

  attempt->effect_sequence = effect.sequence;
  attempt->fan_observation = effect.fan_observation;
  attempt->pressure_observation = effect.pressure_observation;
  switch (resulting) {
    case EffectState::effective:
      attempt->state = AttemptState::effect_established;
      break;
    case EffectState::contradicted:
      attempt->state = AttemptState::effect_contradicted;
      break;
    case EffectState::indeterminate:
      attempt->state = AttemptState::effect_indeterminate;
      break;
    case EffectState::unverified:
      break;
  }
  if (resulting == EffectState::effective || resulting == EffectState::contradicted) {
    attempt->resolved_at = request.at;
    attempt->resolution_reason = sanitize_text(effect.message, kMaxTextLength);
    target->unresolved_attempt.reset();
    target->effect = resulting == EffectState::effective ? EffectState::effective
                                                         : EffectState::contradicted;
  } else if (resulting == EffectState::indeterminate) {
    target->effect = EffectState::indeterminate;
  }
  Result<EffectSequence> advanced = model.next_effect.next();
  if (advanced.ok()) {
    model.next_effect = advanced.value();
  }
  audit(AuditKind::attempt_verified, request.attempt.to_string(),
        std::string("effect ") + to_string(resulting) + ": " + effect.message);

  Status published = publish();
  if (!published.ok()) {
    *attempt = attempt_snapshot;
    target->unresolved_attempt = unresolved_before;
    target->effect = effect_before;
    model.next_effect = sequence_before;
    return published;
  }
  return effect;
}

// ---------------------------------------------------------------------------
// Authority and model registration
// ---------------------------------------------------------------------------

StatusCode AirflowControlEngine::Impl::permission_code(const DeviceRecord& target,
                                                       AuthorityEpoch epoch, LogicalTick at,
                                                       ControlAction action,
                                                       std::string& detail) const {
  bool any_for_device = false;
  bool any_action = false;
  for (const PermissionGrant& grant : model.grants) {
    if (grant.revoked) {
      continue;
    }
    if (grant.device.has_value() && !(*grant.device == target.id)) {
      continue;
    }
    if (grant.device_generation.has_value() && *grant.device_generation != target.generation) {
      continue;
    }
    if (grant.room.has_value() && !(*grant.room == target.room)) {
      continue;
    }
    any_for_device = true;
    if (!grant.actions.contains(action)) {
      continue;
    }
    any_action = true;
    if (grant.epoch != epoch) {
      continue;
    }
    if (grant.expires_at.has_value() && at > *grant.expires_at) {
      continue;
    }
    detail = std::string("action ") + to_string(action) + " is granted by " + grant.id.str();
    return StatusCode::ok;
  }
  if (any_for_device && any_action) {
    detail = "every grant of " + std::string(to_string(action)) + " for device " + target.id.str() +
             " is from another epoch or has expired";
    return StatusCode::permission_stale;
  }
  if (any_for_device) {
    detail = "no grant for device " + target.id.str() + " includes " +
             std::string(to_string(action));
    return StatusCode::permission_denied;
  }
  detail = "no grant issues " + std::string(to_string(action)) + " for device " + target.id.str();
  return StatusCode::permission_missing;
}

Status AirflowControlEngine::Impl::adopt_epoch(AuthorityEpoch epoch, const ActorId& actor,
                                               LogicalTick at) {
  if (epoch.is_zero()) {
    return Status::failure(StatusCode::invalid_argument, "an authority epoch must be non-zero");
  }
  if (!model.epoch.is_zero() && epoch <= model.epoch) {
    return Status::failure(StatusCode::epoch_stale,
                           "epoch " + epoch.to_string() + " is not newer than the adopted epoch " +
                               model.epoch.to_string());
  }
  // Authority cannot be adopted in the future of the logical clock, so adopting
  // an epoch at an instant advances the clock to that instant. This is what
  // makes the bootstrap possible: a fresh store starts at instant zero, and the
  // first act of the control plane is to adopt the epoch it is operating under.
  if (at > model.tick) {
    model.tick = at;
    audit(AuditKind::clock_advanced, actor.str(),
          "clock advanced to " + at.to_string() + " by epoch adoption");
  }
  model.epoch = epoch;
  audit(AuditKind::epoch_adopted, actor.str(), "epoch " + epoch.to_string() + " adopted");
  return publish();
}

Status AirflowControlEngine::Impl::advance_tick(LogicalTick tick) {
  if (tick < model.tick) {
    return Status::failure(StatusCode::out_of_range,
                           "logical clock cannot move backwards from " + model.tick.to_string() +
                               " to " + tick.to_string());
  }
  if (tick == model.tick) {
    return Status::success();
  }
  model.tick = tick;
  audit(AuditKind::clock_advanced, std::string(), "clock advanced to " + tick.to_string());
  return publish();
}

Status AirflowControlEngine::Impl::register_device(const RegisterDeviceRequest& request) {
  if (detail::find_device(model, request.device) != nullptr) {
    return Status::failure(StatusCode::duplicate_identity,
                           "device " + request.device.str() + " is already registered");
  }
  if (model.devices.size() >= ModelBounds::max_devices) {
    return Status::failure(StatusCode::bounds_exceeded,
                           "the model already holds " + std::to_string(model.devices.size()) +
                               " devices, the structural maximum");
  }
  if (request.generation.is_zero()) {
    return Status::failure(StatusCode::invalid_argument, "a device generation must be non-zero");
  }
  if (request.requested_at > model.tick) {
    return Status::failure(StatusCode::out_of_range,
                           "registration instant is later than the logical clock");
  }
  model.devices.push_back(detail::DeviceRecord{.id = request.device,
                                               .generation = request.generation,
                                               .revision = StateRevision::from(1),
                                               .room = request.room,
                                               .lifecycle = DeviceLifecycle::provisioned,
                                               .row = request.row});
  audit(AuditKind::device_registered, request.device.str(),
        "generation " + request.generation.to_string());
  return publish();
}

Status AirflowControlEngine::Impl::set_device_lifecycle(const SetLifecycleRequest& request) {
  DeviceRecord* target = device_mut(request.device);
  if (target == nullptr) {
    return Status::failure(StatusCode::not_found,
                           "device " + request.device.str() + " is not registered");
  }
  if (target->generation != request.generation) {
    return Status::failure(StatusCode::generation_mismatch,
                           "request names device generation " + request.generation.to_string() +
                               " but the device is at " + target->generation.to_string());
  }
  if (request.epoch.is_zero() || request.epoch != model.epoch) {
    return Status::failure(StatusCode::epoch_stale,
                           "request epoch is not the adopted epoch " + model.epoch.to_string());
  }
  if (target->revision != request.expected_revision) {
    return Status::failure(StatusCode::revision_mismatch,
                           "request expects revision " + request.expected_revision.to_string() +
                               " but the device is at " + target->revision.to_string());
  }
  if (!is_declared_transition(target->lifecycle, request.target)) {
    return Status::failure(StatusCode::transition_invalid,
                           std::string("transition from ") + to_string(target->lifecycle) + " to " +
                               to_string(request.target) + " is not declared");
  }
  if (request.target != target->lifecycle) {
    if (requires_maintenance_override(request.target) &&
        !has_live_maintenance_override(*target, request.epoch, request.requested_at)) {
      return Status::failure(
          StatusCode::lifecycle_forbidden,
          "entering maintenance requires a live, scoped, in-epoch maintenance override");
    }
    std::string detail;
    const StatusCode permission = permission_code(*target, request.epoch, request.requested_at,
                                                  ControlAction::set_lifecycle, detail);
    if (permission != StatusCode::ok) {
      return Status::failure(permission, detail);
    }
  }
  const DeviceLifecycle previous = target->lifecycle;
  target->lifecycle = request.target;
  Result<StateRevision> advanced = target->revision.next();
  if (advanced.ok()) {
    target->revision = advanced.value();
  }
  audit(AuditKind::lifecycle_changed, request.device.str(),
        std::string(to_string(previous)) + " -> " + to_string(request.target));
  return publish();
}

Status AirflowControlEngine::Impl::set_fan_policy(const SetFanPolicyRequest& request) {
  DeviceRecord* target = device_mut(request.device);
  if (target == nullptr) {
    return Status::failure(StatusCode::not_found,
                           "device " + request.device.str() + " is not registered");
  }
  if (target->generation != request.generation) {
    return Status::failure(StatusCode::generation_mismatch,
                           "request names device generation " + request.generation.to_string() +
                               " but the device is at " + target->generation.to_string());
  }
  if (request.policy.generation.is_zero()) {
    return Status::failure(StatusCode::invalid_argument, "a policy generation must be non-zero");
  }
  if (request.policy.envelope.evidence_generation.is_zero()) {
    return Status::failure(StatusCode::invalid_argument,
                           "an envelope must carry the evidence generation it was derived from");
  }
  if (target->revision != request.expected_revision) {
    return Status::failure(StatusCode::revision_mismatch,
                           "request expects revision " + request.expected_revision.to_string() +
                               " but the device is at " + target->revision.to_string());
  }
  if (target->policy.has_value() && request.policy.generation <= target->policy->generation) {
    return Status::failure(StatusCode::revision_mismatch,
                           "policy generation " + request.policy.generation.to_string() +
                               " is not newer than the policy in force, generation " +
                               target->policy->generation.to_string());
  }
  Status valid = validate_envelope(request.policy.envelope);
  if (!valid.ok()) {
    return valid;
  }
  std::string detail;
  if (request.policy.envelope.source.str().empty()) {
    return Status::failure(StatusCode::invalid_argument,
                           "an envelope must name the authority that owns its limits");
  }
  const StatusCode permission = permission_code(*target, model.epoch, model.tick,
                                                ControlAction::set_policy, detail);
  if (permission != StatusCode::ok) {
    return Status::failure(permission, detail);
  }
  target->policy = request.policy;
  Result<StateRevision> advanced = target->revision.next();
  if (advanced.ok()) {
    target->revision = advanced.value();
  }
  audit(AuditKind::policy_set, request.device.str(),
        "policy " + request.policy.id.str() + " generation " +
            request.policy.generation.to_string());
  return publish();
}

Status AirflowControlEngine::Impl::define_pressure_relationship(
    const DefineRelationshipRequest& request) {
  if (request.controlled_space == request.reference_space) {
    return Status::failure(StatusCode::invalid_argument,
                           "a pressure relationship needs two distinct spaces");
  }
  if (request.evidence_generation.is_zero()) {
    return Status::failure(StatusCode::invalid_argument,
                           "a pressure relationship must carry an evidence generation");
  }
  Result<PressureBand> band = PressureBand::create(request.polarity, request.lower, request.upper,
                                                   request.tolerance);
  if (!band.ok()) {
    return band.status();
  }
  detail::RelationshipRecord* existing = detail::find_relationship(model, request.id);
  if (existing == nullptr) {
    if (model.relationships.size() >= ModelBounds::max_pressure_relationships) {
      return Status::failure(StatusCode::bounds_exceeded,
                             "the model already holds the structural maximum of pressure "
                             "relationships");
    }
    if (request.expected_revision.has_value()) {
      return Status::failure(StatusCode::revision_mismatch,
                             "the relationship does not exist, so no revision can be expected");
    }
    PressureRelationship relationship{request.id,
                                       request.room,
                                       request.controlled_space,
                                       request.reference_space,
                                       band.value(),
                                       request.evidence_generation,
                                       StateRevision::from(1)};
    model.relationships.push_back(detail::RelationshipRecord{std::move(relationship), {}});
  } else {
    if (request.expected_revision.has_value() &&
        *request.expected_revision != existing->relationship.revision) {
      return Status::failure(StatusCode::revision_mismatch,
                             "request expects revision " +
                                 request.expected_revision->to_string() +
                                 " but the relationship is at " +
                                 existing->relationship.revision.to_string());
    }
    if (request.evidence_generation < existing->relationship.evidence_generation) {
      return Status::failure(StatusCode::evidence_generation_mismatch,
                             "evidence generation " + request.evidence_generation.to_string() +
                                 " is older than the relationship's " +
                                 existing->relationship.evidence_generation.to_string());
    }
    const EvidenceGeneration previous_generation = existing->relationship.evidence_generation;
    existing->relationship.room = request.room;
    existing->relationship.controlled_space = request.controlled_space;
    existing->relationship.reference_space = request.reference_space;
    existing->relationship.band = band.value();
    existing->relationship.evidence_generation = request.evidence_generation;
    Result<StateRevision> advanced = existing->relationship.revision.next();
    if (advanced.ok()) {
      existing->relationship.revision = advanced.value();
    }
    if (request.evidence_generation != previous_generation) {
      // Evidence gathered against a superseded evidence generation is not
      // evidence about the new one.
      existing->evidence.clear();
    }
  }
  audit(AuditKind::relationship_defined, request.id.str(),
        std::string(to_string(request.polarity)) + " " + band.value().to_string());
  return publish();
}

Status AirflowControlEngine::Impl::define_containment_element(
    const DefineContainmentRequest& request) {
  detail::ContainmentRecord* existing = detail::find_containment(model, request.id);
  if (existing == nullptr) {
    if (model.containment.size() >= ModelBounds::max_containment_elements) {
      return Status::failure(StatusCode::bounds_exceeded,
                             "the model already holds the structural maximum of containment "
                             "elements");
    }
    if (request.expected_revision.has_value()) {
      return Status::failure(StatusCode::revision_mismatch,
                             "the element does not exist, so no revision can be expected");
    }
    ContainmentElement element{request.id, request.room, request.row, request.kind,
                               StateRevision::from(1)};
    model.containment.push_back(detail::ContainmentRecord{
        .element = std::move(element),
        .source = SourceId::parse("unreported").value(),
        .evidence_generation = EvidenceGeneration::from(0)});
  } else {
    if (request.expected_revision.has_value() &&
        *request.expected_revision != existing->element.revision) {
      return Status::failure(StatusCode::revision_mismatch,
                             "request expects revision " +
                                 request.expected_revision->to_string() +
                                 " but the element is at " + existing->element.revision.to_string());
    }
    existing->element.room = request.room;
    existing->element.row = request.row;
    existing->element.kind = request.kind;
    Result<StateRevision> advanced = existing->element.revision.next();
    if (advanced.ok()) {
      existing->element.revision = advanced.value();
    }
  }
  audit(AuditKind::containment_defined, request.id.str(), to_string(request.kind));
  return publish();
}

Status AirflowControlEngine::Impl::report_containment(const ReportContainmentRequest& request) {
  detail::ContainmentRecord* record = detail::find_containment(model, request.element);
  if (record == nullptr) {
    return Status::failure(StatusCode::not_found,
                           "containment element " + request.element.str() + " is not declared");
  }
  if (request.sequence <= record->sequence && record->has_report) {
    return Status::failure(StatusCode::evidence_out_of_order,
                           "containment report sequence " + request.sequence.to_string() +
                               " is not after the retained sequence " +
                               record->sequence.to_string());
  }
  if (request.measured_at > model.tick) {
    return Status::failure(StatusCode::evidence_future,
                           "containment report is measured later than the logical clock");
  }
  record->source = request.source;
  record->evidence_generation = request.evidence_generation;
  record->sequence = request.sequence;
  record->measured_at = request.measured_at;
  record->quality = request.quality;
  record->has_report = true;
  // Only good evidence may establish a containment condition. A suspect, bad,
  // or unknown report is recorded and reduces the element to unknown, because
  // an unverifiable claim that containment is intact is not evidence that it
  // is.
  record->state = request.quality == Quality::good ? request.state : ContainmentState::unknown;
  audit(AuditKind::containment_reported, request.element.str(),
        std::string("quality ") + to_string(request.quality) + ", state " +
            to_string(record->state));
  return publish();
}

Status AirflowControlEngine::Impl::declare_obligation(const DeclareObligationRequest& request) {
  if (request.minimum_airflow > request.target_airflow) {
    return Status::failure(StatusCode::invalid_argument,
                           "the minimum airflow exceeds the target airflow");
  }
  if (!is_representable(request.minimum_airflow) || !is_representable(request.target_airflow)) {
    return Status::failure(StatusCode::bounds_exceeded,
                           "an obligation airflow is outside the representable range");
  }
  if (request.binding == ObligationBinding::metered_scope && !request.metered_point.has_value()) {
    return Status::failure(StatusCode::invalid_argument,
                           "a metered obligation must name its metered point");
  }
  if (request.binding == ObligationBinding::device_sum && request.devices.empty()) {
    return Status::failure(StatusCode::invalid_argument,
                           "a device-sum obligation must bind at least one device");
  }
  if (request.devices.size() > ModelBounds::max_devices_per_obligation) {
    return Status::failure(StatusCode::bounds_exceeded,
                           "an obligation may bind at most " +
                               std::to_string(ModelBounds::max_devices_per_obligation) + " devices");
  }
  for (const AirflowDeviceId& bound : request.devices) {
    if (detail::find_device(model, bound) == nullptr) {
      return Status::failure(StatusCode::not_found,
                             "obligation binds device " + bound.str() +
                                 ", which is not registered");
    }
  }
  if (request.scope == ObligationScope::row && !request.row.has_value()) {
    return Status::failure(StatusCode::invalid_argument, "a row obligation must name its row");
  }
  if (request.scope == ObligationScope::rack && !request.rack.has_value()) {
    return Status::failure(StatusCode::invalid_argument, "a rack obligation must name its rack");
  }
  if (request.evidence_generation.is_zero()) {
    return Status::failure(StatusCode::invalid_argument,
                           "an obligation must carry an evidence generation");
  }
  std::vector<AirflowDeviceId> devices = request.devices;
  std::sort(devices.begin(), devices.end());
  for (std::size_t index = 1; index < devices.size(); ++index) {
    if (devices[index - 1] == devices[index]) {
      return Status::failure(StatusCode::duplicate_identity,
                             "an obligation names the same device twice");
    }
  }

  detail::ObligationRecord* slot = nullptr;
  for (detail::ObligationRecord& record : model.obligations) {
    if (record.obligation.id == request.id) {
      slot = &record;
      break;
    }
  }
  if (slot == nullptr) {
    if (model.obligations.size() >= ModelBounds::max_obligations) {
      return Status::failure(StatusCode::bounds_exceeded,
                             "the model already holds the structural maximum of obligations");
    }
    AirflowObligation obligation{request.id,
                                 request.scope,
                                 request.room,
                                 request.row,
                                 request.rack,
                                 request.klass,
                                 request.binding,
                                 request.metered_point,
                                 devices,
                                 request.minimum_airflow,
                                 request.target_airflow,
                                 request.source,
                                 request.evidence_generation,
                                 StateRevision::from(1)};
    model.obligations.push_back(detail::ObligationRecord{std::move(obligation), std::nullopt});
  } else {
    if (request.expected_revision.has_value() &&
        *request.expected_revision != slot->obligation.revision) {
      return Status::failure(StatusCode::revision_mismatch,
                             "request expects revision " +
                                 request.expected_revision->to_string() +
                                 " but the obligation is at " +
                                 slot->obligation.revision.to_string());
    }
    AirflowObligation& obligation = slot->obligation;
    const bool point_changed = obligation.metered_point.has_value() != request.metered_point.has_value() ||
                               (obligation.metered_point.has_value() &&
                                !(*obligation.metered_point == *request.metered_point));
    obligation.scope = request.scope;
    obligation.room = request.room;
    obligation.row = request.row;
    obligation.rack = request.rack;
    obligation.klass = request.klass;
    obligation.binding = request.binding;
    obligation.metered_point = request.metered_point;
    obligation.devices = devices;
    obligation.minimum_airflow = request.minimum_airflow;
    obligation.target_airflow = request.target_airflow;
    obligation.source = request.source;
    obligation.evidence_generation = request.evidence_generation;
    Result<StateRevision> advanced = obligation.revision.next();
    if (advanced.ok()) {
      obligation.revision = advanced.value();
    }
    if (point_changed) {
      slot->metered_observation.reset();
    }
  }
  audit(AuditKind::obligation_declared, request.id.str(), request.minimum_airflow.to_string());
  return publish();
}

Status AirflowControlEngine::Impl::declare_interlock(const DeclareInterlockRequest& request) {
  for (const Interlock& existing : model.interlocks) {
    if (existing.id == request.id) {
      return Status::failure(StatusCode::duplicate_identity,
                             "interlock " + request.id.str() + " is already declared");
    }
  }
  if (model.interlocks.size() >= ModelBounds::max_interlocks) {
    return Status::failure(StatusCode::bounds_exceeded,
                           "the model already holds the structural maximum of interlocks");
  }
  if (request.epoch.is_zero()) {
    return Status::failure(StatusCode::invalid_argument, "an interlock must carry an epoch");
  }
  if (request.declared_at > model.tick) {
    return Status::failure(StatusCode::out_of_range,
                           "declaration instant is later than the logical clock");
  }
  model.interlocks.push_back(Interlock{request.id,
                                       request.room,
                                       request.row,
                                       request.device,
                                       request.klass,
                                       InterlockState::unknown,
                                       EvidenceSequence::from(0),
                                       request.epoch,
                                       request.declared_at,
                                       std::nullopt});
  audit(AuditKind::interlock_declared, request.id.str(), to_string(request.klass));
  return publish();
}

Status AirflowControlEngine::Impl::report_interlock(const ReportInterlockRequest& request) {
  Interlock* found = nullptr;
  for (Interlock& interlock : model.interlocks) {
    if (interlock.id == request.id) {
      found = &interlock;
      break;
    }
  }
  if (found == nullptr) {
    return Status::failure(StatusCode::not_found,
                           "interlock " + request.id.str() + " is not declared");
  }
  if (request.epoch.is_zero() || request.epoch != model.epoch) {
    return Status::failure(StatusCode::epoch_stale,
                           "interlock report epoch is not the adopted epoch");
  }
  if (found->reported_at.has_value() && request.sequence <= found->sequence) {
    return Status::failure(StatusCode::evidence_out_of_order,
                           "interlock report sequence " + request.sequence.to_string() +
                               " is not after the retained sequence " +
                               found->sequence.to_string());
  }
  if (request.reported_at > model.tick) {
    return Status::failure(StatusCode::evidence_future,
                           "interlock report is later than the logical clock");
  }
  found->state = request.state;
  found->sequence = request.sequence;
  found->epoch = request.epoch;
  found->reported_at = request.reported_at;
  audit(AuditKind::interlock_reported, request.id.str(), to_string(request.state));
  return publish();
}

Status AirflowControlEngine::Impl::add_grant(const PermissionGrant& grant, const ActorId& actor,
                                             LogicalTick at) {
  for (const PermissionGrant& existing : model.grants) {
    if (existing.id == grant.id) {
      return Status::failure(StatusCode::duplicate_identity,
                             "grant " + grant.id.str() + " is already declared");
    }
  }
  if (model.grants.size() >= ModelBounds::max_grants) {
    return Status::failure(StatusCode::bounds_exceeded,
                           "the model already holds the structural maximum of grants");
  }
  if (grant.epoch.is_zero()) {
    return Status::failure(StatusCode::invalid_argument, "a grant must carry an epoch");
  }
  if (grant.actions.empty()) {
    return Status::failure(StatusCode::invalid_argument, "a grant must permit at least one action");
  }
  if (grant.device_generation.has_value() && !grant.device.has_value()) {
    return Status::failure(StatusCode::invalid_argument,
                           "a grant bound to a device generation must name its device");
  }
  if (grant.expires_at.has_value() && *grant.expires_at < grant.issued_at) {
    return Status::failure(StatusCode::invalid_argument, "a grant expires before it is issued");
  }
  if (grant.issued_at > model.tick) {
    return Status::failure(StatusCode::out_of_range,
                           "a grant cannot be issued later than the logical clock");
  }
  (void)actor;
  (void)at;
  model.grants.push_back(grant);
  audit(AuditKind::grant_added, grant.id.str(), grant.actions.to_string());
  return publish();
}

Status AirflowControlEngine::Impl::revoke_grant(const GrantId& grant, AuthorityEpoch epoch,
                                                const ActorId& actor, LogicalTick at) {
  for (PermissionGrant& existing : model.grants) {
    if (!(existing.id == grant)) {
      continue;
    }
    if (epoch != model.epoch) {
      return Status::failure(StatusCode::epoch_stale,
                             "revocation epoch is not the adopted epoch");
    }
    if (existing.revoked) {
      return Status::success();
    }
    existing.revoked = true;
    audit(AuditKind::grant_revoked, grant.str(), "revoked by " + actor.str());
    (void)at;
    return publish();
  }
  return Status::failure(StatusCode::not_found, "grant " + grant.str() + " is not declared");
}

Status AirflowControlEngine::Impl::add_maintenance_override(const MaintenanceOverride& entry,
                                                            const ActorId& actor, LogicalTick at) {
  for (const MaintenanceOverride& existing : model.overrides) {
    if (existing.id == entry.id) {
      return Status::failure(StatusCode::duplicate_identity,
                             "override " + entry.id.str() + " is already declared");
    }
  }
  if (model.overrides.size() >= ModelBounds::max_overrides) {
    return Status::failure(StatusCode::bounds_exceeded,
                           "the model already holds the structural maximum of overrides");
  }
  if (entry.epoch.is_zero()) {
    return Status::failure(StatusCode::invalid_argument, "an override must carry an epoch");
  }
  if (entry.expires_at < entry.issued_at) {
    return Status::failure(StatusCode::invalid_argument, "an override expires before it is issued");
  }
  if (detail::find_device(model, entry.device) == nullptr) {
    return Status::failure(StatusCode::not_found,
                           "override names device " + entry.device.str() +
                               ", which is not registered");
  }
  (void)actor;
  (void)at;
  model.overrides.push_back(entry);
  audit(AuditKind::override_added, entry.id.str(), entry.reason);
  return publish();
}

Status AirflowControlEngine::Impl::revoke_maintenance_override(const OverrideId& id,
                                                               const ActorId& actor, LogicalTick at) {
  for (MaintenanceOverride& entry : model.overrides) {
    if (!(entry.id == id)) {
      continue;
    }
    if (entry.revoked) {
      return Status::success();
    }
    entry.revoked = true;
    audit(AuditKind::override_revoked, id.str(), "revoked by " + actor.str());
    (void)at;
    return publish();
  }
  return Status::failure(StatusCode::not_found, "override " + id.str() + " is not declared");
}

Status AirflowControlEngine::Impl::add_safety_permit(const SafetyPermit& permit, const ActorId& actor,
                                                     LogicalTick at) {
  for (const SafetyPermit& existing : model.permits) {
    if (existing.id == permit.id) {
      return Status::failure(StatusCode::duplicate_identity,
                             "safety permit " + permit.id.str() + " is already declared");
    }
  }
  if (model.permits.size() >= ModelBounds::max_safety_permits) {
    return Status::failure(StatusCode::bounds_exceeded,
                           "the model already holds the structural maximum of safety permits");
  }
  if (permit.epoch.is_zero()) {
    return Status::failure(StatusCode::invalid_argument, "a safety permit must carry an epoch");
  }
  if (permit.expires_at.has_value() && *permit.expires_at < permit.issued_at) {
    return Status::failure(StatusCode::invalid_argument,
                           "a safety permit expires before it is issued");
  }
  if (detail::find_device(model, permit.device) == nullptr) {
    return Status::failure(StatusCode::not_found,
                           "safety permit names device " + permit.device.str() +
                               ", which is not registered");
  }
  (void)actor;
  (void)at;
  model.permits.push_back(permit);
  audit(AuditKind::safety_permit_added, permit.id.str(), permit.reason);
  return publish();
}

Status AirflowControlEngine::Impl::resolve_attempt(const ResolveAttemptRequest& request) {
  AttemptRecord* attempt = detail::find_attempt(model, request.attempt);
  if (attempt == nullptr) {
    return Status::failure(StatusCode::not_found,
                           "attempt " + request.attempt.to_string() + " is not in the journal");
  }
  if (request.target != AttemptState::resolved_without_effect &&
      request.target != AttemptState::superseded) {
    return Status::failure(StatusCode::invalid_argument,
                           std::string("an attempt cannot be resolved as ") +
                               to_string(request.target));
  }
  if (!is_unresolved(attempt->state)) {
    return Status::failure(StatusCode::invalid_argument,
                           std::string("attempt ") + request.attempt.to_string() + " is already " +
                               to_string(attempt->state));
  }
  if (request.at > model.tick) {
    return Status::failure(StatusCode::out_of_range,
                           "resolution instant is later than the logical clock");
  }
  const std::string reason =
      sanitize_text(request.reason.empty() ? std::string("resolved by ") + request.actor.str()
                                           : request.reason,
                    kMaxTextLength);
  const AttemptRecord snapshot = *attempt;
  attempt->state = request.target;
  attempt->resolved_at = request.at;
  attempt->resolution_reason = reason;
  DeviceRecord* target = device_mut(attempt->device);
  const std::optional<AttemptId> unresolved_before =
      target != nullptr ? target->unresolved_attempt : std::nullopt;
  if (target != nullptr && target->unresolved_attempt.has_value() &&
      *target->unresolved_attempt == attempt->id) {
    target->unresolved_attempt.reset();
  }
  audit(AuditKind::attempt_resolved, request.attempt.to_string(),
        std::string(to_string(request.target)) + ": " + reason);
  Status published = publish();
  if (!published.ok()) {
    *attempt = snapshot;
    if (target != nullptr) {
      target->unresolved_attempt = unresolved_before;
    }
    return published;
  }
  return Status::success();
}

// ---------------------------------------------------------------------------
// Inspection
// ---------------------------------------------------------------------------

const char* to_string(PressureState state) noexcept {
  switch (state) {
    case PressureState::satisfied:
      return "satisfied";
    case PressureState::violated:
      return "violated";
    case PressureState::conflicted:
      return "conflicted";
    case PressureState::unknown:
      return "unknown";
  }
  return "unknown";
}

const char* to_string(ObligationState state) noexcept {
  switch (state) {
    case ObligationState::satisfied:
      return "satisfied";
    case ObligationState::unsatisfied:
      return "unsatisfied";
    case ObligationState::unknown:
      return "unknown";
  }
  return "unknown";
}

std::vector<DeviceView> AirflowControlEngine::Impl::device_views() const {
  std::vector<DeviceView> views;
  views.reserve(model.devices.size());
  for (const detail::DeviceRecord& record : model.devices) {
    DeviceView view{record.id, record.generation, record.revision, record.lifecycle, record.room,
                    record.row};
    if (record.policy.has_value()) {
      view.policy_id = record.policy->id;
      view.policy_generation = record.policy->generation;
      view.envelope = record.policy->envelope;
    }
    view.unresolved_attempt = record.unresolved_attempt;
    view.effect = record.effect;
    const FreshnessRequirements requirements{model.tick, options.evidence.max_age_ticks,
                                             record.generation,
                                             policy_evidence_generation(record), false};
    if (record.fan_observation.has_value()) {
      view.has_fan_observation = true;
      view.fan_freshness = assess_freshness(*record.fan_observation, requirements);
      view.observed_fan_percent =
          std::get<FanReading>(record.fan_observation->draft.payload).percent;
    }
    if (record.airflow_observation.has_value()) {
      view.has_airflow_observation = true;
      view.airflow_freshness = assess_freshness(*record.airflow_observation, requirements);
      view.observed_airflow =
          std::get<AirflowReading>(record.airflow_observation->draft.payload).value;
    }
    views.push_back(std::move(view));
  }
  return views;
}

Result<DeviceView> AirflowControlEngine::Impl::device_view(const AirflowDeviceId& id) const {
  for (const DeviceView& view : device_views()) {
    if (view.id == id) {
      return view;
    }
  }
  return Status::failure(StatusCode::not_found, "device " + id.str() + " is not registered");
}

std::vector<RelationshipView> AirflowControlEngine::Impl::relationship_views() const {
  std::vector<RelationshipView> views;
  views.reserve(model.relationships.size());
  for (const detail::RelationshipRecord& record : model.relationships) {
    RelationshipView view{record.relationship.id,
                          record.relationship.room,
                          record.relationship.band.polarity(),
                          record.relationship.band.lower(),
                          record.relationship.band.upper(),
                          record.relationship.band.tolerance(),
                          record.relationship.evidence_generation,
                          record.relationship.revision};
    const FreshnessRequirements requirements{model.tick, options.evidence.max_age_ticks,
                                             DeviceGeneration::from(0),
                                             record.relationship.evidence_generation, false};
    std::optional<Pressure> representative;
    std::size_t contributing = 0;
    // Relationship evidence carries its own device generation per reading, so
    // the view adjudicates each reading against the generation it claims.
    std::vector<const Observation*> fresh;
    for (const Observation& observation : record.evidence) {
      FreshnessRequirements per_reading = requirements;
      per_reading.device_generation = observation.draft.device_generation;
      if (is_proof(assess_freshness(observation, per_reading))) {
        fresh.push_back(&observation);
      }
    }
    contributing = fresh.size();
    if (!fresh.empty()) {
      const PressureBand& band = record.relationship.band;
      bool disagrees = false;
      for (std::size_t outer = 0; outer < fresh.size() && !disagrees; ++outer) {
        for (std::size_t inner = outer + 1; inner < fresh.size(); ++inner) {
          const Pressure lhs = std::get<PressureReading>(fresh[outer]->draft.payload).differential;
          const Pressure rhs = std::get<PressureReading>(fresh[inner]->draft.payload).differential;
          if (band.satisfied_by(lhs) != band.satisfied_by(rhs) ||
              absolute_difference(lhs.millipascals(), rhs.millipascals()) >
                  band.tolerance().millipascals()) {
            disagrees = true;
            break;
          }
        }
      }
      const Observation* chosen = fresh.front();
      std::int64_t chosen_distance =
          band_distance(band, std::get<PressureReading>(chosen->draft.payload).differential);
      for (const Observation* candidate : fresh) {
        const std::int64_t distance =
            band_distance(band, std::get<PressureReading>(candidate->draft.payload).differential);
        if (distance < chosen_distance ||
            (distance == chosen_distance && candidate->draft.source < chosen->draft.source)) {
          chosen = candidate;
          chosen_distance = distance;
        }
      }
      representative = std::get<PressureReading>(chosen->draft.payload).differential;
      if (disagrees) {
        view.state = PressureState::conflicted;
        view.conflicting_sources = fresh.size();
      } else {
        view.state = band.satisfied_by(*representative) ? PressureState::satisfied
                                                        : PressureState::violated;
      }
      view.adjudicated = representative;
    }
    view.contributing_sources = contributing;
    views.push_back(std::move(view));
  }
  return views;
}

Result<RelationshipView> AirflowControlEngine::Impl::relationship_view(
    const PressureRelationshipId& id) const {
  for (const RelationshipView& view : relationship_views()) {
    if (view.id == id) {
      return view;
    }
  }
  return Status::failure(StatusCode::not_found,
                         "pressure relationship " + id.str() + " is not declared");
}

std::vector<ContainmentView> AirflowControlEngine::Impl::containment_views() const {
  std::vector<ContainmentView> views;
  views.reserve(model.containment.size());
  for (const detail::ContainmentRecord& record : model.containment) {
    ContainmentView view{record.element.id,
                         record.element.room,
                         record.element.row,
                         record.element.kind,
                         record.state,
                         record.quality,
                         record.source,
                         record.sequence,
                         record.evidence_generation,
                         record.measured_at,
                         record.has_report};
    views.push_back(std::move(view));
  }
  return views;
}

std::vector<ObligationView> AirflowControlEngine::Impl::obligation_views() const {
  std::vector<ObligationView> views;
  views.reserve(model.obligations.size());
  for (const detail::ObligationRecord& record : model.obligations) {
    ObligationView view{record.obligation.id,
                        record.obligation.scope,
                        record.obligation.room,
                        record.obligation.row,
                        record.obligation.rack,
                        record.obligation.klass,
                        record.obligation.binding,
                        record.obligation.minimum_airflow,
                        record.obligation.target_airflow,
                        record.obligation.evidence_generation,
                        record.obligation.revision};
    const FreshnessRequirements requirements{model.tick, options.evidence.max_age_ticks,
                                             DeviceGeneration::from(0),
                                             record.obligation.evidence_generation, false};
    std::optional<Airflow> observed;
    view.state = obligation_state(record, requirements, observed);
    view.observed_airflow = observed;
    views.push_back(std::move(view));
  }
  return views;
}

std::vector<InterlockView> AirflowControlEngine::Impl::interlock_views() const {
  std::vector<InterlockView> views;
  views.reserve(model.interlocks.size());
  for (const Interlock& interlock : model.interlocks) {
    InterlockView view{interlock.id,  interlock.room,     interlock.klass,
                       interlock.state, interlock.sequence, interlock.epoch,
                       interlock.reported_at.has_value()};
    if (view.has_report && interlock.epoch != model.epoch) {
      view.state = InterlockState::unknown;
    }
    views.push_back(std::move(view));
  }
  return views;
}

std::vector<AttemptView> AirflowControlEngine::Impl::attempt_views() const {
  std::vector<AttemptView> views;
  views.reserve(model.attempts.size());
  for (const AttemptRecord& record : model.attempts) {
    AttemptView view{record, false};
    view.superseded_by_known_attempt = record.superseded_by.has_value();
    views.push_back(std::move(view));
  }
  return views;
}

Result<AttemptRecord> AirflowControlEngine::Impl::attempt_view(AttemptId id) const {
  const AttemptRecord* record = detail::find_attempt(model, id);
  if (record == nullptr) {
    return Status::failure(StatusCode::not_found,
                           "attempt " + id.to_string() + " is not in the journal");
  }
  return *record;
}

std::vector<Observation> AirflowControlEngine::Impl::observations_for(
    const AirflowDeviceId& id) const {
  std::vector<Observation> observations;
  const DeviceRecord* record = device(id);
  if (record == nullptr) {
    return observations;
  }
  if (record->fan_observation.has_value()) {
    observations.push_back(*record->fan_observation);
  }
  if (record->airflow_observation.has_value()) {
    observations.push_back(*record->airflow_observation);
  }
  for (const detail::RelationshipRecord& relationship : model.relationships) {
    for (const Observation& observation : relationship.evidence) {
      if (observation.draft.device == id) {
        observations.push_back(observation);
      }
    }
  }
  std::sort(observations.begin(), observations.end(),
            [](const Observation& lhs, const Observation& rhs) { return lhs.id < rhs.id; });
  return observations;
}

HistoryView AirflowControlEngine::Impl::history_view(std::size_t limit) const {
  HistoryView view;
  view.dropped = model.audit_dropped;
  const std::size_t count = std::min(limit, model.audit.size());
  view.entries.reserve(count);
  for (std::size_t index = model.audit.size() - count; index < model.audit.size(); ++index) {
    view.entries.push_back(model.audit[index]);
  }
  return view;
}

// ---------------------------------------------------------------------------
// Canonical state
// ---------------------------------------------------------------------------

namespace {

void append_observation(std::string& out, const char* label, const Observation& observation) {
  out += "  ";
  out += label;
  out += " id=";
  out += observation.id.to_string();
  out += " source=";
  out += observation.draft.source.str();
  out += " sequence=";
  out += observation.draft.sequence.to_string();
  out += " measured=";
  out += observation.draft.measured_at.to_string();
  out += " accepted=";
  out += observation.accepted_at.to_string();
  out += " quality=";
  out += to_string(observation.draft.quality);
  out += " recovered=";
  out += observation.recovered ? "1" : "0";
  out += " value=";
  out += to_string(observation.draft.payload);
  out += '\n';
}

}  // namespace

std::string AirflowControlEngine::Impl::canonical_state() const {
  std::string out;
  out.reserve(8192);
  out += "airflow-control canonical-state 1\n";
  out += "epoch " + model.epoch.to_string() + "\n";
  out += "tick " + model.tick.to_string() + "\n";
  out += "counters attempt=" + model.next_attempt.to_string() +
         " command=" + model.next_command.to_string() +
         " observation=" + model.next_observation.to_string() +
         " effect=" + model.next_effect.to_string() + "\n";
  out += "retention idempotency-window=" + std::to_string(model.idempotency_window) +
         " attempt-journal=" + std::to_string(model.attempt_journal_capacity) +
         " audit-capacity=" + std::to_string(model.audit_capacity) + "\n";

  const std::vector<detail::DeviceRecord> devices =
      detail::canonical_order(model.devices, [](const detail::DeviceRecord& value) { return value.id; });
  for (const detail::DeviceRecord& record : devices) {
    out += "device " + record.id.str() + " generation=" + record.generation.to_string() +
           " revision=" + record.revision.to_string() + " lifecycle=" + to_string(record.lifecycle) +
           " room=" + record.room.str() + " row=" +
           (record.row.has_value() ? record.row->str() : std::string("-")) + "\n";
    if (record.policy.has_value()) {
      const OperatingEnvelope& envelope = record.policy->envelope;
      out += "  policy " + record.policy->id.str() +
             " generation=" + record.policy->generation.to_string() +
             " min-percent=" + envelope.min_fan_percent.to_string() +
             " max-percent=" + envelope.max_fan_percent.to_string() +
             " default-percent=" + envelope.default_fan_percent.to_string() +
             " min-airflow=" + envelope.min_airflow.to_string() +
             " max-airflow=" + envelope.max_airflow.to_string() +
             " max-step=" + envelope.max_step.to_string() + " source=" + envelope.source.str() +
             " evidence-generation=" + envelope.evidence_generation.to_string() + "\n";
    }
    out += "  unresolved=";
    out += record.unresolved_attempt.has_value() ? record.unresolved_attempt->to_string() : "-";
    out += " effect=";
    out += to_string(record.effect);
    out += '\n';
    if (record.fan_observation.has_value()) {
      append_observation(out, "fan-observation", *record.fan_observation);
    }
    if (record.airflow_observation.has_value()) {
      append_observation(out, "airflow-observation", *record.airflow_observation);
    }
  }

  const std::vector<detail::RelationshipRecord> relationships = detail::canonical_order(
      model.relationships,
      [](const detail::RelationshipRecord& value) { return value.relationship.id; });
  for (const detail::RelationshipRecord& record : relationships) {
    out += "relationship " + record.relationship.id.str() + " room=" + record.relationship.room.str() +
           " controlled=" + record.relationship.controlled_space.str() +
           " reference=" + record.relationship.reference_space.str() + " band=" +
           record.relationship.band.to_string() +
           " evidence-generation=" + record.relationship.evidence_generation.to_string() +
           " revision=" + record.relationship.revision.to_string() + "\n";
    const std::vector<Observation> evidence = detail::canonical_order(
        record.evidence, [](const Observation& value) { return value.draft.source; });
    for (const Observation& observation : evidence) {
      append_observation(out, "evidence", observation);
    }
  }

  const std::vector<detail::ContainmentRecord> containment = detail::canonical_order(
      model.containment, [](const detail::ContainmentRecord& value) { return value.element.id; });
  for (const detail::ContainmentRecord& record : containment) {
    out += "containment " + record.element.id.str() + " room=" + record.element.room.str() + " row=" +
           (record.element.row.has_value() ? record.element.row->str() : std::string("-")) +
           " kind=" + to_string(record.element.kind) +
           " revision=" + record.element.revision.to_string() + " state=" + to_string(record.state) +
           " quality=" + to_string(record.quality) + " source=" + record.source.str() +
           " sequence=" + record.sequence.to_string() +
           " measured=" + record.measured_at.to_string() + " reported=" +
           (record.has_report ? "1" : "0") + " evidence-generation=" +
           record.evidence_generation.to_string() + "\n";
  }

  const std::vector<detail::ObligationRecord> obligations = detail::canonical_order(
      model.obligations, [](const detail::ObligationRecord& value) { return value.obligation.id; });
  for (const detail::ObligationRecord& record : obligations) {
    const AirflowObligation& obligation = record.obligation;
    out += "obligation " + obligation.id.str() + " scope=" + to_string(obligation.scope) +
           " room=" + obligation.room.str() + " row=" +
           (obligation.row.has_value() ? obligation.row->str() : std::string("-")) + " rack=" +
           (obligation.rack.has_value() ? obligation.rack->str() : std::string("-")) +
           " class=" + to_string(obligation.klass) + " binding=" + to_string(obligation.binding) +
           " point=" +
           (obligation.metered_point.has_value() ? obligation.metered_point->str()
                                                 : std::string("-")) +
           " minimum=" + obligation.minimum_airflow.to_string() +
           " target=" + obligation.target_airflow.to_string() + " source=" + obligation.source.str() +
           " evidence-generation=" + obligation.evidence_generation.to_string() +
           " revision=" + obligation.revision.to_string() + " devices=";
    for (const AirflowDeviceId& device : obligation.devices) {
      out += device.str();
      out += ',';
    }
    out += '\n';
    if (record.metered_observation.has_value()) {
      append_observation(out, "metered-observation", *record.metered_observation);
    }
  }

  const std::vector<Interlock> interlocks =
      detail::canonical_order(model.interlocks, [](const Interlock& value) { return value.id; });
  for (const Interlock& interlock : interlocks) {
    out += "interlock " + interlock.id.str() + " room=" + interlock.room.str() + " row=" +
           (interlock.row.has_value() ? interlock.row->str() : std::string("-")) + " device=" +
           (interlock.device.has_value() ? interlock.device->str() : std::string("-")) +
           " class=" + to_string(interlock.klass) + " state=" + to_string(interlock.state) +
           " sequence=" + interlock.sequence.to_string() + " epoch=" + interlock.epoch.to_string() +
           " declared=" + interlock.declared_at.to_string() + " reported=" +
           (interlock.reported_at.has_value() ? interlock.reported_at->to_string()
                                              : std::string("-")) +
           "\n";
  }

  const std::vector<PermissionGrant> grants =
      detail::canonical_order(model.grants, [](const PermissionGrant& value) { return value.id; });
  for (const PermissionGrant& grant : grants) {
    out += "grant " + grant.id.str() + " issuer=" + grant.issuer.str() +
           " epoch=" + grant.epoch.to_string() + " device=" +
           (grant.device.has_value() ? grant.device->str() : std::string("-")) +
           " device-generation=" +
           (grant.device_generation.has_value() ? grant.device_generation->to_string()
                                                : std::string("-")) +
           " room=" + (grant.room.has_value() ? grant.room->str() : std::string("-")) +
           " actions=" + grant.actions.to_string() + " issued=" + grant.issued_at.to_string() +
           " expires=" +
           (grant.expires_at.has_value() ? grant.expires_at->to_string() : std::string("-")) +
           " revoked=" + (grant.revoked ? "1" : "0") + "\n";
  }

  const std::vector<MaintenanceOverride> overrides = detail::canonical_order(
      model.overrides, [](const MaintenanceOverride& value) { return value.id; });
  for (const MaintenanceOverride& entry : overrides) {
    out += "override " + entry.id.str() + " device=" + entry.device.str() +
           " device-generation=" + entry.device_generation.to_string() +
           " epoch=" + entry.epoch.to_string() + " issued=" + entry.issued_at.to_string() +
           " expires=" + entry.expires_at.to_string() + " revoked=" +
           (entry.revoked ? "1" : "0") + " reason=" + entry.reason + "\n";
  }

  const std::vector<SafetyPermit> permits =
      detail::canonical_order(model.permits, [](const SafetyPermit& value) { return value.id; });
  for (const SafetyPermit& permit : permits) {
    out += "permit " + permit.id.str() + " issuer=" + permit.issuer.str() +
           " epoch=" + permit.epoch.to_string() + " device=" + permit.device.str() +
           " issued=" + permit.issued_at.to_string() + " expires=" +
           (permit.expires_at.has_value() ? permit.expires_at->to_string() : std::string("-")) +
           " reason=" + permit.reason + "\n";
  }

  const std::vector<AttemptRecord> attempts =
      detail::canonical_order(model.attempts, [](const AttemptRecord& value) { return value.id; });
  for (const AttemptRecord& attempt : attempts) {
    out += "attempt " + attempt.id.to_string() + " ordinal=" + attempt.ordinal.to_string() +
           " key=" + attempt.key.str() + " device=" + attempt.device.str() +
           " device-generation=" + attempt.device_generation.to_string() +
           " epoch=" + attempt.epoch.to_string() +
           " planned-revision=" + attempt.planned_revision.to_string() +
           " policy-generation=" + attempt.policy_generation.to_string() +
           " evidence-generation=" + attempt.evidence_generation.to_string() +
           " intent=" + to_string(attempt.intent) +
           " class=" + to_string(attempt.request_class) +
           " setpoint=" + to_string(attempt.setpoint) + " actor=" + attempt.actor.str() +
           " accepted=" + attempt.accepted_at.to_string() + " state=" + to_string(attempt.state) +
           " command=" + attempt.command.to_string() +
           " adapter-sequence=" + attempt.adapter_sequence.to_string() +
           " disposition=" + to_string(attempt.disposition) + " permit=" +
           (attempt.safety_permit.has_value() ? attempt.safety_permit->str() : std::string("-")) +
           " supersedes=" +
           (attempt.supersedes.has_value() ? attempt.supersedes->to_string() : std::string("-")) +
           " superseded-by=" +
           (attempt.superseded_by.has_value() ? attempt.superseded_by->to_string()
                                              : std::string("-")) +
           " fan-observation=" +
           (attempt.fan_observation.has_value() ? attempt.fan_observation->to_string()
                                                : std::string("-")) +
           " pressure-observation=" +
           (attempt.pressure_observation.has_value() ? attempt.pressure_observation->to_string()
                                                     : std::string("-")) +
           " effect-sequence=" +
           (attempt.effect_sequence.has_value() ? attempt.effect_sequence->to_string()
                                                : std::string("-")) +
           " detail=" + attempt.detail + "\n";
  }

  const std::vector<detail::IdempotencySlot> slots = detail::canonical_order(
      model.idempotency, [](const detail::IdempotencySlot& value) { return value.key; });
  for (const detail::IdempotencySlot& slot : slots) {
    out += "idempotency " + slot.key.str() + " attempt=" + slot.attempt.to_string() +
           " fingerprint=" + std::to_string(slot.fingerprint) +
           " result=" + to_string(slot.result) + "\n";
  }

  return out;
}

std::string AirflowControlEngine::Impl::state_digest() const {
  const std::string text = canonical_state();
  static constexpr char kHex[] = "0123456789abcdef";
  const std::uint64_t digest =
      detail::fnv1a64(reinterpret_cast<const std::uint8_t*>(text.data()), text.size());
  std::string rendered(16, '0');
  for (std::size_t index = 0; index < 16; ++index) {
    rendered[15 - index] = kHex[(digest >> (index * 4)) & 0xF];
  }
  return rendered;
}

// ---------------------------------------------------------------------------
// Recovery, open, and close
// ---------------------------------------------------------------------------

Status AirflowControlEngine::Impl::recover() {
  // Retention the caller configured is a floor, never a ceiling: a store
  // reopened with a smaller window keeps the replay guarantee it was written
  // under rather than silently losing it.
  model.idempotency_window = std::max(options.idempotency_window, model.idempotency_window);
  model.attempt_journal_capacity = std::max(
      std::max(options.attempt_journal_capacity, model.idempotency_window),
      model.attempt_journal_capacity);
  model.audit_capacity = std::max(options.audit_capacity, model.audit_capacity);

  while (model.attempts.size() > model.attempt_journal_capacity) {
    model.attempts.erase(model.attempts.begin());
  }
  while (model.idempotency.size() > model.idempotency_window) {
    auto oldest = std::min_element(
        model.idempotency.begin(), model.idempotency.end(),
        [](const detail::IdempotencySlot& lhs, const detail::IdempotencySlot& rhs) {
          return lhs.attempt < rhs.attempt;
        });
    if (oldest == model.idempotency.end()) {
      break;
    }
    model.idempotency.erase(oldest);
  }
  while (model.audit.size() > model.audit_capacity) {
    model.audit.erase(model.audit.begin());
    ++model.audit_dropped;
  }

  // Recovered dynamic state is not current physical evidence.
  for (detail::DeviceRecord& device : model.devices) {
    if (device.fan_observation.has_value()) {
      device.fan_observation->recovered = true;
    }
    if (device.airflow_observation.has_value()) {
      device.airflow_observation->recovered = true;
    }
  }
  for (detail::RelationshipRecord& record : model.relationships) {
    for (Observation& observation : record.evidence) {
      observation.recovered = true;
    }
  }
  for (detail::ObligationRecord& record : model.obligations) {
    if (record.metered_observation.has_value()) {
      record.metered_observation->recovered = true;
    }
  }

  std::size_t adopted = 0;
  for (AttemptRecord& attempt : model.attempts) {
    if (attempt.state == AttemptState::accepted || attempt.state == AttemptState::dispatched) {
      attempt.state = AttemptState::recovery_required;
      attempt.detail = sanitize_text(
          "recovered after a process restart: a command may have reached the device, and it was "
          "not re-sent",
          kMaxTextLength);
      ++adopted;
    }
  }

  for (detail::DeviceRecord& device : model.devices) {
    device.unresolved_attempt.reset();
    for (auto entry = model.attempts.rbegin(); entry != model.attempts.rend(); ++entry) {
      if (entry->device == device.id && is_unresolved(entry->state)) {
        device.unresolved_attempt = entry->id;
        break;
      }
    }
  }

  incarnation = model.next_incarnation;
  Result<IncarnationId> advanced = incarnation.next();
  if (advanced.ok()) {
    model.next_incarnation = advanced.value();
  }

  audit(AuditKind::recovery_adopted, std::string(),
        "adopted " + std::to_string(adopted) +
            " attempt(s) at the command-attempt boundary without re-sending them");
  audit(AuditKind::engine_opened, std::string(), "incarnation " + incarnation.to_string());
  return publish();
}

namespace {

[[nodiscard]] Status validate_options(const EngineOptions& options) {
  if (options.idempotency_window == 0 ||
      options.idempotency_window > ModelBounds::max_idempotency_window) {
    return Status::failure(StatusCode::out_of_range,
                           "idempotency window must be between 1 and " +
                               std::to_string(ModelBounds::max_idempotency_window));
  }
  if (options.audit_capacity == 0 || options.audit_capacity > ModelBounds::max_audit_capacity) {
    return Status::failure(StatusCode::out_of_range,
                           "audit capacity must be between 1 and " +
                               std::to_string(ModelBounds::max_audit_capacity));
  }
  if (options.attempt_journal_capacity > ModelBounds::max_attempts) {
    return Status::failure(StatusCode::out_of_range,
                           "attempt journal capacity must not exceed " +
                               std::to_string(ModelBounds::max_attempts));
  }
  if (options.evidence.max_relationship_sources == 0 ||
      options.evidence.max_relationship_sources > ModelBounds::max_sources_per_relationship) {
    return Status::failure(StatusCode::out_of_range,
                           "relationship source bound must be between 1 and " +
                               std::to_string(ModelBounds::max_sources_per_relationship));
  }
  if (options.evidence.setpoint_tolerance_basis_points > SetpointBasisPoints::kFull) {
    return Status::failure(StatusCode::out_of_range,
                           "setpoint tolerance must not exceed 10000 basis points");
  }
  if (options.evidence.airflow_tolerance_cubic_metres_per_hour < 0) {
    return Status::failure(StatusCode::out_of_range, "airflow tolerance must not be negative");
  }
  if (options.store.slot_capacity_bytes < kMinSlotCapacityBytes ||
      options.store.slot_capacity_bytes > kMaxSlotCapacityBytes) {
    return Status::failure(StatusCode::out_of_range,
                           "slot capacity must be between " +
                               std::to_string(kMinSlotCapacityBytes) + " and " +
                               std::to_string(kMaxSlotCapacityBytes) + " bytes");
  }
  return Status::success();
}

}  // namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

AirflowControlEngine::AirflowControlEngine() = default;
AirflowControlEngine::~AirflowControlEngine() = default;
AirflowControlEngine::AirflowControlEngine(AirflowControlEngine&& other) noexcept = default;
AirflowControlEngine& AirflowControlEngine::operator=(AirflowControlEngine&& other) noexcept =
    default;

ActuationAuthorization AirflowControlEngine::make_authorization(
    CommandId command, AttemptId attempt, AirflowDeviceId device, DeviceGeneration device_generation,
    AuthorityEpoch epoch, StateRevision planned_revision, PolicyGeneration policy_generation,
    EvidenceGeneration evidence_generation, LogicalTick issued_at) {
  return ActuationAuthorization(command, attempt, std::move(device), device_generation, epoch,
                                planned_revision, policy_generation, evidence_generation, issued_at);
}

Result<AirflowControlEngine> AirflowControlEngine::open(const std::string& path, OpenMode mode,
                                                        const EngineOptions& options) {
  Status validation = validate_options(options);
  if (!validation.ok()) {
    return validation;
  }
  Result<DurableStore> store = DurableStore::open(path, mode, options.store);
  if (!store.ok()) {
    return store.status();
  }

  AirflowControlEngine engine;
  engine.impl_ = std::make_unique<Impl>();
  engine.impl_->options = options;
  engine.impl_->durable = true;
  engine.impl_->store = std::move(store).value();

  const std::vector<std::uint8_t>& payload = engine.impl_->store.payload();
  if (payload.empty()) {
    engine.impl_->model = detail::empty_model();
  } else {
    Result<detail::ModelState> decoded = detail::decode_model(payload.data(), payload.size());
    if (!decoded.ok()) {
      return decoded.status();
    }
    engine.impl_->model = std::move(decoded).value();
  }

  Status recovered = engine.impl_->recover();
  if (!recovered.ok()) {
    return recovered;
  }
  engine.impl_->opened = true;
  return engine;
}

Result<AirflowControlEngine> AirflowControlEngine::open_in_memory(const EngineOptions& options) {
  Status validation = validate_options(options);
  if (!validation.ok()) {
    return validation;
  }
  AirflowControlEngine engine;
  engine.impl_ = std::make_unique<Impl>();
  engine.impl_->options = options;
  engine.impl_->durable = false;
  engine.impl_->model = detail::empty_model();
  Status recovered = engine.impl_->recover();
  if (!recovered.ok()) {
    return recovered;
  }
  engine.impl_->opened = true;
  return engine;
}

bool AirflowControlEngine::is_open() const noexcept { return impl_ != nullptr && impl_->opened; }
bool AirflowControlEngine::is_durable() const noexcept { return impl_ != nullptr && impl_->durable; }

Status AirflowControlEngine::close() {
  if (impl_ == nullptr) {
    return Status::success();
  }
  Status result = Status::success();
  if (impl_->opened) {
    impl_->audit(AuditKind::engine_closed, std::string(), "engine closed");
    Status published = impl_->publish();
    if (!published.ok()) {
      result = published;
    }
    impl_->opened = false;
  }
  Status closed = impl_->store.close();
  if (result.ok()) {
    result = closed;
  }
  impl_.reset();
  return result;
}

const EngineOptions& AirflowControlEngine::options() const noexcept {
  static const EngineOptions kDefault{};
  if (impl_ == nullptr) {
    return kDefault;
  }
  return impl_->options;
}

#define AIRFLOW_CONTROL_REQUIRE_OPEN()                       \
  if (impl_ == nullptr || !impl_->opened) {                  \
    return Status::failure(StatusCode::not_open,             \
                           "the engine is not open");        \
  }

#define AIRFLOW_CONTROL_REQUIRE_OPEN_RESULT(type)             \
  if (impl_ == nullptr || !impl_->opened) {                   \
    return Status::failure(StatusCode::not_open,              \
                           "the engine is not open");         \
  }

Status AirflowControlEngine::adopt_epoch(AuthorityEpoch epoch, const ActorId& actor, LogicalTick at) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->adopt_epoch(epoch, actor, at);
}

Status AirflowControlEngine::advance_tick(LogicalTick tick) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->advance_tick(tick);
}

LogicalTick AirflowControlEngine::current_tick() const noexcept {
  return impl_ == nullptr ? LogicalTick::from(0) : impl_->model.tick;
}

AuthorityEpoch AirflowControlEngine::current_epoch() const noexcept {
  return impl_ == nullptr ? AuthorityEpoch::from(0) : impl_->model.epoch;
}

Status AirflowControlEngine::register_device(const RegisterDeviceRequest& request) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->register_device(request);
}

Status AirflowControlEngine::set_device_lifecycle(const SetLifecycleRequest& request) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->set_device_lifecycle(request);
}

Status AirflowControlEngine::set_fan_policy(const SetFanPolicyRequest& request) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->set_fan_policy(request);
}

Status AirflowControlEngine::define_pressure_relationship(const DefineRelationshipRequest& request) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->define_pressure_relationship(request);
}

Status AirflowControlEngine::define_containment_element(const DefineContainmentRequest& request) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->define_containment_element(request);
}

Status AirflowControlEngine::report_containment(const ReportContainmentRequest& request) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->report_containment(request);
}

Status AirflowControlEngine::declare_obligation(const DeclareObligationRequest& request) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->declare_obligation(request);
}

Status AirflowControlEngine::declare_interlock(const DeclareInterlockRequest& request) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->declare_interlock(request);
}

Status AirflowControlEngine::report_interlock(const ReportInterlockRequest& request) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->report_interlock(request);
}

Status AirflowControlEngine::add_grant(const PermissionGrant& grant, const ActorId& actor,
                                       LogicalTick at) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->add_grant(grant, actor, at);
}

Status AirflowControlEngine::revoke_grant(const GrantId& grant, AuthorityEpoch epoch,
                                          const ActorId& actor, LogicalTick at) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->revoke_grant(grant, epoch, actor, at);
}

Status AirflowControlEngine::add_maintenance_override(const MaintenanceOverride& override_entry,
                                                      const ActorId& actor, LogicalTick at) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->add_maintenance_override(override_entry, actor, at);
}

Status AirflowControlEngine::revoke_maintenance_override(const OverrideId& override_id,
                                                         const ActorId& actor, LogicalTick at) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->revoke_maintenance_override(override_id, actor, at);
}

Status AirflowControlEngine::add_safety_permit(const SafetyPermit& permit, const ActorId& actor,
                                               LogicalTick at) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->add_safety_permit(permit, actor, at);
}

Result<Observation> AirflowControlEngine::observe(const ObservationDraft& draft,
                                                  LogicalTick accepted_at) {
  AIRFLOW_CONTROL_REQUIRE_OPEN_RESULT(Observation);
  Result<Observation> ingested = impl_->ingest(draft, accepted_at, false);
  if (!ingested.ok()) {
    impl_->audit(AuditKind::observation_rejected, draft.device.str(), ingested.message());
    Status ignored = impl_->publish();
    (void)ignored;
    return ingested.status();
  }
  impl_->audit(AuditKind::observation_accepted, draft.device.str(),
               std::string("observation ") + ingested.value().id.to_string() + " from " +
                   draft.source.str());
  Status published = impl_->publish();
  if (!published.ok()) {
    return published;
  }
  return ingested;
}

Result<Decision> AirflowControlEngine::evaluate(const ControlRequest& request) {
  AIRFLOW_CONTROL_REQUIRE_OPEN_RESULT(Decision);
  return impl_->evaluate_impl(request);
}

Result<AttemptRecord> AirflowControlEngine::issue(const ControlRequest& request,
                                                  AirflowAdapter& adapter) {
  AIRFLOW_CONTROL_REQUIRE_OPEN_RESULT(AttemptRecord);
  return impl_->issue_impl(request, adapter);
}

Result<VerifiedEffect> AirflowControlEngine::verify(const VerificationRequest& request) {
  AIRFLOW_CONTROL_REQUIRE_OPEN_RESULT(VerifiedEffect);
  return impl_->verify_impl(request);
}

Status AirflowControlEngine::resolve_attempt(const ResolveAttemptRequest& request) {
  AIRFLOW_CONTROL_REQUIRE_OPEN();
  return impl_->resolve_attempt(request);
}

std::vector<DeviceView> AirflowControlEngine::devices() const {
  return impl_ == nullptr ? std::vector<DeviceView>{} : impl_->device_views();
}

Result<DeviceView> AirflowControlEngine::device(const AirflowDeviceId& id) const {
  AIRFLOW_CONTROL_REQUIRE_OPEN_RESULT(DeviceView);
  return impl_->device_view(id);
}

std::vector<RelationshipView> AirflowControlEngine::relationships() const {
  return impl_ == nullptr ? std::vector<RelationshipView>{} : impl_->relationship_views();
}

Result<RelationshipView> AirflowControlEngine::relationship(const PressureRelationshipId& id) const {
  AIRFLOW_CONTROL_REQUIRE_OPEN_RESULT(RelationshipView);
  return impl_->relationship_view(id);
}

std::vector<ContainmentView> AirflowControlEngine::containment() const {
  return impl_ == nullptr ? std::vector<ContainmentView>{} : impl_->containment_views();
}

std::vector<ObligationView> AirflowControlEngine::obligations() const {
  return impl_ == nullptr ? std::vector<ObligationView>{} : impl_->obligation_views();
}

std::vector<InterlockView> AirflowControlEngine::interlocks() const {
  return impl_ == nullptr ? std::vector<InterlockView>{} : impl_->interlock_views();
}

std::vector<PermissionGrant> AirflowControlEngine::grants() const {
  return impl_ == nullptr ? std::vector<PermissionGrant>{} : impl_->model.grants;
}

std::vector<MaintenanceOverride> AirflowControlEngine::overrides() const {
  return impl_ == nullptr ? std::vector<MaintenanceOverride>{} : impl_->model.overrides;
}

std::vector<SafetyPermit> AirflowControlEngine::safety_permits() const {
  return impl_ == nullptr ? std::vector<SafetyPermit>{} : impl_->model.permits;
}

std::vector<AttemptView> AirflowControlEngine::attempts() const {
  return impl_ == nullptr ? std::vector<AttemptView>{} : impl_->attempt_views();
}

Result<AttemptRecord> AirflowControlEngine::attempt(AttemptId id) const {
  AIRFLOW_CONTROL_REQUIRE_OPEN_RESULT(AttemptRecord);
  return impl_->attempt_view(id);
}

std::vector<Observation> AirflowControlEngine::observations(const AirflowDeviceId& device_id) const {
  return impl_ == nullptr ? std::vector<Observation>{} : impl_->observations_for(device_id);
}

HistoryView AirflowControlEngine::history(std::size_t limit) const {
  return impl_ == nullptr ? HistoryView{} : impl_->history_view(limit);
}

std::uint64_t AirflowControlEngine::dropped_audit_entries() const noexcept {
  return impl_ == nullptr ? 0 : impl_->model.audit_dropped;
}

StoreAudit AirflowControlEngine::store_audit() const {
  return impl_ == nullptr ? StoreAudit{} : impl_->store.audit();
}

std::string AirflowControlEngine::canonical_state() const {
  return impl_ == nullptr ? std::string() : impl_->canonical_state();
}

std::string AirflowControlEngine::state_digest() const {
  return impl_ == nullptr ? std::string() : impl_->state_digest();
}

#undef AIRFLOW_CONTROL_REQUIRE_OPEN
#undef AIRFLOW_CONTROL_REQUIRE_OPEN_RESULT

}  // namespace airflow_control
