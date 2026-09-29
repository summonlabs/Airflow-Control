#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>
#include <vector>

#include "airflow_control/attempt.hpp"
#include "airflow_control/audit.hpp"
#include "airflow_control/authority.hpp"
#include "airflow_control/engine.hpp"
#include "airflow_control/evidence.hpp"
#include "airflow_control/lifecycle.hpp"
#include "airflow_control/model.hpp"

namespace airflow_control::detail {

/// One registered airflow device and everything durable about it.
///
/// The record is an aggregate so that construction is a single explicit
/// designated-initializer expression: there is no partially constructed device
/// state, and no field silently takes a zero because a constructor forgot it.
struct DeviceRecord {
  AirflowDeviceId id;
  DeviceGeneration generation;
  StateRevision revision;
  RoomId room;
  DeviceLifecycle lifecycle = DeviceLifecycle::provisioned;
  std::optional<RowId> row;
  std::optional<FanPolicy> policy;
  std::optional<AttemptId> unresolved_attempt;
  EffectState effect = EffectState::unverified;
  std::optional<Observation> fan_observation;
  std::optional<Observation> airflow_observation;
};

/// One pressure relationship and the evidence retained for it.
struct RelationshipRecord {
  PressureRelationship relationship;
  std::vector<Observation> evidence;
};

/// One containment element and its last accepted report.
struct ContainmentRecord {
  ContainmentElement element;
  SourceId source;
  EvidenceGeneration evidence_generation;
  ContainmentState state = ContainmentState::unknown;
  Quality quality = Quality::unknown;
  EvidenceSequence sequence = EvidenceSequence::from(0);
  LogicalTick measured_at = LogicalTick::from(0);
  bool has_report = false;
};

/// One airflow obligation and, for a metered binding, its retained reading.
struct ObligationRecord {
  AirflowObligation obligation;
  std::optional<Observation> metered_observation;
};

/// One retained idempotency key.
struct IdempotencySlot {
  IdempotencyKey key;
  AttemptId attempt;
  std::uint64_t fingerprint = 0;
  StatusCode result = StatusCode::ok;
};

/// The complete durable model.
///
/// Counters are persisted so that identity never repeats across a restart, and
/// the three bounded rings record the retention they were written with, so a
/// store reopened with a smaller configured bound keeps the guarantee it was
/// written under rather than silently losing replay ability.
struct ModelState {
  AuthorityEpoch epoch;
  LogicalTick tick;
  IncarnationId next_incarnation;
  CommandId next_command;
  AttemptId next_attempt;
  ObservationId next_observation;
  EffectSequence next_effect;
  AuditSequence next_audit;

  // Zero means "this model has no persisted retention yet", so the caller's
  // configured bound is adopted verbatim. Recovery raises these to the larger of
  // the configured bound and the persisted one, which is what makes a reopen with
  // a smaller window keep the replay guarantee the store was written under.
  std::size_t idempotency_window = 0;
  std::size_t attempt_journal_capacity = 0;
  std::size_t audit_capacity = 0;
  std::uint64_t audit_dropped = 0;

  std::vector<DeviceRecord> devices;
  std::vector<RelationshipRecord> relationships;
  std::vector<ContainmentRecord> containment;
  std::vector<ObligationRecord> obligations;
  std::vector<Interlock> interlocks;
  std::vector<PermissionGrant> grants;
  std::vector<MaintenanceOverride> overrides;
  std::vector<SafetyPermit> permits;
  std::vector<AttemptRecord> attempts;
  std::vector<IdempotencySlot> idempotency;
  std::vector<AuditEntry> audit;

  friend bool operator==(const ModelState& lhs, const ModelState& rhs) = default;
};

/// A copy of a collection in the canonical order.
///
/// The durable encoding and the canonical rendering both walk collections in
/// this order, so the same model always produces the same bytes and the same
/// text no matter what order the engine happened to learn about its objects.
template <typename T, typename KeyFn>
[[nodiscard]] inline std::vector<T> canonical_order(const std::vector<T>& values, KeyFn key) {
  std::vector<T> ordered = values;
  std::stable_sort(ordered.begin(), ordered.end(),
                   [&key](const T& lhs, const T& rhs) { return key(lhs) < key(rhs); });
  return ordered;
}

/// A brand-new, empty model. Every counter starts at one so that no identity
/// this build allocates is ever zero, which is reserved for "the model has not
/// allocated one yet".
[[nodiscard]] inline ModelState empty_model() noexcept {
  return ModelState{AuthorityEpoch::from(0),
                    LogicalTick::from(0),
                    IncarnationId::from(1),
                    CommandId::from(1),
                    AttemptId::from(1),
                    ObservationId::from(1),
                    EffectSequence::from(1),
                    AuditSequence::from(1)};
}

/// Looks up a device by identity.
[[nodiscard]] inline DeviceRecord* find_device(ModelState& state, const AirflowDeviceId& id) noexcept {
  for (auto& entry : state.devices) {
    if (entry.id == id) {
      return &entry;
    }
  }
  return nullptr;
}
[[nodiscard]] inline const DeviceRecord* find_device(const ModelState& state,
                                                     const AirflowDeviceId& id) noexcept {
  for (const auto& entry : state.devices) {
    if (entry.id == id) {
      return &entry;
    }
  }
  return nullptr;
}

/// Looks up a relationship by identity.
[[nodiscard]] inline RelationshipRecord* find_relationship(
    ModelState& state, const PressureRelationshipId& id) noexcept {
  for (auto& entry : state.relationships) {
    if (entry.relationship.id == id) {
      return &entry;
    }
  }
  return nullptr;
}
[[nodiscard]] inline const RelationshipRecord* find_relationship(
    const ModelState& state, const PressureRelationshipId& id) noexcept {
  for (const auto& entry : state.relationships) {
    if (entry.relationship.id == id) {
      return &entry;
    }
  }
  return nullptr;
}

/// Looks up a containment element by identity.
[[nodiscard]] inline ContainmentRecord* find_containment(ModelState& state,
                                                         const ContainmentId& id) noexcept {
  for (auto& entry : state.containment) {
    if (entry.element.id == id) {
      return &entry;
    }
  }
  return nullptr;
}
[[nodiscard]] inline const ContainmentRecord* find_containment(const ModelState& state,
                                                               const ContainmentId& id) noexcept {
  for (const auto& entry : state.containment) {
    if (entry.element.id == id) {
      return &entry;
    }
  }
  return nullptr;
}

/// Looks up an attempt in the journal.
[[nodiscard]] inline AttemptRecord* find_attempt(ModelState& state, AttemptId id) noexcept {
  for (auto& entry : state.attempts) {
    if (entry.id == id) {
      return &entry;
    }
  }
  return nullptr;
}
[[nodiscard]] inline const AttemptRecord* find_attempt(const ModelState& state, AttemptId id) noexcept {
  for (const auto& entry : state.attempts) {
    if (entry.id == id) {
      return &entry;
    }
  }
  return nullptr;
}

/// Looks up a retained idempotency slot.
[[nodiscard]] inline IdempotencySlot* find_idempotency(ModelState& state,
                                                       const IdempotencyKey& key) noexcept {
  for (auto& entry : state.idempotency) {
    if (entry.key == key) {
      return &entry;
    }
  }
  return nullptr;
}
[[nodiscard]] inline const IdempotencySlot* find_idempotency(const ModelState& state,
                                                             const IdempotencyKey& key) noexcept {
  for (const auto& entry : state.idempotency) {
    if (entry.key == key) {
      return &entry;
    }
  }
  return nullptr;
}

}  // namespace airflow_control::detail
