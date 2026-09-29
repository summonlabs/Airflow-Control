#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "airflow_control/ids.hpp"

namespace airflow_control {

/// The kind of an audited event.
enum class AuditKind : std::uint32_t {
  engine_opened = 0,
  engine_closed = 1,
  recovery_adopted = 2,
  store_published = 3,
  epoch_adopted = 4,
  clock_advanced = 5,
  device_registered = 6,
  lifecycle_changed = 7,
  policy_set = 8,
  relationship_defined = 9,
  containment_defined = 10,
  containment_reported = 11,
  obligation_declared = 12,
  interlock_declared = 13,
  interlock_reported = 14,
  grant_added = 15,
  grant_revoked = 16,
  override_added = 17,
  override_revoked = 18,
  safety_permit_added = 19,
  observation_accepted = 20,
  observation_rejected = 21,
  control_evaluated = 22,
  attempt_accepted = 23,
  attempt_rejected = 24,
  adapter_outcome = 25,
  attempt_verified = 26,
  attempt_resolved = 27,
  attempt_superseded = 28,
  request_refused = 29,
};

[[nodiscard]] const char* to_string(AuditKind kind) noexcept;
[[nodiscard]] std::optional<AuditKind> parse_audit_kind(std::string_view token) noexcept;

/// One entry in the bounded audit ring.
///
/// An audit entry carries no wall-clock instant: this library never reads the
/// system clock, and a timestamp it invented would not be evidence. A caller
/// that needs wall-clock decoration adds it when it renders the entry.
struct AuditEntry {
  AuditSequence sequence;
  LogicalTick tick;
  AuditKind kind = AuditKind::request_refused;
  std::string subject;
  std::string detail;
};

}  // namespace airflow_control
