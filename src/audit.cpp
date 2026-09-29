#include "airflow_control/audit.hpp"

#include <array>
#include <cstddef>
#include <string_view>

namespace airflow_control {
namespace {

constexpr std::array<const char*, 30> kAuditKindNames{{
    "engine_opened",
    "engine_closed",
    "recovery_adopted",
    "store_published",
    "epoch_adopted",
    "clock_advanced",
    "device_registered",
    "lifecycle_changed",
    "policy_set",
    "relationship_defined",
    "containment_defined",
    "containment_reported",
    "obligation_declared",
    "interlock_declared",
    "interlock_reported",
    "grant_added",
    "grant_revoked",
    "override_added",
    "override_revoked",
    "safety_permit_added",
    "observation_accepted",
    "observation_rejected",
    "control_evaluated",
    "attempt_accepted",
    "attempt_rejected",
    "adapter_outcome",
    "attempt_verified",
    "attempt_resolved",
    "attempt_superseded",
    "request_refused",
}};

}  // namespace

const char* to_string(AuditKind kind) noexcept {
  const auto index = static_cast<std::size_t>(kind);
  if (index >= kAuditKindNames.size()) {
    return "request_refused";
  }
  return kAuditKindNames[index];
}

std::optional<AuditKind> parse_audit_kind(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kAuditKindNames.size(); ++index) {
    if (token == kAuditKindNames[index]) {
      return static_cast<AuditKind>(index);
    }
  }
  return std::nullopt;
}

}  // namespace airflow_control
