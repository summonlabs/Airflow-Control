#include "airflow_control/status.hpp"

#include <array>

namespace airflow_control {
namespace {

struct CodeToken {
  StatusCode code;
  const char* token;
};

// The token list is the machine contract. Adding a code is a compatible change;
// renaming or reusing a token is not, because callers, scripts, and durable
// audit text branch on it. The table is the single place a token is written
// down, so the two directions of the mapping cannot drift apart.
constexpr CodeToken kCodeTokens[] = {
    {StatusCode::ok, "ok"},
    {StatusCode::invalid_argument, "invalid_argument"},
    {StatusCode::out_of_range, "out_of_range"},
    {StatusCode::overflow, "overflow"},
    {StatusCode::bounds_exceeded, "bounds_exceeded"},
    {StatusCode::unsupported, "unsupported"},
    {StatusCode::not_found, "not_found"},
    {StatusCode::duplicate_identity, "duplicate_identity"},
    {StatusCode::identity_mismatch, "identity_mismatch"},
    {StatusCode::lifecycle_forbidden, "lifecycle_forbidden"},
    {StatusCode::transition_invalid, "transition_invalid"},
    {StatusCode::attempt_unresolved, "attempt_unresolved"},
    {StatusCode::recovery_required, "recovery_required"},
    {StatusCode::generation_mismatch, "generation_mismatch"},
    {StatusCode::revision_mismatch, "revision_mismatch"},
    {StatusCode::epoch_stale, "epoch_stale"},
    {StatusCode::permission_missing, "permission_missing"},
    {StatusCode::permission_denied, "permission_denied"},
    {StatusCode::permission_stale, "permission_stale"},
    {StatusCode::safety_permit_missing, "safety_permit_missing"},
    {StatusCode::safety_permit_stale, "safety_permit_stale"},
    {StatusCode::interlock_open, "interlock_open"},
    {StatusCode::interlock_unknown, "interlock_unknown"},
    {StatusCode::containment_breached, "containment_breached"},
    {StatusCode::containment_unknown, "containment_unknown"},
    {StatusCode::containment_open, "containment_open"},
    {StatusCode::obligation_unsatisfied, "obligation_unsatisfied"},
    {StatusCode::obligation_unknown, "obligation_unknown"},
    {StatusCode::limit_invalid, "limit_invalid"},
    {StatusCode::limit_exceeded, "limit_exceeded"},
    {StatusCode::limit_unknown, "limit_unknown"},
    {StatusCode::pressure_unknown, "pressure_unknown"},
    {StatusCode::pressure_band_invalid, "pressure_band_invalid"},
    {StatusCode::pressure_violated, "pressure_violated"},
    {StatusCode::pressure_conflicted, "pressure_conflicted"},
    {StatusCode::evidence_stale, "evidence_stale"},
    {StatusCode::evidence_unknown, "evidence_unknown"},
    {StatusCode::evidence_conflicting, "evidence_conflicting"},
    {StatusCode::evidence_generation_mismatch, "evidence_generation_mismatch"},
    {StatusCode::evidence_out_of_order, "evidence_out_of_order"},
    {StatusCode::evidence_future, "evidence_future"},
    {StatusCode::evidence_quality_insufficient, "evidence_quality_insufficient"},
    {StatusCode::adapter_refused, "adapter_refused"},
    {StatusCode::adapter_unavailable, "adapter_unavailable"},
    {StatusCode::adapter_fault, "adapter_fault"},
    {StatusCode::adapter_indeterminate, "adapter_indeterminate"},
    {StatusCode::adapter_fenced, "adapter_fenced"},
    {StatusCode::idempotency_conflict, "idempotency_conflict"},
    {StatusCode::supersession_unsupported, "supersession_unsupported"},
    {StatusCode::store_missing, "store_missing"},
    {StatusCode::store_corrupt, "store_corrupt"},
    {StatusCode::store_unsupported_version, "store_unsupported_version"},
    {StatusCode::store_capacity_exceeded, "store_capacity_exceeded"},
    {StatusCode::store_io_error, "store_io_error"},
    {StatusCode::store_fenced, "store_fenced"},
    {StatusCode::rollback_detected, "rollback_detected"},
    {StatusCode::lock_denied, "lock_denied"},
    {StatusCode::busy, "busy"},
    {StatusCode::path_invalid, "path_invalid"},
    {StatusCode::not_open, "not_open"},
    {StatusCode::internal_error, "internal_error"},
};

static_assert(std::size(kCodeTokens) == 61, "every status code needs exactly one token");

}  // namespace

const char* to_string(StatusCode code) noexcept {
  for (const CodeToken& entry : kCodeTokens) {
    if (entry.code == code) {
      return entry.token;
    }
  }
  return "internal_error";
}

std::optional<StatusCode> parse_status_code(std::string_view token) noexcept {
  for (const CodeToken& entry : kCodeTokens) {
    if (token == entry.token) {
      return entry.code;
    }
  }
  return std::nullopt;
}

bool is_success(StatusCode code) noexcept { return code == StatusCode::ok; }

std::string Status::to_string() const {
  std::string rendered = airflow_control::to_string(code_);
  rendered += ": ";
  rendered += message_;
  for (const CheckTrace& entry : trace_) {
    if (entry.outcome == StatusCode::ok) {
      continue;
    }
    rendered += "\n  at ";
    rendered += entry.check;
    rendered += ": ";
    rendered += airflow_control::to_string(entry.outcome);
    if (!entry.detail.empty()) {
      rendered += " (";
      rendered += entry.detail;
      rendered += ")";
    }
  }
  return rendered;
}

}  // namespace airflow_control
