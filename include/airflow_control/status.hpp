#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace airflow_control {

/// Machine-readable outcome of an operation.
///
/// The enum value is an implementation detail of the build; the token returned
/// by to_string(StatusCode) is the contract. Callers branch on the code, never
/// on the human-readable message.
enum class StatusCode : std::uint32_t {
  ok = 0,

  // -- request shape -------------------------------------------------------
  invalid_argument,
  out_of_range,
  overflow,
  bounds_exceeded,
  unsupported,

  // -- identity ------------------------------------------------------------
  not_found,
  duplicate_identity,
  identity_mismatch,

  // -- lifecycle and attempt latches ---------------------------------------
  lifecycle_forbidden,
  transition_invalid,
  attempt_unresolved,
  recovery_required,

  // -- generations, revisions, epochs --------------------------------------
  generation_mismatch,
  revision_mismatch,
  epoch_stale,

  // -- authority -----------------------------------------------------------
  permission_missing,
  permission_denied,
  permission_stale,
  safety_permit_missing,
  safety_permit_stale,

  // -- interlocks ----------------------------------------------------------
  interlock_open,
  interlock_unknown,

  // -- containment ---------------------------------------------------------
  containment_breached,
  containment_unknown,
  containment_open,

  // -- airflow obligations -------------------------------------------------
  obligation_unsatisfied,
  obligation_unknown,

  // -- envelopes and limits ------------------------------------------------
  limit_invalid,
  limit_exceeded,
  limit_unknown,

  // -- pressure relationship ----------------------------------------------
  pressure_unknown,
  pressure_band_invalid,
  pressure_violated,
  pressure_conflicted,

  // -- evidence ------------------------------------------------------------
  evidence_stale,
  evidence_unknown,
  evidence_conflicting,
  evidence_generation_mismatch,
  evidence_out_of_order,
  evidence_future,
  evidence_quality_insufficient,

  // -- adapter -------------------------------------------------------------
  adapter_refused,
  adapter_unavailable,
  adapter_fault,
  adapter_indeterminate,
  adapter_fenced,

  // -- idempotency and supersession ---------------------------------------
  idempotency_conflict,
  supersession_unsupported,

  // -- durable store -------------------------------------------------------
  store_missing,
  store_corrupt,
  store_unsupported_version,
  store_capacity_exceeded,
  store_io_error,
  store_fenced,
  rollback_detected,
  lock_denied,
  busy,
  path_invalid,
  not_open,

  // -- catch-all -----------------------------------------------------------
  internal_error,
};

/// Stable lower-case token for a status code. This is the machine contract.
[[nodiscard]] const char* to_string(StatusCode code) noexcept;

/// Parses the token produced by to_string(StatusCode).
[[nodiscard]] std::optional<StatusCode> parse_status_code(std::string_view token) noexcept;

/// True when the code denotes a successful outcome.
[[nodiscard]] bool is_success(StatusCode code) noexcept;

/// One entry in the ordered trace of preconditions considered for a request.
///
/// The trace is what makes a refusal explainable: every check that ran is
/// reported with its own outcome, in the order the engine evaluated it.
struct CheckTrace {
  std::string check;
  StatusCode outcome = StatusCode::ok;
  std::string detail;
};

/// Outcome of an operation: a code, an explanation, and the ordered trace of
/// preconditions that produced it.
class Status {
 public:
  Status() = default;

  static Status success() { return Status{}; }

  static Status failure(StatusCode code, std::string message) {
    Status status;
    status.code_ = code;
    status.message_ = std::move(message);
    return status;
  }

  [[nodiscard]] bool ok() const noexcept { return code_ == StatusCode::ok; }
  [[nodiscard]] StatusCode code() const noexcept { return code_; }
  [[nodiscard]] const std::string& message() const noexcept { return message_; }
  [[nodiscard]] const std::vector<CheckTrace>& trace() const noexcept { return trace_; }
  [[nodiscard]] std::vector<CheckTrace>& trace() noexcept { return trace_; }

  void set_code(StatusCode code) noexcept { code_ = code; }
  void set_message(std::string message) { message_ = std::move(message); }

  Status& with_code(StatusCode code, std::string message) {
    code_ = code;
    message_ = std::move(message);
    return *this;
  }

  Status& add_check(std::string check, StatusCode outcome, std::string detail) {
    trace_.push_back(CheckTrace{std::move(check), outcome, std::move(detail)});
    return *this;
  }

  Status& note(std::string check, std::string detail) {
    return add_check(std::move(check), StatusCode::ok, std::move(detail));
  }

  /// Renders "token: message" followed by the non-ok trace entries understood
  /// as the first refusal. Deterministic and locale independent.
  [[nodiscard]] std::string to_string() const;

 private:
  StatusCode code_ = StatusCode::ok;
  std::string message_;
  std::vector<CheckTrace> trace_;
};

/// Thrown only when a caller reads the value of a failed Result. This is a
/// programming error, never a machine contract; callers must check ok().
class ResultAccessError : public std::logic_error {
 public:
  explicit ResultAccessError(const char* what) : std::logic_error(what) {}
};

/// A value or the status explaining why there is none.
template <typename T>
class Result {
 public:
  Result(T value) : value_(std::move(value)) {}          // NOLINT(google-explicit-constructor)
  Result(Status status) : status_(std::move(status)) {}  // NOLINT(google-explicit-constructor)

  [[nodiscard]] bool ok() const noexcept { return value_.has_value(); }
  [[nodiscard]] const Status& status() const noexcept { return status_; }
  [[nodiscard]] StatusCode code() const noexcept { return status_.code(); }
  [[nodiscard]] const std::string& message() const noexcept { return status_.message(); }

  [[nodiscard]] const T& value() const& {
    require();
    return *value_;
  }
  [[nodiscard]] T& value() & {
    require();
    return *value_;
  }
  [[nodiscard]] T&& value() && {
    require();
    return std::move(*value_);
  }

  [[nodiscard]] const T* operator->() const { return &value(); }
  [[nodiscard]] T* operator->() { return &value(); }
  [[nodiscard]] const T& operator*() const& { return value(); }
  [[nodiscard]] T& operator*() & { return value(); }

  [[nodiscard]] const T& value_or(const T& fallback) const& { return value_ ? *value_ : fallback; }

  [[nodiscard]] const std::optional<T>& optional() const noexcept { return value_; }

 private:
  void require() const {
    if (!value_) {
      throw ResultAccessError("airflow_control::Result::value() called on a failed result");
    }
  }

  std::optional<T> value_;
  Status status_;
};

}  // namespace airflow_control
