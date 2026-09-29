#pragma once

#include <compare>
#include <cstdint>
#include <string>

#include "airflow_control/status.hpp"

namespace airflow_control {

/// Volumetric airflow in exact whole cubic metres per hour.
///
/// Signed, because reverse flow is physically meaningful and must not be
/// silently clamped to zero. The unit is fixed by the type; a caller holding a
/// different unit converts before constructing one.
class Airflow {
 public:
  Airflow() = delete;

  [[nodiscard]] static Airflow from_cubic_metres_per_hour(std::int64_t value) noexcept {
    return Airflow(value);
  }
  [[nodiscard]] std::int64_t cubic_metres_per_hour() const noexcept { return value_; }

  /// Checked sum, or an overflow status.
  [[nodiscard]] static Result<Airflow> add(Airflow lhs, Airflow rhs) noexcept;
  /// Checked difference, or an overflow status.
  [[nodiscard]] static Result<Airflow> subtract(Airflow lhs, Airflow rhs) noexcept;
  /// Truncating-toward-zero ratio, or a status when denominator is zero or the
  /// intermediate product overflows.
  [[nodiscard]] static Result<Airflow> scale(Airflow value, std::int64_t numerator,
                                             std::int64_t denominator) noexcept;

  friend bool operator==(const Airflow& lhs, const Airflow& rhs) noexcept = default;
  friend std::strong_ordering operator<=>(const Airflow& lhs, const Airflow& rhs) noexcept = default;

  [[nodiscard]] std::string to_string() const;

 private:
  explicit Airflow(std::int64_t value) noexcept : value_(value) {}

  std::int64_t value_;
};

/// Differential pressure in exact whole millipascals.
///
/// A pressure relationship is expressed relative to a reference space, so the
/// value is signed: positive means the controlled space is at a higher static
/// pressure than the reference space. Sub-pascal resolution is carried as
/// integer millipascals rather than as floating point.
class Pressure {
 public:
  Pressure() = delete;

  [[nodiscard]] static Pressure from_millipascals(std::int64_t value) noexcept {
    return Pressure(value);
  }
  [[nodiscard]] std::int64_t millipascals() const noexcept { return value_; }

  [[nodiscard]] static Result<Pressure> add(Pressure lhs, Pressure rhs) noexcept;
  [[nodiscard]] static Result<Pressure> subtract(Pressure lhs, Pressure rhs) noexcept;
  [[nodiscard]] Result<Pressure> negate() const noexcept;

  friend bool operator==(const Pressure& lhs, const Pressure& rhs) noexcept = default;
  friend std::strong_ordering operator<=>(const Pressure& lhs, const Pressure& rhs) noexcept = default;

  [[nodiscard]] std::string to_string() const;

 private:
  explicit Pressure(std::int64_t value) noexcept : value_(value) {}

  std::int64_t value_;
};

/// A temperature in exact whole millidegrees Celsius.
///
/// Used only for thermal obligations that an external thermal authority has
/// already decided: this runtime consumes them and never derives one.
class MilliCelsius {
 public:
  MilliCelsius() = delete;

  [[nodiscard]] static MilliCelsius from_millidegrees(std::int64_t value) noexcept {
    return MilliCelsius(value);
  }
  [[nodiscard]] std::int64_t millidegrees() const noexcept { return value_; }

  friend bool operator==(const MilliCelsius& lhs, const MilliCelsius& rhs) noexcept = default;
  friend std::strong_ordering operator<=>(const MilliCelsius& lhs, const MilliCelsius& rhs) noexcept = default;

  [[nodiscard]] std::string to_string() const;

 private:
  explicit MilliCelsius(std::int64_t value) noexcept : value_(value) {}

  std::int64_t value_;
};

/// A fan setpoint as exact basis points of design airflow, 0..10000.
///
/// Basis points rather than a percentage float: 10000 is 100.00 percent, 1 is
/// 0.01 percent, and no setpoint is ever the result of a rounding step that
/// cannot be reproduced.
class SetpointBasisPoints {
 public:
  static constexpr std::uint32_t kFull = 10000;

  SetpointBasisPoints() = delete;

  /// Constructs a setpoint, rejecting anything above 100.00 percent.
  [[nodiscard]] static Result<SetpointBasisPoints> create(std::uint32_t basis_points) noexcept {
    if (basis_points > kFull) {
      return Status::failure(StatusCode::out_of_range,
                             "fan setpoint " + std::to_string(basis_points) +
                                 " basis points exceeds " + std::to_string(kFull));
    }
    return SetpointBasisPoints(basis_points);
  }

  [[nodiscard]] std::uint32_t basis_points() const noexcept { return value_; }
  [[nodiscard]] std::string to_string() const;

  friend bool operator==(const SetpointBasisPoints& lhs, const SetpointBasisPoints& rhs) noexcept = default;
  friend std::strong_ordering operator<=>(const SetpointBasisPoints& lhs,
                                          const SetpointBasisPoints& rhs) noexcept = default;

 private:
  explicit SetpointBasisPoints(std::uint32_t value) noexcept : value_(value) {}

  std::uint32_t value_;
};

/// A maximum permitted setpoint change, in basis points, 0..10000.
///
/// A distinct type from SetpointBasisPoints because a slew bound and a
/// commanded position are materially different quantities that must not be
/// substituted for one another.
class SlewBasisPoints {
 public:
  static constexpr std::uint32_t kFull = 10000;

  SlewBasisPoints() = delete;

  [[nodiscard]] static Result<SlewBasisPoints> create(std::uint32_t basis_points) noexcept {
    if (basis_points > kFull) {
      return Status::failure(StatusCode::out_of_range,
                             "slew bound " + std::to_string(basis_points) +
                                 " basis points exceeds " + std::to_string(kFull));
    }
    return SlewBasisPoints(basis_points);
  }

  [[nodiscard]] std::uint32_t basis_points() const noexcept { return value_; }
  [[nodiscard]] std::string to_string() const;

  friend bool operator==(const SlewBasisPoints& lhs, const SlewBasisPoints& rhs) noexcept = default;
  friend std::strong_ordering operator<=>(const SlewBasisPoints& lhs, const SlewBasisPoints& rhs) noexcept = default;

 private:
  explicit SlewBasisPoints(std::uint32_t value) noexcept : value_(value) {}

  std::uint32_t value_;
};

/// Static, structural bounds on physical quantities.
///
/// These are shape bounds, not policy: they exist so that an absurd or
/// adversarial quantity is refused before it reaches arithmetic. They are not
/// derived from, and do not replace, the limits the owning authority supplies.
struct PhysicalBounds {
  static constexpr std::int64_t max_abs_airflow_cubic_metres_per_hour = 100'000'000;
  static constexpr std::int64_t max_abs_pressure_millipascals = 100'000'000;
  static constexpr std::int64_t max_abs_millidegrees_celsius = 1'000'000;
  /// Largest magnitude a neutral pressure band may declare.
  static constexpr std::int64_t max_neutral_band_millipascals = 50'000;
  /// Largest tolerance a pressure relationship may declare.
  static constexpr std::int64_t max_pressure_tolerance_millipascals = 10'000;
};

/// True when the value is a structurally representable airflow.
[[nodiscard]] bool is_representable(Airflow value) noexcept;
/// True when the value is a structurally representable differential pressure.
[[nodiscard]] bool is_representable(Pressure value) noexcept;
/// True when the value is a structurally representable temperature.
[[nodiscard]] bool is_representable(MilliCelsius value) noexcept;

/// Renders an integer as a fixed-point decimal with the given number of
/// fractional digits. Deterministic, locale independent, and exact.
[[nodiscard]] std::string render_fixed(std::int64_t value, unsigned fractional_digits);

}  // namespace airflow_control
