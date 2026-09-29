#include "airflow_control/units.hpp"

#include <array>
#include <cstdlib>
#include <limits>

namespace airflow_control {
namespace {

/// Adds two signed 64-bit values, failing rather than wrapping.
Result<std::int64_t> checked_add(std::int64_t lhs, std::int64_t rhs) noexcept {
  if (rhs > 0 && lhs > std::numeric_limits<std::int64_t>::max() - rhs) {
    return Status::failure(StatusCode::overflow, "integer addition overflow");
  }
  if (rhs < 0 && lhs < std::numeric_limits<std::int64_t>::min() - rhs) {
    return Status::failure(StatusCode::overflow, "integer addition underflow");
  }
  return lhs + rhs;
}

Result<std::int64_t> checked_subtract(std::int64_t lhs, std::int64_t rhs) noexcept {
  if (rhs == std::numeric_limits<std::int64_t>::min()) {
    return Status::failure(StatusCode::overflow, "integer subtraction overflow");
  }
  return checked_add(lhs, -rhs);
}

Result<std::int64_t> checked_multiply(std::int64_t lhs, std::int64_t rhs) noexcept {
  if (lhs == 0 || rhs == 0) {
    return 0;
  }
  if (lhs == -1 && rhs == std::numeric_limits<std::int64_t>::min()) {
    return Status::failure(StatusCode::overflow, "integer multiplication overflow");
  }
  if (rhs == -1 && lhs == std::numeric_limits<std::int64_t>::min()) {
    return Status::failure(StatusCode::overflow, "integer multiplication overflow");
  }
  const std::int64_t product = lhs * rhs;
  if (product / rhs != lhs) {
    return Status::failure(StatusCode::overflow, "integer multiplication overflow");
  }
  return product;
}

std::string render_signed(std::int64_t value, unsigned fractional_digits, const char* unit) {
  const bool negative = value < 0;
  // Negating the most negative value would overflow, so the magnitude is
  // computed in unsigned arithmetic.
  std::uint64_t magnitude = 0;
  if (negative) {
    magnitude = static_cast<std::uint64_t>(-(value + 1)) + 1u;
  } else {
    magnitude = static_cast<std::uint64_t>(value);
  }

  std::uint64_t scale = 1;
  for (unsigned index = 0; index < fractional_digits; ++index) {
    scale *= 10u;
  }
  const std::uint64_t whole = magnitude / scale;
  const std::uint64_t fraction = magnitude % scale;

  std::string rendered;
  if (negative) {
    rendered += '-';
  }
  rendered += std::to_string(whole);
  if (fractional_digits > 0) {
    rendered += '.';
    std::string digits = std::to_string(fraction);
    rendered.append(fractional_digits - digits.size(), '0');
    rendered += digits;
  }
  rendered += ' ';
  rendered += unit;
  return rendered;
}

}  // namespace

std::string render_fixed(std::int64_t value, unsigned fractional_digits) {
  std::string rendered = render_signed(value, fractional_digits, "");
  if (!rendered.empty() && rendered.back() == ' ') {
    // The shared renderer separates the number from its unit; with no unit the
    // separator is not part of the number.
    rendered.pop_back();
  }
  return rendered;
}

std::string Airflow::to_string() const { return render_signed(value_, 0, "m3/h"); }

Result<Airflow> Airflow::add(Airflow lhs, Airflow rhs) noexcept {
  Result<std::int64_t> sum = checked_add(lhs.value_, rhs.value_);
  if (!sum.ok()) {
    return sum.status();
  }
  return Airflow::from_cubic_metres_per_hour(sum.value());
}

Result<Airflow> Airflow::subtract(Airflow lhs, Airflow rhs) noexcept {
  Result<std::int64_t> difference = checked_subtract(lhs.value_, rhs.value_);
  if (!difference.ok()) {
    return difference.status();
  }
  return Airflow::from_cubic_metres_per_hour(difference.value());
}

Result<Airflow> Airflow::scale(Airflow value, std::int64_t numerator, std::int64_t denominator) noexcept {
  if (denominator == 0) {
    return Status::failure(StatusCode::invalid_argument, "airflow scale denominator is zero");
  }
  Result<std::int64_t> product = checked_multiply(value.value_, numerator);
  if (!product.ok()) {
    return product.status();
  }
  return Airflow::from_cubic_metres_per_hour(product.value() / denominator);
}

bool is_representable(Airflow value) noexcept {
  return value.cubic_metres_per_hour() <= PhysicalBounds::max_abs_airflow_cubic_metres_per_hour &&
         value.cubic_metres_per_hour() >= -PhysicalBounds::max_abs_airflow_cubic_metres_per_hour;
}

std::string Pressure::to_string() const { return render_signed(value_, 3, "Pa"); }

Result<Pressure> Pressure::add(Pressure lhs, Pressure rhs) noexcept {
  Result<std::int64_t> sum = checked_add(lhs.value_, rhs.value_);
  if (!sum.ok()) {
    return sum.status();
  }
  return Pressure::from_millipascals(sum.value());
}

Result<Pressure> Pressure::subtract(Pressure lhs, Pressure rhs) noexcept {
  Result<std::int64_t> difference = checked_subtract(lhs.value_, rhs.value_);
  if (!difference.ok()) {
    return difference.status();
  }
  return Pressure::from_millipascals(difference.value());
}

Result<Pressure> Pressure::negate() const noexcept {
  if (value_ == std::numeric_limits<std::int64_t>::min()) {
    return Status::failure(StatusCode::overflow, "pressure negation overflow");
  }
  return Pressure::from_millipascals(-value_);
}

bool is_representable(Pressure value) noexcept {
  return value.millipascals() <= PhysicalBounds::max_abs_pressure_millipascals &&
         value.millipascals() >= -PhysicalBounds::max_abs_pressure_millipascals;
}

std::string MilliCelsius::to_string() const { return render_signed(value_, 3, "C"); }

bool is_representable(MilliCelsius value) noexcept {
  return value.millidegrees() <= PhysicalBounds::max_abs_millidegrees_celsius &&
         value.millidegrees() >= -PhysicalBounds::max_abs_millidegrees_celsius;
}

std::string SetpointBasisPoints::to_string() const {
  return render_signed(static_cast<std::int64_t>(value_), 2, "%");
}

std::string SlewBasisPoints::to_string() const {
  return render_signed(static_cast<std::int64_t>(value_), 2, "%");
}

}  // namespace airflow_control
