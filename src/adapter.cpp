#include "airflow_control/adapter.hpp"

#include <array>
#include <cstddef>
#include <string_view>

namespace airflow_control {
namespace {

constexpr std::array<const char*, 5> kDispositionNames{{
    "accepted", "refused", "unavailable", "fault", "indeterminate",
}};

struct CapabilityName {
  AdapterCapability capability;
  const char* name;
};

constexpr std::array<CapabilityName, 6> kCapabilityNames{{
    {AdapterCapability::set_fan_percent, "set_fan_percent"},
    {AdapterCapability::set_airflow, "set_airflow"},
    {AdapterCapability::read_pressure, "read_pressure"},
    {AdapterCapability::read_airflow, "read_airflow"},
    {AdapterCapability::read_fan_percent, "read_fan_percent"},
    {AdapterCapability::observe_containment, "observe_containment"},
}};

}  // namespace

const char* to_string(AdapterDisposition disposition) noexcept {
  const auto index = static_cast<std::size_t>(disposition);
  if (index >= kDispositionNames.size()) {
    return "indeterminate";
  }
  return kDispositionNames[index];
}

std::optional<AdapterDisposition> parse_adapter_disposition(std::string_view token) noexcept {
  for (std::size_t index = 0; index < kDispositionNames.size(); ++index) {
    if (token == kDispositionNames[index]) {
      return static_cast<AdapterDisposition>(index);
    }
  }
  return std::nullopt;
}

bool is_definite_refusal(AdapterDisposition disposition) noexcept {
  return disposition == AdapterDisposition::refused || disposition == AdapterDisposition::unavailable ||
         disposition == AdapterDisposition::fault;
}

bool declares(const AdapterDescriptor& descriptor, AdapterCapability capability) noexcept {
  return (descriptor.capabilities & static_cast<std::uint32_t>(capability)) != 0u;
}

std::string describe_capabilities(std::uint32_t capabilities) {
  std::string rendered;
  for (const CapabilityName& entry : kCapabilityNames) {
    if ((capabilities & static_cast<std::uint32_t>(entry.capability)) == 0u) {
      continue;
    }
    if (!rendered.empty()) {
      rendered += ',';
    }
    rendered += entry.name;
  }
  if (rendered.empty()) {
    rendered = "none";
  }
  return rendered;
}

Status verify_outcome_echo(const AdapterCommand& command, const AdapterOutcome& outcome) {
  if (outcome.command != command.id()) {
    return Status::failure(StatusCode::adapter_fenced,
                           "adapter answered command " + outcome.command.to_string() +
                               " for a command " + command.id().to_string());
  }
  if (outcome.attempt != command.attempt()) {
    return Status::failure(StatusCode::adapter_fenced, "adapter answered a different attempt");
  }
  if (!(outcome.device == command.device())) {
    return Status::failure(StatusCode::adapter_fenced, "adapter answered for a different device");
  }
  if (outcome.device_generation != command.device_generation()) {
    return Status::failure(StatusCode::adapter_fenced,
                           "adapter answered for a different device generation");
  }
  if (outcome.acknowledged_at < command.issued_at()) {
    return Status::failure(StatusCode::adapter_fenced,
                           "adapter acknowledged at " + outcome.acknowledged_at.to_string() +
                               ", before the command was issued at " +
                               command.issued_at().to_string());
  }
  return Status::success();
}

}  // namespace airflow_control
