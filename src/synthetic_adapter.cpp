#include "airflow_control/synthetic_adapter.hpp"

#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace airflow_control {
namespace {

struct PlantEntry {
  DeviceGeneration generation;
  SyntheticPlantState state;
  std::optional<SyntheticPlantState> pending;
  std::uint32_t delay_remaining = 0;
};

struct ArmedDisposition {
  AdapterDisposition disposition = AdapterDisposition::accepted;
  std::string detail;
};

struct SecondaryPressure {
  PressureRelationshipId relationship;
  SourceId source;
  Pressure differential;
  Quality quality = Quality::good;
};

/// Applies a requested setpoint to a simulated device.
SetpointBasisPoints project_fan_percent(const SetpointRequest& request, SetpointBasisPoints current,
                                        std::int64_t airflow_per_basis_point, Airflow& airflow_out) {
  if (const auto* percent = std::get_if<SetpointPercent>(&request)) {
    const std::int64_t delivered = static_cast<std::int64_t>(percent->percent.basis_points()) *
                                   airflow_per_basis_point;
    airflow_out = Airflow::from_cubic_metres_per_hour(delivered);
    return percent->percent;
  }
  const auto* airflow = std::get_if<SetpointAirflow>(&request);
  airflow_out = airflow->airflow;
  if (airflow_per_basis_point <= 0) {
    return current;
  }
  const std::int64_t basis_points = airflow->airflow.cubic_metres_per_hour() / airflow_per_basis_point;
  if (basis_points <= 0) {
    return SetpointBasisPoints::create(0).value();
  }
  if (basis_points >= static_cast<std::int64_t>(SetpointBasisPoints::kFull)) {
    return SetpointBasisPoints::create(SetpointBasisPoints::kFull).value();
  }
  return SetpointBasisPoints::create(static_cast<std::uint32_t>(basis_points)).value();
}

}  // namespace

struct SyntheticAirflowAdapter::Impl {
  explicit Impl(AdapterDescriptor descriptor_in) : descriptor(std::move(descriptor_in)) {}

  AdapterDescriptor descriptor;
  std::map<AirflowDeviceId, PlantEntry> plant;
  std::map<AirflowDeviceId, ArmedDisposition> next_disposition;
  std::map<AirflowDeviceId, ArmedDisposition> persistent_disposition;
  std::map<PressureRelationshipId, Pressure> committed_pressure;
  std::map<PressureRelationshipId, Pressure> pending_pressure;
  std::vector<SecondaryPressure> secondary_pressure;

  bool apply_on_execute = true;
  bool fence_echo = false;
  std::uint32_t observation_delay = 0;
  std::uint64_t measurement_backdate = 0;
  std::int64_t airflow_per_basis_point = 1;
  Quality read_quality = Quality::good;
  std::optional<EvidenceGeneration> evidence_generation_override;
  bool read_failure_armed = false;
  StatusCode read_failure_code = StatusCode::adapter_unavailable;
  std::string read_failure_detail;

  std::uint64_t sequence = 0;
  std::uint64_t execute_calls = 0;
  std::uint64_t read_calls = 0;
  std::uint64_t accepted_commands = 0;
  std::uint64_t applied_commands = 0;
  std::uint64_t refused_commands = 0;

  void commit_pending(const AirflowDeviceId& device) {
    auto entry = plant.find(device);
    if (entry == plant.end() || !entry->second.pending.has_value()) {
      return;
    }
    entry->second.state = *entry->second.pending;
    entry->second.pending.reset();
    entry->second.delay_remaining = 0;
    for (const auto& response : pending_pressure) {
      committed_pressure.insert_or_assign(response.first, response.second);
    }
    pending_pressure.clear();
    ++applied_commands;
  }

  void commit_all_pending() {
    for (auto& entry : plant) {
      if (!entry.second.pending.has_value()) {
        continue;
      }
      entry.second.state = *entry.second.pending;
      entry.second.pending.reset();
      entry.second.delay_remaining = 0;
      ++applied_commands;
    }
    for (const auto& response : pending_pressure) {
      committed_pressure.insert_or_assign(response.first, response.second);
    }
    pending_pressure.clear();
  }
};

SyntheticAirflowAdapter::SyntheticAirflowAdapter(AdapterDescriptor descriptor)
    : impl_(std::make_unique<Impl>(std::move(descriptor))) {
  // A synthetic adapter drives no hardware. Whatever the caller passed, this
  // flag stays true, because the tooling relies on it to label results
  // honestly.
  impl_->descriptor.synthetic = true;
}

SyntheticAirflowAdapter::~SyntheticAirflowAdapter() = default;

AdapterDescriptor SyntheticAirflowAdapter::describe() const { return impl_->descriptor; }

void SyntheticAirflowAdapter::seed_device(const AirflowDeviceId& device, DeviceGeneration generation,
                                          SyntheticPlantState state) {
  impl_->plant.insert_or_assign(device, PlantEntry{generation, state, std::nullopt, 0});
}

void SyntheticAirflowAdapter::remove_device(const AirflowDeviceId& device) { impl_->plant.erase(device); }

bool SyntheticAirflowAdapter::has_device(const AirflowDeviceId& device) const {
  return impl_->plant.find(device) != impl_->plant.end();
}

void SyntheticAirflowAdapter::set_next_disposition(const AirflowDeviceId& device,
                                                   AdapterDisposition disposition, std::string detail) {
  impl_->next_disposition[device] = ArmedDisposition{disposition, std::move(detail)};
}

void SyntheticAirflowAdapter::set_persistent_disposition(const AirflowDeviceId& device,
                                                         AdapterDisposition disposition,
                                                         std::string detail) {
  impl_->persistent_disposition[device] = ArmedDisposition{disposition, std::move(detail)};
}

void SyntheticAirflowAdapter::clear_dispositions() {
  impl_->next_disposition.clear();
  impl_->persistent_disposition.clear();
}

void SyntheticAirflowAdapter::set_apply_on_execute(bool apply) noexcept {
  impl_->apply_on_execute = apply;
}

bool SyntheticAirflowAdapter::apply_on_execute() const noexcept { return impl_->apply_on_execute; }

void SyntheticAirflowAdapter::apply_pending() { impl_->commit_all_pending(); }

void SyntheticAirflowAdapter::set_observation_delay(std::uint32_t read_count) noexcept {
  impl_->observation_delay = read_count;
}

void SyntheticAirflowAdapter::set_fence_echo(bool enabled) noexcept { impl_->fence_echo = enabled; }

void SyntheticAirflowAdapter::set_read_quality(Quality quality) noexcept {
  impl_->read_quality = quality;
}

void SyntheticAirflowAdapter::set_read_failure(StatusCode code, std::string detail) {
  impl_->read_failure_armed = true;
  impl_->read_failure_code = code;
  impl_->read_failure_detail = std::move(detail);
}

void SyntheticAirflowAdapter::clear_read_failure() { impl_->read_failure_armed = false; }

void SyntheticAirflowAdapter::set_evidence_generation_override(
    std::optional<EvidenceGeneration> generation) {
  impl_->evidence_generation_override = generation;
}

void SyntheticAirflowAdapter::set_measurement_backdate(std::uint64_t ticks) noexcept {
  impl_->measurement_backdate = ticks;
}

void SyntheticAirflowAdapter::set_secondary_pressure(const PressureRelationshipId& relationship,
                                                     const SourceId& source, Pressure differential,
                                                     Quality quality) {
  impl_->secondary_pressure.push_back(SecondaryPressure{relationship, source, differential, quality});
}

void SyntheticAirflowAdapter::clear_secondary_pressure() { impl_->secondary_pressure.clear(); }

void SyntheticAirflowAdapter::set_airflow_per_basis_point(std::int64_t cubic_metres_per_hour) noexcept {
  impl_->airflow_per_basis_point = cubic_metres_per_hour;
}

void SyntheticAirflowAdapter::set_pending_pressure_response(const PressureRelationshipId& relationship,
                                                            Pressure differential) {
  impl_->pending_pressure.insert_or_assign(relationship, differential);
}

void SyntheticAirflowAdapter::clear_pending_pressure_responses() { impl_->pending_pressure.clear(); }

std::uint64_t SyntheticAirflowAdapter::execute_calls() const noexcept { return impl_->execute_calls; }
std::uint64_t SyntheticAirflowAdapter::read_calls() const noexcept { return impl_->read_calls; }
std::uint64_t SyntheticAirflowAdapter::accepted_commands() const noexcept {
  return impl_->accepted_commands;
}
std::uint64_t SyntheticAirflowAdapter::applied_commands() const noexcept {
  return impl_->applied_commands;
}
std::uint64_t SyntheticAirflowAdapter::refused_commands() const noexcept {
  return impl_->refused_commands;
}

std::optional<SyntheticPlantState> SyntheticAirflowAdapter::plant_state(
    const AirflowDeviceId& device) const {
  const auto entry = impl_->plant.find(device);
  if (entry == impl_->plant.end()) {
    return std::nullopt;
  }
  return entry->second.state;
}

AdapterOutcome SyntheticAirflowAdapter::execute(const AdapterCommand& command) {
  ++impl_->execute_calls;
  ++impl_->sequence;

  AdapterOutcome outcome{command.id(),
                         command.attempt(),
                         command.device(),
                         command.device_generation(),
                         AdapterDisposition::accepted,
                         AdapterSequence::from(impl_->sequence),
                         command.issued_at(),
                         std::string(),
                         std::nullopt};

  const auto persistent = impl_->persistent_disposition.find(command.device());
  if (persistent != impl_->persistent_disposition.end()) {
    outcome.disposition = persistent->second.disposition;
    outcome.detail = persistent->second.detail;
  } else {
    const auto armed = impl_->next_disposition.find(command.device());
    if (armed != impl_->next_disposition.end()) {
      outcome.disposition = armed->second.disposition;
      outcome.detail = armed->second.detail;
      impl_->next_disposition.erase(armed);
    }
  }

  const auto entry = impl_->plant.find(command.device());
  if (entry == impl_->plant.end()) {
    // A device the simulated plant does not hold is indistinguishable from a
    // device that is not there.
    outcome.disposition = AdapterDisposition::unavailable;
    outcome.detail = "device is not present in the simulated plant";
    ++impl_->refused_commands;
  } else if (entry->second.generation != command.device_generation()) {
    outcome.disposition = AdapterDisposition::fault;
    outcome.detail = "command names a different device generation";
    ++impl_->refused_commands;
  } else if (outcome.disposition == AdapterDisposition::accepted) {
    ++impl_->accepted_commands;
    Airflow delivered = entry->second.state.airflow;
    const SetpointBasisPoints percent =
        project_fan_percent(command.setpoint(), entry->second.state.fan_percent,
                            impl_->airflow_per_basis_point, delivered);
    if (impl_->apply_on_execute) {
      const SyntheticPlantState target{percent, delivered, entry->second.state.differential};
      if (impl_->observation_delay > 0) {
        entry->second.pending = target;
        entry->second.delay_remaining = impl_->observation_delay;
      } else {
        entry->second.state = target;
        for (const auto& response : impl_->pending_pressure) {
          impl_->committed_pressure.insert_or_assign(response.first, response.second);
        }
        impl_->pending_pressure.clear();
        ++impl_->applied_commands;
      }
    }
  } else {
    ++impl_->refused_commands;
  }

  if (impl_->fence_echo) {
    outcome.command = CommandId::from(command.id().value() + 1);
  }
  return outcome;
}

Result<ObservationDraft> SyntheticAirflowAdapter::read(const AdapterReadRequest& request) {
  ++impl_->read_calls;

  if (impl_->read_failure_armed) {
    return Status::failure(impl_->read_failure_code, impl_->read_failure_detail);
  }
  const auto entry = impl_->plant.find(request.device);
  if (entry == impl_->plant.end()) {
    return Status::failure(StatusCode::adapter_unavailable,
                           "device " + request.device.str() + " is not present in the simulated plant");
  }
  if (entry->second.generation != request.device_generation) {
    return Status::failure(StatusCode::generation_mismatch,
                           "read names device generation " +
                               request.device_generation.to_string() + " but the plant holds " +
                               entry->second.generation.to_string());
  }

  if (entry->second.pending.has_value()) {
    if (entry->second.delay_remaining > 0) {
      // This read still sees the pre-command value; the pending command becomes
      // visible on the read after the last delayed one.
      --entry->second.delay_remaining;
    } else {
      impl_->commit_pending(request.device);
    }
  }

  if (impl_->measurement_backdate > request.at.value()) {
    return Status::failure(StatusCode::out_of_range,
                           "measurement backdate exceeds the read instant");
  }

  const auto measured = LogicalTick::from(request.at.value() - impl_->measurement_backdate);

  Quality quality = impl_->read_quality;
  ObservationPayload payload = FanReading{entry->second.state.fan_percent};
  switch (request.kind) {
    case ObservationKind::fan_setpoint:
      payload = FanReading{entry->second.state.fan_percent};
      break;
    case ObservationKind::airflow:
      payload = AirflowReading{entry->second.state.airflow};
      break;
    case ObservationKind::pressure: {
      Pressure differential = entry->second.state.differential;
      if (request.relationship.has_value()) {
        const auto committed = impl_->committed_pressure.find(*request.relationship);
        if (committed != impl_->committed_pressure.end()) {
          differential = committed->second;
        }
      }
      for (const SecondaryPressure& secondary : impl_->secondary_pressure) {
        if (!request.relationship.has_value() || !(secondary.relationship == *request.relationship)) {
          continue;
        }
        if (!(secondary.source == request.source)) {
          continue;
        }
        differential = secondary.differential;
        quality = secondary.quality;
      }
      payload = PressureReading{differential};
      break;
    }
  }

  const EvidenceGeneration generation = impl_->evidence_generation_override.has_value()
                                            ? *impl_->evidence_generation_override
                                            : request.evidence_generation;

  ObservationDraft draft{payload,
                         request.device,
                         request.relationship,
                         request.point,
                         request.source,
                         request.sequence,
                         measured,
                         request.device_generation,
                         generation,
                         quality};
  return draft;
}

}  // namespace airflow_control
