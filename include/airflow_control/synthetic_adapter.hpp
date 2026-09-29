#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "airflow_control/adapter.hpp"
#include "airflow_control/ids.hpp"
#include "airflow_control/model.hpp"
#include "airflow_control/status.hpp"
#include "airflow_control/units.hpp"

namespace airflow_control {

/// The state of one simulated airflow device.
struct SyntheticPlantState {
  SetpointBasisPoints fan_percent;
  Airflow airflow;
  Pressure differential;
};

/// A deterministic synthetic airflow plant.
///
/// This adapter drives no hardware. It exists so that every semantic in this
/// repository can be exercised reproducibly: acknowledgement without effect,
/// delayed observation, contradictory sensors, device disappearance, injected
/// failure, and fencing. It advances only when it is told to, never on a clock,
/// so a scenario replays identically.
///
/// Everything this adapter produces is labeled SYNTHETIC.
class SyntheticAirflowAdapter final : public AirflowAdapter {
 public:
  explicit SyntheticAirflowAdapter(AdapterDescriptor descriptor);
  ~SyntheticAirflowAdapter() override;

  SyntheticAirflowAdapter(const SyntheticAirflowAdapter&) = delete;
  SyntheticAirflowAdapter& operator=(const SyntheticAirflowAdapter&) = delete;

  [[nodiscard]] AdapterDescriptor describe() const override;
  AdapterOutcome execute(const AdapterCommand& command) override;
  Result<ObservationDraft> read(const AdapterReadRequest& request) override;

  // -- simulated plant ------------------------------------------------------

  /// Installs a device in the simulated plant.
  void seed_device(const AirflowDeviceId& device, DeviceGeneration generation, SyntheticPlantState state);

  /// Removes a device. Reads for it fail and commands to it are refused, which
  /// is what device disappearance looks like at the adapter boundary.
  void remove_device(const AirflowDeviceId& device);
  [[nodiscard]] bool has_device(const AirflowDeviceId& device) const;

  // -- acknowledgement control ----------------------------------------------

  /// Arms a disposition for the next execute() of one device; cleared after use.
  void set_next_disposition(const AirflowDeviceId& device, AdapterDisposition disposition, std::string detail);

  /// Arms a disposition that applies to every execute() until it is cleared.
  void set_persistent_disposition(const AirflowDeviceId& device, AdapterDisposition disposition,
                                  std::string detail);

  /// Clears every armed disposition.
  void clear_dispositions();

  /// When false, execute() acknowledges without moving the simulated plant:
  /// acknowledgement without effect.
  void set_apply_on_execute(bool apply) noexcept;
  [[nodiscard]] bool apply_on_execute() const noexcept;

  /// Commits the most recent accepted command into the simulated plant.
  void apply_pending();

  /// Makes the plant report its pre-command value for the next read_count reads
  /// before the pending command becomes visible.
  void set_observation_delay(std::uint32_t read_count) noexcept;

  /// Forces the adapter to answer with an outcome that names a different
  /// command, which a fenced engine must discard.
  void set_fence_echo(bool enabled) noexcept;

  // -- observation control --------------------------------------------------

  /// Quality stamped on every draft the adapter reports.
  void set_read_quality(Quality quality) noexcept;

  /// Makes every read fail with the given status.
  void set_read_failure(StatusCode code, std::string detail);
  void clear_read_failure();

  /// Stamps a fixed evidence generation on every draft, ignoring the request.
  void set_evidence_generation_override(std::optional<EvidenceGeneration> generation);
  /// Shifts the measured instant of every draft backwards by this many ticks.
  void set_measurement_backdate(std::uint64_t ticks) noexcept;

  /// Adds a second, disagreeing pressure source for one relationship.
  void set_secondary_pressure(const PressureRelationshipId& relationship, const SourceId& source,
                              Pressure differential, Quality quality);
  void clear_secondary_pressure();

  /// Sets the airflow the plant reports as a function of the fan setpoint.
  /// Defaults to one cubic metre per hour per basis point.
  void set_airflow_per_basis_point(std::int64_t cubic_metres_per_hour) noexcept;

  /// Scripts the differential a relationship will report once the next accepted
  /// command is applied to the plant.
  ///
  /// The simulated plant has no physics: its response is scripted by the caller
  /// so that a scenario is exactly reproducible. This is deliberately not a
  /// thermal or flow model, and nothing here is a claim about a real room.
  void set_pending_pressure_response(const PressureRelationshipId& relationship, Pressure differential);
  void clear_pending_pressure_responses();

  // -- accounting -----------------------------------------------------------

  [[nodiscard]] std::uint64_t execute_calls() const noexcept;
  [[nodiscard]] std::uint64_t read_calls() const noexcept;
  [[nodiscard]] std::uint64_t accepted_commands() const noexcept;
  [[nodiscard]] std::uint64_t applied_commands() const noexcept;
  [[nodiscard]] std::uint64_t refused_commands() const noexcept;
  [[nodiscard]] std::optional<SyntheticPlantState> plant_state(const AirflowDeviceId& device) const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace airflow_control
