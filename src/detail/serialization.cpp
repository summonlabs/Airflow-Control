#include "detail/serialization.hpp"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "detail/codec.hpp"

namespace airflow_control::detail {
namespace {

constexpr std::uint32_t kPayloadMagic = 0x41464D31u;  // "AFM1"
constexpr std::uint32_t kPayloadVersion = 1u;

/// The largest number of bytes any single text field may occupy on disk. The
/// identifier and text validators are stricter; this is the outer bound the
/// decoder applies before it hands a length to the text validator.
constexpr std::size_t kMaxDecodedTextBytes = kMaxTextLength + 4;

[[nodiscard]] Status non_canonical(const char* what) {
  return Status::failure(StatusCode::store_corrupt,
                         std::string("durable state is not in canonical order: ") + what);
}

[[nodiscard]] Status duplicate_identity(const char* what) {
  return Status::failure(StatusCode::duplicate_identity,
                         std::string("durable state contains a duplicate ") + what);
}

template <typename T>
[[nodiscard]] Result<T> decode_enum(ByteReader& reader, std::uint32_t max_value, const char* what) {
  Result<std::uint32_t> raw = reader.u32();
  if (!raw.ok()) {
    return raw.status();
  }
  if (raw.value() > max_value) {
    return Status::failure(StatusCode::store_corrupt,
                           std::string("impossible ") + what + " value " +
                               std::to_string(raw.value()));
  }
  return static_cast<T>(raw.value());
}

template <typename Id>
void write_identifier(ByteWriter& writer, const Id& id) {
  writer.text(id.str());
}

template <typename Id>
[[nodiscard]] Result<Id> read_identifier(ByteReader& reader) {
  return reader.identifier<Id>();
}

void write_optional_identifier(ByteWriter& writer, const std::optional<std::string>& value) {
  writer.optional(value, [&writer](const std::string& text) { writer.text(text); });
}

template <typename Id>
void write_opt_id(ByteWriter& writer, const std::optional<Id>& value) {
  if (value.has_value()) {
    writer.u8(1);
    write_identifier(writer, *value);
  } else {
    writer.u8(0);
  }
}

template <typename Id>
[[nodiscard]] Result<std::optional<Id>> read_opt_id(ByteReader& reader) {
  Result<bool> present = reader.present();
  if (!present.ok()) {
    return present.status();
  }
  if (!present.value()) {
    return std::optional<Id>{};
  }
  Result<Id> value = read_identifier<Id>(reader);
  if (!value.ok()) {
    return value.status();
  }
  return std::optional<Id>{std::move(value).value()};
}

template <typename Ord>
void write_ordinal(ByteWriter& writer, Ord value) {
  writer.u64(value.value());
}

template <typename Ord>
[[nodiscard]] Result<Ord> read_ordinal(ByteReader& reader) {
  Result<std::uint64_t> raw = reader.u64();
  if (!raw.ok()) {
    return raw.status();
  }
  return Ord::from(raw.value());
}

void write_tick(ByteWriter& writer, LogicalTick tick) { writer.u64(tick.value()); }

[[nodiscard]] Result<LogicalTick> read_tick(ByteReader& reader) {
  Result<std::uint64_t> raw = reader.u64();
  if (!raw.ok()) {
    return raw.status();
  }
  return LogicalTick::from(raw.value());
}

void write_opt_tick(ByteWriter& writer, const std::optional<LogicalTick>& tick) {
  writer.optional(tick, [&writer](LogicalTick value) { write_tick(writer, value); });
}

[[nodiscard]] Result<std::optional<LogicalTick>> read_opt_tick(ByteReader& reader) {
  Result<bool> present = reader.present();
  if (!present.ok()) {
    return present.status();
  }
  if (!present.value()) {
    return std::optional<LogicalTick>{};
  }
  Result<LogicalTick> value = read_tick(reader);
  if (!value.ok()) {
    return value.status();
  }
  return std::optional<LogicalTick>{value.value()};
}

/// A presence byte followed by an ordinal. Ordinals are not identifiers: they
/// are written as integers so that an identity counter cannot be confused with
/// an opaque name.
template <typename Ord>
void write_opt_ordinal(ByteWriter& writer, const std::optional<Ord>& value) {
  writer.optional(value, [&writer](Ord ordinal) { write_ordinal(writer, ordinal); });
}

template <typename Ord>
[[nodiscard]] Result<std::optional<Ord>> read_opt_ordinal(ByteReader& reader) {
  Result<bool> present = reader.present();
  if (!present.ok()) {
    return present.status();
  }
  if (!present.value()) {
    return std::optional<Ord>{};
  }
  Result<Ord> value = read_ordinal<Ord>(reader);
  if (!value.ok()) {
    return value.status();
  }
  return std::optional<Ord>{value.value()};
}

void write_text(ByteWriter& writer, const std::string& text) { writer.text(text); }

[[nodiscard]] Result<std::string> read_text(ByteReader& reader, std::size_t bound) {
  return reader.text(bound);
}

// ---------------------------------------------------------------------------
// Observations
// ---------------------------------------------------------------------------

void write_payload(ByteWriter& writer, const ObservationPayload& payload) {
  if (const auto* airflow = std::get_if<AirflowReading>(&payload)) {
    writer.u8(0);
    writer.i64(airflow->value.cubic_metres_per_hour());
    return;
  }
  if (const auto* pressure = std::get_if<PressureReading>(&payload)) {
    writer.u8(1);
    writer.i64(pressure->differential.millipascals());
    return;
  }
  writer.u8(2);
  writer.u32(std::get<FanReading>(payload).percent.basis_points());
}

[[nodiscard]] Result<ObservationPayload> read_payload(ByteReader& reader) {
  Result<std::uint8_t> tag = reader.u8();
  if (!tag.ok()) {
    return tag.status();
  }
  switch (tag.value()) {
    case 0: {
      Result<std::int64_t> value = reader.i64();
      if (!value.ok()) {
        return value.status();
      }
      return ObservationPayload{AirflowReading{Airflow::from_cubic_metres_per_hour(value.value())}};
    }
    case 1: {
      Result<std::int64_t> value = reader.i64();
      if (!value.ok()) {
        return value.status();
      }
      return ObservationPayload{PressureReading{Pressure::from_millipascals(value.value())}};
    }
    case 2: {
      Result<std::uint32_t> value = reader.u32();
      if (!value.ok()) {
        return value.status();
      }
      Result<SetpointBasisPoints> percent = SetpointBasisPoints::create(value.value());
      if (!percent.ok()) {
        return Status::failure(StatusCode::store_corrupt,
                               "durable observation holds an impossible fan setpoint");
      }
      return ObservationPayload{FanReading{percent.value()}};
    }
    default:
      return Status::failure(StatusCode::store_corrupt,
                             "durable observation holds an unknown payload tag");
  }
}

void write_draft(ByteWriter& writer, const ObservationDraft& draft) {
  write_payload(writer, draft.payload);
  write_identifier(writer, draft.device);
  write_opt_id(writer, draft.relationship);
  write_identifier(writer, draft.point);
  write_identifier(writer, draft.source);
  write_ordinal(writer, draft.sequence);
  write_tick(writer, draft.measured_at);
  write_ordinal(writer, draft.device_generation);
  write_ordinal(writer, draft.evidence_generation);
  writer.u32(static_cast<std::uint32_t>(draft.quality));
}

[[nodiscard]] Result<ObservationDraft> read_draft(ByteReader& reader) {
  Result<ObservationPayload> payload = read_payload(reader);
  if (!payload.ok()) {
    return payload.status();
  }
  Result<AirflowDeviceId> device = read_identifier<AirflowDeviceId>(reader);
  if (!device.ok()) {
    return device.status();
  }
  Result<std::optional<PressureRelationshipId>> relationship =
      read_opt_id<PressureRelationshipId>(reader);
  if (!relationship.ok()) {
    return relationship.status();
  }
  Result<SpaceRefId> point = read_identifier<SpaceRefId>(reader);
  if (!point.ok()) {
    return point.status();
  }
  Result<SourceId> source = read_identifier<SourceId>(reader);
  if (!source.ok()) {
    return source.status();
  }
  Result<EvidenceSequence> sequence = read_ordinal<EvidenceSequence>(reader);
  if (!sequence.ok()) {
    return sequence.status();
  }
  Result<LogicalTick> measured_at = read_tick(reader);
  if (!measured_at.ok()) {
    return measured_at.status();
  }
  Result<DeviceGeneration> device_generation = read_ordinal<DeviceGeneration>(reader);
  if (!device_generation.ok()) {
    return device_generation.status();
  }
  Result<EvidenceGeneration> evidence_generation = read_ordinal<EvidenceGeneration>(reader);
  if (!evidence_generation.ok()) {
    return evidence_generation.status();
  }
  Result<Quality> quality = decode_enum<Quality>(reader, 3, "observation quality");
  if (!quality.ok()) {
    return quality.status();
  }
  // A pressure reading must name a relationship and must not name a
  // relationship when it carries airflow or a fan setpoint: the correlation
  // between payload kind and subject is part of the model, not a convention.
  const ObservationKind kind = observation_kind(payload.value());
  if (kind == ObservationKind::pressure && !relationship.value().has_value()) {
    return Status::failure(StatusCode::store_corrupt,
                           "durable pressure observation does not name a relationship");
  }
  ObservationDraft draft{std::move(payload).value(),
                         std::move(device).value(),
                         relationship.value(),
                         std::move(point).value(),
                         std::move(source).value(),
                         sequence.value(),
                         measured_at.value(),
                         device_generation.value(),
                         evidence_generation.value(),
                         quality.value()};
  return draft;
}

void write_observation(ByteWriter& writer, const Observation& observation) {
  write_ordinal(writer, observation.id);
  write_draft(writer, observation.draft);
  write_tick(writer, observation.accepted_at);
  writer.u8(observation.recovered ? 1 : 0);
}

[[nodiscard]] Result<Observation> read_observation(ByteReader& reader) {
  Result<ObservationId> id = read_ordinal<ObservationId>(reader);
  if (!id.ok()) {
    return id.status();
  }
  Result<ObservationDraft> draft = read_draft(reader);
  if (!draft.ok()) {
    return draft.status();
  }
  Result<LogicalTick> accepted_at = read_tick(reader);
  if (!accepted_at.ok()) {
    return accepted_at.status();
  }
  Result<std::uint8_t> recovered = reader.u8();
  if (!recovered.ok()) {
    return recovered.status();
  }
  if (recovered.value() > 1) {
    return Status::failure(StatusCode::store_corrupt, "durable observation holds a bad flag byte");
  }
  if (accepted_at.value() < draft.value().measured_at) {
    return Status::failure(StatusCode::store_corrupt,
                           "durable observation was accepted before it was measured");
  }
  return Observation{id.value(), std::move(draft).value(), accepted_at.value(),
                     recovered.value() == 1};
}

// ---------------------------------------------------------------------------
// Model fragments
// ---------------------------------------------------------------------------

void write_band(ByteWriter& writer, const PressureBand& band) {
  writer.u32(static_cast<std::uint32_t>(band.polarity()));
  writer.i64(band.lower().millipascals());
  writer.i64(band.upper().millipascals());
  writer.i64(band.tolerance().millipascals());
}

[[nodiscard]] Result<PressureBand> read_band(ByteReader& reader) {
  Result<PressurePolarity> polarity = decode_enum<PressurePolarity>(reader, 2, "pressure polarity");
  if (!polarity.ok()) {
    return polarity.status();
  }
  Result<std::int64_t> lower = reader.i64();
  if (!lower.ok()) {
    return lower.status();
  }
  Result<std::int64_t> upper = reader.i64();
  if (!upper.ok()) {
    return upper.status();
  }
  Result<std::int64_t> tolerance = reader.i64();
  if (!tolerance.ok()) {
    return tolerance.status();
  }
  return PressureBand::create(polarity.value(), Pressure::from_millipascals(lower.value()),
                              Pressure::from_millipascals(upper.value()),
                              Pressure::from_millipascals(tolerance.value()));
}

void write_envelope(ByteWriter& writer, const OperatingEnvelope& envelope) {
  writer.u32(envelope.min_fan_percent.basis_points());
  writer.u32(envelope.max_fan_percent.basis_points());
  writer.u32(envelope.default_fan_percent.basis_points());
  writer.i64(envelope.min_airflow.cubic_metres_per_hour());
  writer.i64(envelope.max_airflow.cubic_metres_per_hour());
  writer.u32(envelope.max_step.basis_points());
  write_identifier(writer, envelope.source);
  write_ordinal(writer, envelope.evidence_generation);
}

[[nodiscard]] Result<OperatingEnvelope> read_envelope(ByteReader& reader) {
  Result<std::uint32_t> min_percent = reader.u32();
  if (!min_percent.ok()) {
    return min_percent.status();
  }
  Result<std::uint32_t> max_percent = reader.u32();
  if (!max_percent.ok()) {
    return max_percent.status();
  }
  Result<std::uint32_t> default_percent = reader.u32();
  if (!default_percent.ok()) {
    return default_percent.status();
  }
  Result<std::int64_t> min_airflow = reader.i64();
  if (!min_airflow.ok()) {
    return min_airflow.status();
  }
  Result<std::int64_t> max_airflow = reader.i64();
  if (!max_airflow.ok()) {
    return max_airflow.status();
  }
  Result<std::uint32_t> max_step = reader.u32();
  if (!max_step.ok()) {
    return max_step.status();
  }
  Result<SourceId> source = read_identifier<SourceId>(reader);
  if (!source.ok()) {
    return source.status();
  }
  Result<EvidenceGeneration> evidence_generation = read_ordinal<EvidenceGeneration>(reader);
  if (!evidence_generation.ok()) {
    return evidence_generation.status();
  }
  Result<SetpointBasisPoints> min_bp = SetpointBasisPoints::create(min_percent.value());
  Result<SetpointBasisPoints> max_bp = SetpointBasisPoints::create(max_percent.value());
  Result<SetpointBasisPoints> default_bp = SetpointBasisPoints::create(default_percent.value());
  Result<SlewBasisPoints> step = SlewBasisPoints::create(max_step.value());
  if (!min_bp.ok() || !max_bp.ok() || !default_bp.ok() || !step.ok()) {
    return Status::failure(StatusCode::store_corrupt,
                           "durable envelope holds an impossible setpoint or slew bound");
  }
  OperatingEnvelope envelope{min_bp.value(),     max_bp.value(),        default_bp.value(),
                             Airflow::from_cubic_metres_per_hour(min_airflow.value()),
                             Airflow::from_cubic_metres_per_hour(max_airflow.value()),
                             step.value(),       std::move(source).value(),
                             evidence_generation.value()};
  Status valid = validate_envelope(envelope);
  if (!valid.ok()) {
    return Status::failure(StatusCode::store_corrupt,
                           "durable envelope is not a valid operating envelope: " + valid.message());
  }
  return envelope;
}

void write_policy(ByteWriter& writer, const FanPolicy& policy) {
  write_identifier(writer, policy.id);
  write_ordinal(writer, policy.generation);
  write_envelope(writer, policy.envelope);
}

[[nodiscard]] Result<FanPolicy> read_policy(ByteReader& reader) {
  Result<PolicyId> id = read_identifier<PolicyId>(reader);
  if (!id.ok()) {
    return id.status();
  }
  Result<PolicyGeneration> generation = read_ordinal<PolicyGeneration>(reader);
  if (!generation.ok()) {
    return generation.status();
  }
  Result<OperatingEnvelope> envelope = read_envelope(reader);
  if (!envelope.ok()) {
    return envelope.status();
  }
  return FanPolicy{std::move(id).value(), generation.value(), std::move(envelope).value()};
}

void write_setpoint(ByteWriter& writer, const SetpointRequest& setpoint) {
  if (const auto* percent = std::get_if<SetpointPercent>(&setpoint)) {
    writer.u8(0);
    writer.u32(percent->percent.basis_points());
    return;
  }
  writer.u8(1);
  writer.i64(std::get<SetpointAirflow>(setpoint).airflow.cubic_metres_per_hour());
}

[[nodiscard]] Result<SetpointRequest> read_setpoint(ByteReader& reader) {
  Result<std::uint8_t> tag = reader.u8();
  if (!tag.ok()) {
    return tag.status();
  }
  switch (tag.value()) {
    case 0: {
      Result<std::uint32_t> value = reader.u32();
      if (!value.ok()) {
        return value.status();
      }
      Result<SetpointBasisPoints> percent = SetpointBasisPoints::create(value.value());
      if (!percent.ok()) {
        return Status::failure(StatusCode::store_corrupt,
                               "durable setpoint holds an impossible fan setpoint");
      }
      return SetpointRequest{SetpointPercent{percent.value()}};
    }
    case 1: {
      Result<std::int64_t> value = reader.i64();
      if (!value.ok()) {
        return value.status();
      }
      return SetpointRequest{
          SetpointAirflow{Airflow::from_cubic_metres_per_hour(value.value())}};
    }
    default:
      return Status::failure(StatusCode::store_corrupt, "durable setpoint holds an unknown tag");
  }
}

void write_actions(ByteWriter& writer, const ActionSet& actions) { writer.u32(actions.mask()); }

[[nodiscard]] Result<ActionSet> read_actions(ByteReader& reader) {
  Result<std::uint32_t> mask = reader.u32();
  if (!mask.ok()) {
    return mask.status();
  }
  Result<ActionSet> actions = ActionSet::from_mask(mask.value());
  if (!actions.ok()) {
    return Status::failure(StatusCode::store_corrupt,
                           "durable grant sets an undefined action bit");
  }
  return actions;
}


[[nodiscard]] bool request_class_consistent(ControlIntent intent, RequestClass klass) noexcept;

template <typename T, typename KeyFn>
[[nodiscard]] Status check_canonical(const std::vector<T>& items, KeyFn key, const char* what) {
  for (std::size_t index = 1; index < items.size(); ++index) {
    const auto order = key(items[index - 1]) <=> key(items[index]);
    if (order == 0) {
      return duplicate_identity(what);
    }
    if (order > 0) {
      return non_canonical(what);
    }
  }
  return Status::success();
}

template <typename Id>
void write_id_sequence(ByteWriter& writer, const std::vector<Id>& values) {
  writer.u32(static_cast<std::uint32_t>(values.size()));
  for (const Id& value : values) {
    write_identifier(writer, value);
  }
}

[[nodiscard]] Result<std::vector<AirflowDeviceId>> read_device_sequence(ByteReader& reader) {
  Result<std::size_t> count = reader.count(ModelBounds::max_devices_per_obligation, "bound device");
  if (!count.ok()) {
    return count.status();
  }
  std::vector<AirflowDeviceId> values;
  values.reserve(count.value());
  for (std::size_t index = 0; index < count.value(); ++index) {
    Result<AirflowDeviceId> value = read_identifier<AirflowDeviceId>(reader);
    if (!value.ok()) {
      return value.status();
    }
    values.push_back(std::move(value).value());
  }
  std::sort(values.begin(), values.end());
  for (std::size_t index = 1; index < values.size(); ++index) {
    if (values[index - 1] == values[index]) {
      return Status::failure(StatusCode::duplicate_identity,
                             "an obligation names the same device twice");
    }
  }
  return values;
}

void write_observation_slot(ByteWriter& writer, const std::optional<Observation>& value) {
  writer.optional(value, [&writer](const Observation& observation) {
    write_observation(writer, observation);
  });
}

[[nodiscard]] Result<std::optional<Observation>> read_observation_slot(ByteReader& reader) {
  Result<bool> present = reader.present();
  if (!present.ok()) {
    return present.status();
  }
  if (!present.value()) {
    return std::optional<Observation>{};
  }
  Result<Observation> observation = read_observation(reader);
  if (!observation.ok()) {
    return observation.status();
  }
  return std::optional<Observation>{std::move(observation).value()};
}

void write_device(ByteWriter& writer, const DeviceRecord& device) {
  write_identifier(writer, device.id);
  write_ordinal(writer, device.generation);
  write_ordinal(writer, device.revision);
  write_identifier(writer, device.room);
  writer.u32(static_cast<std::uint32_t>(device.lifecycle));
  write_opt_id(writer, device.row);
  writer.optional(device.policy, [&writer](const FanPolicy& policy) { write_policy(writer, policy); });
  writer.optional(device.unresolved_attempt,
                  [&writer](AttemptId value) { write_ordinal(writer, value); });
  writer.u32(static_cast<std::uint32_t>(device.effect));
  write_observation_slot(writer, device.fan_observation);
  write_observation_slot(writer, device.airflow_observation);
}

[[nodiscard]] Result<DeviceRecord> read_device(ByteReader& reader) {
  Result<AirflowDeviceId> id = read_identifier<AirflowDeviceId>(reader);
  if (!id.ok()) {
    return id.status();
  }
  Result<DeviceGeneration> generation = read_ordinal<DeviceGeneration>(reader);
  if (!generation.ok()) {
    return generation.status();
  }
  Result<StateRevision> revision = read_ordinal<StateRevision>(reader);
  if (!revision.ok()) {
    return revision.status();
  }
  Result<RoomId> room = read_identifier<RoomId>(reader);
  if (!room.ok()) {
    return room.status();
  }
  Result<DeviceLifecycle> lifecycle = decode_enum<DeviceLifecycle>(reader, 6, "device lifecycle");
  if (!lifecycle.ok()) {
    return lifecycle.status();
  }
  Result<std::optional<RowId>> row = read_opt_id<RowId>(reader);
  if (!row.ok()) {
    return row.status();
  }
  Result<bool> has_policy = reader.present();
  if (!has_policy.ok()) {
    return has_policy.status();
  }
  std::optional<FanPolicy> policy;
  if (has_policy.value()) {
    Result<FanPolicy> decoded = read_policy(reader);
    if (!decoded.ok()) {
      return decoded.status();
    }
    policy = std::move(decoded).value();
  }
  Result<std::optional<AttemptId>> unresolved = read_opt_ordinal<AttemptId>(reader);
  if (!unresolved.ok()) {
    return unresolved.status();
  }
  Result<EffectState> effect = decode_enum<EffectState>(reader, 3, "effect state");
  if (!effect.ok()) {
    return effect.status();
  }
  Result<std::optional<Observation>> fan = read_observation_slot(reader);
  if (!fan.ok()) {
    return fan.status();
  }
  Result<std::optional<Observation>> airflow = read_observation_slot(reader);
  if (!airflow.ok()) {
    return airflow.status();
  }
  if (fan.value().has_value() &&
      observation_kind(fan.value()->draft.payload) != ObservationKind::fan_setpoint) {
    return Status::failure(StatusCode::store_corrupt,
                           "a device fan slot holds an observation that is not a fan setpoint");
  }
  if (airflow.value().has_value() &&
      observation_kind(airflow.value()->draft.payload) != ObservationKind::airflow) {
    return Status::failure(StatusCode::store_corrupt,
                           "a device airflow slot holds an observation that is not an airflow");
  }
  DeviceRecord record{std::move(id).value(),
                      generation.value(),
                      revision.value(),
                      std::move(room).value(),
                      lifecycle.value(),
                      row.value(),
                      std::move(policy),
                      unresolved.value(),
                      effect.value(),
                      std::move(fan).value(),
                      std::move(airflow).value()};
  return record;
}

void write_relationship(ByteWriter& writer, const RelationshipRecord& record) {
  write_identifier(writer, record.relationship.id);
  write_identifier(writer, record.relationship.room);
  write_identifier(writer, record.relationship.controlled_space);
  write_identifier(writer, record.relationship.reference_space);
  write_band(writer, record.relationship.band);
  write_ordinal(writer, record.relationship.evidence_generation);
  write_ordinal(writer, record.relationship.revision);
  writer.u32(static_cast<std::uint32_t>(record.evidence.size()));
  for (const Observation& observation : record.evidence) {
    write_observation(writer, observation);
  }
}

[[nodiscard]] Result<RelationshipRecord> read_relationship(ByteReader& reader) {
  Result<PressureRelationshipId> id = read_identifier<PressureRelationshipId>(reader);
  if (!id.ok()) {
    return id.status();
  }
  Result<RoomId> room = read_identifier<RoomId>(reader);
  if (!room.ok()) {
    return room.status();
  }
  Result<SpaceRefId> controlled = read_identifier<SpaceRefId>(reader);
  if (!controlled.ok()) {
    return controlled.status();
  }
  Result<SpaceRefId> reference = read_identifier<SpaceRefId>(reader);
  if (!reference.ok()) {
    return reference.status();
  }
  Result<PressureBand> band = read_band(reader);
  if (!band.ok()) {
    return band.status();
  }
  Result<EvidenceGeneration> evidence_generation = read_ordinal<EvidenceGeneration>(reader);
  if (!evidence_generation.ok()) {
    return evidence_generation.status();
  }
  Result<StateRevision> revision = read_ordinal<StateRevision>(reader);
  if (!revision.ok()) {
    return revision.status();
  }
  Result<std::size_t> count =
      reader.count(ModelBounds::max_sources_per_relationship, "relationship source");
  if (!count.ok()) {
    return count.status();
  }
  if (controlled.value() == reference.value()) {
    return Status::failure(StatusCode::store_corrupt,
                           "a pressure relationship names the same space as its reference");
  }
  std::vector<Observation> evidence;
  evidence.reserve(count.value());
  for (std::size_t index = 0; index < count.value(); ++index) {
    Result<Observation> observation = read_observation(reader);
    if (!observation.ok()) {
      return observation.status();
    }
    evidence.push_back(std::move(observation).value());
  }
  for (const Observation& observation : evidence) {
    if (!observation.draft.relationship.has_value() ||
        !(*observation.draft.relationship == id.value())) {
      return Status::failure(StatusCode::store_corrupt,
                             "relationship evidence names a different relationship");
    }
    if (observation_kind(observation.draft.payload) != ObservationKind::pressure) {
      return Status::failure(StatusCode::store_corrupt,
                             "relationship evidence is not a pressure observation");
    }
  }
  Status ordered = check_canonical(
      evidence, [](const Observation& value) { return value.draft.source; },
      "relationship evidence source");
  if (!ordered.ok()) {
    return ordered;
  }
  PressureRelationship relationship{std::move(id).value(), std::move(room).value(),
                                    std::move(controlled).value(), std::move(reference).value(),
                                    std::move(band).value(), evidence_generation.value(),
                                    revision.value()};
  return RelationshipRecord{std::move(relationship), std::move(evidence)};
}

void write_containment(ByteWriter& writer, const ContainmentRecord& record) {
  write_identifier(writer, record.element.id);
  write_identifier(writer, record.element.room);
  write_opt_id(writer, record.element.row);
  writer.u32(static_cast<std::uint32_t>(record.element.kind));
  write_ordinal(writer, record.element.revision);
  write_identifier(writer, record.source);
  write_ordinal(writer, record.evidence_generation);
  writer.u32(static_cast<std::uint32_t>(record.state));
  writer.u32(static_cast<std::uint32_t>(record.quality));
  write_ordinal(writer, record.sequence);
  write_tick(writer, record.measured_at);
  writer.u8(record.has_report ? 1 : 0);
}

[[nodiscard]] Result<ContainmentRecord> read_containment(ByteReader& reader) {
  Result<ContainmentId> id = read_identifier<ContainmentId>(reader);
  if (!id.ok()) {
    return id.status();
  }
  Result<RoomId> room = read_identifier<RoomId>(reader);
  if (!room.ok()) {
    return room.status();
  }
  Result<std::optional<RowId>> row = read_opt_id<RowId>(reader);
  if (!row.ok()) {
    return row.status();
  }
  Result<ContainmentKind> kind = decode_enum<ContainmentKind>(reader, 5, "containment kind");
  if (!kind.ok()) {
    return kind.status();
  }
  Result<StateRevision> revision = read_ordinal<StateRevision>(reader);
  if (!revision.ok()) {
    return revision.status();
  }
  Result<SourceId> source = read_identifier<SourceId>(reader);
  if (!source.ok()) {
    return source.status();
  }
  Result<EvidenceGeneration> evidence_generation = read_ordinal<EvidenceGeneration>(reader);
  if (!evidence_generation.ok()) {
    return evidence_generation.status();
  }
  Result<ContainmentState> state = decode_enum<ContainmentState>(reader, 3, "containment state");
  if (!state.ok()) {
    return state.status();
  }
  Result<Quality> quality = decode_enum<Quality>(reader, 3, "containment quality");
  if (!quality.ok()) {
    return quality.status();
  }
  Result<EvidenceSequence> sequence = read_ordinal<EvidenceSequence>(reader);
  if (!sequence.ok()) {
    return sequence.status();
  }
  Result<LogicalTick> measured_at = read_tick(reader);
  if (!measured_at.ok()) {
    return measured_at.status();
  }
  Result<std::uint8_t> has_report = reader.u8();
  if (!has_report.ok()) {
    return has_report.status();
  }
  if (has_report.value() > 1) {
    return Status::failure(StatusCode::store_corrupt,
                           "a containment element holds a bad report flag byte");
  }
  if (has_report.value() == 0 && state.value() != ContainmentState::unknown) {
    return Status::failure(StatusCode::store_corrupt,
                           "a containment element with no report claims a known state");
  }
  ContainmentElement element{std::move(id).value(), std::move(room).value(), row.value(),
                             kind.value(), revision.value()};
  ContainmentRecord record{std::move(element),
                           std::move(source).value(),
                           evidence_generation.value(),
                           state.value(),
                           quality.value(),
                           sequence.value(),
                           measured_at.value(),
                           has_report.value() == 1};
  return record;
}

void write_obligation(ByteWriter& writer, const ObligationRecord& record) {
  const AirflowObligation& obligation = record.obligation;
  write_identifier(writer, obligation.id);
  writer.u32(static_cast<std::uint32_t>(obligation.scope));
  write_identifier(writer, obligation.room);
  write_opt_id(writer, obligation.row);
  write_opt_id(writer, obligation.rack);
  writer.u32(static_cast<std::uint32_t>(obligation.klass));
  writer.u32(static_cast<std::uint32_t>(obligation.binding));
  write_opt_id(writer, obligation.metered_point);
  write_id_sequence(writer, obligation.devices);
  writer.i64(obligation.minimum_airflow.cubic_metres_per_hour());
  writer.i64(obligation.target_airflow.cubic_metres_per_hour());
  write_identifier(writer, obligation.source);
  write_ordinal(writer, obligation.evidence_generation);
  write_ordinal(writer, obligation.revision);
  write_observation_slot(writer, record.metered_observation);
}

[[nodiscard]] Result<ObligationRecord> read_obligation(ByteReader& reader) {
  Result<ObligationId> id = read_identifier<ObligationId>(reader);
  if (!id.ok()) {
    return id.status();
  }
  Result<ObligationScope> scope = decode_enum<ObligationScope>(reader, 2, "obligation scope");
  if (!scope.ok()) {
    return scope.status();
  }
  Result<RoomId> room = read_identifier<RoomId>(reader);
  if (!room.ok()) {
    return room.status();
  }
  Result<std::optional<RowId>> row = read_opt_id<RowId>(reader);
  if (!row.ok()) {
    return row.status();
  }
  Result<std::optional<RackId>> rack = read_opt_id<RackId>(reader);
  if (!rack.ok()) {
    return rack.status();
  }
  Result<ObligationClass> klass = decode_enum<ObligationClass>(reader, 1, "obligation class");
  if (!klass.ok()) {
    return klass.status();
  }
  Result<ObligationBinding> binding = decode_enum<ObligationBinding>(reader, 1, "obligation binding");
  if (!binding.ok()) {
    return binding.status();
  }
  Result<std::optional<SpaceRefId>> metered = read_opt_id<SpaceRefId>(reader);
  if (!metered.ok()) {
    return metered.status();
  }
  Result<std::vector<AirflowDeviceId>> devices = read_device_sequence(reader);
  if (!devices.ok()) {
    return devices.status();
  }
  Result<std::int64_t> minimum = reader.i64();
  if (!minimum.ok()) {
    return minimum.status();
  }
  Result<std::int64_t> target = reader.i64();
  if (!target.ok()) {
    return target.status();
  }
  Result<SourceId> source = read_identifier<SourceId>(reader);
  if (!source.ok()) {
    return source.status();
  }
  Result<EvidenceGeneration> evidence_generation = read_ordinal<EvidenceGeneration>(reader);
  if (!evidence_generation.ok()) {
    return evidence_generation.status();
  }
  Result<StateRevision> revision = read_ordinal<StateRevision>(reader);
  if (!revision.ok()) {
    return revision.status();
  }
  Result<std::optional<Observation>> metered_observation = read_observation_slot(reader);
  if (!metered_observation.ok()) {
    return metered_observation.status();
  }
  if (minimum.value() > target.value()) {
    return Status::failure(StatusCode::store_corrupt,
                           "an obligation demands a minimum above its own target");
  }
  if (binding.value() == ObligationBinding::metered_scope && !metered.value().has_value()) {
    return Status::failure(StatusCode::store_corrupt,
                           "a metered obligation does not name its metered point");
  }
  if (binding.value() == ObligationBinding::device_sum && devices.value().empty()) {
    return Status::failure(StatusCode::store_corrupt, "a device-sum obligation binds no devices");
  }
  if (scope.value() == ObligationScope::row && !row.value().has_value()) {
    return Status::failure(StatusCode::store_corrupt, "a row obligation names no row");
  }
  if (scope.value() == ObligationScope::rack && !rack.value().has_value()) {
    return Status::failure(StatusCode::store_corrupt, "a rack obligation names no rack");
  }
  AirflowObligation obligation{std::move(id).value(),
                               scope.value(),
                               std::move(room).value(),
                               row.value(),
                               rack.value(),
                               klass.value(),
                               binding.value(),
                               metered.value(),
                               std::move(devices).value(),
                               Airflow::from_cubic_metres_per_hour(minimum.value()),
                               Airflow::from_cubic_metres_per_hour(target.value()),
                               std::move(source).value(),
                               evidence_generation.value(),
                               revision.value()};
  return ObligationRecord{std::move(obligation), std::move(metered_observation).value()};
}

void write_interlock(ByteWriter& writer, const Interlock& interlock) {
  write_identifier(writer, interlock.id);
  write_identifier(writer, interlock.room);
  write_opt_id(writer, interlock.row);
  write_opt_id(writer, interlock.device);
  writer.u32(static_cast<std::uint32_t>(interlock.klass));
  writer.u32(static_cast<std::uint32_t>(interlock.state));
  write_ordinal(writer, interlock.sequence);
  write_ordinal(writer, interlock.epoch);
  write_tick(writer, interlock.declared_at);
  write_opt_tick(writer, interlock.reported_at);
}

[[nodiscard]] Result<Interlock> read_interlock(ByteReader& reader) {
  Result<InterlockId> id = read_identifier<InterlockId>(reader);
  if (!id.ok()) {
    return id.status();
  }
  Result<RoomId> room = read_identifier<RoomId>(reader);
  if (!room.ok()) {
    return room.status();
  }
  Result<std::optional<RowId>> row = read_opt_id<RowId>(reader);
  if (!row.ok()) {
    return row.status();
  }
  Result<std::optional<AirflowDeviceId>> device = read_opt_id<AirflowDeviceId>(reader);
  if (!device.ok()) {
    return device.status();
  }
  Result<InterlockClass> klass = decode_enum<InterlockClass>(reader, 1, "interlock class");
  if (!klass.ok()) {
    return klass.status();
  }
  Result<InterlockState> state = decode_enum<InterlockState>(reader, 2, "interlock state");
  if (!state.ok()) {
    return state.status();
  }
  Result<EvidenceSequence> sequence = read_ordinal<EvidenceSequence>(reader);
  if (!sequence.ok()) {
    return sequence.status();
  }
  Result<AuthorityEpoch> epoch = read_ordinal<AuthorityEpoch>(reader);
  if (!epoch.ok()) {
    return epoch.status();
  }
  Result<LogicalTick> declared_at = read_tick(reader);
  if (!declared_at.ok()) {
    return declared_at.status();
  }
  Result<std::optional<LogicalTick>> reported_at = read_opt_tick(reader);
  if (!reported_at.ok()) {
    return reported_at.status();
  }
  if (reported_at.value().has_value() && reported_at.value().value() < declared_at.value()) {
    return Status::failure(StatusCode::store_corrupt,
                           "an interlock was reported before it was declared");
  }
  Interlock interlock{std::move(id).value(), std::move(room).value(), row.value(), device.value(),
                      klass.value(),          state.value(),           sequence.value(),
                      epoch.value(),          declared_at.value(),     reported_at.value()};
  return interlock;
}

void write_grant(ByteWriter& writer, const PermissionGrant& grant) {
  write_identifier(writer, grant.id);
  write_identifier(writer, grant.issuer);
  write_ordinal(writer, grant.epoch);
  write_opt_id(writer, grant.device);
  writer.optional(grant.device_generation,
                  [&writer](DeviceGeneration value) { write_ordinal(writer, value); });
  write_opt_id(writer, grant.room);
  write_actions(writer, grant.actions);
  write_tick(writer, grant.issued_at);
  write_opt_tick(writer, grant.expires_at);
  writer.u8(grant.revoked ? 1 : 0);
}

[[nodiscard]] Result<PermissionGrant> read_grant(ByteReader& reader) {
  Result<GrantId> id = read_identifier<GrantId>(reader);
  if (!id.ok()) {
    return id.status();
  }
  Result<SourceId> issuer = read_identifier<SourceId>(reader);
  if (!issuer.ok()) {
    return issuer.status();
  }
  Result<AuthorityEpoch> epoch = read_ordinal<AuthorityEpoch>(reader);
  if (!epoch.ok()) {
    return epoch.status();
  }
  Result<std::optional<AirflowDeviceId>> device = read_opt_id<AirflowDeviceId>(reader);
  if (!device.ok()) {
    return device.status();
  }
  Result<bool> has_generation = reader.present();
  if (!has_generation.ok()) {
    return has_generation.status();
  }
  std::optional<DeviceGeneration> device_generation;
  if (has_generation.value()) {
    Result<DeviceGeneration> generation = read_ordinal<DeviceGeneration>(reader);
    if (!generation.ok()) {
      return generation.status();
    }
    device_generation = generation.value();
  }
  Result<std::optional<RoomId>> room = read_opt_id<RoomId>(reader);
  if (!room.ok()) {
    return room.status();
  }
  Result<ActionSet> actions = read_actions(reader);
  if (!actions.ok()) {
    return actions.status();
  }
  Result<LogicalTick> issued_at = read_tick(reader);
  if (!issued_at.ok()) {
    return issued_at.status();
  }
  Result<std::optional<LogicalTick>> expires_at = read_opt_tick(reader);
  if (!expires_at.ok()) {
    return expires_at.status();
  }
  Result<std::uint8_t> revoked = reader.u8();
  if (!revoked.ok()) {
    return revoked.status();
  }
  if (revoked.value() > 1) {
    return Status::failure(StatusCode::store_corrupt, "a grant holds a bad revoked flag byte");
  }
  if (device_generation.has_value() && !device.value().has_value()) {
    return Status::failure(StatusCode::store_corrupt,
                           "a grant is bound to a device generation but names no device");
  }
  if (expires_at.value().has_value() && expires_at.value().value() < issued_at.value()) {
    return Status::failure(StatusCode::store_corrupt, "a grant expires before it is issued");
  }
  PermissionGrant grant{std::move(id).value(),
                        std::move(issuer).value(),
                        epoch.value(),
                        device.value(),
                        device_generation,
                        room.value(),
                        actions.value(),
                        issued_at.value(),
                        expires_at.value(),
                        revoked.value() == 1};
  return grant;
}

void write_override(ByteWriter& writer, const MaintenanceOverride& entry) {
  write_identifier(writer, entry.id);
  write_identifier(writer, entry.device);
  write_ordinal(writer, entry.device_generation);
  write_ordinal(writer, entry.epoch);
  write_tick(writer, entry.issued_at);
  write_tick(writer, entry.expires_at);
  write_text(writer, entry.reason);
  writer.u8(entry.revoked ? 1 : 0);
}

[[nodiscard]] Result<MaintenanceOverride> read_override(ByteReader& reader) {
  Result<OverrideId> id = read_identifier<OverrideId>(reader);
  if (!id.ok()) {
    return id.status();
  }
  Result<AirflowDeviceId> device = read_identifier<AirflowDeviceId>(reader);
  if (!device.ok()) {
    return device.status();
  }
  Result<DeviceGeneration> device_generation = read_ordinal<DeviceGeneration>(reader);
  if (!device_generation.ok()) {
    return device_generation.status();
  }
  Result<AuthorityEpoch> epoch = read_ordinal<AuthorityEpoch>(reader);
  if (!epoch.ok()) {
    return epoch.status();
  }
  Result<LogicalTick> issued_at = read_tick(reader);
  if (!issued_at.ok()) {
    return issued_at.status();
  }
  Result<LogicalTick> expires_at = read_tick(reader);
  if (!expires_at.ok()) {
    return expires_at.status();
  }
  Result<std::string> reason = read_text(reader, kMaxDecodedTextBytes);
  if (!reason.ok()) {
    return reason.status();
  }
  Result<std::string> validated_reason = validate_text(reason.value(), kMaxTextLength);
  if (!validated_reason.ok()) {
    return Status::failure(StatusCode::store_corrupt, "an override holds text that is not acceptable");
  }
  Result<std::uint8_t> revoked = reader.u8();
  if (!revoked.ok()) {
    return revoked.status();
  }
  if (revoked.value() > 1) {
    return Status::failure(StatusCode::store_corrupt, "an override holds a bad revoked flag byte");
  }
  if (expires_at.value() < issued_at.value()) {
    return Status::failure(StatusCode::store_corrupt, "an override expires before it is issued");
  }
  MaintenanceOverride entry{std::move(id).value(),
                            std::move(device).value(),
                            device_generation.value(),
                            epoch.value(),
                            issued_at.value(),
                            expires_at.value(),
                            std::move(validated_reason).value(),
                            revoked.value() == 1};
  return entry;
}

void write_permit(ByteWriter& writer, const SafetyPermit& permit) {
  write_identifier(writer, permit.id);
  write_identifier(writer, permit.issuer);
  write_ordinal(writer, permit.epoch);
  write_identifier(writer, permit.device);
  write_tick(writer, permit.issued_at);
  write_opt_tick(writer, permit.expires_at);
  write_text(writer, permit.reason);
}

[[nodiscard]] Result<SafetyPermit> read_permit(ByteReader& reader) {
  Result<SafetyPermitId> id = read_identifier<SafetyPermitId>(reader);
  if (!id.ok()) {
    return id.status();
  }
  Result<SourceId> issuer = read_identifier<SourceId>(reader);
  if (!issuer.ok()) {
    return issuer.status();
  }
  Result<AuthorityEpoch> epoch = read_ordinal<AuthorityEpoch>(reader);
  if (!epoch.ok()) {
    return epoch.status();
  }
  Result<AirflowDeviceId> device = read_identifier<AirflowDeviceId>(reader);
  if (!device.ok()) {
    return device.status();
  }
  Result<LogicalTick> issued_at = read_tick(reader);
  if (!issued_at.ok()) {
    return issued_at.status();
  }
  Result<std::optional<LogicalTick>> expires_at = read_opt_tick(reader);
  if (!expires_at.ok()) {
    return expires_at.status();
  }
  Result<std::string> reason = read_text(reader, kMaxDecodedTextBytes);
  if (!reason.ok()) {
    return reason.status();
  }
  Result<std::string> validated_reason = validate_text(reason.value(), kMaxTextLength);
  if (!validated_reason.ok()) {
    return Status::failure(StatusCode::store_corrupt, "a permit holds text that is not acceptable");
  }
  if (expires_at.value().has_value() && expires_at.value().value() < issued_at.value()) {
    return Status::failure(StatusCode::store_corrupt, "a permit expires before it is issued");
  }
  SafetyPermit permit{std::move(id).value(), std::move(issuer).value(), epoch.value(),
                      std::move(device).value(), issued_at.value(), expires_at.value(),
                      std::move(validated_reason).value()};
  return permit;
}

void write_attempt(ByteWriter& writer, const AttemptRecord& record) {
  write_ordinal(writer, record.id);
  write_ordinal(writer, record.ordinal);
  write_identifier(writer, record.key);
  write_identifier(writer, record.device);
  write_ordinal(writer, record.device_generation);
  write_ordinal(writer, record.epoch);
  write_ordinal(writer, record.planned_revision);
  write_ordinal(writer, record.policy_generation);
  write_ordinal(writer, record.evidence_generation);
  writer.u32(static_cast<std::uint32_t>(record.intent));
  writer.u32(static_cast<std::uint32_t>(record.request_class));
  write_setpoint(writer, record.setpoint);
  write_identifier(writer, record.actor);
  write_tick(writer, record.accepted_at);
  write_opt_tick(writer, record.dispatched_at);
  writer.u32(static_cast<std::uint32_t>(record.state));
  write_ordinal(writer, record.command);
  write_ordinal(writer, record.adapter_sequence);
  writer.u32(static_cast<std::uint32_t>(record.disposition));
  write_text(writer, record.detail);
  write_opt_id(writer, record.safety_permit);
  write_opt_ordinal(writer, record.supersedes);
  write_opt_ordinal(writer, record.superseded_by);
  writer.optional(record.fan_observation,
                  [&writer](ObservationId value) { write_ordinal(writer, value); });
  writer.optional(record.pressure_observation,
                  [&writer](ObservationId value) { write_ordinal(writer, value); });
  writer.optional(record.effect_sequence,
                  [&writer](EffectSequence value) { write_ordinal(writer, value); });
  write_opt_tick(writer, record.resolved_at);
  write_text(writer, record.resolution_reason);
}

[[nodiscard]] Result<AttemptRecord> read_attempt(ByteReader& reader) {
  Result<AttemptId> id = read_ordinal<AttemptId>(reader);
  if (!id.ok()) {
    return id.status();
  }
  Result<AttemptOrdinal> ordinal = read_ordinal<AttemptOrdinal>(reader);
  if (!ordinal.ok()) {
    return ordinal.status();
  }
  Result<IdempotencyKey> key = read_identifier<IdempotencyKey>(reader);
  if (!key.ok()) {
    return key.status();
  }
  Result<AirflowDeviceId> device = read_identifier<AirflowDeviceId>(reader);
  if (!device.ok()) {
    return device.status();
  }
  Result<DeviceGeneration> device_generation = read_ordinal<DeviceGeneration>(reader);
  if (!device_generation.ok()) {
    return device_generation.status();
  }
  Result<AuthorityEpoch> epoch = read_ordinal<AuthorityEpoch>(reader);
  if (!epoch.ok()) {
    return epoch.status();
  }
  Result<StateRevision> planned_revision = read_ordinal<StateRevision>(reader);
  if (!planned_revision.ok()) {
    return planned_revision.status();
  }
  Result<PolicyGeneration> policy_generation = read_ordinal<PolicyGeneration>(reader);
  if (!policy_generation.ok()) {
    return policy_generation.status();
  }
  Result<EvidenceGeneration> evidence_generation = read_ordinal<EvidenceGeneration>(reader);
  if (!evidence_generation.ok()) {
    return evidence_generation.status();
  }
  Result<ControlIntent> intent = decode_enum<ControlIntent>(reader, 7, "control intent");
  if (!intent.ok()) {
    return intent.status();
  }
  Result<RequestClass> request_class = decode_enum<RequestClass>(reader, 2, "request class");
  if (!request_class.ok()) {
    return request_class.status();
  }
  Result<SetpointRequest> setpoint = read_setpoint(reader);
  if (!setpoint.ok()) {
    return setpoint.status();
  }
  Result<ActorId> actor = read_identifier<ActorId>(reader);
  if (!actor.ok()) {
    return actor.status();
  }
  Result<LogicalTick> accepted_at = read_tick(reader);
  if (!accepted_at.ok()) {
    return accepted_at.status();
  }
  Result<std::optional<LogicalTick>> dispatched_at = read_opt_tick(reader);
  if (!dispatched_at.ok()) {
    return dispatched_at.status();
  }
  Result<AttemptState> state = decode_enum<AttemptState>(reader, 12, "attempt state");
  if (!state.ok()) {
    return state.status();
  }
  Result<CommandId> command = read_ordinal<CommandId>(reader);
  if (!command.ok()) {
    return command.status();
  }
  Result<AdapterSequence> adapter_sequence = read_ordinal<AdapterSequence>(reader);
  if (!adapter_sequence.ok()) {
    return adapter_sequence.status();
  }
  Result<AdapterDisposition> disposition =
      decode_enum<AdapterDisposition>(reader, 4, "adapter disposition");
  if (!disposition.ok()) {
    return disposition.status();
  }
  Result<std::string> detail = read_text(reader, kMaxDecodedTextBytes);
  if (!detail.ok()) {
    return detail.status();
  }
  Result<std::string> validated_detail = validate_text(detail.value(), kMaxTextLength);
  if (!validated_detail.ok()) {
    return Status::failure(StatusCode::store_corrupt, "an attempt holds text that is not acceptable");
  }
  Result<std::optional<SafetyPermitId>> permit = read_opt_id<SafetyPermitId>(reader);
  if (!permit.ok()) {
    return permit.status();
  }
  Result<std::optional<AttemptId>> supersedes = read_opt_ordinal<AttemptId>(reader);
  if (!supersedes.ok()) {
    return supersedes.status();
  }
  Result<std::optional<AttemptId>> superseded_by = read_opt_ordinal<AttemptId>(reader);
  if (!superseded_by.ok()) {
    return superseded_by.status();
  }
  Result<bool> has_fan = reader.present();
  if (!has_fan.ok()) {
    return has_fan.status();
  }
  std::optional<ObservationId> fan_observation;
  if (has_fan.value()) {
    Result<ObservationId> value = read_ordinal<ObservationId>(reader);
    if (!value.ok()) {
      return value.status();
    }
    fan_observation = value.value();
  }
  Result<bool> has_pressure = reader.present();
  if (!has_pressure.ok()) {
    return has_pressure.status();
  }
  std::optional<ObservationId> pressure_observation;
  if (has_pressure.value()) {
    Result<ObservationId> value = read_ordinal<ObservationId>(reader);
    if (!value.ok()) {
      return value.status();
    }
    pressure_observation = value.value();
  }
  Result<bool> has_effect = reader.present();
  if (!has_effect.ok()) {
    return has_effect.status();
  }
  std::optional<EffectSequence> effect_sequence;
  if (has_effect.value()) {
    Result<EffectSequence> value = read_ordinal<EffectSequence>(reader);
    if (!value.ok()) {
      return value.status();
    }
    effect_sequence = value.value();
  }
  Result<std::optional<LogicalTick>> resolved_at = read_opt_tick(reader);
  if (!resolved_at.ok()) {
    return resolved_at.status();
  }
  Result<std::string> resolution_reason = read_text(reader, kMaxDecodedTextBytes);
  if (!resolution_reason.ok()) {
    return resolution_reason.status();
  }
  Result<std::string> validated_reason = validate_text(resolution_reason.value(), kMaxTextLength);
  if (!validated_reason.ok()) {
    return Status::failure(StatusCode::store_corrupt,
                           "an attempt holds a resolution reason that is not acceptable");
  }
  if (!request_class_consistent(intent.value(), request_class.value())) {
    return Status::failure(StatusCode::store_corrupt,
                           "an attempt records a request class its intent does not have");
  }
  if (dispatched_at.value().has_value() && dispatched_at.value().value() < accepted_at.value()) {
    return Status::failure(StatusCode::store_corrupt,
                           "an attempt was dispatched before it was accepted");
  }
  if (resolved_at.value().has_value() && resolved_at.value().value() < accepted_at.value()) {
    return Status::failure(StatusCode::store_corrupt,
                           "an attempt was resolved before it was accepted");
  }
  AttemptRecord record{std::move(id).value(),
                       ordinal.value(),
                       std::move(key).value(),
                       std::move(device).value(),
                       device_generation.value(),
                       epoch.value(),
                       planned_revision.value(),
                       policy_generation.value(),
                       evidence_generation.value(),
                       intent.value(),
                       request_class.value(),
                       std::move(setpoint).value(),
                       std::move(actor).value(),
                       accepted_at.value(),
                       dispatched_at.value(),
                       state.value(),
                       command.value(),
                       adapter_sequence.value(),
                       disposition.value(),
                       std::move(validated_detail).value(),
                       permit.value(),
                       supersedes.value(),
                       superseded_by.value(),
                       fan_observation,
                       pressure_observation,
                       effect_sequence,
                       resolved_at.value(),
                       std::move(validated_reason).value()};
  return record;
}

void write_idempotency(ByteWriter& writer, const IdempotencySlot& slot) {
  write_identifier(writer, slot.key);
  write_ordinal(writer, slot.attempt);
  writer.u64(slot.fingerprint);
  write_text(writer, std::string(to_string(slot.result)));
}

[[nodiscard]] Result<IdempotencySlot> read_idempotency(ByteReader& reader) {
  Result<IdempotencyKey> key = read_identifier<IdempotencyKey>(reader);
  if (!key.ok()) {
    return key.status();
  }
  Result<AttemptId> attempt = read_ordinal<AttemptId>(reader);
  if (!attempt.ok()) {
    return attempt.status();
  }
  Result<std::uint64_t> fingerprint = reader.u64();
  if (!fingerprint.ok()) {
    return fingerprint.status();
  }
  Result<std::string> token = read_text(reader, 64);
  if (!token.ok()) {
    return token.status();
  }
  std::optional<StatusCode> code = parse_status_code(token.value());
  if (!code.has_value()) {
    return Status::failure(StatusCode::store_corrupt,
                           "an idempotency slot holds an unknown status token");
  }
  return IdempotencySlot{std::move(key).value(), attempt.value(), fingerprint.value(), *code};
}

void write_audit(ByteWriter& writer, const AuditEntry& entry) {
  write_ordinal(writer, entry.sequence);
  write_tick(writer, entry.tick);
  writer.u32(static_cast<std::uint32_t>(entry.kind));
  write_text(writer, entry.subject);
  write_text(writer, entry.detail);
}

[[nodiscard]] Result<AuditEntry> read_audit(ByteReader& reader) {
  Result<AuditSequence> sequence = read_ordinal<AuditSequence>(reader);
  if (!sequence.ok()) {
    return sequence.status();
  }
  Result<LogicalTick> tick = read_tick(reader);
  if (!tick.ok()) {
    return tick.status();
  }
  Result<AuditKind> kind = decode_enum<AuditKind>(reader, 29, "audit kind");
  if (!kind.ok()) {
    return kind.status();
  }
  Result<std::string> subject = read_text(reader, kMaxDecodedTextBytes);
  if (!subject.ok()) {
    return subject.status();
  }
  Result<std::string> detail = read_text(reader, kMaxDecodedTextBytes);
  if (!detail.ok()) {
    return detail.status();
  }
  AuditEntry entry{sequence.value(), tick.value(), kind.value(), std::move(subject).value(),
                   std::move(detail).value()};
  return entry;
}

[[nodiscard]] bool request_class_consistent(ControlIntent intent, RequestClass klass) noexcept {
  return classify(intent) == klass;
}

}  // namespace

std::uint64_t fnv1a64(const std::uint8_t* data, std::size_t size) noexcept {
  return fnv1a64_extend(1469598103934665603ull, data, size);
}

std::uint64_t fnv1a64_extend(std::uint64_t seed, const std::uint8_t* data, std::size_t size) noexcept {
  std::uint64_t hash = seed;
  for (std::size_t index = 0; index < size; ++index) {
    hash ^= static_cast<std::uint64_t>(data[index]);
    hash *= 1099511628211ull;
  }
  return hash;
}

std::uint64_t fnv1a64_text(std::uint64_t seed, const std::string& text) noexcept {
  return fnv1a64_extend(seed, reinterpret_cast<const std::uint8_t*>(text.data()), text.size());
}

namespace {

template <typename T, typename WriteFn>
void write_collection(ByteWriter& writer, const std::vector<T>& values, WriteFn write_one) {
  writer.u32(static_cast<std::uint32_t>(values.size()));
  for (const T& value : values) {
    write_one(writer, value);
  }
}

template <typename T, typename ReadFn, typename KeyFn>
[[nodiscard]] Result<std::vector<T>> read_collection(ByteReader& reader, std::size_t bound,
                                                     const char* what, ReadFn read_one, KeyFn key) {
  Result<std::size_t> count = reader.count(bound, what);
  if (!count.ok()) {
    return count.status();
  }
  std::vector<T> values;
  values.reserve(count.value());
  for (std::size_t index = 0; index < count.value(); ++index) {
    Result<T> value = read_one(reader);
    if (!value.ok()) {
      return value.status();
    }
    values.push_back(std::move(value).value());
  }
  Status ordered = check_canonical(values, key, what);
  if (!ordered.ok()) {
    return ordered;
  }
  return values;
}

[[nodiscard]] Status require(bool condition, const char* what) {
  if (condition) {
    return Status::success();
  }
  return Status::failure(StatusCode::store_corrupt, std::string("durable state is inconsistent: ") + what);
}

}  // namespace

Status encode_model(const ModelState& state, std::vector<std::uint8_t>& out) {
  ByteWriter writer;
  writer.u32(kPayloadMagic);
  writer.u32(kPayloadVersion);

  write_ordinal(writer, state.epoch);
  write_tick(writer, state.tick);
  write_ordinal(writer, state.next_incarnation);
  write_ordinal(writer, state.next_command);
  write_ordinal(writer, state.next_attempt);
  write_ordinal(writer, state.next_observation);
  write_ordinal(writer, state.next_effect);
  write_ordinal(writer, state.next_audit);
  writer.u64(static_cast<std::uint64_t>(state.idempotency_window));
  writer.u64(static_cast<std::uint64_t>(state.attempt_journal_capacity));
  writer.u64(static_cast<std::uint64_t>(state.audit_capacity));
  writer.u64(state.audit_dropped);

  write_collection(writer, canonical_order(state.devices, [](const DeviceRecord& value) { return value.id; }),
                   [](ByteWriter& target, const DeviceRecord& value) { write_device(target, value); });
  write_collection(writer,
                   canonical_order(state.relationships,
                                   [](const RelationshipRecord& value) { return value.relationship.id; }),
                   [](ByteWriter& target, const RelationshipRecord& value) {
                     write_relationship(target, value);
                   });
  write_collection(writer,
                   canonical_order(state.containment,
                                   [](const ContainmentRecord& value) { return value.element.id; }),
                   [](ByteWriter& target, const ContainmentRecord& value) {
                     write_containment(target, value);
                   });
  write_collection(writer,
                   canonical_order(state.obligations,
                                   [](const ObligationRecord& value) { return value.obligation.id; }),
                   [](ByteWriter& target, const ObligationRecord& value) {
                     write_obligation(target, value);
                   });
  write_collection(writer, canonical_order(state.interlocks, [](const Interlock& value) { return value.id; }),
                   [](ByteWriter& target, const Interlock& value) { write_interlock(target, value); });
  write_collection(writer,
                   canonical_order(state.grants, [](const PermissionGrant& value) { return value.id; }),
                   [](ByteWriter& target, const PermissionGrant& value) { write_grant(target, value); });
  write_collection(writer,
                   canonical_order(state.overrides, [](const MaintenanceOverride& value) { return value.id; }),
                   [](ByteWriter& target, const MaintenanceOverride& value) {
                     write_override(target, value);
                   });
  write_collection(writer,
                   canonical_order(state.permits, [](const SafetyPermit& value) { return value.id; }),
                   [](ByteWriter& target, const SafetyPermit& value) { write_permit(target, value); });
  write_collection(writer, canonical_order(state.attempts, [](const AttemptRecord& value) { return value.id; }),
                   [](ByteWriter& target, const AttemptRecord& value) { write_attempt(target, value); });
  write_collection(writer,
                   canonical_order(state.idempotency, [](const IdempotencySlot& value) { return value.key; }),
                   [](ByteWriter& target, const IdempotencySlot& value) {
                     write_idempotency(target, value);
                   });
  write_collection(writer, canonical_order(state.audit, [](const AuditEntry& value) { return value.sequence; }),
                   [](ByteWriter& target, const AuditEntry& value) { write_audit(target, value); });

  out = std::move(writer).take();
  return Status::success();
}

Result<ModelState> decode_model(const std::uint8_t* data, std::size_t size) {
  ByteReader reader(data, size);

  Result<std::uint32_t> magic = reader.u32();
  if (!magic.ok()) {
    return magic.status();
  }
  if (magic.value() != kPayloadMagic) {
    return Status::failure(StatusCode::store_corrupt, "durable payload has the wrong magic");
  }
  Result<std::uint32_t> version = reader.u32();
  if (!version.ok()) {
    return version.status();
  }
  if (version.value() != kPayloadVersion) {
    return Status::failure(StatusCode::store_unsupported_version,
                           "durable payload version " + std::to_string(version.value()) +
                               " is not supported");
  }

  Result<AuthorityEpoch> epoch = read_ordinal<AuthorityEpoch>(reader);
  if (!epoch.ok()) {
    return epoch.status();
  }
  Result<LogicalTick> tick = read_tick(reader);
  if (!tick.ok()) {
    return tick.status();
  }
  Result<IncarnationId> next_incarnation = read_ordinal<IncarnationId>(reader);
  if (!next_incarnation.ok()) {
    return next_incarnation.status();
  }
  Result<CommandId> next_command = read_ordinal<CommandId>(reader);
  if (!next_command.ok()) {
    return next_command.status();
  }
  Result<AttemptId> next_attempt = read_ordinal<AttemptId>(reader);
  if (!next_attempt.ok()) {
    return next_attempt.status();
  }
  Result<ObservationId> next_observation = read_ordinal<ObservationId>(reader);
  if (!next_observation.ok()) {
    return next_observation.status();
  }
  Result<EffectSequence> next_effect = read_ordinal<EffectSequence>(reader);
  if (!next_effect.ok()) {
    return next_effect.status();
  }
  Result<AuditSequence> next_audit = read_ordinal<AuditSequence>(reader);
  if (!next_audit.ok()) {
    return next_audit.status();
  }
  Result<std::uint64_t> idempotency_window = reader.u64();
  if (!idempotency_window.ok()) {
    return idempotency_window.status();
  }
  Result<std::uint64_t> journal_capacity = reader.u64();
  if (!journal_capacity.ok()) {
    return journal_capacity.status();
  }
  Result<std::uint64_t> audit_capacity = reader.u64();
  if (!audit_capacity.ok()) {
    return audit_capacity.status();
  }
  Result<std::uint64_t> audit_dropped = reader.u64();
  if (!audit_dropped.ok()) {
    return audit_dropped.status();
  }

  if (idempotency_window.value() == 0 ||
      idempotency_window.value() > ModelBounds::max_idempotency_window) {
    return Status::failure(StatusCode::store_corrupt,
                           "durable idempotency window is outside the structural bound");
  }
  if (journal_capacity.value() < idempotency_window.value() ||
      journal_capacity.value() > ModelBounds::max_attempts) {
    return Status::failure(StatusCode::store_corrupt,
                           "durable attempt journal capacity is outside the structural bound");
  }
  if (audit_capacity.value() == 0 || audit_capacity.value() > ModelBounds::max_audit_capacity) {
    return Status::failure(StatusCode::store_corrupt,
                           "durable audit capacity is outside the structural bound");
  }

  Result<std::vector<DeviceRecord>> devices =
      read_collection<DeviceRecord>(reader, ModelBounds::max_devices, "device",
                                    [](ByteReader& source) { return read_device(source); },
                                    [](const DeviceRecord& value) { return value.id; });
  if (!devices.ok()) {
    return devices.status();
  }
  Result<std::vector<RelationshipRecord>> relationships =
      read_collection<RelationshipRecord>(reader, ModelBounds::max_pressure_relationships,
                                          "pressure relationship",
                                          [](ByteReader& source) { return read_relationship(source); },
                                          [](const RelationshipRecord& value) {
                                            return value.relationship.id;
                                          });
  if (!relationships.ok()) {
    return relationships.status();
  }
  Result<std::vector<ContainmentRecord>> containment =
      read_collection<ContainmentRecord>(reader, ModelBounds::max_containment_elements,
                                         "containment element",
                                         [](ByteReader& source) { return read_containment(source); },
                                         [](const ContainmentRecord& value) {
                                           return value.element.id;
                                         });
  if (!containment.ok()) {
    return containment.status();
  }
  Result<std::vector<ObligationRecord>> obligations =
      read_collection<ObligationRecord>(reader, ModelBounds::max_obligations, "obligation",
                                        [](ByteReader& source) { return read_obligation(source); },
                                        [](const ObligationRecord& value) {
                                          return value.obligation.id;
                                        });
  if (!obligations.ok()) {
    return obligations.status();
  }
  Result<std::vector<Interlock>> interlocks =
      read_collection<Interlock>(reader, ModelBounds::max_interlocks, "interlock",
                                 [](ByteReader& source) { return read_interlock(source); },
                                 [](const Interlock& value) { return value.id; });
  if (!interlocks.ok()) {
    return interlocks.status();
  }
  Result<std::vector<PermissionGrant>> grants =
      read_collection<PermissionGrant>(reader, ModelBounds::max_grants, "grant",
                                       [](ByteReader& source) { return read_grant(source); },
                                       [](const PermissionGrant& value) { return value.id; });
  if (!grants.ok()) {
    return grants.status();
  }
  Result<std::vector<MaintenanceOverride>> overrides =
      read_collection<MaintenanceOverride>(reader, ModelBounds::max_overrides, "override",
                                           [](ByteReader& source) { return read_override(source); },
                                           [](const MaintenanceOverride& value) { return value.id; });
  if (!overrides.ok()) {
    return overrides.status();
  }
  Result<std::vector<SafetyPermit>> permits =
      read_collection<SafetyPermit>(reader, ModelBounds::max_safety_permits, "safety permit",
                                    [](ByteReader& source) { return read_permit(source); },
                                    [](const SafetyPermit& value) { return value.id; });
  if (!permits.ok()) {
    return permits.status();
  }
  Result<std::vector<AttemptRecord>> attempts =
      read_collection<AttemptRecord>(reader, ModelBounds::max_attempts, "attempt",
                                     [](ByteReader& source) { return read_attempt(source); },
                                     [](const AttemptRecord& value) { return value.id; });
  if (!attempts.ok()) {
    return attempts.status();
  }
  Result<std::vector<IdempotencySlot>> idempotency =
      read_collection<IdempotencySlot>(reader, ModelBounds::max_idempotency_window,
                                       "idempotency slot",
                                       [](ByteReader& source) { return read_idempotency(source); },
                                       [](const IdempotencySlot& value) { return value.key; });
  if (!idempotency.ok()) {
    return idempotency.status();
  }
  Result<std::vector<AuditEntry>> audit =
      read_collection<AuditEntry>(reader, ModelBounds::max_audit_capacity, "audit entry",
                                  [](ByteReader& source) { return read_audit(source); },
                                  [](const AuditEntry& value) { return value.sequence; });
  if (!audit.ok()) {
    return audit.status();
  }

  Status exhausted = reader.require_exhausted();
  if (!exhausted.ok()) {
    return exhausted;
  }

  // Retention the payload claims must actually hold the records it carries.
  Status retention =
      require(attempts.value().size() <= journal_capacity.value(),
              "attempt journal exceeds its declared capacity");
  if (!retention.ok()) {
    return retention;
  }
  retention = require(idempotency.value().size() <= idempotency_window.value(),
                      "idempotency window exceeds its declared capacity");
  if (!retention.ok()) {
    return retention;
  }
  retention = require(audit.value().size() <= audit_capacity.value(),
                      "audit ring exceeds its declared capacity");
  if (!retention.ok()) {
    return retention;
  }

  // Identity counters must be ahead of every identity the payload uses, or the
  // next allocation would repeat an identity that already exists. The ceilings
  // are plain integers so that the comparisons below cannot accidentally
  // compare an ordinal with a container of one.
  const std::uint64_t observation_ceiling = next_observation.value().value();
  const std::uint64_t attempt_ceiling = next_attempt.value().value();
  const std::uint64_t command_ceiling = next_command.value().value();
  const std::uint64_t effect_ceiling = next_effect.value().value();
  const std::uint64_t audit_ceiling = next_audit.value().value();

  for (const DeviceRecord& device : devices.value()) {
    Status ok = require(device.revision.value() >= 1, "a device has no revision");
    if (!ok.ok()) {
      return ok;
    }
    for (const std::optional<Observation>* slot :
         {&device.fan_observation, &device.airflow_observation}) {
      if (!slot->has_value()) {
        continue;
      }
      const Observation& observation = **slot;
      Status consistent =
          require(observation.id.value() < observation_ceiling,
                  "an observation identity is not below the observation counter");
      if (!consistent.ok()) {
        return consistent;
      }
      consistent = require(observation.draft.device == device.id,
                           "a device holds an observation for a different device");
      if (!consistent.ok()) {
        return consistent;
      }
      consistent = require(observation.draft.device_generation == device.generation,
                           "a device holds an observation from a different device generation");
      if (!consistent.ok()) {
        return consistent;
      }
      if (observation.draft.relationship.has_value()) {
        return Status::failure(StatusCode::store_corrupt,
                               "a device observation names a pressure relationship");
      }
    }
    if (device.unresolved_attempt.has_value()) {
      bool found = false;
      for (const AttemptRecord& attempt : attempts.value()) {
        if (attempt.id == *device.unresolved_attempt) {
          found = true;
          break;
        }
      }
      Status consistent = require(found, "a device names an unresolved attempt the journal lacks");
      if (!consistent.ok()) {
        return consistent;
      }
    }
  }

  for (const RelationshipRecord& record : relationships.value()) {
    for (const Observation& observation : record.evidence) {
      Status ok = require(observation.id.value() < observation_ceiling,
                          "an observation identity is not below the observation counter");
      if (!ok.ok()) {
        return ok;
      }
    }
  }

  for (const ObligationRecord& record : obligations.value()) {
    for (const AirflowDeviceId& bound : record.obligation.devices) {
      bool found = false;
      for (const DeviceRecord& device : devices.value()) {
        if (device.id == bound) {
          found = true;
          break;
        }
      }
      Status ok = require(found, "an obligation binds a device the model does not hold");
      if (!ok.ok()) {
        return ok;
      }
    }
    if (record.metered_observation.has_value()) {
      Status ok = require(record.metered_observation->id.value() < observation_ceiling,
                          "a metered observation identity is not below the observation counter");
      if (!ok.ok()) {
        return ok;
      }
    }
  }

  for (const AttemptRecord& attempt : attempts.value()) {
    Status ok = require(attempt.id.value() < attempt_ceiling,
                        "an attempt identity is not below the attempt counter");
    if (!ok.ok()) {
      return ok;
    }
    ok = require(attempt.command.value() < command_ceiling,
                 "an attempt names a command identity that is not below the command counter");
    if (!ok.ok()) {
      return ok;
    }
    ok = require(attempt.accepted_at <= tick.value(),
                 "an attempt was accepted in the future of the stored clock");
    if (!ok.ok()) {
      return ok;
    }
    bool device_known = false;
    for (const DeviceRecord& device : devices.value()) {
      if (device.id == attempt.device) {
        device_known = true;
        break;
      }
    }
    ok = require(device_known, "an attempt names a device the model does not hold");
    if (!ok.ok()) {
      return ok;
    }
    if (attempt.supersedes.has_value()) {
      ok = require(attempt.supersedes->value() < attempt_ceiling,
                   "an attempt supersedes an identity that was never allocated");
      if (!ok.ok()) {
        return ok;
      }
    }
    if (attempt.superseded_by.has_value()) {
      ok = require(attempt.superseded_by->value() < attempt_ceiling,
                   "an attempt was superseded by an identity that was never allocated");
      if (!ok.ok()) {
        return ok;
      }
    }
    if (attempt.effect_sequence.has_value()) {
      ok = require(attempt.effect_sequence->value() < effect_ceiling,
                   "an attempt names an effect sequence that is not below the effect counter");
      if (!ok.ok()) {
        return ok;
      }
    }
  }

  for (const IdempotencySlot& slot : idempotency.value()) {
    bool found = false;
    for (const AttemptRecord& attempt : attempts.value()) {
      if (attempt.id == slot.attempt) {
        found = true;
        break;
      }
    }
    Status ok = require(found, "an idempotency slot names an attempt the journal lacks");
    if (!ok.ok()) {
      return ok;
    }
  }

  for (const AuditEntry& entry : audit.value()) {
    Status ok = require(entry.sequence.value() < audit_ceiling,
                        "an audit sequence is not below the audit counter");
    if (!ok.ok()) {
      return ok;
    }
    ok = require(entry.tick <= tick.value(), "an audit entry records a future instant");
    if (!ok.ok()) {
      return ok;
    }
  }

  for (const PermissionGrant& grant : grants.value()) {
    if (!grant.device.has_value()) {
      continue;
    }
    bool found = false;
    for (const DeviceRecord& device : devices.value()) {
      if (device.id == *grant.device) {
        found = true;
        break;
      }
    }
    Status ok = require(found, "a grant names a device the model does not hold");
    if (!ok.ok()) {
      return ok;
    }
  }

  for (const MaintenanceOverride& entry : overrides.value()) {
    bool found = false;
    for (const DeviceRecord& device : devices.value()) {
      if (device.id == entry.device) {
        found = true;
        break;
      }
    }
    Status ok = require(found, "an override names a device the model does not hold");
    if (!ok.ok()) {
      return ok;
    }
  }

  for (const SafetyPermit& permit : permits.value()) {
    bool found = false;
    for (const DeviceRecord& device : devices.value()) {
      if (device.id == permit.device) {
        found = true;
        break;
      }
    }
    Status ok = require(found, "a safety permit names a device the model does not hold");
    if (!ok.ok()) {
      return ok;
    }
  }

  for (const Interlock& interlock : interlocks.value()) {
    if (!interlock.device.has_value()) {
      continue;
    }
    bool found = false;
    for (const DeviceRecord& device : devices.value()) {
      if (device.id == *interlock.device) {
        found = true;
        break;
      }
    }
    Status ok = require(found, "an interlock names a device the model does not hold");
    if (!ok.ok()) {
      return ok;
    }
  }

  ModelState state{epoch.value(),
                   tick.value(),
                   next_incarnation.value(),
                   next_command.value(),
                   next_attempt.value(),
                   next_observation.value(),
                   next_effect.value(),
                   next_audit.value()};
  state.idempotency_window = static_cast<std::size_t>(idempotency_window.value());
  state.attempt_journal_capacity = static_cast<std::size_t>(journal_capacity.value());
  state.audit_capacity = static_cast<std::size_t>(audit_capacity.value());
  state.audit_dropped = audit_dropped.value();
  state.devices = std::move(devices).value();
  state.relationships = std::move(relationships).value();
  state.containment = std::move(containment).value();
  state.obligations = std::move(obligations).value();
  state.interlocks = std::move(interlocks).value();
  state.grants = std::move(grants).value();
  state.overrides = std::move(overrides).value();
  state.permits = std::move(permits).value();
  state.attempts = std::move(attempts).value();
  state.idempotency = std::move(idempotency).value();
  state.audit = std::move(audit).value();
  return state;
}

}  // namespace airflow_control::detail
