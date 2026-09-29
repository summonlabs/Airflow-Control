// Persistence: what the durable store preserves, what it refuses, and what the
// canonical rendering must never contain.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include "test_harness.hpp"
#include "fixture.hpp"

using namespace airflow_control;
using namespace airflow_test;

namespace {

/// The documented file layout. The test names these constants rather than the
/// library's internals so that the format is asserted, not assumed.
constexpr std::uint64_t kHeaderBytes = 512;
constexpr std::uint64_t kHeadBytes = 128;

struct RawHead {
  bool committed = false;
  std::uint64_t generation = 0;
  std::uint64_t payload_length = 0;
};

[[nodiscard]] std::vector<unsigned char> read_bytes(const std::string& path, std::uint64_t offset,
                                                    std::size_t count) {
  std::ifstream stream(path, std::ios::binary);
  if (!stream) {
    return {};
  }
  stream.seekg(static_cast<std::streamoff>(offset));
  std::vector<unsigned char> bytes(count, 0);
  stream.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(count));
  bytes.resize(static_cast<std::size_t>(stream.gcount()));
  return bytes;
}

[[nodiscard]] std::uint64_t load_u64(const std::vector<unsigned char>& bytes, std::size_t offset) {
  std::uint64_t value = 0;
  for (int shift = 0; shift < 64; shift += 8) {
    value |= static_cast<std::uint64_t>(bytes[offset + static_cast<std::size_t>(shift) / 8]) << shift;
  }
  return value;
}

[[nodiscard]] RawHead read_head(const std::string& path, std::uint64_t slot_capacity, unsigned slot) {
  const std::uint64_t base = kHeaderBytes + static_cast<std::uint64_t>(slot) * slot_capacity;
  const std::vector<unsigned char> bytes = read_bytes(path, base, static_cast<std::size_t>(kHeadBytes));
  RawHead head;
  const char magic[8] = {'A', 'F', 'C', 'L', 'H', 'E', 'A', 'D'};
  if (bytes.size() < kHeadBytes) {
    return head;
  }
  for (std::size_t index = 0; index < 8; ++index) {
    if (bytes[index] != static_cast<unsigned char>(magic[index])) {
      return head;
    }
  }
  head.committed = true;
  head.generation = load_u64(bytes, 8);
  head.payload_length = load_u64(bytes, 16);
  return head;
}

void write_byte(const std::string& path, std::uint64_t offset, unsigned char value) {
  std::fstream stream(path, std::ios::binary | std::ios::in | std::ios::out);
  stream.seekp(static_cast<std::streamoff>(offset));
  const char byte = static_cast<char>(value);
  stream.write(&byte, 1);
}

void copy_file(const std::string& from, const std::string& to) {
  std::ifstream source(from, std::ios::binary);
  std::ofstream target(to, std::ios::binary | std::ios::trunc);
  target << source.rdbuf();
}

void truncate_to(const std::string& path, std::size_t bytes) {
  std::vector<unsigned char> content = read_bytes(path, 0, bytes);
  std::ofstream stream(path, std::ios::binary | std::ios::trunc);
  stream.write(reinterpret_cast<const char*>(content.data()),
               static_cast<std::streamsize>(content.size()));
}

/// Recovery marks every restored observation, which is the only difference a
/// round trip is allowed to introduce in the canonical rendering.
[[nodiscard]] std::string normalize_recovered(std::string text) {
  const std::string needle = "recovered=0";
  std::size_t position = 0;
  while ((position = text.find(needle, position)) != std::string::npos) {
    text.replace(position, needle.size(), "recovered=1");
    position += 11;
  }
  return text;
}

}  // namespace

AIRFLOW_TEST(a_populated_store_survives_a_round_trip) {
  TempDir directory("persistence");
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  const std::string path = directory.store_path();
  std::string first_incarnation;
  std::string second_incarnation;
  std::string third_incarnation;
  std::string first_digest;
  std::string second_digest;
  std::uint64_t generations[3] = {0, 0, 0};

  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_or_create, base_options());
    REQUIRE(opened.ok());
    AirflowControlEngine& engine = opened.value();
    CHECK(apply_scenario(engine, s).ok());
    const LogicalTick tick = engine.current_tick();
    // Every kind of durable fact the model can hold.
    CHECK(engine.observe(ObservationDraft{.payload = PressureReading{Pressure::from_millipascals(-20'000)},
                                           .device = s.device,
                                           .relationship = s.relationship,
                                           .point = SpaceRefId::parse("point-pressure-1").value(),
                                           .source = s.source,
                                           .sequence = EvidenceSequence::from(1),
                                           .measured_at = tick,
                                           .device_generation = s.device_generation,
                                           .evidence_generation = s.evidence_generation,
                                           .quality = Quality::good},
                         tick)
              .ok());
    CHECK(engine.observe(ObservationDraft{.payload = AirflowReading{Airflow::from_cubic_metres_per_hour(5'000)},
                                           .device = s.device,
                                           .relationship = std::nullopt,
                                           .point = SpaceRefId::parse("point-airflow-1").value(),
                                           .source = s.source,
                                           .sequence = EvidenceSequence::from(1),
                                           .measured_at = tick,
                                           .device_generation = s.device_generation,
                                           .evidence_generation = s.evidence_generation,
                                           .quality = Quality::good},
                         tick)
              .ok());
    CHECK(engine.observe(ObservationDraft{.payload = FanReading{SetpointBasisPoints::create(5'000).value()},
                                           .device = s.device,
                                           .relationship = std::nullopt,
                                           .point = SpaceRefId::parse("point-fan-1").value(),
                                           .source = s.source,
                                           .sequence = EvidenceSequence::from(1),
                                           .measured_at = tick,
                                           .device_generation = s.device_generation,
                                           .evidence_generation = s.evidence_generation,
                                           .quality = Quality::good},
                         tick)
              .ok());
    CHECK(engine.report_containment(ReportContainmentRequest{.element = s.containment,
                                                            .state = ContainmentState::open_for_service,
                                                            .quality = Quality::good,
                                                            .source = s.source,
                                                            .sequence = EvidenceSequence::from(2),
                                                            .evidence_generation = s.evidence_generation,
                                                            .measured_at = tick})
              .ok());
    CHECK(engine.report_interlock(ReportInterlockRequest{.id = s.interlock,
                                                        .state = InterlockState::satisfied,
                                                        .sequence = EvidenceSequence::from(2),
                                                        .epoch = s.epoch,
                                                        .reported_at = tick})
              .ok());
    CHECK(engine.declare_obligation(DeclareObligationRequest{
              .id = ObligationId::parse("ob-1").value(),
              .scope = ObligationScope::room,
              .room = s.room,
              .row = std::nullopt,
              .rack = std::nullopt,
              .klass = ObligationClass::protected_obligation,
              .binding = ObligationBinding::device_sum,
              .metered_point = std::nullopt,
              .devices = {s.device},
              .minimum_airflow = Airflow::from_cubic_metres_per_hour(2'000),
              .target_airflow = Airflow::from_cubic_metres_per_hour(5'000),
              .source = s.source,
              .evidence_generation = s.evidence_generation,
              .expected_revision = std::nullopt,
              .actor = s.actor,
              .requested_at = tick})
              .ok());
    CHECK(engine.add_maintenance_override(MaintenanceOverride{.id = OverrideId::parse("ov-1").value(),
                                                             .device = s.device,
                                                             .device_generation = s.device_generation,
                                                             .epoch = s.epoch,
                                                             .issued_at = tick,
                                                             .expires_at = LogicalTick::from(tick.value() + 500),
                                                             .reason = "filter change",
                                                             .revoked = false},
                                         s.actor, tick)
              .ok());
    CHECK(engine.add_safety_permit(SafetyPermit{.id = SafetyPermitId::parse("permit-1").value(),
                                               .issuer = s.issuer,
                                               .epoch = s.epoch,
                                               .device = s.device,
                                               .issued_at = tick,
                                               .expires_at = std::nullopt,
                                               .reason = "smoke test"},
                                  s.actor, tick)
              .ok());
    // A definite refusal resolves the attempt, so recovery does not change it.
    SyntheticAirflowAdapter adapter(synthetic_descriptor());
    seed_synthetic_adapter(adapter, s);
    adapter.set_next_disposition(s.device, AdapterDisposition::refused, "declined");
    const Result<AttemptRecord> issued =
        engine.issue(ControlRequest{.key = IdempotencyKey::parse("persisted-key").value(),
                                    .device = s.device,
                                    .device_generation = s.device_generation,
                                    .epoch = s.epoch,
                                    .expected_revision = std::nullopt,
                                    .intent = ControlIntent::hold_setpoint,
                                    .setpoint = SetpointRequest{SetpointPercent{
                                        .percent = SetpointBasisPoints::create(6'000).value()}},
                                    .actor = s.actor,
                                    .requested_at = tick,
                                    .safety_permit = std::nullopt,
                                    .supersede = std::nullopt,
                                    .relationship = std::nullopt},
                       adapter);
    REQUIRE(issued.ok());
    CHECK_EQ(issued.value().state, AttemptState::refused);
    first_incarnation = engine.canonical_state();
    first_digest = engine.state_digest();
    generations[0] = engine.store_audit().generation.value();
    CHECK(engine.close().ok());
  }
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
    REQUIRE(opened.ok());
    AirflowControlEngine& engine = opened.value();
    second_incarnation = engine.canonical_state();
    second_digest = engine.state_digest();
    generations[1] = engine.store_audit().generation.value();
    CHECK(generations[1] > generations[0]);
    // The only difference the round trip may introduce is the recovery mark.
    CHECK_EQ(second_incarnation, normalize_recovered(first_incarnation));
    CHECK(second_incarnation != first_incarnation);
    // Spot-check the facts through the public views.
    const Result<DeviceView> device = engine.device(s.device);
    REQUIRE(device.ok());
    CHECK_EQ(device.value().lifecycle, DeviceLifecycle::active);
    CHECK(device.value().policy_id.has_value());
    CHECK_EQ(device.value().generation, s.device_generation);
    CHECK_EQ(device.value().observed_airflow.value(), Airflow::from_cubic_metres_per_hour(5'000));
    CHECK_EQ(device.value().observed_fan_percent.value(), SetpointBasisPoints::create(5'000).value());
    const Result<RelationshipView> relationship = engine.relationship(s.relationship);
    REQUIRE(relationship.ok());
    // The reading survived the round trip, but restored dynamic state is not
    // current physical evidence, so the relationship is adjudicated unknown.
    CHECK_EQ(relationship.value().state, PressureState::unknown);
    CHECK(!relationship.value().adjudicated.has_value());
    CHECK_EQ(relationship.value().evidence_generation, s.evidence_generation);
    CHECK_EQ(relationship.value().lower, s.band_lower);
    CHECK_EQ(relationship.value().upper, s.band_upper);
    CHECK_EQ(relationship.value().polarity, PressurePolarity::negative);
    const std::vector<ContainmentView> containment = engine.containment();
    REQUIRE(containment.size() == 1);
    CHECK_EQ(containment[0].state, ContainmentState::open_for_service);
    CHECK_EQ(containment[0].sequence, EvidenceSequence::from(2));
    const std::vector<InterlockView> interlocks = engine.interlocks();
    REQUIRE(interlocks.size() == 1);
    CHECK_EQ(interlocks[0].state, InterlockState::satisfied);
    CHECK_EQ(interlocks[0].sequence, EvidenceSequence::from(2));
    const std::vector<ObligationView> obligations = engine.obligations();
    REQUIRE(obligations.size() == 1);
    CHECK_EQ(obligations[0].minimum_airflow, Airflow::from_cubic_metres_per_hour(2'000));
    CHECK_EQ(obligations[0].target_airflow, Airflow::from_cubic_metres_per_hour(5'000));
    const std::vector<MaintenanceOverride> overrides = engine.overrides();
    REQUIRE(overrides.size() == 1);
    CHECK_EQ(overrides[0].reason, std::string("filter change"));
    const std::vector<SafetyPermit> permits = engine.safety_permits();
    REQUIRE(permits.size() == 1);
    CHECK_EQ(permits[0].reason, std::string("smoke test"));
    const std::vector<AttemptView> attempts = engine.attempts();
    REQUIRE(attempts.size() == 1);
    CHECK_EQ(attempts[0].record.state, AttemptState::refused);
    CHECK_EQ(attempts[0].record.disposition, AdapterDisposition::refused);
    CHECK_EQ(attempts[0].record.key, IdempotencyKey::parse("persisted-key").value());
    CHECK_EQ(attempts[0].record.detail, std::string("declined"));
    // Recovered evidence is not current physical evidence.
    const std::vector<Observation> observations = engine.observations(s.device);
    CHECK(observations.size() >= 2);
    for (const Observation& observation : observations) {
      CHECK(observation.recovered);
    }
    CHECK_EQ(device.value().fan_freshness, FreshnessVerdict::recovered);
    CHECK_EQ(device.value().airflow_freshness, FreshnessVerdict::recovered);
    CHECK(engine.close().ok());
  }
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
    REQUIRE(opened.ok());
    AirflowControlEngine& engine = opened.value();
    third_incarnation = engine.canonical_state();
    generations[2] = engine.store_audit().generation.value();
    CHECK(generations[2] > generations[1]);
    CHECK_EQ(third_incarnation, second_incarnation);
    CHECK_EQ(engine.state_digest(), second_digest);
    CHECK_NE(second_digest, first_digest);
    CHECK(engine.close().ok());
  }
}

AIRFLOW_TEST(the_file_carries_the_documented_magic_version_and_extent) {
  TempDir directory("persistence-format");
  const std::string path = directory.store_path();
  std::uint64_t slot_capacity = 0;
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::create_new, base_options());
    REQUIRE(opened.ok());
    Result<Scenario> scenario = make_scenario("1", 1);
    REQUIRE(scenario.ok());
    CHECK(apply_scenario(opened.value(), *scenario).ok());
    slot_capacity = opened.value().store_audit().slot_capacity_bytes;
    CHECK_EQ(slot_capacity, kDefaultSlotCapacityBytes);
    CHECK(opened.value().close().ok());
  }
  const std::vector<unsigned char> header = read_bytes(path, 0, 16);
  REQUIRE(header.size() == 16);
  const std::string magic(reinterpret_cast<const char*>(header.data()), 8);
  CHECK_EQ(magic, std::string("AFCTLST1"));
  const auto version = static_cast<std::uint32_t>(header[8]) |
                       (static_cast<std::uint32_t>(header[9]) << 8) |
                       (static_cast<std::uint32_t>(header[10]) << 16) |
                       (static_cast<std::uint32_t>(header[11]) << 24);
  CHECK_EQ(version, std::uint32_t{1});
  const auto header_length = static_cast<std::uint32_t>(header[12]) |
                             (static_cast<std::uint32_t>(header[13]) << 8) |
                             (static_cast<std::uint32_t>(header[14]) << 16) |
                             (static_cast<std::uint32_t>(header[15]) << 24);
  CHECK_EQ(header_length, std::uint32_t{512});
  // The file reaches its final extent when it is created.
  std::error_code error;
  const std::uintmax_t size = std::filesystem::file_size(path, error);
  CHECK(!error);
  CHECK_EQ(static_cast<std::uint64_t>(size), kHeaderBytes + 2 * slot_capacity);
  // Both slots carry a committed head after a full open and close.
  const RawHead first = read_head(path, slot_capacity, 0);
  const RawHead second = read_head(path, slot_capacity, 1);
  CHECK(first.committed);
  CHECK(second.committed);
  CHECK_NE(first.generation, second.generation);
}

AIRFLOW_TEST(corruption_is_refused_and_never_adopted) {
  TempDir directory("persistence-corruption");
  const std::string root = directory.path();
  const std::string base = directory.store_path();
  std::uint64_t slot_capacity = 0;
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(base, OpenMode::create_new, base_options());
    REQUIRE(opened.ok());
    Result<Scenario> scenario = make_scenario("1", 1);
    REQUIRE(scenario.ok());
    CHECK(apply_scenario(opened.value(), *scenario).ok());
    slot_capacity = opened.value().store_audit().slot_capacity_bytes;
    CHECK(opened.value().close().ok());
  }

  // One payload byte, in the middle of the payload of every committed slot.
  {
    const std::string copy = root + "\\payload-corrupt.afcstore";
    copy_file(base, copy);
    for (unsigned slot = 0; slot < 2; ++slot) {
      const RawHead head = read_head(copy, slot_capacity, slot);
      REQUIRE(head.committed);
      REQUIRE(head.payload_length > 1);
      const std::uint64_t base_offset =
          kHeaderBytes + static_cast<std::uint64_t>(slot) * slot_capacity;
      const std::vector<unsigned char> original =
          read_bytes(copy, base_offset + kHeadBytes + head.payload_length / 2, 1);
      REQUIRE(original.size() == 1);
      write_byte(copy, base_offset + kHeadBytes + head.payload_length / 2,
                 static_cast<unsigned char>(original[0] ^ 0x5Au));
    }
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(copy, OpenMode::open_existing, base_options());
    CHECK(!opened.ok());
    CHECK_EQ(opened.code(), StatusCode::store_corrupt);
  }

  // One head byte, so that no slot holds a complete generation.
  {
    const std::string copy = root + "\\head-corrupt.afcstore";
    copy_file(base, copy);
    for (unsigned slot = 0; slot < 2; ++slot) {
      const RawHead head = read_head(copy, slot_capacity, slot);
      REQUIRE(head.committed);
      const std::uint64_t base_offset =
          kHeaderBytes + static_cast<std::uint64_t>(slot) * slot_capacity;
      write_byte(copy, base_offset + 9, 0x7Fu);
    }
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(copy, OpenMode::open_existing, base_options());
    CHECK(!opened.ok());
    CHECK_EQ(opened.code(), StatusCode::store_corrupt);
  }

  // The header magic, the header checksum, and the reserved header region.
  struct HeaderCase {
    const char* name;
    std::uint64_t offset;
    unsigned char value;
  };
  const HeaderCase header_cases[] = {{"magic", 0, 'X'}, {"crc", 504, 0x5Au}, {"reserved", 48, 0x11u}};
  for (const HeaderCase& item : header_cases) {
    const std::string copy = root + "\\header-" + item.name + ".afcstore";
    copy_file(base, copy);
    const std::vector<unsigned char> before = read_bytes(copy, item.offset, 1);
    REQUIRE(before.size() == 1);
    write_byte(copy, item.offset, static_cast<unsigned char>(before[0] ^ item.value));
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(copy, OpenMode::open_existing, base_options());
    CHECK(!opened.ok());
    if (opened.code() != StatusCode::store_corrupt) {
      std::cout << "  header " << item.name << " -> " << to_string(opened.code()) << "\n";
    }
    CHECK_EQ(opened.code(), StatusCode::store_corrupt);
  }

  // A truncated file: below the header, and inside a slot's payload.
  {
    const std::string copy = root + "\\truncated-header.afcstore";
    copy_file(base, copy);
    truncate_to(copy, 400);
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(copy, OpenMode::open_existing, base_options());
    CHECK(!opened.ok());
    CHECK_EQ(opened.code(), StatusCode::store_corrupt);
  }
  {
    const std::string copy = root + "\\truncated-payload.afcstore";
    copy_file(base, copy);
    truncate_to(copy, static_cast<std::size_t>(kHeaderBytes + kHeadBytes + 8));
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(copy, OpenMode::open_existing, base_options());
    CHECK(!opened.ok());
    CHECK_EQ(opened.code(), StatusCode::store_corrupt);
  }

  // The untouched store still opens: the refusals above are about the copies.
  Result<AirflowControlEngine> intact =
      AirflowControlEngine::open(base, OpenMode::open_existing, base_options());
  CHECK(intact.ok());
  if (intact.ok()) {
    CHECK_EQ(intact.value().devices().size(), std::size_t{1});
    CHECK(intact.value().close().ok());
  }
}

AIRFLOW_TEST(a_damaged_newest_head_falls_back_to_the_whole_previous_generation) {
  TempDir directory("persistence-fallback");
  const std::string path = directory.store_path();
  std::uint64_t slot_capacity = 0;
  std::string before_close;
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::create_new, base_options());
    REQUIRE(opened.ok());
    CHECK(apply_scenario(opened.value(), s).ok());
    CHECK(opened.value().observe(ObservationDraft{.payload = FanReading{SetpointBasisPoints::create(4'000).value()},
                                                   .device = s.device,
                                                   .relationship = std::nullopt,
                                                   .point = SpaceRefId::parse("point-fan-1").value(),
                                                   .source = s.source,
                                                   .sequence = EvidenceSequence::from(1),
                                                   .measured_at = opened.value().current_tick(),
                                                   .device_generation = s.device_generation,
                                                   .evidence_generation = s.evidence_generation,
                                                   .quality = Quality::good},
                                 opened.value().current_tick())
              .ok());
    slot_capacity = opened.value().store_audit().slot_capacity_bytes;
    before_close = opened.value().canonical_state();
    CHECK(opened.value().close().ok());
  }
  const RawHead first = read_head(path, slot_capacity, 0);
  const RawHead second = read_head(path, slot_capacity, 1);
  REQUIRE(first.committed);
  REQUIRE(second.committed);
  const unsigned newest_slot = first.generation > second.generation ? 0u : 1u;
  const std::uint64_t newest_generation = first.generation > second.generation ? first.generation
                                                                               : second.generation;
  const std::uint64_t older_generation = first.generation > second.generation ? second.generation
                                                                              : first.generation;
  CHECK(newest_generation > older_generation);
  // The damaged publication is the newest one. It must not be adopted, and the
  // previous whole generation must be, with the rollback reported.
  write_byte(path, kHeaderBytes + static_cast<std::uint64_t>(newest_slot) * slot_capacity, 'X');
  // Read through the store first: the adopted generation is the previous whole
  // one, and the refused publication is reported rather than hidden. (An engine
  // open would publish its own recovery generation, which is a later fact.)
  {
    Result<DurableStore> store = DurableStore::open(path, OpenMode::open_existing, StoreOptions{});
    REQUIRE(store.ok());
    const StoreAudit audit = store.value().audit();
    CHECK_EQ(audit.generation.value(), older_generation);
    CHECK(audit.rollback_observed);
    CHECK_EQ(audit.valid_head_records, std::uint32_t{1});
    CHECK(!store.value().payload().empty());
    CHECK(store.value().close().ok());
  }
  // The engine adopts the same whole generation and is fully usable.
  Result<AirflowControlEngine> opened =
      AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
  REQUIRE(opened.ok());
  CHECK_EQ(opened.value().devices().size(), std::size_t{1});
  CHECK_EQ(normalize_recovered(before_close), opened.value().canonical_state());
  CHECK(opened.value().close().ok());
}

AIRFLOW_TEST(open_modes_are_exact) {
  TempDir directory("persistence-modes");
  const std::string root = directory.path();
  const std::string path = directory.store_path();
  {
    Result<AirflowControlEngine> created =
        AirflowControlEngine::open(path, OpenMode::create_new, base_options());
    REQUIRE(created.ok());
    CHECK(created.value().close().ok());
  }
  // create_new never replaces an existing store.
  Result<AirflowControlEngine> again = AirflowControlEngine::open(path, OpenMode::create_new, base_options());
  CHECK(!again.ok());
  CHECK_EQ(again.code(), StatusCode::duplicate_identity);
  // open_existing never invents a store.
  Result<AirflowControlEngine> missing =
      AirflowControlEngine::open(root + "\\absent.afcstore", OpenMode::open_existing, base_options());
  CHECK(!missing.ok());
  CHECK_EQ(missing.code(), StatusCode::store_missing);
  // One writer at a time, inside one process too.
  Result<AirflowControlEngine> first = AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
  REQUIRE(first.ok());
  Result<AirflowControlEngine> second = AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
  CHECK(!second.ok());
  CHECK_EQ(second.code(), StatusCode::busy);
  Result<DurableStore> raw = DurableStore::open(path, OpenMode::open_existing, StoreOptions{});
  CHECK(!raw.ok());
  CHECK_EQ(raw.code(), StatusCode::busy);
  // The lock is released with the engine, and the store is then usable again.
  CHECK(first.value().close().ok());
  Result<AirflowControlEngine> third = AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
  CHECK(third.ok());
  if (third.ok()) {
    CHECK(third.value().is_durable());
    CHECK(third.value().store_audit().valid_head_records >= std::uint32_t{1});
    CHECK(third.value().store_audit().payload_bytes > 0);
    CHECK(third.value().close().ok());
  }
}

AIRFLOW_TEST(an_oversized_payload_is_refused_and_the_store_stays_usable) {
  TempDir directory("persistence-capacity");
  const std::string root = directory.path();
  const std::string source_path = root + "\\source.afcstore";
  std::vector<unsigned char> model_payload;
  std::uint64_t source_slot_capacity = 0;
  std::string source_state;
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(source_path, OpenMode::create_new, base_options());
    REQUIRE(opened.ok());
    Result<Scenario> scenario = make_scenario("1", 1);
    REQUIRE(scenario.ok());
    CHECK(apply_scenario(opened.value(), *scenario).ok());
    source_slot_capacity = opened.value().store_audit().slot_capacity_bytes;
    source_state = opened.value().canonical_state();
    CHECK(opened.value().close().ok());
  }
  const RawHead first = read_head(source_path, source_slot_capacity, 0);
  const RawHead second = read_head(source_path, source_slot_capacity, 1);
  REQUIRE(first.committed);
  REQUIRE(second.committed);
  const unsigned newest_slot = first.generation > second.generation ? 0u : 1u;
  const RawHead newest = newest_slot == 0u ? first : second;
  REQUIRE(newest.payload_length > 0);
  model_payload = read_bytes(source_path,
                             kHeaderBytes + static_cast<std::uint64_t>(newest_slot) * source_slot_capacity +
                                 kHeadBytes,
                             static_cast<std::size_t>(newest.payload_length));
  REQUIRE(model_payload.size() == newest.payload_length);
  CHECK(model_payload.size() < kMinSlotCapacityBytes);

  const std::string small_path = root + "\\small.afcstore";
  {
    StoreOptions options;
    options.slot_capacity_bytes = kMinSlotCapacityBytes;
    Result<DurableStore> store = DurableStore::open(small_path, OpenMode::create_new, options);
    REQUIRE(store.ok());
    CHECK(store.value().publish(model_payload, IncarnationId::from(1)).ok());
    const std::vector<std::uint8_t> oversized(static_cast<std::size_t>(kMinSlotCapacityBytes), 0x5Au);
    const Status refused = store.value().publish(oversized, IncarnationId::from(1));
    CHECK(!refused.ok());
    CHECK_EQ(refused.code(), StatusCode::store_capacity_exceeded);
    // The store is left usable and at the generation it had reached.
    const StoreGeneration generation = store.value().generation();
    CHECK_EQ(generation, StoreGeneration::from(1));
    CHECK(store.value().is_open());
    CHECK(store.value().payload() == model_payload);
    CHECK(store.value().publish(model_payload, IncarnationId::from(1)).ok());
    CHECK_EQ(store.value().generation(), StoreGeneration::from(2));
    CHECK(store.value().close().ok());
  }
  // An engine opens the small store, decodes the model, and keeps working.
  {
    EngineOptions options = base_options();
    options.store.slot_capacity_bytes = kMinSlotCapacityBytes;
    Result<AirflowControlEngine> engine =
        AirflowControlEngine::open(small_path, OpenMode::open_existing, options);
    REQUIRE(engine.ok());
    CHECK_EQ(normalize_recovered(source_state), engine.value().canonical_state());
    CHECK_EQ(engine.value().devices().size(), std::size_t{1});
    CHECK(engine.value().advance_tick(LogicalTick::from(engine.value().current_tick().value() + 1)).ok());
    CHECK_EQ(engine.value().store_audit().slot_capacity_bytes, kMinSlotCapacityBytes);
    CHECK(engine.value().close().ok());
  }
}

AIRFLOW_TEST(canonical_state_carries_no_incarnation_generation_or_audit) {
  TempDir directory("persistence-canonical");
  const std::string root = directory.path();
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  std::string first_state;
  std::string second_state;
  std::uint64_t first_generation = 0;
  std::uint64_t second_generation = 0;

  const auto drive = [&](const std::string& path, int extra_open_cycles, bool extra_audit,
                        std::string& state_out, std::uint64_t& generation_out) {
    for (int cycle = 0; cycle <= extra_open_cycles; ++cycle) {
      Result<AirflowControlEngine> opened =
          AirflowControlEngine::open(path, OpenMode::open_or_create, base_options());
      if (!opened.ok()) {
        CHECK(false);
        return;
      }
      if (cycle == 0) {
        CHECK(apply_scenario(opened.value(), s).ok());
        CHECK(opened.value().advance_tick(LogicalTick::from(opened.value().current_tick().value() + 3)).ok());
        SyntheticAirflowAdapter adapter(synthetic_descriptor());
        seed_synthetic_adapter(adapter, s);
        adapter.set_next_disposition(s.device, AdapterDisposition::refused, "declined");
        CHECK(opened.value()
                  .issue(ControlRequest{.key = IdempotencyKey::parse("canonical-key").value(),
                                        .device = s.device,
                                        .device_generation = s.device_generation,
                                        .epoch = s.epoch,
                                        .expected_revision = std::nullopt,
                                        .intent = ControlIntent::hold_setpoint,
                                        .setpoint = SetpointRequest{SetpointPercent{
                                            .percent = SetpointBasisPoints::create(6'000).value()}},
                                        .actor = s.actor,
                                        .requested_at = opened.value().current_tick(),
                                        .safety_permit = std::nullopt,
                                        .supersede = std::nullopt,
                                        .relationship = std::nullopt},
                         adapter)
                  .ok());
      }
      if (extra_audit) {
        // Audit entries are a bounded ring, not the authoritative model. A
        // refused command is journalled, so the ring grows and the store
        // publishes, while the canonical rendering must not move at all.
        SyntheticAirflowAdapter adapter(synthetic_descriptor());
        seed_synthetic_adapter(adapter, s);
        for (int index = 0; index < 20; ++index) {
          ControlRequest request{.key = IdempotencyKey::parse("audit-" + std::to_string(index)).value(),
                                 .device = s.device,
                                 .device_generation = DeviceGeneration::from(9),
                                 .epoch = s.epoch,
                                 .expected_revision = std::nullopt,
                                 .intent = ControlIntent::hold_setpoint,
                                 .setpoint = SetpointRequest{SetpointPercent{
                                     .percent = SetpointBasisPoints::create(5'000).value()}},
                                 .actor = s.actor,
                                 .requested_at = opened.value().current_tick(),
                                 .safety_permit = std::nullopt,
                                 .supersede = std::nullopt,
                                 .relationship = std::nullopt};
          CHECK_EQ(opened.value().issue(request, adapter).code(), StatusCode::generation_mismatch);
        }
        CHECK(opened.value().history(1'000).entries.size() >= 20);
        CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});
      }
      if (cycle == extra_open_cycles) {
        state_out = opened.value().canonical_state();
        generation_out = opened.value().store_audit().generation.value();
        return;
      }
      CHECK(opened.value().close().ok());
    }
  };

  drive(root + "\\canonical-a.afcstore", 0, false, first_state, first_generation);
  drive(root + "\\canonical-b.afcstore", 3, true, second_state, second_generation);
  // Different stores, different incarnation counts, different store generations,
  // different audit rings: the same logical events, so the same text.
  CHECK_NE(first_generation, second_generation);
  CHECK_EQ(first_state, second_state);
  CHECK(!first_state.empty());
  CHECK(first_state.find("incarnation") == std::string::npos);
  CHECK(first_state.find("store-generation") == std::string::npos);
  CHECK(first_state.find("AFCTLST") == std::string::npos);
  // No line of the canonical rendering is an audit entry. The retention line
  // names the audit capacity, which is a configured bound rather than an entry.
  {
    std::size_t start = 0;
    int lines = 0;
    while (start < first_state.size()) {
      const std::size_t end = first_state.find(static_cast<char>(10), start);
      const std::string line = first_state.substr(start, end - start);
      CHECK(line.compare(0, 6, "audit ") != 0);
      CHECK(line.compare(0, 1, " ") != 0 || line.compare(2, 6, "audit ") != 0);
      ++lines;
      if (end == std::string::npos) {
        break;
      }
      start = end + 1;
    }
    CHECK(lines > 10);
  }
}
