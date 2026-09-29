// Multiprocess: real operating-system processes, a real lock, a real death, and
// a real forged publication.
//
// Every child in this suite is this same executable, re-executed with a marker in
// its environment. The marker is read in a static initializer, before main(), so
// the child never runs the test harness: it performs one scenario and leaves.

#define _CRT_SECURE_NO_WARNINGS 1

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "test_harness.hpp"
#include "fixture.hpp"
#include "proc.hpp"

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "detail/path.hpp"
#include "detail/store_file.hpp"

using namespace airflow_control;
using namespace airflow_test;

namespace {

/// Environment markers the parent sets before it starts a child.
constexpr const char* kChildMode = "AIRFLOW_TEST_CHILD_MODE";
constexpr const char* kChildPath = "AIRFLOW_TEST_CHILD_PATH";
constexpr const char* kChildSuffix = "AIRFLOW_TEST_CHILD_SUFFIX";
/// The exit code a child uses when it dies inside the adapter, exactly as the
/// probe's crash exit code does.
constexpr int kAdapterCrashExitCode = 97;

void set_environment(const char* name, const std::string& value) {
#ifdef _WIN32
  _putenv_s(name, value.c_str());
#else
  if (value.empty()) {
    unsetenv(name);
  } else {
    setenv(name, value.c_str(), 1);
  }
#endif
}

[[nodiscard]] std::string environment(const char* name) {
  const char* value = std::getenv(name);
  return value == nullptr ? std::string() : std::string(value);
}

/// The path of this executable, so a child is this same program.
[[nodiscard]] std::string executable_path() {
#ifdef _WIN32
  std::wstring wide(4096, L'\0');
  const DWORD length = GetModuleFileNameW(nullptr, wide.data(), static_cast<DWORD>(wide.size()));
  if (length == 0 || length >= wide.size()) {
    return std::string();
  }
  wide.resize(length);
  const int needed = WideCharToMultiByte(CP_UTF8, 0, wide.data(), static_cast<int>(wide.size()),
                                         nullptr, 0, nullptr, nullptr);
  if (needed <= 0) {
    return std::string();
  }
  std::string narrow(static_cast<std::size_t>(needed), '\0');
  WideCharToMultiByte(CP_UTF8, 0, wide.data(), static_cast<int>(wide.size()), narrow.data(), needed,
                      nullptr, nullptr);
  return narrow;
#else
  std::vector<char> buffer(4096, '\0');
  const ssize_t length = ::readlink("/proc/self/exe", buffer.data(), buffer.size() - 1);
  if (length <= 0) {
    return std::string();
  }
  return std::string(buffer.data(), static_cast<std::size_t>(length));
#endif
}

/// An adapter that dies inside execute(). The durable record was published before
/// the call, which is what the child process is there to prove.
class DyingAdapter final : public AirflowAdapter {
 public:
  [[nodiscard]] AdapterDescriptor describe() const override { return synthetic_descriptor(); }
  AdapterOutcome execute(const AdapterCommand&) override { std::_Exit(kAdapterCrashExitCode); }
  Result<ObservationDraft> read(const AdapterReadRequest&) override {
    return Status::failure(StatusCode::adapter_unavailable, "this adapter does not read");
  }
};

/// Applies the fixture scenario and issues the command the child dies on. Shared
/// by the child path only; the parent never calls it.
[[noreturn]] void run_adapter_crash_child(const std::string& path) {
  Result<Scenario> scenario = make_scenario("1", 1);
  if (!scenario.ok()) {
    std::cout << "child-scenario-failed\n";
    std::cout.flush();
    std::_Exit(3);
  }
  Result<AirflowControlEngine> opened =
      AirflowControlEngine::open(path, OpenMode::open_or_create, base_options());
  if (!opened.ok()) {
    std::cout << "child-open-failed " << to_string(opened.code()) << "\n";
    std::cout.flush();
    std::_Exit(3);
  }
  const Status applied = apply_scenario(opened.value(), scenario.value());
  if (!applied.ok() && applied.code() != StatusCode::duplicate_identity) {
    std::cout << "child-apply-failed " << to_string(applied.code()) << "\n";
    std::cout.flush();
    std::_Exit(3);
  }
  DyingAdapter adapter;
  const ControlRequest request{.key = IdempotencyKey::parse("child-crash-key").value(),
                               .device = scenario.value().device,
                               .device_generation = scenario.value().device_generation,
                               .epoch = scenario.value().epoch,
                               .expected_revision = std::nullopt,
                               .intent = ControlIntent::hold_setpoint,
                               .setpoint = SetpointRequest{SetpointPercent{
                                   .percent = scenario.value().default_fan_percent}},
                               .actor = scenario.value().actor,
                               .requested_at = opened.value().current_tick(),
                               .safety_permit = std::nullopt,
                               .supersede = std::nullopt,
                               .relationship = std::nullopt};
  const Result<AttemptRecord> issued = opened.value().issue(request, adapter);
  // Reaching this line means the adapter returned, which it never does.
  std::cout << "child-unexpected-return " << to_string(issued.code()) << "\n";
  std::cout.flush();
  std::_Exit(4);
}

/// Holds the store's write authority until the operating system ends this
/// process. Nothing is written: the proof is the lock itself.
[[noreturn]] void run_hold_child(const std::string& path) {
  Result<DurableStore> store = DurableStore::open(path, OpenMode::open_or_create, StoreOptions{});
  if (!store.ok()) {
    std::cout << "child-store-failed " << to_string(store.code()) << "\n";
    std::cout.flush();
    std::_Exit(3);
  }
  std::cout << "READY\n";
  std::cout.flush();
  for (;;) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
}

/// Writes a head record for a generation the live writer never committed, out of
/// band: the store file is opened directly, and the lock is not consulted.
[[noreturn]] void run_forge_child(const std::string& path) {
  Result<std::string> canonical = detail::canonical_store_path(path);
  if (!canonical.ok()) {
    std::cout << "child-canonical-failed\n";
    std::cout.flush();
    std::_Exit(3);
  }
  Result<detail::StoreFile> file = detail::StoreFile::open(canonical.value());
  if (!file.ok()) {
    std::cout << "child-file-failed " << to_string(file.code()) << "\n";
    std::cout.flush();
    std::_Exit(3);
  }
  unsigned slot = 0;
  detail::SlotHead newest;
  bool found = false;
  for (unsigned index = 0; index < detail::kSlotCount; ++index) {
    const Result<detail::SlotHead> head = file.value().read_head(index);
    if (!head.ok() || !head.value().valid) {
      continue;
    }
    if (!found || head.value().generation > newest.generation) {
      found = true;
      newest = head.value();
      slot = index;
    }
  }
  if (!found) {
    std::cout << "child-no-head\n";
    std::cout.flush();
    std::_Exit(3);
  }
  Result<std::vector<std::uint8_t>> payload = file.value().read_payload(newest);
  if (!payload.ok()) {
    std::cout << "child-payload-failed\n";
    std::cout.flush();
    std::_Exit(3);
  }
  const unsigned target = (slot + 1U) % detail::kSlotCount;
  const Status written =
      file.value().write_slot(target, newest.generation + 1, payload.value(),
                              newest.writer_incarnation + 1000);
  if (!written.ok()) {
    std::cout << "child-write-failed " << to_string(written.code()) << "\n";
    std::cout.flush();
    std::_Exit(3);
  }
  (void)file.value().flush();
  std::cout << "FORGED " << (newest.generation + 1) << "\n";
  std::cout.flush();
  std::_Exit(0);
}

/// The child bootstrap. It runs before main() in every process, and returns
/// immediately unless the marker is present.
struct ChildBootstrap {
  ChildBootstrap() {
    const std::string mode = environment(kChildMode);
    if (mode.empty()) {
      return;
    }
    const std::string path = environment(kChildPath);
    if (mode == "hold") {
      run_hold_child(path);
    }
    if (mode == "forge") {
      run_forge_child(path);
    }
    if (mode == "crash-in-adapter") {
      run_adapter_crash_child(path);
    }
    std::cout << "child-mode-unknown\n";
    std::cout.flush();
    std::_Exit(5);
  }
};
ChildBootstrap g_child_bootstrap;

/// Starts a child of this executable in the named mode and clears the marker
/// again, so the parent's own process never looks like a child.
Result<ProcessHandle> spawn_child(const std::string& mode, const std::string& path,
                                  bool captured) {
  const std::string self = executable_path();
  if (self.empty()) {
    return Status::failure(StatusCode::internal_error,
                           "this executable cannot name its own path");
  }
  set_environment(kChildMode, mode);
  set_environment(kChildPath, path);
  Result<ProcessHandle> child = captured ? ProcessHandle::spawn_captured({self, mode})
                                         : ProcessHandle::spawn({self, mode});
  set_environment(kChildMode, std::string());
  set_environment(kChildPath, std::string());
  return child;
}

/// Waits for a captured child to report the ready line. A child that exits first
/// is a failure of the scenario, not a timeout of the test.
bool wait_for_ready(ProcessHandle& child, const char* marker, std::string& output) {
  for (int attempt = 0; attempt < 4000; ++attempt) {
    output = child.captured_output();
    if (output.find(marker) != std::string::npos) {
      return true;
    }
    if (child.exited()) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return false;
}

[[nodiscard]] std::string normalize_recovered(std::string text) {
  const std::string needle = "recovered=0";
  std::size_t position = 0;
  while ((position = text.find(needle, position)) != std::string::npos) {
    text.replace(position, needle.size(), "recovered=1");
    position += 11;
  }
  return text;
}

[[nodiscard]] bool file_exists(const std::string& path) {
  std::ifstream stream(path, std::ios::binary);
  return stream.good();
}

}  // namespace

AIRFLOW_TEST(a_real_second_process_holds_the_store_lock) {
  TempDir directory("multiprocess-lock");
  const std::string path = directory.store_path();
  {
    Result<AirflowControlEngine> seeded =
        AirflowControlEngine::open(path, OpenMode::open_or_create, base_options());
    REQUIRE(seeded.ok());
    CHECK(seeded.value().close().ok());
  }
  Result<DurableStore> before = DurableStore::open(path, OpenMode::open_existing, StoreOptions{});
  REQUIRE(before.ok());
  const std::uint64_t generation = before.value().generation().value();
  const std::vector<std::uint8_t> payload = before.value().payload();
  CHECK(before.value().close().ok());

  Result<ProcessHandle> child = spawn_child("hold", path, true);
  REQUIRE(child.ok());
  std::string output;
  const bool ready = wait_for_ready(child.value(), "READY", output);
  if (!ready) {
    std::cout << "  holder child output: " << output << " exit=" << child.value().exit_code()
              << std::endl;
  }
  CHECK(ready);
  // The holder is a real process: it holds the store's write authority.
  Result<AirflowControlEngine> blocked =
      AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
  CHECK(!blocked.ok());
  CHECK_EQ(blocked.code(), StatusCode::busy);
  Result<DurableStore> blocked_store =
      DurableStore::open(path, OpenMode::open_existing, StoreOptions{});
  CHECK(!blocked_store.ok());
  CHECK_EQ(blocked_store.code(), StatusCode::busy);

  // The operating system's own termination call releases the lock with the
  // process, and the store is untouched by it.
  CHECK(child.value().kill().ok());
  CHECK(!child.value().running());
  // A process that died holding the lock left the store byte-for-byte as it was.
  {
    Result<DurableStore> intact = DurableStore::open(path, OpenMode::open_existing, StoreOptions{});
    REQUIRE(intact.ok());
    CHECK_EQ(intact.value().payload(), payload);
    CHECK_EQ(intact.value().generation().value(), generation);
    CHECK_EQ(intact.value().audit().rollback_observed, false);
    CHECK(intact.value().close().ok());
  }
  Result<AirflowControlEngine> reopened =
      AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
  CHECK(reopened.ok());
  if (reopened.ok()) {
    CHECK(reopened.value().devices().empty());
    CHECK(!reopened.value().canonical_state().empty());
    CHECK(reopened.value().close().ok());
  }
  Result<DurableStore> after = DurableStore::open(path, OpenMode::open_existing, StoreOptions{});
  REQUIRE(after.ok());
  CHECK(after.value().generation().value() > generation);
  CHECK(after.value().payload().size() > payload.size());
  CHECK(after.value().close().ok());
}

AIRFLOW_TEST(a_forged_head_fences_the_live_writer) {
  TempDir directory("multiprocess-forge");
  const std::string path = directory.store_path();
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  Result<AirflowControlEngine> opened =
      AirflowControlEngine::open(path, OpenMode::open_or_create, base_options());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value();
  CHECK(apply_scenario(engine, scenario.value()).ok());
  const std::uint64_t generation = engine.store_audit().generation.value();
  const std::string canonical = engine.canonical_state();

  // A second, independent process publishes a generation this writer never saw,
  // without taking the lock.
  Result<ProcessHandle> forged = spawn_child("forge", path, true);
  REQUIRE(forged.ok());
  const Result<int> forged_code = forged.value().wait();
  CHECK(forged_code.ok());
  CHECK_EQ(forged_code.value(), 0);
  CHECK(forged.value().captured_output().find("FORGED") != std::string::npos);

  // The live writer is fenced rather than allowed to overwrite it.
  const Status advanced =
      engine.advance_tick(LogicalTick::from(engine.current_tick().value() + 1));
  CHECK(!advanced.ok());
  CHECK_EQ(advanced.code(), StatusCode::store_fenced);
  // Closing reports the same fence and still releases the lock.
  const Status closed = engine.close();
  CHECK(!closed.ok());
  CHECK_EQ(closed.code(), StatusCode::store_fenced);

  // The forged generation is a whole generation, so a reader adopts it, and the
  // store is usable again.
  Result<AirflowControlEngine> reopened =
      AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
  CHECK(reopened.ok());
  if (reopened.ok()) {
    CHECK(reopened.value().store_audit().generation.value() > generation);
    CHECK_EQ(normalize_recovered(canonical), reopened.value().canonical_state());
    CHECK(reopened.value().device(scenario.value().device).ok());
    CHECK(reopened.value().close().ok());
  }
}

AIRFLOW_TEST(a_real_process_death_at_the_adapter_boundary_is_recovered) {
  TempDir directory("multiprocess-crash");
  const std::string path = directory.store_path();
  {
    Result<AirflowControlEngine> seeded =
        AirflowControlEngine::open(path, OpenMode::create_new, base_options());
    REQUIRE(seeded.ok());
    CHECK(seeded.value().close().ok());
  }
  Result<ProcessHandle> child = spawn_child("crash-in-adapter", path, true);
  REQUIRE(child.ok());
  const Result<int> code = child.value().wait();
  REQUIRE(code.ok());
  const std::string output = child.value().captured_output();
  if (code.value() != kAdapterCrashExitCode) {
    std::cout << "  crash child exit=" << code.value() << " output=" << output << std::endl;
  }
  CHECK_EQ(code.value(), kAdapterCrashExitCode);

  // The process is gone, so the lock it held is gone with it. What it left
  // behind is a command that may have reached the device.
  Result<AirflowControlEngine> opened =
      AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
  REQUIRE(opened.ok());
  AirflowControlEngine& engine = opened.value();
  const std::vector<AttemptView> attempts = engine.attempts();
  REQUIRE(attempts.size() == 1);
  CHECK_EQ(attempts[0].record.state, AttemptState::recovery_required);
  CHECK_EQ(attempts[0].record.key, IdempotencyKey::parse("child-crash-key").value());
  CHECK_EQ(attempts[0].record.disposition, AdapterDisposition::indeterminate);
  const Result<DeviceView> device = engine.device(attempts[0].record.device);
  REQUIRE(device.ok());
  CHECK(device.value().unresolved_attempt.has_value());
  CHECK_EQ(*device.value().unresolved_attempt, attempts[0].record.id);

  // The command is never re-sent: a fresh adapter in this process is never called.
  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  seed_synthetic_adapter(adapter, make_scenario("1", 1).value());
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});
  const Result<AttemptRecord> refused =
      engine.issue(ControlRequest{.key = IdempotencyKey::parse("parent-key").value(),
                                  .device = attempts[0].record.device,
                                  .device_generation = attempts[0].record.device_generation,
                                  .epoch = attempts[0].record.epoch,
                                  .expected_revision = std::nullopt,
                                  .intent = ControlIntent::hold_setpoint,
                                  .setpoint = attempts[0].record.setpoint,
                                  .actor = attempts[0].record.actor,
                                  .requested_at = engine.current_tick(),
                                  .safety_permit = std::nullopt,
                                  .supersede = std::nullopt,
                                  .relationship = std::nullopt},
                     adapter);
  CHECK(!refused.ok());
  CHECK_EQ(refused.code(), StatusCode::attempt_unresolved);
  CHECK_EQ(adapter.execute_calls(), std::uint64_t{0});
  CHECK(engine.close().ok());
}

AIRFLOW_TEST(repeated_open_and_close_returns_to_a_baseline) {
  TempDir directory("multiprocess-cycles");
  const std::string path = directory.store_path();
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  std::size_t devices = 0;
  std::uint64_t previous_generation = 0;
  std::uint64_t previous_payload = 0;
  for (int cycle = 0; cycle < 6; ++cycle) {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_or_create, base_options());
    REQUIRE(opened.ok());
    AirflowControlEngine& engine = opened.value();
    if (cycle == 0) {
      CHECK(apply_scenario(engine, s).ok());
      devices = engine.devices().size();
    }
    // A completed command cycle: issue, verify nothing, resolve explicitly. The
    // device set never grows and the payload never runs away.
    CHECK_EQ(engine.devices().size(), devices);
    CHECK(engine.advance_tick(LogicalTick::from(engine.current_tick().value() + 1)).ok());
    const StoreAudit audit = engine.store_audit();
    CHECK(audit.publications > 0);
    CHECK(audit.payload_bytes < kDefaultSlotCapacityBytes);
    CHECK(audit.generation.value() > previous_generation);
    previous_generation = audit.generation.value();
    previous_payload = audit.payload_bytes;
    const HistoryView history = engine.history(1'000'000);
    CHECK(history.entries.size() <= 256);
    CHECK(engine.close().ok());
  }
  CHECK(previous_payload > 0);
  // The lock is free and the baseline is intact for the next process.
  Result<AirflowControlEngine> opened =
      AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
  REQUIRE(opened.ok());
  CHECK_EQ(opened.value().devices().size(), devices);
  CHECK(opened.value().attempts().empty());
  CHECK(!opened.value().canonical_state().empty());
  CHECK(opened.value().close().ok());
}

AIRFLOW_TEST(an_independent_process_reads_the_same_state) {
  const std::string probe = probe_executable();
  if (probe.empty() || !file_exists(probe)) {
    // The probe is built by the test tree; without it this case asserts what it
    // can from this process alone and says so.
    std::cout << "  probe executable is not available in this build" << std::endl;
  }
  TempDir directory("multiprocess-probe");
  const std::string path = directory.store_path();
  Result<Scenario> scenario = make_scenario("1", 1);
  REQUIRE(scenario.ok());
  const Scenario s = scenario.value();
  std::string canonical;
  std::string digest;
  {
    Result<AirflowControlEngine> opened =
        AirflowControlEngine::open(path, OpenMode::open_or_create, base_options());
    REQUIRE(opened.ok());
    CHECK(apply_scenario(opened.value(), s).ok());
    CHECK(opened.value()
              .observe(ObservationDraft{.payload = FanReading{SetpointBasisPoints::create(5'000).value()},
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
    CHECK(opened.value().close().ok());
  }
  // The state is read again from a second incarnation, so that this process sees
  // exactly what the independent process will see: a rendering that already
  // carries the recovery marks it introduced. The two must then agree byte for
  // byte, digest included.
  {
    Result<AirflowControlEngine> reopened =
        AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
    REQUIRE(reopened.ok());
    const std::vector<Observation> observations = reopened.value().observations(s.device);
    REQUIRE(!observations.empty());
    for (const Observation& observation : observations) {
      CHECK(observation.recovered);
    }
    canonical = reopened.value().canonical_state();
    digest = reopened.value().state_digest();
    CHECK_EQ(digest.size(), std::size_t{16});
    for (const char character : digest) {
      const bool hex = (character >= '0' && character <= '9') || (character >= 'a' && character <= 'f');
      CHECK(hex);
    }
    CHECK(reopened.value().close().ok());
  }
  if (!probe.empty() && file_exists(probe)) {
    // An independent process, started by the build, reads the same durable state.
    // Its output is read from a file the operating system wrote in text mode, so
    // carriage returns are dropped before the text is compared; the digest the
    // probe prints is the library's own and is compared exactly.
    std::string output;
    const Result<int> code = run_process_capture({probe, "inspect", path}, output);
    REQUIRE(code.ok());
    CHECK_EQ(code.value(), 0);
    std::string plain;
    for (const char character : output) {
      if (character != '') {
        plain.push_back(character);
      }
    }
    const std::size_t marker = plain.find("state-digest=");
    CHECK(marker != std::string::npos);
    if (marker != std::string::npos) {
      const std::string probe_canonical = plain.substr(0, marker);
      const std::string probe_digest = plain.substr(marker + 13, 16);
      // The independent process read the same durable state and rendered it the
      // same way, down to the digest it computed over that rendering.
      CHECK_EQ(probe_canonical, canonical);
      CHECK_EQ(probe_digest, digest);
      CHECK(probe_canonical.find("fan-observation") != std::string::npos);
      CHECK(probe_canonical.find("recovered=1") != std::string::npos);
    }
    // The probe also proves exclusion from the other side: it holds the store's
    // authority, and this process is refused while it does. It is terminated with
    // the operating system's own call, which is what releases the lock.
    Result<ProcessHandle> holder =
        ProcessHandle::spawn_captured({probe, "hold-lock", path, "60000"});
    REQUIRE(holder.ok());
    std::string holder_output;
    const bool holding = wait_for_ready(holder.value(), "holding=", holder_output);
    if (!holding) {
      std::cout << "  probe holder output: " << holder_output
                << " exit=" << holder.value().exit_code() << std::endl;
    }
    CHECK(holding);
    Result<AirflowControlEngine> blocked =
        AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
    CHECK(!blocked.ok());
    CHECK_EQ(blocked.code(), StatusCode::busy);
    CHECK(holder.value().kill().ok());
    Result<AirflowControlEngine> after =
        AirflowControlEngine::open(path, OpenMode::open_existing, base_options());
    CHECK(after.ok());
    if (after.ok()) {
      CHECK(after.value().close().ok());
    }
  }
}
