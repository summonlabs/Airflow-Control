// Example: a real process death at the command-attempt boundary.
//
// The program re-executes its own executable with an extra argument that selects
// the child path. The child opens the durable store, issues exactly one command
// through an adapter whose execute() calls std::_Exit(97) without returning, and
// therefore dies strictly after the engine published the attempt record and
// before any outcome could be recorded.
//
// The parent then reopens the store and proves:
//   * the attempt survived in state recovery_required and still latches the
//     device, so the command is never re-sent;
//   * a retry with the same idempotency key replays the durable attempt record
//     and never reaches an adapter, because the key is published together with
//     the attempt at the command-attempt boundary;
//   * a new key is refused with attempt_unresolved, because the physical result
//     of the killed command is still unknown.
//
// The example then crashes a second child after its outcome and verification
// have been published, and proves the replay there too, with an adapter whose
// execute() marks the example failed if it is ever called.
//
// Everything here is SYNTHETIC with respect to the device: the adapters are
// deterministic simulators and drive no hardware.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "airflow_control/engine.hpp"
#include "airflow_control/synthetic_adapter.hpp"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#else
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace airflow_control;

namespace {

int failures = 0;

void expect(bool condition, const std::string& description) {
  if (!condition) {
    std::cout << "EXPECTATION FAILED: " << description << "\n";
    failures += 1;
  } else {
    std::cout << "ok: " << description << "\n";
  }
}

const AirflowDeviceId kDevice = AirflowDeviceId::parse("room-e-fan-1").value();
const RoomId kRoom = RoomId::parse("room-e").value();
const ActorId kActor = ActorId::parse("example-operator").value();
const SourceId kField = SourceId::parse("field-instrumentation").value();

/// The key the child issues under, and the key the parent retries under.
const char* const kKilledKey = "crash-reopen-killed";
/// The key whose command completes and whose outcome is published before the
/// second child dies.
const char* const kPublishedKey = "crash-reopen-published";
/// A key that was never used for anything.
const char* const kUnusedKey = "crash-reopen-unused";

/// The child that dies inside execute() issues at this instant.
constexpr std::uint64_t kKilledTick = 4;
/// The child that dies after its publications issues at this instant.
constexpr std::uint64_t kPublishedTick = 8;
/// The commanded fan setpoint used by every command in this example.
constexpr std::uint32_t kSetpointBasisPoints = 7000;

EngineOptions example_options() {
  EngineOptions options;
  options.evidence.max_age_ticks = LogicalTick::from(1000);
  options.audit_capacity = 128;
  options.idempotency_window = 32;
  options.attempt_journal_capacity = 64;
  return options;
}

std::string store_path(const std::string& directory) {
  std::error_code error;
  std::filesystem::create_directories(directory, error);
  return directory + "/airflow-crash-reopen.airflowstore";
}

Status build_model(AirflowControlEngine& engine) {
  Status status = engine.adopt_epoch(AuthorityEpoch::from(1), kActor, LogicalTick::from(0));
  if (!status.ok()) {
    return status;
  }
  status = engine.register_device(RegisterDeviceRequest{
      .device = kDevice,
      .generation = DeviceGeneration::from(1),
      .room = kRoom,
      .row = std::nullopt,
      .actor = kActor,
      .requested_at = LogicalTick::from(0),
  });
  if (!status.ok()) {
    return status;
  }
  status = engine.add_grant(
      PermissionGrant{
          .id = GrantId::parse("room-e-grant").value(),
          .issuer = SourceId::parse("authority-airflow").value(),
          .epoch = AuthorityEpoch::from(1),
          .device = kDevice,
          .device_generation = DeviceGeneration::from(1),
          .room = kRoom,
          .actions = ActionSet::all(),
          .issued_at = LogicalTick::from(0),
          .expires_at = std::nullopt,
          .revoked = false,
      },
      kActor, LogicalTick::from(0));
  if (!status.ok()) {
    return status;
  }
  status = engine.set_device_lifecycle(SetLifecycleRequest{
      .device = kDevice,
      .generation = DeviceGeneration::from(1),
      .expected_revision = StateRevision::from(1),
      .target = DeviceLifecycle::active,
      .epoch = AuthorityEpoch::from(1),
      .actor = kActor,
      .requested_at = LogicalTick::from(0),
  });
  if (!status.ok()) {
    return status;
  }
  return engine.set_fan_policy(SetFanPolicyRequest{
      .device = kDevice,
      .generation = DeviceGeneration::from(1),
      .expected_revision = StateRevision::from(2),
      .policy =
          FanPolicy{
              .id = PolicyId::parse("room-e-policy").value(),
              .generation = PolicyGeneration::from(1),
              .envelope =
                  OperatingEnvelope{
                      .min_fan_percent = SetpointBasisPoints::create(0).value(),
                      .max_fan_percent = SetpointBasisPoints::create(10000).value(),
                      .default_fan_percent = SetpointBasisPoints::create(5000).value(),
                      .min_airflow = Airflow::from_cubic_metres_per_hour(0),
                      .max_airflow = Airflow::from_cubic_metres_per_hour(100000),
                      .max_step = SlewBasisPoints::create(10000).value(),
                      .source = SourceId::parse("authority-airflow").value(),
                      .evidence_generation = EvidenceGeneration::from(1),
                  },
          },
      .actor = kActor,
      .requested_at = LogicalTick::from(0),
  });
}

/// The request the child issues and the parent retries. It is built by one
/// function so that the retry is byte-for-byte the request that was issued,
/// which is what makes the replay a replay rather than a conflict.
ControlRequest make_request(const char* key, std::uint64_t tick) {
  return ControlRequest{
      .key = IdempotencyKey::parse(key).value(),
      .device = kDevice,
      .device_generation = DeviceGeneration::from(1),
      .epoch = AuthorityEpoch::from(1),
      .expected_revision = std::nullopt,
      .intent = ControlIntent::raise_airflow,
      .setpoint = SetpointPercent{SetpointBasisPoints::create(kSetpointBasisPoints).value()},
      .actor = kActor,
      .requested_at = LogicalTick::from(tick),
      .safety_permit = std::nullopt,
      .supersede = std::nullopt,
      .relationship = std::nullopt,
  };
}

AdapterDescriptor synthetic_descriptor() {
  AdapterDescriptor descriptor;
  descriptor.vendor = "Airflow Control";
  descriptor.model = "synthetic-crash-reopen-plant";
  descriptor.protocol = "in-process";
  descriptor.capabilities = static_cast<std::uint32_t>(AdapterCapability::set_fan_percent) |
                            static_cast<std::uint32_t>(AdapterCapability::read_fan_percent);
  descriptor.synthetic = true;
  return descriptor;
}

void seed_plant(SyntheticAirflowAdapter& adapter) {
  adapter.seed_device(kDevice, DeviceGeneration::from(1),
                      SyntheticPlantState{
                          .fan_percent = SetpointBasisPoints::create(kSetpointBasisPoints).value(),
                          .airflow =
                              Airflow::from_cubic_metres_per_hour(
                                  static_cast<std::int64_t>(kSetpointBasisPoints)),
                          .differential = Pressure::from_millipascals(-6000),
                      });
}

/// The adapter of the child that dies at the command-attempt boundary. It never
/// returns: std::_Exit runs no destructors and raises no dialog.
class DyingAdapter final : public AirflowAdapter {
 public:
  [[nodiscard]] AdapterDescriptor describe() const override { return synthetic_descriptor(); }

  AdapterOutcome execute(const AdapterCommand&) override {
    std::cout << "child: adapter execute() reached; the attempt is already durable, and the "
                 "process dies here\n";
    std::cout.flush();
    std::_Exit(97);
  }

  [[nodiscard]] Result<ObservationDraft> read(const AdapterReadRequest&) override {
    return Status::failure(StatusCode::adapter_unavailable, "the dying adapter does not read");
  }
};

/// An adapter that must never be called. It records the fact instead of aborting
/// the process, and the example fails if the flag is ever set.
class NeverCalledAdapter final : public AirflowAdapter {
 public:
  explicit NeverCalledAdapter(bool& reached) : reached_(reached) {}

  [[nodiscard]] AdapterDescriptor describe() const override { return synthetic_descriptor(); }

  AdapterOutcome execute(const AdapterCommand& command) override {
    reached_ = true;
    return AdapterOutcome{.command = command.id(),
                          .attempt = command.attempt(),
                          .device = command.device(),
                          .device_generation = command.device_generation(),
                          .disposition = AdapterDisposition::refused,
                          .sequence = AdapterSequence::from(0),
                          .acknowledged_at = command.issued_at(),
                          .detail = "this adapter must never be reached",
                          .reading = std::nullopt};
  }

  [[nodiscard]] Result<ObservationDraft> read(const AdapterReadRequest&) override {
    return Status::failure(StatusCode::adapter_unavailable,
                           "this adapter must never be read");
  }

 private:
  bool& reached_;
};

// ---------------------------------------------------------------------------
// Starting the child process
// ---------------------------------------------------------------------------

#ifdef _WIN32

std::wstring widen(const std::string& text) {
  if (text.empty()) {
    return std::wstring();
  }
  const int length = MultiByteToWideChar(CP_ACP, 0, text.data(), static_cast<int>(text.size()),
                                         nullptr, 0);
  if (length <= 0) {
    return std::wstring();
  }
  std::wstring wide(static_cast<std::size_t>(length), L'\0');
  MultiByteToWideChar(CP_ACP, 0, text.data(), static_cast<int>(text.size()), wide.data(), length);
  return wide;
}

/// The executable this process is running, so the child is the same program.
std::wstring executable_path(const std::string& fallback) {
  std::wstring buffer(1024, L'\0');
  for (;;) {
    const DWORD length =
        GetModuleFileNameW(nullptr, buffer.data(), static_cast<DWORD>(buffer.size()));
    if (length == 0) {
      return widen(fallback);
    }
    if (static_cast<std::size_t>(length) < buffer.size()) {
      buffer.resize(static_cast<std::size_t>(length));
      return buffer;
    }
    buffer.resize(buffer.size() * 2);
  }
}

/// Standard Windows command-line quoting: a run of backslashes is doubled when
/// it precedes a quote, and the closing quote is preceded by the same rule.
std::wstring quote(const std::wstring& argument) {
  std::wstring quoted(1, L'"');
  std::size_t backslashes = 0;
  for (const wchar_t character : argument) {
    if (character == L'\\') {
      ++backslashes;
      continue;
    }
    if (character == L'"') {
      quoted.append(backslashes * 2 + 1, L'\\');
      quoted.push_back(L'"');
      backslashes = 0;
      continue;
    }
    quoted.append(backslashes, L'\\');
    backslashes = 0;
    quoted.push_back(character);
  }
  quoted.append(backslashes * 2, L'\\');
  quoted.push_back(L'"');
  return quoted;
}

int run_child_process(const std::string& self, const std::string& mode,
                      const std::string& directory) {
  std::wstring command =
      quote(executable_path(self)) + L" " + quote(widen(mode)) + L" " + quote(widen(directory));
  std::vector<wchar_t> mutable_command(command.begin(), command.end());
  mutable_command.push_back(L'\0');
  STARTUPINFOW startup{};
  startup.cb = sizeof(startup);
  PROCESS_INFORMATION info{};
  if (CreateProcessW(nullptr, mutable_command.data(), nullptr, nullptr, FALSE, 0, nullptr, nullptr,
                     &startup, &info) == FALSE) {
    return -1;
  }
  WaitForSingleObject(info.hProcess, INFINITE);
  DWORD code = 0;
  const BOOL read = GetExitCodeProcess(info.hProcess, &code);
  CloseHandle(info.hThread);
  CloseHandle(info.hProcess);
  if (read == FALSE) {
    return -1;
  }
  return static_cast<int>(code);
}

#else

int run_child_process(const std::string& self, const std::string& mode,
                      const std::string& directory) {
  const pid_t child = ::fork();
  if (child < 0) {
    return -1;
  }
  if (child == 0) {
    std::vector<std::string> arguments{self, mode, directory};
    std::vector<char*> argv;
    argv.reserve(arguments.size() + 1);
    for (std::string& argument : arguments) {
      argv.push_back(argument.data());
    }
    argv.push_back(nullptr);
    ::execv(self.c_str(), argv.data());
    ::_exit(96);
  }
  int status = 0;
  if (::waitpid(child, &status, 0) < 0) {
    return -1;
  }
  if (WIFEXITED(status)) {
    return WEXITSTATUS(status);
  }
  return 128 + WTERMSIG(status);
}

#endif

// ---------------------------------------------------------------------------
// The child paths
// ---------------------------------------------------------------------------

/// Dies inside the adapter call, strictly after the durable attempt record.
int run_child(const std::string& directory) {
  auto opened =
      AirflowControlEngine::open(store_path(directory), OpenMode::open_existing,
                                 example_options());
  if (!opened.ok()) {
    std::cout << "child open failed: " << opened.status().to_string() << "\n";
    return 2;
  }
  AirflowControlEngine& engine = opened.value();
  if (!engine.advance_tick(LogicalTick::from(kKilledTick)).ok()) {
    return 2;
  }
  DyingAdapter adapter;
  const auto issued = engine.issue(make_request(kKilledKey, kKilledTick), adapter);
  std::cout << "child: the issue returned unexpectedly: " << issued.status().to_string() << "\n";
  return 3;
}

/// Completes one whole accepted command and then dies abruptly, after the
/// attempt record, the outcome, and the verification were published.
int run_child_complete(const std::string& directory) {
  auto opened =
      AirflowControlEngine::open(store_path(directory), OpenMode::open_existing,
                                 example_options());
  if (!opened.ok()) {
    std::cout << "child open failed: " << opened.status().to_string() << "\n";
    return 2;
  }
  AirflowControlEngine& engine = opened.value();
  if (!engine.advance_tick(LogicalTick::from(kPublishedTick)).ok()) {
    return 2;
  }
  AdapterDescriptor descriptor = synthetic_descriptor();
  SyntheticAirflowAdapter adapter(descriptor);
  seed_plant(adapter);
  const auto issued = engine.issue(make_request(kPublishedKey, kPublishedTick), adapter);
  if (!issued.ok()) {
    std::cout << "child issue failed: " << issued.status().to_string() << "\n";
    return 2;
  }
  const LogicalTick verify_at = LogicalTick::from(kPublishedTick + 1);
  if (!engine.advance_tick(verify_at).ok()) {
    return 2;
  }
  const auto verified = engine.verify(VerificationRequest{
      .attempt = issued.value().id,
      .adapter = &adapter,
      .source = kField,
      .fan_point = SpaceRefId::parse("room-e-fan-point").value(),
      .pressure_point = SpaceRefId::parse("room-e-pressure-point").value(),
      .fan_sequence = EvidenceSequence::from(2),
      .pressure_sequence = EvidenceSequence::from(1),
      .at = verify_at,
      .relationship = std::nullopt,
  });
  if (!verified.ok()) {
    std::cout << "child verify failed: " << verified.status().to_string() << "\n";
    return 2;
  }
  std::cout << "child: attempt " << issued.value().id.to_string()
            << " effect=" << to_string(verified.value().state)
            << "; the outcome and the verification are durable, and the process dies here\n";
  std::cout.flush();
  std::_Exit(97);
}

}  // namespace

int main(int argc, char** argv) {
  std::string directory = "example-crash-reopen";
  bool child = false;
  bool child_complete = false;
  for (int index = 1; index < argc; ++index) {
    const std::string argument = argv[index];
    if (argument == "--child") {
      child = true;
    } else if (argument == "--child-complete") {
      child_complete = true;
    } else {
      directory = argument;
    }
  }
  if (child) {
    return run_child(directory);
  }
  if (child_complete) {
    return run_child_complete(directory);
  }

  const std::string self = argc > 0 ? argv[0] : std::string();
  const std::string store = store_path(directory);
  // The example starts from nothing so that it can be run repeatedly: an
  // existing store from an earlier run would make the child's command a replay
  // instead of a fresh dispatch.
  std::remove(store.c_str());
  std::remove((store + ".lock").c_str());
  std::cout << "store: " << store << "\n";
  std::cout << "adapter=SYNTHETIC (no hardware is driven)\n";

  // Build the model that both children operate on.
  {
    auto opened = AirflowControlEngine::open(store, OpenMode::create_new, example_options());
    if (!opened.ok()) {
      std::cout << "open failed: " << opened.status().to_string() << "\n";
      return 2;
    }
    AirflowControlEngine& engine = opened.value();
    const Status built = build_model(engine);
    if (!built.ok()) {
      std::cout << "setup failed: " << built.to_string() << "\n";
      return 2;
    }
    if (!engine.close().ok()) {
      std::cout << "setup close failed\n";
      return 2;
    }
  }

  // Step 1: a real child process dies inside the adapter call.
  const int killed_exit = run_child_process(self, "--child", directory);
  std::cout << "step 1 child exit code: " << killed_exit << "\n";
  expect(killed_exit == 97, "the child died inside the adapter call: std::_Exit(97)");

  std::optional<AttemptId> recovered;
  {
    auto opened =
        AirflowControlEngine::open(store, OpenMode::open_existing, example_options());
    if (!opened.ok()) {
      std::cout << "reopen failed: " << opened.status().to_string() << "\n";
      return 2;
    }
    AirflowControlEngine& engine = opened.value();
    const std::vector<AttemptView> attempts = engine.attempts();
    expect(attempts.size() == 1, "exactly one attempt survived the process death");
    if (attempts.size() != 1) {
      std::cout << "recovery state: " << attempts.size() << " attempt(s)\n";
      return 1;
    }
    recovered = attempts.front().record.id;
    std::cout << "step 2 recovered attempt=" << recovered->to_string()
              << " key=" << attempts.front().record.key.str()
              << " attempt-state=" << to_string(attempts.front().record.state) << "\n";
    expect(attempts.front().record.state == AttemptState::recovery_required,
           "the attempt exists in state recovery_required: a command may have reached the device");
    expect(attempts.front().record.dispatched_at.has_value(),
           "the adopted attempt records that the command was dispatched");
    const auto view = engine.device(kDevice);
    expect(view.ok() && view.value().unresolved_attempt.has_value() &&
               *view.value().unresolved_attempt == *recovered,
           "the device still latches the unresolved attempt, so the command is not re-sent");

    // Step 3: the same key replays the durable record and never reaches an
    // adapter, and an unused key fails closed on the re-send latch.
    bool reached = false;
    NeverCalledAdapter adapter(reached);
    const ControlRequest retry = make_request(kKilledKey, kKilledTick);
    const auto decision = engine.evaluate(retry);
    if (!decision.ok()) {
      std::cout << "evaluate failed: " << decision.status().to_string() << "\n";
      return 2;
    }
    std::cout << "step 3 same-key retry: eligible=" << (decision.value().eligible ? "true" : "false")
              << " status-token=" << to_string(decision.value().code)
              << " replayed=" << (decision.value().replayed ? "true" : "false") << "\n";
    expect(decision.value().replayed,
           "the key was retained at the command-attempt boundary, so the retry is a replay");
    expect(decision.value().replayed_attempt.has_value() &&
               *decision.value().replayed_attempt == *recovered,
           "the replay names the attempt the dead process left behind");
    const auto retried = engine.issue(retry, adapter);
    expect(retried.ok() && retried.value().id == *recovered,
           "the same-key retry answers from the durable record without actuating anything");
    expect(retried.ok() && retried.value().state == AttemptState::recovery_required,
           "the replayed record still says a command may have reached the device");
    expect(!reached, "the never-called adapter's execute() was not called by the same-key retry");
    std::cout << "step 3 same-key retry adapter-reached=" << (reached ? "true" : "false")
              << " attempt-state="
              << (retried.ok() ? std::string(to_string(retried.value().state)) : std::string("-"))
              << "\n";
    std::cout << "step 3 note: the idempotency key is published with the attempt record at the "
                 "command-attempt boundary, so a death inside execute() leaves the key retained and "
                 "the retry replays instead of becoming a new request\n";

    const auto unused = engine.evaluate(make_request(kUnusedKey, kKilledTick));
    expect(unused.ok() && !unused.value().eligible &&
               unused.value().code == StatusCode::attempt_unresolved,
           "a command under a new key is refused with attempt_unresolved while the effect of the "
           "retained attempt is unknown");

    // Step 4: the retained attempt is settled from fresh evidence, which is the
    // documented way out of the latch. It is never re-sent.
    const LogicalTick settle_at = LogicalTick::from(kKilledTick + 1);
    if (!engine.advance_tick(settle_at).ok()) {
      return 2;
    }
    AdapterDescriptor descriptor = synthetic_descriptor();
    SyntheticAirflowAdapter plant(descriptor);
    seed_plant(plant);
    const auto settled = engine.verify(VerificationRequest{
        .attempt = *recovered,
        .adapter = &plant,
        .source = kField,
        .fan_point = SpaceRefId::parse("room-e-fan-point").value(),
        .pressure_point = SpaceRefId::parse("room-e-pressure-point").value(),
        .fan_sequence = EvidenceSequence::from(1),
        .pressure_sequence = EvidenceSequence::from(1),
        .at = settle_at,
        .relationship = std::nullopt,
    });
    if (!settled.ok()) {
      std::cout << "settle verify failed: " << settled.status().to_string() << "\n";
      return 2;
    }
    std::cout << "step 4 recovered attempt effect=" << to_string(settled.value().state)
              << " detail=" << settled.value().message << "\n";
    expect(settled.value().state == EffectState::effective,
           "fresh evidence settles the recovered attempt without re-sending the command");
    const auto released = engine.device(kDevice);
    expect(released.ok() && !released.value().unresolved_attempt.has_value(),
           "the device latch is released once the retained attempt is decided");
    if (!engine.close().ok()) {
      std::cout << "close failed\n";
      return 2;
    }
  }

  // Step 5: a second child completes a whole accepted command and then dies
  // abruptly, after the attempt record, the adapter outcome, and the
  // verification were published.
  const int published_exit = run_child_process(self, "--child-complete", directory);
  std::cout << "step 5 child exit code: " << published_exit << "\n";
  expect(published_exit == 97,
         "the second child died after its durable publications: std::_Exit(97)");

  // Step 6: the retry with the published key replays the retained result and
  // never reaches an adapter.
  {
    auto opened =
        AirflowControlEngine::open(store, OpenMode::open_existing, example_options());
    if (!opened.ok()) {
      std::cout << "reopen failed: " << opened.status().to_string() << "\n";
      return 2;
    }
    AirflowControlEngine& engine = opened.value();
    const std::vector<AttemptView> attempts = engine.attempts();
    expect(attempts.size() == 2, "the attempt journal holds both attempts after the reopen");
    std::optional<AttemptId> published;
    for (const AttemptView& view : attempts) {
      if (view.record.key.str() == kPublishedKey) {
        published = view.record.id;
      }
    }
    expect(published.has_value() &&
               attempts.back().record.state == AttemptState::effect_established,
           "the attempt whose outcome was published is retained as effect_established");
    if (!published.has_value()) {
      return 1;
    }

    bool reached = false;
    NeverCalledAdapter adapter(reached);
    const ControlRequest replay_request = make_request(kPublishedKey, kPublishedTick);
    const auto decision = engine.evaluate(replay_request);
    if (!decision.ok()) {
      std::cout << "evaluate failed: " << decision.status().to_string() << "\n";
      return 2;
    }
    std::cout << "step 6 replay: replayed=" << (decision.value().replayed ? "true" : "false")
              << " eligible=" << (decision.value().eligible ? "true" : "false")
              << " replayed-attempt="
              << (decision.value().replayed_attempt.has_value()
                      ? decision.value().replayed_attempt->to_string()
                      : std::string("-"))
              << "\n";
    expect(decision.value().replayed && decision.value().eligible,
           "a retry with the same key replays the retained result");
    expect(decision.value().replayed_attempt.has_value() &&
               *decision.value().replayed_attempt == *published,
           "the replay names the retained attempt");
    const auto replayed = engine.issue(replay_request, adapter);
    expect(replayed.ok() && replayed.value().id == *published,
           "the replay returns the retained attempt record");
    expect(replayed.ok() && replayed.value().state == AttemptState::effect_established,
           "the retained effect survived the process death");
    expect(!reached,
           "the replay never reached an adapter: the retained result is not re-actuated");
    std::cout << "step 6 replay adapter-reached=" << (reached ? "true" : "false")
              << " attempt=" << (replayed.ok() ? replayed.value().id.to_string() : std::string("-"))
              << " attempt-state="
              << (replayed.ok() ? std::string(to_string(replayed.value().state)) : std::string("-"))
              << "\n";

    // The retained key is bound to the exact request: the same key with a
    // different request is a conflict, not a replay.
    const auto conflict = engine.evaluate(make_request(kPublishedKey, kPublishedTick + 1));
    expect(conflict.ok() && !conflict.value().eligible &&
               conflict.value().code == StatusCode::idempotency_conflict,
           "the retained key names the exact request it was issued for: a different request "
           "under the same key is refused with idempotency_conflict");

    const Status closed = engine.close();
    expect(closed.ok(), "the durable store closed cleanly");
  }

  if (failures != 0) {
    std::cout << failures << " expectation(s) failed\n";
    return 1;
  }
  std::cout << "a command that may have reached the device was never re-sent, and a published "
               "result was replayed without touching an adapter\n";
  return 0;
}
