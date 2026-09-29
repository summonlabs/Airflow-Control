#pragma once

// Real independent operating-system processes.
//
// The crash, fencing, and exclusion proofs need processes that really exist,
// really die, and really hold operating-system locks. Child output is redirected
// to a file rather than to a pipe: file redirection cannot deadlock the child on
// a full pipe buffer, and the parent reads the file once the child has settled.
//
// A child is always created as an ordinary console process and the parent never
// attaches a debugger, because a child that faults must die rather than raise an
// interactive dialog that would hang an unattended run.

#include <cstdint>
#include <string>
#include <vector>

#include "airflow_control/status.hpp"

namespace airflow_test {

/// A running child process.
///
/// The child is a real operating-system process: it can be waited for, it can be
/// polled without blocking, and it can be terminated abruptly with the operating
/// system's own termination call, which is what a hard kill looks like to a
/// crash-consistency proof.
class ProcessHandle {
 public:
  ProcessHandle() = default;
  /// A handle that is destroyed while the child is still running terminates it,
  /// so a test that forgets cannot leave a process holding a store lock.
  ~ProcessHandle();
  ProcessHandle(ProcessHandle&& other) noexcept;
  ProcessHandle& operator=(ProcessHandle&& other) noexcept;
  ProcessHandle(const ProcessHandle&) = delete;
  ProcessHandle& operator=(const ProcessHandle&) = delete;

  /// Starts argv[0] with the remaining elements as its arguments. The child
  /// inherits this process's environment and working directory.
  [[nodiscard]] static ::airflow_control::Result<ProcessHandle> spawn(
      const std::vector<std::string>& argv);

  /// Starts the child with its standard output and standard error redirected to
  /// a file in the working directory, so what it wrote can be read without a
  /// pipe that could deadlock on a full buffer. The file belongs to the handle
  /// and is removed with it.
  [[nodiscard]] static ::airflow_control::Result<ProcessHandle> spawn_captured(
      const std::vector<std::string>& argv);

  /// Everything the child has written so far. Empty for a child started without
  /// redirection.
  [[nodiscard]] std::string captured_output() const;

  [[nodiscard]] bool valid() const noexcept { return process_ != 0; }
  [[nodiscard]] std::uint64_t id() const noexcept { return id_; }
  /// True until the child has been observed to settle.
  [[nodiscard]] bool running() const noexcept { return process_ != 0 && !settled_; }

  /// True once the child has settled, asked of the operating system rather than
  /// trusted from a local flag. Never blocks.
  [[nodiscard]] bool exited();

  /// Waits for the child to settle and returns its exit code. There is no
  /// deadline: a child that does not finish is a defect to diagnose, not to
  /// bound, and a watchdog would hide it.
  [[nodiscard]] ::airflow_control::Result<int> wait();

  /// Terminates the child immediately with the operating system's termination
  /// call (TerminateProcess, or SIGKILL) and reaps it. Idempotent, and safe
  /// after the child already settled.
  ::airflow_control::Status kill();

  /// The exit code observed by the most recent wait(), exited(), or kill().
  [[nodiscard]] int exit_code() const noexcept { return exit_code_; }

 private:
  static ::airflow_control::Result<ProcessHandle> spawn_impl(
      const std::vector<std::string>& argv, const std::string& capture_path);
  void release();

  std::intptr_t process_{0};
  std::uint64_t id_{0};
  int exit_code_{0};
  bool settled_{false};
  std::string capture_path_;
};

/// Runs the child to completion and returns its exit code.
[[nodiscard]] ::airflow_control::Result<int> run_process(const std::vector<std::string>& argv);

/// Runs the child to completion, returns its exit code, and leaves everything it
/// wrote in output. The child's standard error is redirected to the same file,
/// so a failing child's own explanation is never lost.
[[nodiscard]] ::airflow_control::Result<int> run_process_capture(
    const std::vector<std::string>& argv, std::string& output);

/// Path of the probe executable, injected by the build.
[[nodiscard]] std::string probe_executable();

/// Path of the command line tool, injected by the build. Empty when the tool was
/// not built.
[[nodiscard]] std::string cli_executable();

}  // namespace airflow_test
