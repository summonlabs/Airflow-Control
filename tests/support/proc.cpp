#include "proc.hpp"

#include <cstdint>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "detail/process.hpp"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#else
#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace airflow_test {

// The support layer names library values by their library names.
using namespace ::airflow_control;  // NOLINT(google-build-using-namespace)

namespace {

std::uint64_t next_serial() {
  static std::uint64_t serial = 0;
  serial += 1;
  return serial;
}

/// A capture file name unique to this process and this call. It is created in
/// the working directory the test binary was started in, and it is removed by
/// the handle that owns it.
std::string capture_path() {
  return "child-" + std::to_string(::airflow_control::detail::current_process_id()) + "-" +
         std::to_string(next_serial()) + ".out";
}

std::string read_text(const std::string& path) {
  std::ifstream stream(path, std::ios::binary);
  if (!stream) {
    return std::string();
  }
  std::ostringstream buffer;
  buffer << stream.rdbuf();
  return buffer.str();
}

#if defined(_WIN32)

std::wstring widen(const std::string& text) {
  if (text.empty()) {
    return std::wstring();
  }
  const int size = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, text.c_str(),
                                       static_cast<int>(text.size()), nullptr, 0);
  if (size <= 0) {
    return std::wstring();
  }
  std::wstring wide(static_cast<std::size_t>(size), L'\0');
  MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, text.c_str(), static_cast<int>(text.size()),
                      wide.data(), size);
  return wide;
}

/// Quotes one argument the way the C runtime parses a command line: a run of
/// backslashes before a quote is doubled, and a trailing run is doubled before
/// the closing quote. Quoting only the arguments that contain a space would
/// leave an argument containing a quote ambiguous, and a store path may contain
/// either.
std::string quote_argument(const std::string& argument) {
  std::string quoted = "\"";
  std::size_t backslashes = 0;
  for (const char value : argument) {
    if (value == '\\') {
      backslashes += 1;
      continue;
    }
    if (value == '"') {
      quoted.append(backslashes * 2 + 1, '\\');
      quoted.push_back('"');
      backslashes = 0;
      continue;
    }
    quoted.append(backslashes, '\\');
    backslashes = 0;
    quoted.push_back(value);
  }
  quoted.append(backslashes * 2, '\\');
  quoted.push_back('"');
  return quoted;
}

#endif  // _WIN32

}  // namespace

ProcessHandle::~ProcessHandle() { release(); }

ProcessHandle::ProcessHandle(ProcessHandle&& other) noexcept
    : process_(other.process_),
      id_(other.id_),
      exit_code_(other.exit_code_),
      settled_(other.settled_),
      capture_path_(std::move(other.capture_path_)) {
  other.process_ = 0;
  other.id_ = 0;
  other.settled_ = false;
}

ProcessHandle& ProcessHandle::operator=(ProcessHandle&& other) noexcept {
  if (this != &other) {
    release();
    process_ = other.process_;
    id_ = other.id_;
    exit_code_ = other.exit_code_;
    settled_ = other.settled_;
    capture_path_ = std::move(other.capture_path_);
    other.process_ = 0;
    other.id_ = 0;
    other.settled_ = false;
  }
  return *this;
}

void ProcessHandle::release() {
  if (process_ != 0) {
    if (!settled_) {
      // A handle that goes away while its child runs would leave a process
      // behind holding whatever the child held, so it is terminated here.
      (void)kill();
    }
#ifdef _WIN32
    CloseHandle(reinterpret_cast<HANDLE>(process_));
#else
    // The child was reaped by kill(); this is a no-op for a child that exited on
    // its own and was already collected by wait() or exited().
    if (settled_) {
      waitpid(static_cast<pid_t>(process_), nullptr, WNOHANG);
    }
#endif
    process_ = 0;
  }
  if (!capture_path_.empty()) {
    std::remove(capture_path_.c_str());
    capture_path_.clear();
  }
}

Result<ProcessHandle> ProcessHandle::spawn(const std::vector<std::string>& argv) {
  return spawn_impl(argv, std::string());
}

Result<ProcessHandle> ProcessHandle::spawn_captured(const std::vector<std::string>& argv) {
  return spawn_impl(argv, capture_path());
}

std::string ProcessHandle::captured_output() const { return read_text(capture_path_); }

Result<ProcessHandle> ProcessHandle::spawn_impl(const std::vector<std::string>& argv,
                                                const std::string& capture) {
  if (argv.empty() || argv.front().empty()) {
    return Status::failure(StatusCode::invalid_argument,
                           "a child process needs the path of an executable to start");
  }
  ProcessHandle handle;
  handle.capture_path_ = capture;

#ifdef _WIN32
  HANDLE capture_handle = INVALID_HANDLE_VALUE;
  if (!capture.empty()) {
    SECURITY_ATTRIBUTES attributes{};
    attributes.nLength = sizeof(attributes);
    attributes.bInheritHandle = TRUE;
    const std::wstring capture_wide = widen(capture);
    if (capture_wide.empty()) {
      return Status::failure(StatusCode::invalid_argument,
                             "the child capture path is not valid UTF-8");
    }
    // FILE_SHARE_READ lets this process read the file while the child writes it,
    // which is what makes a running child's output observable at all.
    capture_handle = CreateFileW(capture_wide.c_str(), GENERIC_WRITE, FILE_SHARE_READ, &attributes,
                                 CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (capture_handle == INVALID_HANDLE_VALUE) {
      return Status::failure(StatusCode::store_io_error,
                             "could not create the child capture file " + capture);
    }
  }

  std::string command;
  for (const std::string& argument : argv) {
    if (!command.empty()) {
      command.push_back(' ');
    }
    command.append(quote_argument(argument));
  }
  std::wstring command_wide = widen(command);
  const std::wstring executable_wide = widen(argv.front());
  if (command_wide.empty() || executable_wide.empty()) {
    if (capture_handle != INVALID_HANDLE_VALUE) {
      CloseHandle(capture_handle);
    }
    return Status::failure(StatusCode::invalid_argument,
                           "the child command line is not valid UTF-8");
  }

  STARTUPINFOW startup{};
  startup.cb = sizeof(startup);
  const bool redirect = capture_handle != INVALID_HANDLE_VALUE;
  if (redirect) {
    startup.dwFlags = STARTF_USESTDHANDLES;
    startup.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
    startup.hStdOutput = capture_handle;
    startup.hStdError = capture_handle;
  }
  PROCESS_INFORMATION info{};
  // The child is started in this console with no debug flag, so it can never
  // raise an interactive dialog: a faulting child dies and the test reads its
  // exit code.
  const BOOL started =
      CreateProcessW(executable_wide.c_str(), command_wide.data(), nullptr, nullptr,
                     redirect ? TRUE : FALSE, 0, nullptr, nullptr, &startup, &info);
  if (redirect) {
    CloseHandle(capture_handle);
  }
  if (started == FALSE) {
    return Status::failure(StatusCode::store_io_error,
                           "CreateProcess failed with error " + std::to_string(GetLastError()));
  }
  CloseHandle(info.hThread);
  handle.process_ = reinterpret_cast<std::intptr_t>(info.hProcess);
  handle.id_ = static_cast<std::uint64_t>(info.dwProcessId);
#else
  std::vector<std::string> storage;
  storage.reserve(argv.size());
  for (const std::string& argument : argv) {
    storage.push_back(argument);
  }
  std::vector<char*> arguments;
  arguments.reserve(storage.size() + 1);
  for (std::string& value : storage) {
    arguments.push_back(value.data());
  }
  arguments.push_back(nullptr);

  const pid_t pid = fork();
  if (pid < 0) {
    return Status::failure(StatusCode::store_io_error, "fork failed");
  }
  if (pid == 0) {
    // The child never returns to the test process: every failure path leaves
    // through _exit with a code the parent can read.
    if (!capture.empty()) {
      const int descriptor = ::open(capture.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (descriptor >= 0) {
        dup2(descriptor, STDOUT_FILENO);
        dup2(descriptor, STDERR_FILENO);
        close(descriptor);
      }
    }
    execv(storage.front().c_str(), arguments.data());
    _exit(127);
  }
  handle.process_ = static_cast<std::intptr_t>(pid);
  handle.id_ = static_cast<std::uint64_t>(pid);
#endif
  return handle;
}

bool ProcessHandle::exited() {
  if (settled_) {
    return true;
  }
  if (process_ == 0) {
    return true;
  }
#ifdef _WIN32
  // A zero interval asks the operating system whether the child has settled; it
  // bounds nothing and waits for nothing.
  if (WaitForSingleObject(reinterpret_cast<HANDLE>(process_), 0) != WAIT_OBJECT_0) {
    return false;
  }
  DWORD code = 0;
  GetExitCodeProcess(reinterpret_cast<HANDLE>(process_), &code);
  exit_code_ = static_cast<int>(code);
#else
  int status = 0;
  const pid_t result = waitpid(static_cast<pid_t>(process_), &status, WNOHANG);
  if (result != static_cast<pid_t>(process_)) {
    return false;
  }
  exit_code_ = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
#endif
  settled_ = true;
  return true;
}

Result<int> ProcessHandle::wait() {
  if (settled_) {
    return exit_code_;
  }
  if (process_ == 0) {
    return Status::failure(StatusCode::invalid_argument,
                           "the child process was never started");
  }
#ifdef _WIN32
  WaitForSingleObject(reinterpret_cast<HANDLE>(process_), INFINITE);
  DWORD code = 0;
  GetExitCodeProcess(reinterpret_cast<HANDLE>(process_), &code);
  exit_code_ = static_cast<int>(code);
#else
  int status = 0;
  waitpid(static_cast<pid_t>(process_), &status, 0);
  exit_code_ = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
#endif
  settled_ = true;
  return exit_code_;
}

Status ProcessHandle::kill() {
  if (process_ == 0 || settled_) {
    return Status::success();
  }
#ifdef _WIN32
  const BOOL terminated = TerminateProcess(reinterpret_cast<HANDLE>(process_), 1);
  // The wait is what makes the kill complete: after it returns, the child is
  // gone and every operating-system resource it held is released.
  WaitForSingleObject(reinterpret_cast<HANDLE>(process_), INFINITE);
  DWORD code = 0;
  GetExitCodeProcess(reinterpret_cast<HANDLE>(process_), &code);
  exit_code_ = static_cast<int>(code);
  settled_ = true;
  if (terminated == FALSE) {
    return Status::failure(StatusCode::internal_error,
                           "TerminateProcess failed with error " +
                               std::to_string(GetLastError()));
  }
#else
  ::kill(static_cast<pid_t>(process_), SIGKILL);
  int status = 0;
  waitpid(static_cast<pid_t>(process_), &status, 0);
  exit_code_ = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
  settled_ = true;
#endif
  return Status::success();
}

Result<int> run_process(const std::vector<std::string>& argv) {
  Result<ProcessHandle> child = ProcessHandle::spawn(argv);
  if (!child.ok()) {
    return child.status();
  }
  return child.value().wait();
}

Result<int> run_process_capture(const std::vector<std::string>& argv, std::string& output) {
  output.clear();
  Result<ProcessHandle> child = ProcessHandle::spawn_captured(argv);
  if (!child.ok()) {
    return child.status();
  }
  Result<int> code = child.value().wait();
  if (!code.ok()) {
    return code.status();
  }
  // Read before the handle goes away: the capture file belongs to it and is
  // removed when it is destroyed.
  output = child.value().captured_output();
  return code.value();
}

std::string probe_executable() {
#ifdef AIRFLOW_PROBE_EXECUTABLE
  return std::string(AIRFLOW_PROBE_EXECUTABLE);
#else
  return std::string();
#endif
}

std::string cli_executable() {
#ifdef AIRFLOW_CLI_EXECUTABLE
  return std::string(AIRFLOW_CLI_EXECUTABLE);
#else
  return std::string();
#endif
}

}  // namespace airflow_test
