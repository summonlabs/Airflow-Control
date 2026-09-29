#include "detail/crash_point.hpp"

#include <atomic>
#include <cstdint>
#include <cstdlib>

#include "detail/store_file.hpp"

namespace airflow_control::detail {
namespace {

/// Nothing is armed unless the process asks for it, so the hook is inert in
/// every process that does not test crash consistency.
std::atomic<CrashPoint> g_armed{CrashPoint::none};

}  // namespace

void arm_crash_point(CrashPoint point) noexcept {
  g_armed.store(point, std::memory_order_seq_cst);
}

CrashPoint armed_crash_point() noexcept {
  return g_armed.load(std::memory_order_seq_cst);
}

void reach_crash_point(CrashPoint point) noexcept {
  // Disarming is not the same as arming "none": a mismatched point is a no-op
  // and only an exact match ends the process.
  if (point == CrashPoint::none) {
    return;
  }
  if (armed_crash_point() == point) {
    // std::_Exit terminates without unwinding, without running destructors, and
    // without giving the platform a chance to raise a crash dialog, which is
    // what makes the resulting file state a real crash state.
    std::_Exit(kCrashExitCode);
  }
}

}  // namespace airflow_control::detail
