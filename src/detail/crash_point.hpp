#pragma once

#include <cstdint>

namespace airflow_control::detail {

/// A stage of durable publication at which a test may terminate the process.
///
/// This exists so that crash consistency is proven against a real process that
/// really dies at a real stage, rather than against a simulation of one. The
/// hook is internal: it is declared in a header that is not installed, so a
/// consumer of the installed package has no way to reach it. Nothing is armed
/// unless the process explicitly arms a point, and an armed point terminates
/// the process immediately without running destructors or producing a crash
/// dialog.
enum class CrashPoint : std::uint32_t {
  none = 0,
  /// After the staging slot bytes are written, before they are flushed.
  after_slot_write = 1,
  /// After the staging slot bytes are flushed, before the head is written.
  after_slot_flush = 2,
  /// After the head record is written, before it is flushed.
  after_head_write = 3,
  /// After the head record is flushed: the store has committed.
  after_head_flush = 4,
  /// After the file header is written on a freshly created store.
  after_header_write = 5,
};

/// Arms a crash point for this process. Pass CrashPoint::none to disarm.
void arm_crash_point(CrashPoint point) noexcept;

/// The currently armed crash point.
[[nodiscard]] CrashPoint armed_crash_point() noexcept;

/// Terminates the process immediately when the armed point matches.
void reach_crash_point(CrashPoint point) noexcept;

}  // namespace airflow_control::detail
