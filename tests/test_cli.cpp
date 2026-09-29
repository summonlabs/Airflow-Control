// The command line tool, exercised as a real program through real invocations.
//
// Every verb opens the store, performs one act, and closes, so each of these
// runs the recovery path. The checks are about the contract a script depends on:
// the exit codes, the machine-readable refusal line, and the fact that state
// written by one invocation is what the next invocation sees.

#include <string>
#include <vector>

#include "support/fixture.hpp"
#include "support/proc.hpp"
#include "support/test_harness.hpp"

#include "airflow_control/version.hpp"

namespace {

using airflow_test::TempDir;

/// One completed invocation: what it exited with and everything it wrote.
struct Invocation {
  int exit_code = -1;
  std::string output;
};

[[nodiscard]] Invocation run(const std::vector<std::string>& arguments) {
  Invocation result;
  std::vector<std::string> argv;
  argv.push_back(airflow_test::cli_executable());
  for (const std::string& argument : arguments) {
    argv.push_back(argument);
  }
  auto exit_code = airflow_test::run_process_capture(argv, result.output);
  if (exit_code.ok()) {
    result.exit_code = exit_code.value();
  }
  return result;
}

[[nodiscard]] bool has(const std::string& text, const std::string& needle) {
  return text.find(needle) != std::string::npos;
}

/// A refusal must name a machine-readable token on the documented line.
[[nodiscard]] bool refuses_with(const Invocation& invocation, const std::string& token) {
  return invocation.exit_code == 1 && has(invocation.output, "error: " + token + ":");
}

}  // namespace

AIRFLOW_TEST(cli_requires_a_known_verb) {
  if (airflow_test::cli_executable().empty()) {
    CHECK(false);
    return;
  }
  const Invocation unknown = run({"definitely-not-a-verb"});
  CHECK_EQ(unknown.exit_code, 2);

  const Invocation missing_option = run({"device", "add", "--device", "dev-1"});
  CHECK_EQ(missing_option.exit_code, 2);

  const Invocation malformed_number = run({"tick", "advance", "--tick", "12x"});
  CHECK_EQ(malformed_number.exit_code, 2);

  const Invocation overflow = run({"tick", "advance", "--tick", "99999999999999999999999"});
  CHECK_EQ(overflow.exit_code, 2);

  const Invocation help = run({"--help"});
  CHECK_EQ(help.exit_code, 0);
  CHECK(has(help.output, "device"));
}

AIRFLOW_TEST(cli_runs_a_whole_control_lifecycle) {
  if (airflow_test::cli_executable().empty()) {
    CHECK(false);
    return;
  }
  TempDir scratch("cli-lifecycle");
  const std::string store = scratch.store_path();
  const std::string s = "--store=" + store;

  CHECK_EQ(run({"init", s}).exit_code, 0);
  CHECK_EQ(run({"epoch", "adopt", s, "--epoch=1", "--actor=op-1", "--tick=1"}).exit_code, 0);
  CHECK_EQ(run({"device", "add", s, "--device=dev-1", "--generation=1", "--room=room-1",
                "--row=row-1", "--actor=op-1", "--tick=1"})
               .exit_code,
           0);

  // A lifecycle change needs authority, so without a grant it is refused with a
  // machine-readable token rather than silently ignored.
  const Invocation uncommissioned =
      run({"lifecycle", "set", s, "--device=dev-1", "--generation=1", "--revision=1",
           "--target=active", "--epoch=1", "--actor=op-1", "--tick=1"});
  CHECK(refuses_with(uncommissioned, "permission_missing"));

  CHECK_EQ(run({"grant", "add", s, "--grant=g1", "--issuer=airflow-authority", "--epoch=1",
                "--device=dev-1", "--device-generation=1", "--room=room-1",
                "--actions=apply_setpoint,raise_airflow,lower_airflow,change_pressure_target,"
                "emergency_purge,set_lifecycle,set_policy",
                "--issued=1"})
               .exit_code,
           0);
  CHECK_EQ(run({"lifecycle", "set", s, "--device=dev-1", "--generation=1", "--revision=1",
                "--target=active", "--epoch=1", "--actor=op-1", "--tick=1"})
               .exit_code,
           0);
  CHECK_EQ(run({"policy", "set", s, "--device=dev-1", "--generation=1", "--revision=2",
                "--policy=pol-1", "--policy-generation=1", "--min-percent=2000",
                "--max-percent=9000", "--default-percent=5000", "--min-airflow=2000",
                "--max-airflow=9000", "--max-step=10000", "--source=thermal-policy",
                "--evidence-generation=1", "--actor=op-1", "--tick=1"})
               .exit_code,
           0);
  CHECK_EQ(run({"containment", "define", s, "--containment=cont-1", "--room=room-1",
                "--row=row-1", "--kind=aisle_containment", "--actor=op-1", "--tick=1"})
               .exit_code,
           0);
  CHECK_EQ(run({"containment", "report", s, "--containment=cont-1", "--state=intact",
                "--quality=good", "--source=dcim", "--sequence=1", "--evidence-generation=1",
                "--tick=1"})
               .exit_code,
           0);
  CHECK_EQ(run({"interlock", "declare", s, "--interlock=il-1", "--room=room-1", "--row=row-1",
                "--device=dev-1", "--class=protected", "--epoch=1", "--actor=op-1", "--tick=1"})
               .exit_code,
           0);
  CHECK_EQ(run({"interlock", "report", s, "--interlock=il-1", "--state=satisfied",
                "--sequence=1", "--epoch=1", "--tick=1"})
               .exit_code,
           0);

  // An optimization request with no containment-independent proof still needs
  // pressure evidence for every relationship in the room, and there is none.
  CHECK_EQ(run({"tick", "advance", s, "--tick=2"}).exit_code, 0);
  const Invocation noEvidence =
      run({"evaluate", s, "--device=dev-1", "--device-generation=1", "--epoch=1",
           "--intent=trim_for_efficiency", "--setpoint-percent=4000", "--key=k0",
           "--actor=op-1", "--tick=2"});
  CHECK_EQ(noEvidence.exit_code, 0);

  const Invocation issued =
      run({"issue", s, "--device=dev-1", "--device-generation=1", "--epoch=1",
           "--intent=raise_airflow", "--setpoint-percent=6000", "--key=k1", "--actor=op-1",
           "--tick=2", "--adapter=synthetic", "--verify", "--source=field", "--sequence=10",
           "--fan-point=row-1-fan"});
  CHECK_EQ(issued.exit_code, 0);
  CHECK(has(issued.output, "SYNTHETIC"));

  const Invocation attempts = run({"attempts", s});
  CHECK_EQ(attempts.exit_code, 0);
  CHECK(has(attempts.output, "dev-1"));

  const Invocation state = run({"state", s});
  CHECK_EQ(state.exit_code, 0);
  CHECK(has(state.output, "attempt 1"));

  const Invocation digest = run({"digest", s});
  CHECK_EQ(digest.exit_code, 0);
  CHECK(!digest.output.empty());

  const Invocation audit = run({"store-audit", s});
  CHECK_EQ(audit.exit_code, 0);
  CHECK(has(audit.output, "generation"));

  // The store is durable: a later invocation sees what the earlier ones wrote.
  const Invocation reread = run({"device", "show", s, "--device=dev-1"});
  CHECK_EQ(reread.exit_code, 0);
  CHECK(has(reread.output, "dev-1"));
}

AIRFLOW_TEST(cli_reports_refusals_and_json) {
  if (airflow_test::cli_executable().empty()) {
    CHECK(false);
    return;
  }
  TempDir scratch("cli-json");
  const std::string store = scratch.store_path();
  const std::string s = "--store=" + store;

  CHECK_EQ(run({"init", s}).exit_code, 0);
  CHECK_EQ(run({"epoch", "adopt", s, "--epoch=1", "--actor=op-1", "--tick=1"}).exit_code, 0);

  // A device that was never registered is refused, not invented.
  const Invocation missing =
      run({"device", "show", s, "--device=ghost", "--json"});
  CHECK(refuses_with(missing, "not_found"));

  // Advancing to the instant the clock already holds is a no-op that succeeds;
  // moving it backwards is refused, because the clock is monotonic.
  const std::string after_adopt = "--tick=1";
  const Invocation same = run({"tick", "advance", s, after_adopt});
  CHECK_EQ(same.exit_code, 0);
  const Invocation backwards_clock = run({"tick", "advance", s, "--tick=0"});
  CHECK(refuses_with(backwards_clock, "out_of_range"));

  const Invocation backwards = run({"epoch", "adopt", s, "--epoch=1", "--actor=op-1",
                                    "--tick=1", "--json"});
  CHECK(refuses_with(backwards, "epoch_stale"));

  // A successful JSON verb emits exactly one object.
  const Invocation json = run({"device", "list", s, "--json"});
  CHECK_EQ(json.exit_code, 0);
  const std::size_t first = json.output.find('{');
  const std::size_t last = json.output.rfind('}');
  CHECK(first != std::string::npos);
  CHECK(last != std::string::npos);
  CHECK(first < last);
}
