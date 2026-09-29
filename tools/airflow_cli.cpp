// airflow-control: the inspection and administration tool.
//
// The tool is a thin shell over the library. Each invocation opens an engine,
// performs exactly one verb, prints what the library reported, and closes. It
// never reimplements a validation the library already owns, never invents a
// decision, and never prints a success line for a refusal.
//
// Control verbs drive a deterministic synthetic adapter whose simulated plant
// is seeded from the engine's own view of the device. Everything that came from
// it is labeled SYNTHETIC on standard error: this tool drives no hardware, and
// no output of this tool is hardware evidence.
//
// Exit codes. 0 means the verb ran and the engine did not refuse it. 1 means a
// request was built and the engine refused it, and the status token printed on
// standard error is the contract ("error: <token>: <message>"). 2 means the
// command line could not be turned into a request at all: an unknown verb, a
// missing option, a number that does not parse, an identifier the runtime will
// not accept, or a value the library's own type refuses to represent.

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "airflow_control/adapter.hpp"
#include "airflow_control/attempt.hpp"
#include "airflow_control/audit.hpp"
#include "airflow_control/authority.hpp"
#include "airflow_control/engine.hpp"
#include "airflow_control/evidence.hpp"
#include "airflow_control/ids.hpp"
#include "airflow_control/lifecycle.hpp"
#include "airflow_control/model.hpp"
#include "airflow_control/status.hpp"
#include "airflow_control/store.hpp"
#include "airflow_control/synthetic_adapter.hpp"
#include "airflow_control/units.hpp"
#include "airflow_control/version.hpp"
#include "cli_json.hpp"

using namespace airflow_control;  // NOLINT(google-build-using-namespace)
using airflow_cli::Emitter;
using airflow_cli::JsonObject;
using airflow_cli::Row;

namespace {

constexpr int kExitOk = 0;
constexpr int kExitRefused = 1;
constexpr int kExitUsage = 2;
constexpr std::uint64_t kMaxListLimit = 10000;
constexpr std::uint64_t kDefaultHistoryLimit = 20;
constexpr std::uint64_t kBytesPerMib = 1024ull * 1024ull;
constexpr std::uint64_t kMaxUint32 = 4294967295ull;

/// Capabilities this tool's simulated plant answers. It is a simulator: the
/// bits describe what the plant models, not what a vendor device would do.
constexpr std::uint32_t kSyntheticCapabilities =
    static_cast<std::uint32_t>(AdapterCapability::set_fan_percent) |
    static_cast<std::uint32_t>(AdapterCapability::set_airflow) |
    static_cast<std::uint32_t>(AdapterCapability::read_pressure) |
    static_cast<std::uint32_t>(AdapterCapability::read_airflow) |
    static_cast<std::uint32_t>(AdapterCapability::read_fan_percent);

const char* kUsage = R"USAGE(usage: airflow-control <verb> [options]
  init            --store <path> [--slot-capacity-mib N]
  epoch adopt     --epoch N --actor A --tick T
  tick advance    --tick T
  device add      --device D --generation G --room R [--row W] --actor A --tick T
  device list
  device show     --device D
  lifecycle set   --device D --generation G --revision R --target <state> --epoch E --actor A --tick T
  policy set      --device D --generation G --revision R --policy P --policy-generation PG
                  --min-percent B --max-percent B --default-percent B --min-airflow A --max-airflow A
                  --max-step B --source S --evidence-generation EG --actor A --tick T
  relationship define --relationship R --room ROOM --controlled C --reference REF
                  --polarity positive|negative|neutral --lower L --upper U --tolerance T
                  --evidence-generation EG [--revision REV] --actor A --tick T
  relationship list | relationship show --relationship R
  containment define --containment C --room ROOM [--row W] --kind K [--revision REV]
                  --actor A --tick T
  containment report --containment C --state S --quality Q --source S --sequence N
                  --evidence-generation EG --tick T
  containment list
  obligation declare --obligation O --scope room|row|rack --room R [--row W] [--rack K]
                  --class protected|advisory --binding metered|device_sum [--point P]
                  [--devices d1,d2] --minimum M --target T --source S --evidence-generation EG
                  [--revision REV] --actor A --tick T
  obligation list
  interlock declare --interlock I --room R [--row W] [--device D] --class protected|advisory
                  --epoch E --actor A --tick T
  interlock report --interlock I --state satisfied|open|unknown --sequence N --epoch E --tick T
  interlock list
  grant add       --grant G --issuer S --epoch E [--device D] [--device-generation G] [--room R]
                  --actions a[,b] --issued T [--expires T]
  grant revoke    --grant G --epoch E --actor A --tick T
  grant list
  override add    --override O --device D --device-generation G --epoch E --issued T --expires T
                  --reason R --actor A --tick T
  override revoke --override O --actor A --tick T
  override list
  permit add      --permit P --issuer S --epoch E --device D --issued T [--expires T] --reason R
                  --actor A --tick T
  permit list
  observe         --device D --device-generation G --kind fan|airflow|pressure [--relationship R]
                  --point P --source S --sequence N --measured-at T --evidence-generation EG
                  [--quality Q] --value V --tick T
  evaluate        --device D --device-generation G --epoch E [--revision R] --intent I
                  [--setpoint-percent B | --setpoint-airflow A] [--relationship R] --key K
                  --actor A --tick T [--safety-permit P] [--supersede ATTEMPT]
  issue           the options of evaluate, plus --adapter synthetic|synthetic-refusing|synthetic-ack-only
                  and optional --verify (which uses --source/--sequence/--point/--tick)
  verify          --attempt N [--adapter synthetic|synthetic-ack-only] --source S
                  [--relationship R] [--fan-sequence N] [--pressure-sequence N]
                  [--fan-point P] [--pressure-point P] --tick T
  resolve         --attempt N --target resolved_without_effect|superseded --actor A --tick T
                  --reason R
  attempts | attempt show --attempt N
  state           (the canonical model as deterministic text)
  digest          (the digest of the canonical model)
  history         [--limit N]
  store-audit
  help | version
global: --store <path> (omit for an in-memory engine with no durability), --json, --help, --version
exit:   0 success, 1 the request was understood and refused, 2 usage error
)USAGE";

/// The parsed command line.
///
/// Every option is accepted as "--name value" and as "--name=value"; the two
/// spellings reach the same member, so no option can be reachable in one form
/// and silently ignored in the other.
///
/// An option that appears without a value, or with an empty value, is treated
/// as absent: a required option that was not given and one that was given as
/// nothing are both "missing", and the caller is told which option it was.
struct Args {
  std::string store;
  std::string verb;
  std::string action;
  std::vector<std::pair<std::string, std::string>> options;
  bool json = false;
  bool help = false;
  bool version = false;
  bool verify = false;

  [[nodiscard]] const std::string* find(std::string_view key) const {
    for (const std::pair<std::string, std::string>& entry : options) {
      if (entry.first == key) {
        return &entry.second;
      }
    }
    return nullptr;
  }
  [[nodiscard]] bool has(std::string_view key) const { return find(key) != nullptr; }
};

bool parse_arguments(int argc, char** argv, Args& args, std::string& problem) {
  for (int index = 1; index < argc; ++index) {
    const std::string argument = argv[index];
    if (argument == "-h") {
      args.help = true;
      continue;
    }
    if (argument.size() > 2 && argument[0] == '-' && argument[1] == '-') {
      const std::string body = argument.substr(2);
      const std::size_t equals = body.find('=');
      const bool attached = equals != std::string::npos;
      const std::string name = attached ? body.substr(0, equals) : body;
      std::string value = attached ? body.substr(equals + 1) : std::string();
      if (name.empty()) {
        problem = "an option name is missing before '='";
        return false;
      }
      // A flag never takes a separate value: the argument after a bare --json
      // is not its value. An attached value is honoured so that --json=false
      // says what it looks like it says.
      if (name == "help" || name == "version" || name == "json" || name == "verify") {
        const bool enabled = !attached || !(value == "false" || value == "0");
        if (name == "help") {
          args.help = enabled;
        } else if (name == "version") {
          args.version = enabled;
        } else if (name == "json") {
          args.json = enabled;
        } else {
          args.verify = enabled;
        }
        continue;
      }
      if (!attached) {
        if (index + 1 >= argc) {
          problem = "--" + name + " needs a value";
          return false;
        }
        value = argv[++index];
      }
      if (name == "store") {
        args.store = value;
      } else {
        args.options.emplace_back(name, value);
      }
      continue;
    }
    if (!argument.empty() && argument[0] == '-' && argument != "-") {
      problem = "unrecognized option '" + argument + "'";
      return false;
    }
    if (args.verb.empty()) {
      args.verb = argument;
      continue;
    }
    if (args.action.empty()) {
      args.action = argument;
      continue;
    }
    problem = "unexpected argument '" + argument + "'";
    return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Checked numbers and option lookups
// ---------------------------------------------------------------------------

Result<std::uint64_t> parse_unsigned(std::string_view text) {
  if (text.empty()) {
    return Status::failure(StatusCode::invalid_argument, "an empty value is not a number");
  }
  if (text.front() == '+') {
    return Status::failure(StatusCode::invalid_argument,
                           "'" + std::string(text) + "' carries a leading '+'");
  }
  if (text.front() == '-') {
    return Status::failure(StatusCode::invalid_argument,
                           "'" + std::string(text) + "' is not a non-negative integer");
  }
  std::uint64_t value = 0;
  for (const char digit : text) {
    if (digit < '0' || digit > '9') {
      return Status::failure(StatusCode::invalid_argument,
                             "'" + std::string(text) + "' is not a non-negative integer");
    }
    const std::uint64_t unit = static_cast<std::uint64_t>(digit - '0');
    if (value > (std::numeric_limits<std::uint64_t>::max() - unit) / 10ull) {
      return Status::failure(StatusCode::overflow,
                             "'" + std::string(text) + "' overflows a 64-bit count");
    }
    value = value * 10ull + unit;
  }
  return value;
}

Result<std::int64_t> parse_signed(std::string_view text) {
  if (text.empty()) {
    return Status::failure(StatusCode::invalid_argument, "an empty value is not a number");
  }
  if (text.front() == '+') {
    return Status::failure(StatusCode::invalid_argument,
                           "'" + std::string(text) + "' carries a leading '+'");
  }
  bool negative = false;
  std::string_view digits = text;
  if (text.front() == '-') {
    negative = true;
    digits = text.substr(1);
    if (digits.empty()) {
      return Status::failure(StatusCode::invalid_argument, "'-' alone is not a number");
    }
  }
  const std::uint64_t limit = negative
                                  ? (1ull << 63)
                                  : static_cast<std::uint64_t>(
                                        std::numeric_limits<std::int64_t>::max());
  std::uint64_t magnitude = 0;
  for (const char digit : digits) {
    if (digit < '0' || digit > '9') {
      return Status::failure(StatusCode::invalid_argument,
                             "'" + std::string(text) + "' is not an integer");
    }
    const std::uint64_t unit = static_cast<std::uint64_t>(digit - '0');
    if (magnitude > (limit - unit) / 10ull) {
      return Status::failure(StatusCode::overflow,
                             "'" + std::string(text) + "' overflows a 64-bit count");
    }
    magnitude = magnitude * 10ull + unit;
  }
  if (negative) {
    return static_cast<std::int64_t>(0ull - magnitude);
  }
  return static_cast<std::int64_t>(magnitude);
}

Result<std::string> required_text(const Args& args, std::string_view key) {
  const std::string* text = args.find(key);
  if (text == nullptr || text->empty()) {
    return Status::failure(StatusCode::invalid_argument,
                           "--" + std::string(key) + " is required");
  }
  return *text;
}

Result<std::uint64_t> unsigned_option(const Args& args, std::string_view key) {
  const auto text = required_text(args, key);
  if (!text.ok()) {
    return text.status();
  }
  Result<std::uint64_t> value = parse_unsigned(text.value());
  if (!value.ok()) {
    return Status::failure(value.code(),
                           "--" + std::string(key) + ": " + value.message());
  }
  return value;
}

Result<std::int64_t> signed_option(const Args& args, std::string_view key) {
  const auto text = required_text(args, key);
  if (!text.ok()) {
    return text.status();
  }
  Result<std::int64_t> value = parse_signed(text.value());
  if (!value.ok()) {
    return Status::failure(value.code(),
                           "--" + std::string(key) + ": " + value.message());
  }
  return value;
}

template <typename Id>
Result<Id> id_option(const Args& args, std::string_view key) {
  const auto text = required_text(args, key);
  if (!text.ok()) {
    return text.status();
  }
  return Id::parse(text.value());
}

template <typename Id>
Result<std::optional<Id>> optional_id_option(const Args& args, std::string_view key) {
  if (!args.has(key)) {
    return std::optional<Id>{};
  }
  auto parsed = id_option<Id>(args, key);
  if (!parsed.ok()) {
    return parsed.status();
  }
  return std::optional<Id>{parsed.value()};
}

template <typename Tag>
Result<Ordinal<Tag>> ordinal_option(const Args& args, std::string_view key) {
  auto value = unsigned_option(args, key);
  if (!value.ok()) {
    return value.status();
  }
  return Ordinal<Tag>::from(value.value());
}

template <typename Tag>
Result<std::optional<Ordinal<Tag>>> optional_ordinal_option(const Args& args, std::string_view key) {
  if (!args.has(key)) {
    return std::optional<Ordinal<Tag>>{};
  }
  auto value = ordinal_option<Tag>(args, key);
  if (!value.ok()) {
    return value.status();
  }
  return std::optional<Ordinal<Tag>>{value.value()};
}

/// A logical instant. LogicalTick is its own type rather than an ordinal, so
/// it is read with its own helpers: a tick is not an ordinal and the tool never
/// treats one as the other.
Result<LogicalTick> tick_option(const Args& args, std::string_view key) {
  auto value = unsigned_option(args, key);
  if (!value.ok()) {
    return value.status();
  }
  return LogicalTick::from(value.value());
}

Result<std::optional<LogicalTick>> optional_tick_option(const Args& args, std::string_view key) {
  if (!args.has(key)) {
    return std::optional<LogicalTick>{};
  }
  auto value = tick_option(args, key);
  if (!value.ok()) {
    return value.status();
  }
  return std::optional<LogicalTick>{value.value()};
}

/// A fan setpoint, in basis points, checked by the type that owns the range.
Result<SetpointBasisPoints> percent_option(const Args& args, std::string_view key) {
  auto value = unsigned_option(args, key);
  if (!value.ok()) {
    return value.status();
  }
  if (value.value() > kMaxUint32) {
    return Status::failure(StatusCode::out_of_range,
                           "--" + std::string(key) + ": " + std::to_string(value.value()) +
                               " is larger than a 32-bit basis-point count");
  }
  return SetpointBasisPoints::create(static_cast<std::uint32_t>(value.value()));
}

/// A slew bound, in basis points, checked by the type that owns the range.
Result<SlewBasisPoints> slew_option(const Args& args, std::string_view key) {
  auto value = unsigned_option(args, key);
  if (!value.ok()) {
    return value.status();
  }
  if (value.value() > kMaxUint32) {
    return Status::failure(StatusCode::out_of_range,
                           "--" + std::string(key) + ": " + std::to_string(value.value()) +
                               " is larger than a 32-bit basis-point count");
  }
  return SlewBasisPoints::create(static_cast<std::uint32_t>(value.value()));
}

/// True when a parse failed, recording the first explanation for the caller.
template <typename T>
bool failed(const Result<T>& value, std::string& problem) {
  if (value.ok()) {
    return false;
  }
  if (problem.empty()) {
    problem = value.message();
  }
  return true;
}

Result<ControlIntent> intent_option(const Args& args, std::string_view key) {
  const auto text = required_text(args, key);
  if (!text.ok()) {
    return text.status();
  }
  const std::optional<ControlIntent> intent = parse_control_intent(text.value());
  if (!intent.has_value()) {
    return Status::failure(StatusCode::invalid_argument,
                           "unrecognized --" + std::string(key) + " '" + text.value() + "'");
  }
  return *intent;
}

/// Splits a comma-separated list, refusing an empty element rather than
/// skipping it: a typo in the middle of a list must not silently shrink it.
Result<std::vector<std::string>> split_list(std::string_view text) {
  std::vector<std::string> items;
  std::string current;
  for (const char character : text) {
    if (character == ',') {
      if (current.empty()) {
        return Status::failure(StatusCode::invalid_argument,
                               "the list '" + std::string(text) + "' has an empty element");
      }
      items.push_back(current);
      current.clear();
      continue;
    }
    current.push_back(character);
  }
  if (current.empty()) {
    return Status::failure(StatusCode::invalid_argument,
                           "the list '" + std::string(text) + "' has an empty element");
  }
  items.push_back(current);
  return items;
}

// ---------------------------------------------------------------------------
// The engine and the exit paths
// ---------------------------------------------------------------------------

EngineOptions cli_options() {
  EngineOptions options;
  // The audit ring is raised so that one administration session's history is
  // still in it. Every other bound, including the exact (zero) evidence
  // tolerances, keeps the library's default: a tool that quietly widened them
  // would report effects the library itself would not accept.
  options.audit_capacity = 1024;
  return options;
}

Result<AirflowControlEngine> open_engine(const Args& args, OpenMode mode) {
  if (args.store.empty()) {
    return AirflowControlEngine::open_in_memory(cli_options());
  }
  return AirflowControlEngine::open(args.store, mode, cli_options());
}

int usage_error(const std::string& message) {
  std::cerr << "error: usage: " << message << "\n";
  return kExitUsage;
}

int unknown_verb(const std::string& verb) {
  std::cerr << "error: usage: unknown verb '" << verb << "'\n" << kUsage;
  return kExitUsage;
}

int refuse(const Status& status, bool json);

/// Reports a failure that happened while an engine was open, closing it first.
/// A store that could not be closed is the more serious fault, so it wins.
int refuse_after_close(AirflowControlEngine& engine, const Status& status, bool json) {
  const Status closed = engine.close();
  if (!closed.ok()) {
    return refuse(closed, json);
  }
  return refuse(status, json);
}

// ---------------------------------------------------------------------------
// Reporting
//
// Every report function fills a sink through the same field calls, so a record
// renders identically whether it is printed alone or as one row of a listing.
// JSON carries exact integers; the human rendering carries the library's own
// fixed-point rendering beside them.
// ---------------------------------------------------------------------------

template <typename Sink>
void report_trace(Sink& sink, const std::vector<CheckTrace>& trace) {
  std::vector<Row> rows;
  rows.reserve(trace.size());
  for (const CheckTrace& entry : trace) {
    Row row;
    row.string("check", entry.check);
    row.string("outcome", to_string(entry.outcome));
    row.string("detail", entry.detail);
    rows.push_back(std::move(row));
  }
  sink.rows("trace", rows);
}

template <typename Sink>
void report_device(Sink& sink, const DeviceView& view) {
  sink.string("device", view.id.str());
  sink.uinteger("generation", view.generation.value());
  sink.uinteger("revision", view.revision.value());
  sink.string("lifecycle", to_string(view.lifecycle));
  sink.string("room", view.room.str());
  if (view.row.has_value()) {
    sink.string("row", view.row->str());
  }
  if (view.policy_id.has_value()) {
    sink.string("policy", view.policy_id->str());
  }
  if (view.policy_generation.has_value()) {
    sink.uinteger("policy-generation", view.policy_generation->value());
  }
  if (view.envelope.has_value()) {
    const OperatingEnvelope& envelope = *view.envelope;
    sink.quantity("min-percent-basis-points",
                  static_cast<std::int64_t>(envelope.min_fan_percent.basis_points()),
                  envelope.min_fan_percent.to_string());
    sink.quantity("max-percent-basis-points",
                  static_cast<std::int64_t>(envelope.max_fan_percent.basis_points()),
                  envelope.max_fan_percent.to_string());
    sink.quantity("default-percent-basis-points",
                  static_cast<std::int64_t>(envelope.default_fan_percent.basis_points()),
                  envelope.default_fan_percent.to_string());
    sink.quantity("min-airflow-cubic-metres-per-hour",
                  envelope.min_airflow.cubic_metres_per_hour(), envelope.min_airflow.to_string());
    sink.quantity("max-airflow-cubic-metres-per-hour",
                  envelope.max_airflow.cubic_metres_per_hour(), envelope.max_airflow.to_string());
    sink.quantity("max-step-basis-points",
                  static_cast<std::int64_t>(envelope.max_step.basis_points()),
                  envelope.max_step.to_string());
    sink.string("envelope-source", envelope.source.str());
    sink.uinteger("envelope-evidence-generation", envelope.evidence_generation.value());
  }
  sink.string("effect", to_string(view.effect));
  if (view.unresolved_attempt.has_value()) {
    sink.uinteger("unresolved-attempt", view.unresolved_attempt->value());
  }
  if (view.has_fan_observation && view.observed_fan_percent.has_value()) {
    sink.quantity("observed-fan-percent-basis-points",
                  static_cast<std::int64_t>(view.observed_fan_percent->basis_points()),
                  view.observed_fan_percent->to_string());
    sink.string("fan-freshness", to_string(view.fan_freshness));
  }
  if (view.has_airflow_observation && view.observed_airflow.has_value()) {
    sink.quantity("observed-airflow-cubic-metres-per-hour",
                  view.observed_airflow->cubic_metres_per_hour(),
                  view.observed_airflow->to_string());
    sink.string("airflow-freshness", to_string(view.airflow_freshness));
  }
}

template <typename Sink>
void report_relationship(Sink& sink, const RelationshipView& view) {
  sink.string("relationship", view.id.str());
  sink.string("room", view.room.str());
  sink.string("polarity", to_string(view.polarity));
  sink.quantity("lower-millipascals", view.lower.millipascals(), view.lower.to_string());
  sink.quantity("upper-millipascals", view.upper.millipascals(), view.upper.to_string());
  sink.quantity("tolerance-millipascals", view.tolerance.millipascals(),
                view.tolerance.to_string());
  sink.uinteger("evidence-generation", view.evidence_generation.value());
  sink.uinteger("revision", view.revision.value());
  sink.string("state", to_string(view.state));
  if (view.adjudicated.has_value()) {
    sink.quantity("adjudicated-millipascals", view.adjudicated->millipascals(),
                  view.adjudicated->to_string());
  }
  sink.uinteger("contributing-sources", static_cast<std::uint64_t>(view.contributing_sources));
  sink.uinteger("conflicting-sources", static_cast<std::uint64_t>(view.conflicting_sources));
}

template <typename Sink>
void report_containment(Sink& sink, const ContainmentView& view) {
  sink.string("containment", view.id.str());
  sink.string("room", view.room.str());
  if (view.row.has_value()) {
    sink.string("row", view.row->str());
  }
  sink.string("kind", to_string(view.kind));
  sink.string("state", to_string(view.state));
  sink.string("quality", to_string(view.quality));
  sink.string("source", view.source.str());
  sink.uinteger("sequence", view.sequence.value());
  sink.uinteger("evidence-generation", view.evidence_generation.value());
  sink.uinteger("measured-at", view.measured_at.value());
  sink.boolean("reported", view.has_report);
}

template <typename Sink>
void report_obligation(Sink& sink, const ObligationView& view) {
  sink.string("obligation", view.id.str());
  sink.string("scope", to_string(view.scope));
  sink.string("room", view.room.str());
  if (view.row.has_value()) {
    sink.string("row", view.row->str());
  }
  if (view.rack.has_value()) {
    sink.string("rack", view.rack->str());
  }
  sink.string("class", to_string(view.klass));
  sink.string("binding", to_string(view.binding));
  sink.quantity("minimum-airflow-cubic-metres-per-hour",
                view.minimum_airflow.cubic_metres_per_hour(), view.minimum_airflow.to_string());
  sink.quantity("target-airflow-cubic-metres-per-hour",
                view.target_airflow.cubic_metres_per_hour(), view.target_airflow.to_string());
  sink.uinteger("evidence-generation", view.evidence_generation.value());
  sink.uinteger("revision", view.revision.value());
  sink.string("state", to_string(view.state));
  if (view.observed_airflow.has_value()) {
    sink.quantity("observed-airflow-cubic-metres-per-hour",
                  view.observed_airflow->cubic_metres_per_hour(),
                  view.observed_airflow->to_string());
  }
}

template <typename Sink>
void report_interlock(Sink& sink, const InterlockView& view) {
  sink.string("interlock", view.id.str());
  sink.string("room", view.room.str());
  sink.string("class", to_string(view.klass));
  sink.string("state", to_string(view.state));
  sink.uinteger("sequence", view.sequence.value());
  sink.uinteger("epoch", view.epoch.value());
  sink.boolean("reported", view.has_report);
}

std::vector<std::string> action_names(const ActionSet& actions) {
  std::vector<std::string> names;
  for (std::uint32_t index = 0; index < kControlActionCount; ++index) {
    const auto action = static_cast<ControlAction>(index);
    if (actions.contains(action)) {
      names.emplace_back(to_string(action));
    }
  }
  return names;
}

template <typename Sink>
void report_grant(Sink& sink, const PermissionGrant& grant) {
  sink.string("grant", grant.id.str());
  sink.string("issuer", grant.issuer.str());
  sink.uinteger("epoch", grant.epoch.value());
  if (grant.device.has_value()) {
    sink.string("device", grant.device->str());
  }
  if (grant.device_generation.has_value()) {
    sink.uinteger("device-generation", grant.device_generation->value());
  }
  if (grant.room.has_value()) {
    sink.string("room", grant.room->str());
  }
  sink.strings("actions", action_names(grant.actions));
  sink.uinteger("issued-at", grant.issued_at.value());
  if (grant.expires_at.has_value()) {
    sink.uinteger("expires-at", grant.expires_at->value());
  }
  sink.boolean("revoked", grant.revoked);
}

template <typename Sink>
void report_override(Sink& sink, const MaintenanceOverride& entry) {
  sink.string("override", entry.id.str());
  sink.string("device", entry.device.str());
  sink.uinteger("device-generation", entry.device_generation.value());
  sink.uinteger("epoch", entry.epoch.value());
  sink.uinteger("issued-at", entry.issued_at.value());
  sink.uinteger("expires-at", entry.expires_at.value());
  sink.string("reason", entry.reason);
  sink.boolean("revoked", entry.revoked);
}

template <typename Sink>
void report_permit(Sink& sink, const SafetyPermit& permit) {
  sink.string("permit", permit.id.str());
  sink.string("issuer", permit.issuer.str());
  sink.uinteger("epoch", permit.epoch.value());
  sink.string("device", permit.device.str());
  sink.uinteger("issued-at", permit.issued_at.value());
  if (permit.expires_at.has_value()) {
    sink.uinteger("expires-at", permit.expires_at->value());
  }
  sink.string("reason", permit.reason);
}

template <typename Sink>
void report_attempt(Sink& sink, const AttemptRecord& record, bool superseded_by_known_attempt) {
  sink.uinteger("attempt", record.id.value());
  sink.uinteger("ordinal", record.ordinal.value());
  sink.string("key", record.key.str());
  sink.string("device", record.device.str());
  sink.uinteger("device-generation", record.device_generation.value());
  sink.uinteger("epoch", record.epoch.value());
  sink.uinteger("planned-revision", record.planned_revision.value());
  sink.uinteger("policy-generation", record.policy_generation.value());
  sink.uinteger("evidence-generation", record.evidence_generation.value());
  sink.string("intent", to_string(record.intent));
  sink.string("request-class", to_string(record.request_class));
  sink.string("setpoint", to_string(record.setpoint));
  sink.string("setpoint-kind", to_string(setpoint_kind(record.setpoint)));
  sink.string("actor", record.actor.str());
  sink.uinteger("accepted-at", record.accepted_at.value());
  if (record.dispatched_at.has_value()) {
    sink.uinteger("dispatched-at", record.dispatched_at->value());
  }
  sink.string("state", to_string(record.state));
  sink.boolean("unresolved", is_unresolved(record.state));
  sink.uinteger("command", record.command.value());
  sink.uinteger("adapter-sequence", record.adapter_sequence.value());
  sink.string("adapter-disposition", to_string(record.disposition));
  if (!record.detail.empty()) {
    sink.string("detail", record.detail);
  }
  if (record.safety_permit.has_value()) {
    sink.string("safety-permit", record.safety_permit->str());
  }
  if (record.supersedes.has_value()) {
    sink.uinteger("supersedes", record.supersedes->value());
  }
  if (record.superseded_by.has_value()) {
    sink.uinteger("superseded-by", record.superseded_by->value());
  }
  sink.boolean("superseded-by-known-attempt", superseded_by_known_attempt);
  if (record.fan_observation.has_value()) {
    sink.uinteger("fan-observation", record.fan_observation->value());
  }
  if (record.pressure_observation.has_value()) {
    sink.uinteger("pressure-observation", record.pressure_observation->value());
  }
  if (record.effect_sequence.has_value()) {
    sink.uinteger("effect-sequence", record.effect_sequence->value());
  }
  if (record.resolved_at.has_value()) {
    sink.uinteger("resolved-at", record.resolved_at->value());
  }
  if (!record.resolution_reason.empty()) {
    sink.string("resolution-reason", record.resolution_reason);
  }
}

void report_effect(Emitter& out, const VerifiedEffect& effect) {
  out.uinteger("attempt", effect.attempt.value());
  out.string("effect", to_string(effect.state));
  out.boolean("established", effect.state == EffectState::effective);
  out.uinteger("effect-sequence", effect.sequence.value());
  out.uinteger("verified-at", effect.verified_at.value());
  out.string("code", to_string(effect.code));
  out.string("message", effect.message);
  if (effect.observed_fan_percent.has_value()) {
    out.quantity("observed-fan-percent-basis-points",
                 static_cast<std::int64_t>(effect.observed_fan_percent->basis_points()),
                 effect.observed_fan_percent->to_string());
  }
  if (effect.observed_airflow.has_value()) {
    out.quantity("observed-airflow-cubic-metres-per-hour",
                 effect.observed_airflow->cubic_metres_per_hour(),
                 effect.observed_airflow->to_string());
  }
  if (effect.observed_differential.has_value()) {
    out.quantity("observed-differential-millipascals",
                 effect.observed_differential->millipascals(),
                 effect.observed_differential->to_string());
  }
  if (effect.fan_observation.has_value()) {
    out.uinteger("fan-observation", effect.fan_observation->value());
  }
  if (effect.pressure_observation.has_value()) {
    out.uinteger("pressure-observation", effect.pressure_observation->value());
  }
  report_trace(out, effect.trace);
}

void report_decision(Emitter& out, const Decision& decision) {
  out.boolean("eligible", decision.eligible);
  out.string("code", to_string(decision.code));
  out.string("message", decision.message);
  out.string("request-class", to_string(decision.request_class));
  out.boolean("replayed", decision.replayed);
  if (decision.replayed_attempt.has_value()) {
    out.uinteger("replayed-attempt", decision.replayed_attempt->value());
  }
  out.uinteger("planned-revision", decision.planned_revision.value());
  out.uinteger("device-generation", decision.device_generation.value());
  out.uinteger("epoch", decision.epoch.value());
  out.uinteger("policy-generation", decision.policy_generation.value());
  out.uinteger("evidence-generation", decision.evidence_generation.value());
  if (decision.resolved_setpoint.has_value()) {
    out.string("resolved-setpoint", to_string(*decision.resolved_setpoint));
    out.string("setpoint-kind", to_string(setpoint_kind(*decision.resolved_setpoint)));
  }
  if (decision.unresolved_attempt.has_value()) {
    out.uinteger("unresolved-attempt", decision.unresolved_attempt->value());
  }
  out.string("current-effect", to_string(decision.current_effect));
  report_trace(out, decision.trace);
}

void report_adapter(Emitter& out, const AdapterDescriptor& descriptor, const std::string& mode) {
  out.string("adapter", mode);
  out.boolean("synthetic", true);
  out.string("adapter-vendor", descriptor.vendor);
  out.string("adapter-model", descriptor.model);
  out.string("adapter-protocol", descriptor.protocol);
  out.strings("adapter-capabilities", [&descriptor] {
    std::vector<std::string> capabilities;
    const struct {
      AdapterCapability capability;
      const char* name;
    } kTable[] = {{AdapterCapability::set_fan_percent, "set_fan_percent"},
                  {AdapterCapability::set_airflow, "set_airflow"},
                  {AdapterCapability::read_pressure, "read_pressure"},
                  {AdapterCapability::read_airflow, "read_airflow"},
                  {AdapterCapability::read_fan_percent, "read_fan_percent"},
                  {AdapterCapability::observe_containment, "observe_containment"}};
    for (const auto& entry : kTable) {
      if (declares(descriptor, entry.capability)) {
        capabilities.emplace_back(entry.name);
      }
    }
    return capabilities;
  }());
}

void report_store_audit(Emitter& out, const StoreAudit& audit, bool durable) {
  out.boolean("durable", durable);
  out.string("store", audit.path.empty() ? std::string("(in memory)") : audit.path);
  out.uinteger("slot-capacity-bytes", audit.slot_capacity_bytes);
  out.uinteger("payload-bytes", audit.payload_bytes);
  out.uinteger("valid-head-records", static_cast<std::uint64_t>(audit.valid_head_records));
  out.uinteger("superseded-generations", audit.superseded_generations);
  out.boolean("rollback-observed", audit.rollback_observed);
  out.uinteger("store-generation", audit.generation.value());
  out.uinteger("last-writer", audit.last_writer.value());
  out.uinteger("publications", audit.publications);
}

int refuse(const Status& status, bool json) {
  std::cerr << "error: " << to_string(status.code()) << ": " << status.message() << "\n";
  if (!json) {
    return kExitRefused;
  }
  Emitter out(true);
  out.boolean("ok", false);
  out.string("code", to_string(status.code()));
  out.string("message", status.message());
  if (!status.trace().empty()) {
    report_trace(out, status.trace());
  }
  out.finish(std::cout);
  return kExitRefused;
}

/// The engine produced a decision that does not permit the transition. That is
/// a refusal, not an error: the request was understood, and the whole ordered
/// trace of checks that produced the answer is reported.
int refuse_decision(const Decision& decision, Emitter& out) {
  std::cerr << "error: " << to_string(decision.code) << ": " << decision.message << "\n";
  out.finish(std::cout);
  return kExitRefused;
}

// ---------------------------------------------------------------------------
// The synthetic adapter
// ---------------------------------------------------------------------------

enum class AdapterMode { synthetic, refusing, acknowledge_only };

const char* adapter_mode_name(AdapterMode mode) {
  switch (mode) {
    case AdapterMode::synthetic:
      return "synthetic";
    case AdapterMode::refusing:
      return "synthetic-refusing";
    case AdapterMode::acknowledge_only:
      return "synthetic-ack-only";
  }
  return "synthetic";
}

/// Selecting an adapter is how the caller chooses what the simulated device
/// does. It is never how the caller chooses what the engine decides: the
/// command is validated before the adapter is consulted.
Result<AdapterMode> adapter_mode_option(const Args& args, bool refusing_permitted) {
  const auto text = required_text(args, "adapter");
  if (!text.ok()) {
    return text.status();
  }
  if (text.value() == "synthetic") {
    return AdapterMode::synthetic;
  }
  if (text.value() == "synthetic-ack-only") {
    return AdapterMode::acknowledge_only;
  }
  if (text.value() == "synthetic-refusing" && refusing_permitted) {
    return AdapterMode::refusing;
  }
  return Status::failure(StatusCode::invalid_argument,
                         "unrecognized --adapter '" + text.value() + "'");
}

AdapterDescriptor synthetic_descriptor() {
  return AdapterDescriptor{"synthetic", "airflow-control-cli", "in-process",
                           kSyntheticCapabilities, true};
}

/// The honesty label every run of issue and verify carries. It goes to standard
/// error rather than into the JSON object, because a JSON run prints exactly one
/// object on standard output and nothing else.
void announce_adapter() {
  std::cerr << "adapter=SYNTHETIC (no hardware is driven)\n";
}

SetpointBasisPoints basis_points_clamped(std::int64_t value) {
  if (value <= 0) {
    return SetpointBasisPoints::create(0).value();
  }
  if (value >= static_cast<std::int64_t>(SetpointBasisPoints::kFull)) {
    return SetpointBasisPoints::create(SetpointBasisPoints::kFull).value();
  }
  return SetpointBasisPoints::create(static_cast<std::uint32_t>(value)).value();
}

/// The airflow one basis point of fan delivers on the simulated plant.
///
/// It is derived from the declared envelope corners so that the simulated
/// delivery is consistent with the limits the owning authority published, and
/// the same number is handed to the adapter, so the plant and the reader agree.
/// This is a property of a simulator, never a claim about a real fan curve.
std::int64_t plant_airflow_per_basis_point(const std::optional<OperatingEnvelope>& envelope) {
  if (!envelope.has_value()) {
    return 0;
  }
  const std::int64_t span = static_cast<std::int64_t>(envelope->max_fan_percent.basis_points()) -
                            static_cast<std::int64_t>(envelope->min_fan_percent.basis_points());
  if (span <= 0) {
    return 0;
  }
  return (envelope->max_airflow.cubic_metres_per_hour() -
          envelope->min_airflow.cubic_metres_per_hour()) /
         span;
}

/// One device's simulated plant: a fan position, the airflow it delivers, and
/// the differential the relationship reports.
struct PlantSeed {
  SetpointBasisPoints fan_percent;
  Airflow airflow;
  Pressure differential;
};

std::optional<DeviceView> find_device_view(const AirflowControlEngine& engine,
                                           const AirflowDeviceId& device) {
  for (const DeviceView& view : engine.devices()) {
    if (view.id == device) {
      return view;
    }
  }
  return std::nullopt;
}

/// Where the engine last observed the device: the plant that answers before a
/// command is delivered. A device the engine has never observed starts at the
/// policy's declared default position, and a device with no policy at all
/// starts at rest, which is the only honest value left.
PlantSeed observed_plant(const DeviceView& view) {
  const std::int64_t scale = plant_airflow_per_basis_point(view.envelope);
  SetpointBasisPoints fan = SetpointBasisPoints::create(0).value();
  if (view.observed_fan_percent.has_value()) {
    fan = *view.observed_fan_percent;
  } else if (view.envelope.has_value()) {
    fan = view.envelope->default_fan_percent;
  }
  Airflow airflow =
      Airflow::from_cubic_metres_per_hour(static_cast<std::int64_t>(fan.basis_points()) * scale);
  if (view.observed_airflow.has_value()) {
    airflow = *view.observed_airflow;
  }
  return PlantSeed{fan, airflow, Pressure::from_millipascals(0)};
}

/// Where a delivered command puts the plant, projected exactly the way the
/// synthetic adapter projects it, so the seeded plant and the adapter's own
/// answer cannot disagree about the same setpoint.
PlantSeed delivered_plant(const DeviceView& view, const SetpointRequest& commanded) {
  PlantSeed seed = observed_plant(view);
  const std::int64_t scale = plant_airflow_per_basis_point(view.envelope);
  if (const auto* percent = std::get_if<SetpointPercent>(&commanded)) {
    seed.fan_percent = percent->percent;
    seed.airflow = Airflow::from_cubic_metres_per_hour(
        static_cast<std::int64_t>(percent->percent.basis_points()) * scale);
    return seed;
  }
  const auto* airflow = std::get_if<SetpointAirflow>(&commanded);
  seed.airflow = airflow->airflow;
  if (scale > 0) {
    seed.fan_percent = basis_points_clamped(airflow->airflow.cubic_metres_per_hour() / scale);
  }
  return seed;
}

/// The differential the simulated plant reports for a relationship: what the
/// engine last adjudicated, or the middle of the declared band while it has no
/// evidence. A relationship the model does not hold leaves the plant at zero,
/// which is a value no band claims.
Pressure plant_differential(const AirflowControlEngine& engine,
                            const std::optional<PressureRelationshipId>& relationship) {
  if (!relationship.has_value()) {
    return Pressure::from_millipascals(0);
  }
  const auto view = engine.relationship(*relationship);
  if (!view.ok()) {
    return Pressure::from_millipascals(0);
  }
  if (view.value().adjudicated.has_value()) {
    return *view.value().adjudicated;
  }
  const std::int64_t lower = view.value().lower.millipascals();
  const std::int64_t upper = view.value().upper.millipascals();
  return Pressure::from_millipascals(lower + (upper - lower) / 2);
}

/// Seeds the simulated plant and arms the behaviour the caller asked for.
///
/// The adapter is built in place rather than returned because it is neither
/// copyable nor movable: a plant that could be copied would let two adapters
/// disagree about one device.
void seed_adapter(SyntheticAirflowAdapter& adapter, const AirflowControlEngine& engine,
                  const DeviceView& view, const PlantSeed& seed,
                  const std::optional<PressureRelationshipId>& relationship, AdapterMode mode) {
  adapter.set_airflow_per_basis_point(plant_airflow_per_basis_point(view.envelope));
  adapter.seed_device(view.id, view.generation,
                      SyntheticPlantState{seed.fan_percent, seed.airflow,
                                          plant_differential(engine, relationship)});
  if (mode == AdapterMode::refusing) {
    // Every command this adapter is handed is refused outright, which is what a
    // device that will not act looks like at the vendor boundary.
    adapter.set_persistent_disposition(view.id, AdapterDisposition::refused,
                                       "armed by the CLI: --adapter synthetic-refusing");
  }
  if (mode == AdapterMode::acknowledge_only) {
    // The adapter takes responsibility for the command and never moves the
    // plant: acknowledgement is not an effect, and a verification that reads
    // this plant must not find one.
    adapter.set_apply_on_execute(false);
  }
}

/// The point a delivery reading is attributed to when the caller named none:
/// the device itself, which is where a fan position is read.
SpaceRefId delivery_point(const AirflowDeviceId& device) {
  // The text was validated as an identifier of the same alphabet and bound when
  // the device was registered, so re-validating it as a space reference cannot
  // fail; the value is taken rather than assumed.
  return SpaceRefId::parse(device.str()).value();
}

/// The point a differential is attributed to when the caller named none: the
/// declared relationship, which is what the reading is about.
SpaceRefId differential_point(const AirflowDeviceId& device,
                              const std::optional<PressureRelationshipId>& relationship) {
  if (relationship.has_value()) {
    return SpaceRefId::parse(relationship->str()).value();
  }
  return SpaceRefId::parse(device.str()).value();
}

/// Closes the engine and then prints the accumulated result.
///
/// Every verb closes what it opened before it returns: the durable store's
/// cross-process lock is released at close, and the close is what records that
/// this incarnation is finished with the file. Nothing is printed until the
/// close succeeded, so a result is never printed for a run whose last
/// generation could not be committed.
int finish_verb(AirflowControlEngine& engine, Emitter& out, bool json) {
  const Status closed = engine.close();
  if (!closed.ok()) {
    return refuse(closed, json);
  }
  out.finish(std::cout);
  return kExitOk;
}

// ---------------------------------------------------------------------------
// Verbs: the control plane
// ---------------------------------------------------------------------------

int verb_init(const Args& args) {
  EngineOptions options = cli_options();
  if (args.has("slot-capacity-mib")) {
    const auto mib = unsigned_option(args, "slot-capacity-mib");
    if (!mib.ok()) {
      return usage_error(mib.message());
    }
    if (mib.value() > std::numeric_limits<std::uint64_t>::max() / kBytesPerMib) {
      return usage_error("--slot-capacity-mib: " + std::to_string(mib.value()) +
                         " is too large to express in bytes");
    }
    // The store owns the range check, so the value is converted and handed over
    // rather than second-guessed here; its refusal names the capacity it
    // validated.
    options.store.slot_capacity_bytes = mib.value() * kBytesPerMib;
  }
  // A store is created, never replaced: init refuses an existing file rather
  // than overwriting durable evidence a caller did not ask to lose.
  auto opened = args.store.empty()
                    ? AirflowControlEngine::open_in_memory(options)
                    : AirflowControlEngine::open(args.store, OpenMode::create_new, options);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  Emitter out(args.json);
  report_store_audit(out, engine.store_audit(), engine.is_durable());
  out.string("version", kVersionString);
  return finish_verb(engine, out, args.json);
}

int verb_epoch(const Args& args) {
  if (args.action != "adopt") {
    return usage_error("epoch adopt --epoch N --actor A --tick T");
  }
  const auto epoch = ordinal_option<AuthorityEpochTag>(args, "epoch");
  const auto actor = id_option<ActorId>(args, "actor");
  const auto tick = tick_option(args, "tick");
  std::string problem;
  if (failed(epoch, problem) || failed(actor, problem) || failed(tick, problem)) {
    return usage_error(problem);
  }
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  const Status status = engine.adopt_epoch(epoch.value(), actor.value(), tick.value());
  if (!status.ok()) {
    return refuse_after_close(engine, status, args.json);
  }
  Emitter out(args.json);
  out.uinteger("epoch", engine.current_epoch().value());
  out.string("actor", actor.value().str());
  out.uinteger("tick", engine.current_tick().value());
  out.uinteger("store-generation", engine.store_audit().generation.value());
  return finish_verb(engine, out, args.json);
}

int verb_tick(const Args& args) {
  if (args.action != "advance") {
    return usage_error("tick advance --tick T");
  }
  const auto tick = tick_option(args, "tick");
  std::string problem;
  if (failed(tick, problem)) {
    return usage_error(problem);
  }
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  const Status status = engine.advance_tick(tick.value());
  if (!status.ok()) {
    return refuse_after_close(engine, status, args.json);
  }
  Emitter out(args.json);
  out.uinteger("tick", engine.current_tick().value());
  out.uinteger("store-generation", engine.store_audit().generation.value());
  return finish_verb(engine, out, args.json);
}

int verb_device(const Args& args) {
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  Emitter out(args.json);

  if (args.action == "list") {
    const std::vector<DeviceView> devices = engine.devices();
    std::vector<Row> rows;
    rows.reserve(devices.size());
    for (const DeviceView& view : devices) {
      Row row;
      report_device(row, view);
      rows.push_back(std::move(row));
    }
    out.uinteger("count", static_cast<std::uint64_t>(rows.size()));
    out.rows("devices", rows);
    return finish_verb(engine, out, args.json);
  }
  if (args.action == "show") {
    const auto device = id_option<AirflowDeviceId>(args, "device");
    std::string problem;
    if (failed(device, problem)) {
      return usage_error(problem);
    }
    const auto view = engine.device(device.value());
    if (!view.ok()) {
      return refuse_after_close(engine, view.status(), args.json);
    }
    report_device(out, view.value());
    return finish_verb(engine, out, args.json);
  }
  if (args.action == "add") {
    const auto device = id_option<AirflowDeviceId>(args, "device");
    const auto generation = ordinal_option<DeviceGenerationTag>(args, "generation");
    const auto room = id_option<RoomId>(args, "room");
    const auto row = optional_id_option<RowId>(args, "row");
    const auto actor = id_option<ActorId>(args, "actor");
    const auto tick = tick_option(args, "tick");
    std::string problem;
    if (failed(device, problem) || failed(generation, problem) || failed(room, problem) ||
        failed(row, problem) || failed(actor, problem) || failed(tick, problem)) {
      return usage_error(problem);
    }
    const RegisterDeviceRequest request{.device = device.value(),
                                        .generation = generation.value(),
                                        .room = room.value(),
                                        .row = row.value(),
                                        .actor = actor.value(),
                                        .requested_at = tick.value()};
    const Status status = engine.register_device(request);
    if (!status.ok()) {
      return refuse_after_close(engine, status, args.json);
    }
    // Read back rather than assumed: the revision the engine chose is reported,
    // and no caller has to hard-code one.
    const auto view = engine.device(device.value());
    if (!view.ok()) {
      return refuse_after_close(engine, view.status(), args.json);
    }
    report_device(out, view.value());
    return finish_verb(engine, out, args.json);
  }
  return usage_error("device add|list|show");
}

int verb_lifecycle(const Args& args) {
  if (args.action != "set") {
    return usage_error(
        "lifecycle set --device D --generation G --revision R --target <state> "
        "--epoch E --actor A --tick T");
  }
  const auto device = id_option<AirflowDeviceId>(args, "device");
  const auto generation = ordinal_option<DeviceGenerationTag>(args, "generation");
  const auto revision = ordinal_option<StateRevisionTag>(args, "revision");
  const auto epoch = ordinal_option<AuthorityEpochTag>(args, "epoch");
  const auto actor = id_option<ActorId>(args, "actor");
  const auto tick = tick_option(args, "tick");
  const auto target = required_text(args, "target");
  std::string problem;
  if (failed(device, problem) || failed(generation, problem) || failed(revision, problem) ||
      failed(epoch, problem) || failed(actor, problem) || failed(tick, problem) ||
      failed(target, problem)) {
    return usage_error(problem);
  }
  const std::optional<DeviceLifecycle> lifecycle = parse_lifecycle(target.value());
  if (!lifecycle.has_value()) {
    return usage_error("unrecognized --target '" + target.value() + "'");
  }
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  const SetLifecycleRequest request{.device = device.value(),
                                    .generation = generation.value(),
                                    .expected_revision = revision.value(),
                                    .target = *lifecycle,
                                    .epoch = epoch.value(),
                                    .actor = actor.value(),
                                    .requested_at = tick.value()};
  const Status status = engine.set_device_lifecycle(request);
  if (!status.ok()) {
    return refuse_after_close(engine, status, args.json);
  }
  Emitter out(args.json);
  const auto view = engine.device(device.value());
  if (!view.ok()) {
    return refuse_after_close(engine, view.status(), args.json);
  }
  report_device(out, view.value());
  return finish_verb(engine, out, args.json);
}

int verb_policy(const Args& args) {
  if (args.action != "set") {
    return usage_error(
        "policy set --device D --generation G --revision R --policy P --policy-generation PG "
        "--min-percent B --max-percent B --default-percent B --min-airflow A --max-airflow A "
        "--max-step B --source S --evidence-generation EG --actor A --tick T");
  }
  const auto device = id_option<AirflowDeviceId>(args, "device");
  const auto generation = ordinal_option<DeviceGenerationTag>(args, "generation");
  const auto revision = ordinal_option<StateRevisionTag>(args, "revision");
  const auto policy = id_option<PolicyId>(args, "policy");
  const auto policy_generation = ordinal_option<PolicyGenerationTag>(args, "policy-generation");
  const auto min_percent = percent_option(args, "min-percent");
  const auto max_percent = percent_option(args, "max-percent");
  const auto default_percent = percent_option(args, "default-percent");
  const auto min_airflow = signed_option(args, "min-airflow");
  const auto max_airflow = signed_option(args, "max-airflow");
  const auto max_step = slew_option(args, "max-step");
  const auto source = id_option<SourceId>(args, "source");
  const auto evidence_generation = ordinal_option<EvidenceGenerationTag>(args, "evidence-generation");
  const auto actor = id_option<ActorId>(args, "actor");
  const auto tick = tick_option(args, "tick");
  std::string problem;
  if (failed(device, problem) || failed(generation, problem) || failed(revision, problem) ||
      failed(policy, problem) || failed(policy_generation, problem) ||
      failed(min_percent, problem) || failed(max_percent, problem) ||
      failed(default_percent, problem) || failed(min_airflow, problem) ||
      failed(max_airflow, problem) || failed(max_step, problem) || failed(source, problem) ||
      failed(evidence_generation, problem) || failed(actor, problem) || failed(tick, problem)) {
    return usage_error(problem);
  }
  const OperatingEnvelope envelope{.min_fan_percent = min_percent.value(),
                                   .max_fan_percent = max_percent.value(),
                                   .default_fan_percent = default_percent.value(),
                                   .min_airflow =
                                       Airflow::from_cubic_metres_per_hour(min_airflow.value()),
                                   .max_airflow =
                                       Airflow::from_cubic_metres_per_hour(max_airflow.value()),
                                   .max_step = max_step.value(),
                                   .source = source.value(),
                                   .evidence_generation = evidence_generation.value()};
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  const FanPolicy fan_policy{policy.value(), policy_generation.value(), envelope};
  const SetFanPolicyRequest request{.device = device.value(),
                                    .generation = generation.value(),
                                    .expected_revision = revision.value(),
                                    .policy = fan_policy,
                                    .actor = actor.value(),
                                    .requested_at = tick.value()};
  const Status status = engine.set_fan_policy(request);
  if (!status.ok()) {
    return refuse_after_close(engine, status, args.json);
  }
  Emitter out(args.json);
  const auto view = engine.device(device.value());
  if (!view.ok()) {
    return refuse_after_close(engine, view.status(), args.json);
  }
  report_device(out, view.value());
  return finish_verb(engine, out, args.json);
}

// ---------------------------------------------------------------------------
// Verbs: the model
// ---------------------------------------------------------------------------

int verb_relationship(const Args& args) {
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  Emitter out(args.json);

  if (args.action == "list") {
    const std::vector<RelationshipView> relationships = engine.relationships();
    std::vector<Row> rows;
    rows.reserve(relationships.size());
    for (const RelationshipView& view : relationships) {
      Row row;
      report_relationship(row, view);
      rows.push_back(std::move(row));
    }
    out.uinteger("count", static_cast<std::uint64_t>(rows.size()));
    out.rows("relationships", rows);
    return finish_verb(engine, out, args.json);
  }
  if (args.action == "show") {
    const auto relationship = id_option<PressureRelationshipId>(args, "relationship");
    std::string problem;
    if (failed(relationship, problem)) {
      return usage_error(problem);
    }
    const auto view = engine.relationship(relationship.value());
    if (!view.ok()) {
      return refuse_after_close(engine, view.status(), args.json);
    }
    report_relationship(out, view.value());
    return finish_verb(engine, out, args.json);
  }
  if (args.action == "define") {
    const auto relationship = id_option<PressureRelationshipId>(args, "relationship");
    const auto room = id_option<RoomId>(args, "room");
    const auto controlled = id_option<SpaceRefId>(args, "controlled");
    const auto reference = id_option<SpaceRefId>(args, "reference");
    const auto polarity = required_text(args, "polarity");
    const auto lower = signed_option(args, "lower");
    const auto upper = signed_option(args, "upper");
    const auto tolerance = signed_option(args, "tolerance");
    const auto evidence_generation =
        ordinal_option<EvidenceGenerationTag>(args, "evidence-generation");
    const auto revision = optional_ordinal_option<StateRevisionTag>(args, "revision");
    const auto actor = id_option<ActorId>(args, "actor");
    const auto tick = tick_option(args, "tick");
    std::string problem;
    if (failed(relationship, problem) || failed(room, problem) || failed(controlled, problem) ||
        failed(reference, problem) || failed(polarity, problem) || failed(lower, problem) ||
        failed(upper, problem) || failed(tolerance, problem) ||
        failed(evidence_generation, problem) || failed(revision, problem) ||
        failed(actor, problem) || failed(tick, problem)) {
      return usage_error(problem);
    }
    const std::optional<PressurePolarity> declared = parse_pressure_polarity(polarity.value());
    if (!declared.has_value()) {
      return usage_error("unrecognized --polarity '" + polarity.value() + "'");
    }
    const DefineRelationshipRequest request{
        .id = relationship.value(),
        .room = room.value(),
        .controlled_space = controlled.value(),
        .reference_space = reference.value(),
        .polarity = *declared,
        .lower = Pressure::from_millipascals(lower.value()),
        .upper = Pressure::from_millipascals(upper.value()),
        .tolerance = Pressure::from_millipascals(tolerance.value()),
        .evidence_generation = evidence_generation.value(),
        .expected_revision = revision.value(),
        .actor = actor.value(),
        .requested_at = tick.value()};
    const Status status = engine.define_pressure_relationship(request);
    if (!status.ok()) {
      return refuse_after_close(engine, status, args.json);
    }
    const auto view = engine.relationship(relationship.value());
    if (!view.ok()) {
      return refuse_after_close(engine, view.status(), args.json);
    }
    report_relationship(out, view.value());
    return finish_verb(engine, out, args.json);
  }
  return usage_error("relationship define|list|show");
}

std::optional<ContainmentView> find_containment_view(const AirflowControlEngine& engine,
                                                     const ContainmentId& id) {
  for (const ContainmentView& view : engine.containment()) {
    if (view.id == id) {
      return view;
    }
  }
  return std::nullopt;
}

int verb_containment(const Args& args) {
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  Emitter out(args.json);

  if (args.action == "list") {
    const std::vector<ContainmentView> elements = engine.containment();
    std::vector<Row> rows;
    rows.reserve(elements.size());
    for (const ContainmentView& view : elements) {
      Row row;
      report_containment(row, view);
      rows.push_back(std::move(row));
    }
    out.uinteger("count", static_cast<std::uint64_t>(rows.size()));
    out.rows("containment", rows);
    return finish_verb(engine, out, args.json);
  }
  if (args.action == "define") {
    const auto containment = id_option<ContainmentId>(args, "containment");
    const auto room = id_option<RoomId>(args, "room");
    const auto row = optional_id_option<RowId>(args, "row");
    const auto kind = required_text(args, "kind");
    const auto revision = optional_ordinal_option<StateRevisionTag>(args, "revision");
    const auto actor = id_option<ActorId>(args, "actor");
    const auto tick = tick_option(args, "tick");
    std::string problem;
    if (failed(containment, problem) || failed(room, problem) || failed(row, problem) ||
        failed(kind, problem) || failed(revision, problem) || failed(actor, problem) ||
        failed(tick, problem)) {
      return usage_error(problem);
    }
    const std::optional<ContainmentKind> declared = parse_containment_kind(kind.value());
    if (!declared.has_value()) {
      return usage_error("unrecognized --kind '" + kind.value() + "'");
    }
    const DefineContainmentRequest request{.id = containment.value(),
                                           .room = room.value(),
                                           .row = row.value(),
                                           .kind = *declared,
                                           .expected_revision = revision.value(),
                                           .actor = actor.value(),
                                           .requested_at = tick.value()};
    const Status status = engine.define_containment_element(request);
    if (!status.ok()) {
      return refuse_after_close(engine, status, args.json);
    }
    const std::optional<ContainmentView> view = find_containment_view(engine, containment.value());
    if (!view.has_value()) {
      return refuse_after_close(
          engine,
          Status::failure(StatusCode::internal_error,
                          "containment element " + containment.value().str() +
                              " was not retained"),
          args.json);
    }
    report_containment(out, *view);
    return finish_verb(engine, out, args.json);
  }
  if (args.action == "report") {
    const auto containment = id_option<ContainmentId>(args, "containment");
    const auto state = required_text(args, "state");
    const auto quality = required_text(args, "quality");
    const auto source = id_option<SourceId>(args, "source");
    const auto sequence = ordinal_option<EvidenceSequenceTag>(args, "sequence");
    const auto evidence_generation =
        ordinal_option<EvidenceGenerationTag>(args, "evidence-generation");
    const auto tick = tick_option(args, "tick");
    std::string problem;
    if (failed(containment, problem) || failed(state, problem) || failed(quality, problem) ||
        failed(source, problem) || failed(sequence, problem) ||
        failed(evidence_generation, problem) || failed(tick, problem)) {
      return usage_error(problem);
    }
    const std::optional<ContainmentState> declared = parse_containment_state(state.value());
    if (!declared.has_value()) {
      return usage_error("unrecognized --state '" + state.value() + "'");
    }
    std::optional<Quality> declared_quality = parse_quality(quality.value());
    if (!declared_quality.has_value()) {
      return usage_error("unrecognized --quality '" + quality.value() + "'");
    }
    const ReportContainmentRequest request{.element = containment.value(),
                                           .state = *declared,
                                           .quality = *declared_quality,
                                           .source = source.value(),
                                           .sequence = sequence.value(),
                                           .evidence_generation = evidence_generation.value(),
                                           .measured_at = tick.value()};
    const Status status = engine.report_containment(request);
    if (!status.ok()) {
      return refuse_after_close(engine, status, args.json);
    }
    // The element is read back, not echoed: a report whose quality is not good
    // reduces the element to unknown, and only the engine decides that.
    const std::optional<ContainmentView> view = find_containment_view(engine, containment.value());
    if (!view.has_value()) {
      return refuse_after_close(
          engine,
          Status::failure(StatusCode::internal_error,
                          "containment element " + containment.value().str() +
                              " was not retained"),
          args.json);
    }
    report_containment(out, *view);
    return finish_verb(engine, out, args.json);
  }
  return usage_error("containment define|report|list");
}

int verb_obligation(const Args& args) {
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  Emitter out(args.json);

  if (args.action == "list") {
    const std::vector<ObligationView> obligations = engine.obligations();
    std::vector<Row> rows;
    rows.reserve(obligations.size());
    for (const ObligationView& view : obligations) {
      Row row;
      report_obligation(row, view);
      rows.push_back(std::move(row));
    }
    out.uinteger("count", static_cast<std::uint64_t>(rows.size()));
    out.rows("obligations", rows);
    return finish_verb(engine, out, args.json);
  }
  if (args.action == "declare") {
    const auto obligation = id_option<ObligationId>(args, "obligation");
    const auto scope = required_text(args, "scope");
    const auto room = id_option<RoomId>(args, "room");
    const auto row = optional_id_option<RowId>(args, "row");
    const auto rack = optional_id_option<RackId>(args, "rack");
    const auto klass = required_text(args, "class");
    const auto binding = required_text(args, "binding");
    const auto point = optional_id_option<SpaceRefId>(args, "point");
    const auto devices = required_text(args, "devices");
    const auto minimum = signed_option(args, "minimum");
    const auto target = signed_option(args, "target");
    const auto source = id_option<SourceId>(args, "source");
    const auto evidence_generation =
        ordinal_option<EvidenceGenerationTag>(args, "evidence-generation");
    const auto revision = optional_ordinal_option<StateRevisionTag>(args, "revision");
    const auto actor = id_option<ActorId>(args, "actor");
    const auto tick = tick_option(args, "tick");
    std::string problem;
    if (failed(obligation, problem) || failed(scope, problem) || failed(room, problem) ||
        failed(row, problem) || failed(rack, problem) || failed(klass, problem) ||
        failed(binding, problem) || failed(point, problem) || failed(devices, problem) ||
        failed(minimum, problem) || failed(target, problem) || failed(source, problem) ||
        failed(evidence_generation, problem) || failed(revision, problem) ||
        failed(actor, problem) || failed(tick, problem)) {
      return usage_error(problem);
    }
    const std::optional<ObligationScope> declared_scope = parse_obligation_scope(scope.value());
    if (!declared_scope.has_value()) {
      return usage_error("unrecognized --scope '" + scope.value() + "'");
    }
    const std::optional<ObligationClass> declared_class = parse_obligation_class(klass.value());
    if (!declared_class.has_value()) {
      return usage_error("unrecognized --class '" + klass.value() + "'");
    }
    const std::optional<ObligationBinding> declared_binding =
        parse_obligation_binding(binding.value());
    if (!declared_binding.has_value()) {
      return usage_error("unrecognized --binding '" + binding.value() + "'");
    }
    std::vector<AirflowDeviceId> bound;
    const Result<std::vector<std::string>> names = split_list(devices.value());
    if (!names.ok()) {
      return usage_error("--devices: " + names.message());
    }
    for (const std::string& name : names.value()) {
      Result<AirflowDeviceId> parsed = AirflowDeviceId::parse(name);
      if (!parsed.ok()) {
        return usage_error("--devices: " + parsed.message());
      }
      bound.push_back(parsed.value());
    }
    const DeclareObligationRequest request{
        .id = obligation.value(),
        .scope = *declared_scope,
        .room = room.value(),
        .row = row.value(),
        .rack = rack.value(),
        .klass = *declared_class,
        .binding = *declared_binding,
        .metered_point = point.value(),
        .devices = bound,
        .minimum_airflow = Airflow::from_cubic_metres_per_hour(minimum.value()),
        .target_airflow = Airflow::from_cubic_metres_per_hour(target.value()),
        .source = source.value(),
        .evidence_generation = evidence_generation.value(),
        .expected_revision = revision.value(),
        .actor = actor.value(),
        .requested_at = tick.value()};
    const Status status = engine.declare_obligation(request);
    if (!status.ok()) {
      return refuse_after_close(engine, status, args.json);
    }
    for (const ObligationView& view : engine.obligations()) {
      if (view.id == obligation.value()) {
        report_obligation(out, view);
        return finish_verb(engine, out, args.json);
      }
    }
    return refuse_after_close(
        engine,
        Status::failure(StatusCode::internal_error,
                        "obligation " + obligation.value().str() + " was not retained"),
        args.json);
  }
  return usage_error("obligation declare|list");
}

int verb_interlock(const Args& args) {
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  Emitter out(args.json);

  if (args.action == "list") {
    const std::vector<InterlockView> interlocks = engine.interlocks();
    std::vector<Row> rows;
    rows.reserve(interlocks.size());
    for (const InterlockView& view : interlocks) {
      Row row;
      report_interlock(row, view);
      rows.push_back(std::move(row));
    }
    out.uinteger("count", static_cast<std::uint64_t>(rows.size()));
    out.rows("interlocks", rows);
    return finish_verb(engine, out, args.json);
  }
  if (args.action == "declare") {
    const auto interlock = id_option<InterlockId>(args, "interlock");
    const auto room = id_option<RoomId>(args, "room");
    const auto row = optional_id_option<RowId>(args, "row");
    const auto device = optional_id_option<AirflowDeviceId>(args, "device");
    const auto epoch = ordinal_option<AuthorityEpochTag>(args, "epoch");
    const auto actor = id_option<ActorId>(args, "actor");
    const auto tick = tick_option(args, "tick");
    std::string problem;
    if (failed(interlock, problem) || failed(room, problem) || failed(row, problem) ||
        failed(device, problem) || failed(epoch, problem) || failed(actor, problem) ||
        failed(tick, problem)) {
      return usage_error(problem);
    }
    InterlockClass klass = InterlockClass::protected_obligation;
    if (args.has("class")) {
      const auto text = required_text(args, "class");
      if (failed(text, problem)) {
        return usage_error(problem);
      }
      const std::optional<InterlockClass> declared = parse_interlock_class(text.value());
      if (!declared.has_value()) {
        return usage_error("unrecognized --class '" + text.value() + "'");
      }
      klass = *declared;
    }
    const DeclareInterlockRequest request{.id = interlock.value(),
                                          .room = room.value(),
                                          .row = row.value(),
                                          .device = device.value(),
                                          .klass = klass,
                                          .epoch = epoch.value(),
                                          .actor = actor.value(),
                                          .declared_at = tick.value()};
    const Status status = engine.declare_interlock(request);
    if (!status.ok()) {
      return refuse_after_close(engine, status, args.json);
    }
    for (const InterlockView& view : engine.interlocks()) {
      if (view.id == interlock.value()) {
        report_interlock(out, view);
        return finish_verb(engine, out, args.json);
      }
    }
    return refuse_after_close(
        engine,
        Status::failure(StatusCode::internal_error,
                        "interlock " + interlock.value().str() + " was not retained"),
        args.json);
  }
  if (args.action == "report") {
    const auto interlock = id_option<InterlockId>(args, "interlock");
    const auto state = required_text(args, "state");
    const auto sequence = ordinal_option<EvidenceSequenceTag>(args, "sequence");
    const auto epoch = ordinal_option<AuthorityEpochTag>(args, "epoch");
    const auto tick = tick_option(args, "tick");
    std::string problem;
    if (failed(interlock, problem) || failed(state, problem) || failed(sequence, problem) ||
        failed(epoch, problem) || failed(tick, problem)) {
      return usage_error(problem);
    }
    const std::optional<InterlockState> declared = parse_interlock_state(state.value());
    if (!declared.has_value()) {
      return usage_error("unrecognized --state '" + state.value() + "'");
    }
    const ReportInterlockRequest request{.id = interlock.value(),
                                         .state = *declared,
                                         .sequence = sequence.value(),
                                         .epoch = epoch.value(),
                                         .reported_at = tick.value()};
    const Status status = engine.report_interlock(request);
    if (!status.ok()) {
      return refuse_after_close(engine, status, args.json);
    }
    for (const InterlockView& view : engine.interlocks()) {
      if (view.id == interlock.value()) {
        report_interlock(out, view);
        return finish_verb(engine, out, args.json);
      }
    }
    return refuse_after_close(
        engine,
        Status::failure(StatusCode::internal_error,
                        "interlock " + interlock.value().str() + " was not retained"),
        args.json);
  }
  return usage_error("interlock declare|report|list");
}

// ---------------------------------------------------------------------------
// Verbs: authority
// ---------------------------------------------------------------------------

Result<ActionSet> actions_option(const Args& args) {
  const auto text = required_text(args, "actions");
  if (!text.ok()) {
    return text.status();
  }
  const Result<std::vector<std::string>> names = split_list(text.value());
  if (!names.ok()) {
    return names.status();
  }
  std::uint32_t mask = 0;
  for (const std::string& name : names.value()) {
    const std::optional<ControlAction> action = parse_control_action(name);
    if (!action.has_value()) {
      return Status::failure(StatusCode::invalid_argument,
                             "unrecognized action '" + name + "'");
    }
    mask |= 1u << static_cast<std::uint32_t>(*action);
  }
  return ActionSet::from_mask(mask);
}

int verb_grant(const Args& args) {
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  Emitter out(args.json);

  if (args.action == "list") {
    const std::vector<PermissionGrant> grants = engine.grants();
    std::vector<Row> rows;
    rows.reserve(grants.size());
    for (const PermissionGrant& grant : grants) {
      Row row;
      report_grant(row, grant);
      rows.push_back(std::move(row));
    }
    out.uinteger("count", static_cast<std::uint64_t>(rows.size()));
    out.rows("grants", rows);
    return finish_verb(engine, out, args.json);
  }
  if (args.action == "add") {
    const auto grant = id_option<GrantId>(args, "grant");
    const auto issuer = id_option<SourceId>(args, "issuer");
    const auto epoch = ordinal_option<AuthorityEpochTag>(args, "epoch");
    const auto device = optional_id_option<AirflowDeviceId>(args, "device");
    const auto device_generation = optional_ordinal_option<DeviceGenerationTag>(args, "device-generation");
    const auto room = optional_id_option<RoomId>(args, "room");
    const auto issued = tick_option(args, "issued");
    const auto expires = optional_tick_option(args, "expires");
    const auto actions = actions_option(args);
    std::string problem;
    if (failed(grant, problem) || failed(issuer, problem) || failed(epoch, problem) ||
        failed(device, problem) || failed(device_generation, problem) || failed(room, problem) ||
        failed(issued, problem) || failed(expires, problem) || failed(actions, problem)) {
      return usage_error(problem);
    }
    const PermissionGrant value{.id = grant.value(),
                                .issuer = issuer.value(),
                                .epoch = epoch.value(),
                                .device = device.value(),
                                .device_generation = device_generation.value(),
                                .room = room.value(),
                                .actions = actions.value(),
                                .issued_at = issued.value(),
                                .expires_at = expires.value(),
                                .revoked = false};
    // The engine records the actor an audit entry names and the instant the
    // grant was recorded. The only actor the command line supplies here is the
    // authority that issued the grant, and the only instant it supplies is the
    // issuance instant, so those are what is passed: the same text as an actor
    // identity, which is the same identifier alphabet and cannot fail to parse.
    const Status status = engine.add_grant(value, ActorId::parse(issuer.value().str()).value(),
                                           issued.value());
    if (!status.ok()) {
      return refuse_after_close(engine, status, args.json);
    }
    for (const PermissionGrant& stored : engine.grants()) {
      if (stored.id == grant.value()) {
        report_grant(out, stored);
        return finish_verb(engine, out, args.json);
      }
    }
    return refuse_after_close(
        engine,
        Status::failure(StatusCode::internal_error,
                        "grant " + grant.value().str() + " was not retained"),
        args.json);
  }
  if (args.action == "revoke") {
    const auto grant = id_option<GrantId>(args, "grant");
    const auto epoch = ordinal_option<AuthorityEpochTag>(args, "epoch");
    const auto actor = id_option<ActorId>(args, "actor");
    const auto tick = tick_option(args, "tick");
    std::string problem;
    if (failed(grant, problem) || failed(epoch, problem) || failed(actor, problem) ||
        failed(tick, problem)) {
      return usage_error(problem);
    }
    const Status status = engine.revoke_grant(grant.value(), epoch.value(), actor.value(),
                                              tick.value());
    if (!status.ok()) {
      return refuse_after_close(engine, status, args.json);
    }
    for (const PermissionGrant& stored : engine.grants()) {
      if (stored.id == grant.value()) {
        report_grant(out, stored);
        return finish_verb(engine, out, args.json);
      }
    }
    return refuse_after_close(
        engine,
        Status::failure(StatusCode::internal_error,
                        "grant " + grant.value().str() + " was not retained"),
        args.json);
  }
  return usage_error("grant add|revoke|list");
}

int verb_override(const Args& args) {
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  Emitter out(args.json);

  if (args.action == "list") {
    const std::vector<MaintenanceOverride> overrides = engine.overrides();
    std::vector<Row> rows;
    rows.reserve(overrides.size());
    for (const MaintenanceOverride& entry : overrides) {
      Row row;
      report_override(row, entry);
      rows.push_back(std::move(row));
    }
    out.uinteger("count", static_cast<std::uint64_t>(rows.size()));
    out.rows("overrides", rows);
    return finish_verb(engine, out, args.json);
  }
  if (args.action == "add") {
    const auto override_id = id_option<OverrideId>(args, "override");
    const auto device = id_option<AirflowDeviceId>(args, "device");
    const auto device_generation = ordinal_option<DeviceGenerationTag>(args, "device-generation");
    const auto epoch = ordinal_option<AuthorityEpochTag>(args, "epoch");
    const auto issued = tick_option(args, "issued");
    const auto expires = tick_option(args, "expires");
    const auto reason = required_text(args, "reason");
    const auto actor = id_option<ActorId>(args, "actor");
    const auto tick = tick_option(args, "tick");
    std::string problem;
    if (failed(override_id, problem) || failed(device, problem) ||
        failed(device_generation, problem) || failed(epoch, problem) || failed(issued, problem) ||
        failed(expires, problem) || failed(reason, problem) || failed(actor, problem) ||
        failed(tick, problem)) {
      return usage_error(problem);
    }
    // The reason is durable text, so the runtime's own text validation decides
    // whether it can be stored; a reason it will not store is a command line
    // that cannot be turned into a request.
    const Result<std::string> text = validate_text(reason.value(), kMaxTextLength);
    if (!text.ok()) {
      return usage_error(text.message());
    }
    const MaintenanceOverride entry{.id = override_id.value(),
                                    .device = device.value(),
                                    .device_generation = device_generation.value(),
                                    .epoch = epoch.value(),
                                    .issued_at = issued.value(),
                                    .expires_at = expires.value(),
                                    .reason = text.value(),
                                    .revoked = false};
    const Status status = engine.add_maintenance_override(entry, actor.value(), tick.value());
    if (!status.ok()) {
      return refuse_after_close(engine, status, args.json);
    }
    for (const MaintenanceOverride& stored : engine.overrides()) {
      if (stored.id == override_id.value()) {
        report_override(out, stored);
        return finish_verb(engine, out, args.json);
      }
    }
    return refuse_after_close(
        engine,
        Status::failure(StatusCode::internal_error,
                        "override " + override_id.value().str() + " was not retained"),
        args.json);
  }
  if (args.action == "revoke") {
    const auto override_id = id_option<OverrideId>(args, "override");
    const auto actor = id_option<ActorId>(args, "actor");
    const auto tick = tick_option(args, "tick");
    std::string problem;
    if (failed(override_id, problem) || failed(actor, problem) || failed(tick, problem)) {
      return usage_error(problem);
    }
    const Status status =
        engine.revoke_maintenance_override(override_id.value(), actor.value(), tick.value());
    if (!status.ok()) {
      return refuse_after_close(engine, status, args.json);
    }
    for (const MaintenanceOverride& stored : engine.overrides()) {
      if (stored.id == override_id.value()) {
        report_override(out, stored);
        return finish_verb(engine, out, args.json);
      }
    }
    return refuse_after_close(
        engine,
        Status::failure(StatusCode::internal_error,
                        "override " + override_id.value().str() + " was not retained"),
        args.json);
  }
  return usage_error("override add|revoke|list");
}

int verb_permit(const Args& args) {
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  Emitter out(args.json);

  if (args.action == "list") {
    const std::vector<SafetyPermit> permits = engine.safety_permits();
    std::vector<Row> rows;
    rows.reserve(permits.size());
    for (const SafetyPermit& permit : permits) {
      Row row;
      report_permit(row, permit);
      rows.push_back(std::move(row));
    }
    out.uinteger("count", static_cast<std::uint64_t>(rows.size()));
    out.rows("permits", rows);
    return finish_verb(engine, out, args.json);
  }
  if (args.action == "add") {
    const auto permit = id_option<SafetyPermitId>(args, "permit");
    const auto issuer = id_option<SourceId>(args, "issuer");
    const auto epoch = ordinal_option<AuthorityEpochTag>(args, "epoch");
    const auto device = id_option<AirflowDeviceId>(args, "device");
    const auto issued = tick_option(args, "issued");
    const auto expires = optional_tick_option(args, "expires");
    const auto reason = required_text(args, "reason");
    const auto actor = id_option<ActorId>(args, "actor");
    const auto tick = tick_option(args, "tick");
    std::string problem;
    if (failed(permit, problem) || failed(issuer, problem) || failed(epoch, problem) ||
        failed(device, problem) || failed(issued, problem) || failed(expires, problem) ||
        failed(reason, problem) || failed(actor, problem) || failed(tick, problem)) {
      return usage_error(problem);
    }
    const Result<std::string> text = validate_text(reason.value(), kMaxTextLength);
    if (!text.ok()) {
      return usage_error(text.message());
    }
    const SafetyPermit value{.id = permit.value(),
                             .issuer = issuer.value(),
                             .epoch = epoch.value(),
                             .device = device.value(),
                             .issued_at = issued.value(),
                             .expires_at = expires.value(),
                             .reason = text.value()};
    const Status status = engine.add_safety_permit(value, actor.value(), tick.value());
    if (!status.ok()) {
      return refuse_after_close(engine, status, args.json);
    }
    for (const SafetyPermit& stored : engine.safety_permits()) {
      if (stored.id == permit.value()) {
        report_permit(out, stored);
        return finish_verb(engine, out, args.json);
      }
    }
    return refuse_after_close(
        engine,
        Status::failure(StatusCode::internal_error,
                        "safety permit " + permit.value().str() + " was not retained"),
        args.json);
  }
  return usage_error("permit add|list");
}

// ---------------------------------------------------------------------------
// Verbs: evidence
// ---------------------------------------------------------------------------

int verb_observe(const Args& args) {
  const auto device = id_option<AirflowDeviceId>(args, "device");
  const auto generation = ordinal_option<DeviceGenerationTag>(args, "device-generation");
  const auto kind = required_text(args, "kind");
  const auto relationship = optional_id_option<PressureRelationshipId>(args, "relationship");
  const auto point = id_option<SpaceRefId>(args, "point");
  const auto source = id_option<SourceId>(args, "source");
  const auto sequence = ordinal_option<EvidenceSequenceTag>(args, "sequence");
  const auto measured_at = tick_option(args, "measured-at");
  const auto evidence_generation = ordinal_option<EvidenceGenerationTag>(args, "evidence-generation");
  const auto tick = tick_option(args, "tick");
  std::string problem;
  if (failed(device, problem) || failed(generation, problem) || failed(kind, problem) ||
      failed(relationship, problem) || failed(point, problem) || failed(source, problem) ||
      failed(sequence, problem) || failed(measured_at, problem) ||
      failed(evidence_generation, problem) || failed(tick, problem)) {
    return usage_error(problem);
  }
  std::optional<ObservationKind> declared = parse_observation_kind(kind.value());
  if (!declared.has_value() && kind.value() == "fan") {
    // "fan" is the spelling this tool's usage block uses for the fan setpoint a
    // device reports; the library calls the same kind fan_setpoint.
    declared = ObservationKind::fan_setpoint;
  }
  if (!declared.has_value()) {
    return usage_error("unrecognized --kind '" + kind.value() + "'");
  }
  Quality quality = Quality::good;
  if (args.has("quality")) {
    const auto text = required_text(args, "quality");
    if (failed(text, problem)) {
      return usage_error(problem);
    }
    const std::optional<Quality> stated = parse_quality(text.value());
    if (!stated.has_value()) {
      return usage_error("unrecognized --quality '" + text.value() + "'");
    }
    quality = *stated;
  }

  ObservationPayload payload = FanReading{SetpointBasisPoints::create(0).value()};
  if (*declared == ObservationKind::fan_setpoint) {
    const auto value = percent_option(args, "value");
    if (failed(value, problem)) {
      return usage_error(problem);
    }
    payload = FanReading{value.value()};
  } else {
    const auto value = signed_option(args, "value");
    if (failed(value, problem)) {
      return usage_error(problem);
    }
    if (*declared == ObservationKind::airflow) {
      payload = AirflowReading{Airflow::from_cubic_metres_per_hour(value.value())};
    } else {
      payload = PressureReading{Pressure::from_millipascals(value.value())};
    }
  }
  if (*declared == ObservationKind::pressure && !relationship.value().has_value()) {
    // Stated here rather than left to the engine, because the tool cannot build
    // the draft without deciding what the reading is about.
    return usage_error("--kind pressure requires --relationship");
  }

  const ObservationDraft draft{.payload = payload,
                                .device = device.value(),
                                .relationship = relationship.value(),
                                .point = point.value(),
                                .source = source.value(),
                                .sequence = sequence.value(),
                                .measured_at = measured_at.value(),
                                .device_generation = generation.value(),
                                .evidence_generation = evidence_generation.value(),
                                .quality = quality};
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  const Result<Observation> accepted = engine.observe(draft, tick.value());
  if (!accepted.ok()) {
    return refuse_after_close(engine, accepted.status(), args.json);
  }
  Emitter out(args.json);
  const Observation& observation = accepted.value();
  out.uinteger("observation", observation.id.value());
  out.string("device", observation.draft.device.str());
  out.uinteger("device-generation", observation.draft.device_generation.value());
  out.string("kind", to_string(observation_kind(observation.draft.payload)));
  if (observation.draft.relationship.has_value()) {
    out.string("relationship", observation.draft.relationship->str());
  }
  out.string("point", observation.draft.point.str());
  out.string("source", observation.draft.source.str());
  out.uinteger("sequence", observation.draft.sequence.value());
  out.uinteger("measured-at", observation.draft.measured_at.value());
  out.uinteger("accepted-at", observation.accepted_at.value());
  out.uinteger("evidence-generation", observation.draft.evidence_generation.value());
  out.string("quality", to_string(observation.draft.quality));
  out.boolean("recovered", observation.recovered);
  if (const auto* fan = std::get_if<FanReading>(&observation.draft.payload)) {
    out.quantity("value-basis-points", static_cast<std::int64_t>(fan->percent.basis_points()),
                 fan->percent.to_string());
    out.string("unit", "basis_points");
  } else if (const auto* airflow = std::get_if<AirflowReading>(&observation.draft.payload)) {
    out.quantity("value-cubic-metres-per-hour", airflow->value.cubic_metres_per_hour(),
                 airflow->value.to_string());
    out.string("unit", "cubic_metres_per_hour");
  } else {
    const auto* pressure = std::get_if<PressureReading>(&observation.draft.payload);
    out.quantity("value-millipascals", pressure->differential.millipascals(),
                 pressure->differential.to_string());
    out.string("unit", "millipascals");
  }
  return finish_verb(engine, out, args.json);
}

// ---------------------------------------------------------------------------
// Verbs: attempts, inspection, and the durable store
// ---------------------------------------------------------------------------

int verb_resolve(const Args& args) {
  const auto attempt = ordinal_option<AttemptIdTag>(args, "attempt");
  const auto target = required_text(args, "target");
  const auto actor = id_option<ActorId>(args, "actor");
  const auto tick = tick_option(args, "tick");
  const auto reason = required_text(args, "reason");
  std::string problem;
  if (failed(attempt, problem) || failed(target, problem) || failed(actor, problem) ||
      failed(tick, problem) || failed(reason, problem)) {
    return usage_error(problem);
  }
  AttemptState state = AttemptState::resolved_without_effect;
  if (target.value() == "resolved_without_effect") {
    state = AttemptState::resolved_without_effect;
  } else if (target.value() == "superseded") {
    state = AttemptState::superseded;
  } else {
    return usage_error("--target must be resolved_without_effect or superseded");
  }
  // The reason is durable text, so the runtime's own text validation decides
  // whether it can be stored; a reason it will not store is a command line that
  // cannot be turned into a request, not an engine refusal.
  const Result<std::string> text = validate_text(reason.value(), kMaxTextLength);
  if (!text.ok()) {
    return usage_error(text.message());
  }
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  const ResolveAttemptRequest request{.attempt = attempt.value(),
                                      .target = state,
                                      .actor = actor.value(),
                                      .at = tick.value(),
                                      .reason = text.value()};
  const Status status = engine.resolve_attempt(request);
  if (!status.ok()) {
    return refuse_after_close(engine, status, args.json);
  }
  Emitter out(args.json);
  const auto record = engine.attempt(attempt.value());
  if (!record.ok()) {
    return refuse_after_close(engine, record.status(), args.json);
  }
  report_attempt(out, record.value(), false);
  return finish_verb(engine, out, args.json);
}

int verb_attempts(const Args& args) {
  if (args.verb == "attempt" && args.action != "show") {
    return usage_error("attempt show --attempt N");
  }
  std::optional<AttemptId> single;
  if (args.verb == "attempt") {
    const auto attempt = ordinal_option<AttemptIdTag>(args, "attempt");
    std::string problem;
    if (failed(attempt, problem)) {
      return usage_error(problem);
    }
    single = attempt.value();
  }
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  Emitter out(args.json);
  if (single.has_value()) {
    const AttemptId attempt = *single;
    const auto record = engine.attempt(attempt);
    if (!record.ok()) {
      return refuse_after_close(engine, record.status(), args.json);
    }
    bool superseded_by_known_attempt = false;
    for (const AttemptView& view : engine.attempts()) {
      if (view.record.id == attempt) {
        superseded_by_known_attempt = view.superseded_by_known_attempt;
      }
    }
    report_attempt(out, record.value(), superseded_by_known_attempt);
    return finish_verb(engine, out, args.json);
  }
  const std::vector<AttemptView> attempts = engine.attempts();
  std::vector<Row> rows;
  rows.reserve(attempts.size());
  for (const AttemptView& view : attempts) {
    Row row;
    report_attempt(row, view.record, view.superseded_by_known_attempt);
    rows.push_back(std::move(row));
  }
  out.uinteger("count", static_cast<std::uint64_t>(rows.size()));
  out.rows("attempts", rows);
  return finish_verb(engine, out, args.json);
}

int verb_state(const Args& args) {
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  const std::string state = engine.canonical_state();
  const std::string digest = engine.state_digest();
  Emitter out(args.json);
  if (args.json) {
    out.string("state", state);
    out.uinteger("bytes", static_cast<std::uint64_t>(state.size()));
  } else {
    // The canonical model is text a human reads as it stands; wrapping it in a
    // "state=" line would put a key on a paragraph.
    out.note(state);
  }
  out.string("digest", digest);
  return finish_verb(engine, out, args.json);
}

int verb_digest(const Args& args) {
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  Emitter out(args.json);
  out.string("digest", engine.state_digest());
  return finish_verb(engine, out, args.json);
}

int verb_history(const Args& args) {
  std::uint64_t limit = kDefaultHistoryLimit;
  if (args.has("limit")) {
    const auto parsed = unsigned_option(args, "limit");
    if (!parsed.ok()) {
      return usage_error(parsed.message());
    }
    if (parsed.value() > kMaxListLimit) {
      return usage_error("--limit must not exceed " + std::to_string(kMaxListLimit));
    }
    limit = parsed.value();
  }
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  const HistoryView history = engine.history(static_cast<std::size_t>(limit));
  Emitter out(args.json);
  std::vector<Row> rows;
  rows.reserve(history.entries.size());
  for (const AuditEntry& entry : history.entries) {
    Row row;
    row.uinteger("sequence", entry.sequence.value());
    row.uinteger("tick", entry.tick.value());
    row.string("kind", to_string(entry.kind));
    if (!entry.subject.empty()) {
      row.string("subject", entry.subject);
    }
    if (!entry.detail.empty()) {
      row.string("detail", entry.detail);
    }
    rows.push_back(std::move(row));
  }
  out.uinteger("count", static_cast<std::uint64_t>(rows.size()));
  out.uinteger("dropped", history.dropped);
  out.rows("history", rows);
  return finish_verb(engine, out, args.json);
}

int verb_store_audit(const Args& args) {
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  Emitter out(args.json);
  report_store_audit(out, engine.store_audit(), engine.is_durable());
  out.uinteger("devices", static_cast<std::uint64_t>(engine.devices().size()));
  out.uinteger("relationships", static_cast<std::uint64_t>(engine.relationships().size()));
  out.uinteger("containment", static_cast<std::uint64_t>(engine.containment().size()));
  out.uinteger("obligations", static_cast<std::uint64_t>(engine.obligations().size()));
  out.uinteger("interlocks", static_cast<std::uint64_t>(engine.interlocks().size()));
  out.uinteger("grants", static_cast<std::uint64_t>(engine.grants().size()));
  out.uinteger("overrides", static_cast<std::uint64_t>(engine.overrides().size()));
  out.uinteger("permits", static_cast<std::uint64_t>(engine.safety_permits().size()));
  out.uinteger("attempts", static_cast<std::uint64_t>(engine.attempts().size()));
  out.uinteger("audit-dropped", engine.dropped_audit_entries());
  out.uinteger("authority-epoch", engine.current_epoch().value());
  out.uinteger("tick", engine.current_tick().value());
  out.string("version", kVersionString);
  return finish_verb(engine, out, args.json);
}

// ---------------------------------------------------------------------------
// Verbs: control
// ---------------------------------------------------------------------------

/// Builds the one control request evaluate and issue share.
///
/// Every failure here is a command line that cannot be turned into a request,
/// which is a usage error rather than a refusal: there is no engine decision to
/// report, and the caller has to ask again with a well-formed command.
Result<ControlRequest> control_request(const Args& args) {
  const auto device = id_option<AirflowDeviceId>(args, "device");
  const auto generation = ordinal_option<DeviceGenerationTag>(args, "device-generation");
  const auto epoch = ordinal_option<AuthorityEpochTag>(args, "epoch");
  const auto revision = optional_ordinal_option<StateRevisionTag>(args, "revision");
  const auto intent = intent_option(args, "intent");
  const auto key = id_option<IdempotencyKey>(args, "key");
  const auto actor = id_option<ActorId>(args, "actor");
  const auto tick = tick_option(args, "tick");
  const auto permit = optional_id_option<SafetyPermitId>(args, "safety-permit");
  const auto supersede = optional_ordinal_option<AttemptIdTag>(args, "supersede");
  const auto relationship = optional_id_option<PressureRelationshipId>(args, "relationship");
  std::string problem;
  if (failed(device, problem) || failed(generation, problem) || failed(epoch, problem) ||
      failed(revision, problem) || failed(intent, problem) || failed(key, problem) ||
      failed(actor, problem) || failed(tick, problem) || failed(permit, problem) ||
      failed(supersede, problem) || failed(relationship, problem)) {
    return Status::failure(StatusCode::invalid_argument, problem);
  }
  if (args.has("setpoint-percent") && args.has("setpoint-airflow")) {
    return Status::failure(StatusCode::invalid_argument,
                           "--setpoint-percent and --setpoint-airflow are alternatives, not a "
                           "pair: give exactly one");
  }
  std::optional<SetpointRequest> setpoint;
  if (args.has("setpoint-percent")) {
    const auto percent = percent_option(args, "setpoint-percent");
    if (!percent.ok()) {
      return percent.status();
    }
    setpoint = SetpointRequest{SetpointPercent{percent.value()}};
  } else if (args.has("setpoint-airflow")) {
    const auto airflow = signed_option(args, "setpoint-airflow");
    if (!airflow.ok()) {
      return airflow.status();
    }
    setpoint =
        SetpointRequest{SetpointAirflow{Airflow::from_cubic_metres_per_hour(airflow.value())}};
  }
  return ControlRequest{.key = key.value(),
                        .device = device.value(),
                        .device_generation = generation.value(),
                        .epoch = epoch.value(),
                        .expected_revision = revision.value(),
                        .intent = intent.value(),
                        .setpoint = setpoint,
                        .actor = actor.value(),
                        .requested_at = tick.value(),
                        .safety_permit = permit.value(),
                        .supersede = supersede.value(),
                        .relationship = relationship.value()};
}

int verb_evaluate(const Args& args) {
  const auto request = control_request(args);
  if (!request.ok()) {
    return usage_error(request.message());
  }
  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  const Result<Decision> decision = engine.evaluate(request.value());
  if (!decision.ok()) {
    return refuse_after_close(engine, decision.status(), args.json);
  }
  Emitter out(args.json);
  report_decision(out, decision.value());
  if (!decision.value().eligible) {
    // The request was understood and refused: the whole ordered trace of the
    // checks that produced the answer is reported, and the exit code says so.
    const Status closed = engine.close();
    if (!closed.ok()) {
      return refuse(closed, args.json);
    }
    return refuse_decision(decision.value(), out);
  }
  return finish_verb(engine, out, args.json);
}

int verb_issue(const Args& args) {
  const auto request = control_request(args);
  if (!request.ok()) {
    return usage_error(request.message());
  }
  const auto mode = adapter_mode_option(args, true);
  if (!mode.ok()) {
    return usage_error(mode.message());
  }

  // Everything a following verification needs is parsed before an engine is
  // opened, so a malformed command line never reaches an adapter.
  std::optional<SourceId> verified_by;
  std::optional<SpaceRefId> fan_point;
  std::optional<SpaceRefId> pressure_point;
  std::optional<EvidenceSequence> fan_sequence;
  std::optional<EvidenceSequence> pressure_sequence;
  if (args.verify) {
    const auto source = id_option<SourceId>(args, "source");
    const auto sequence = optional_ordinal_option<EvidenceSequenceTag>(args, "sequence");
    const auto named_fan_sequence = optional_ordinal_option<EvidenceSequenceTag>(args, "fan-sequence");
    const auto named_pressure_sequence =
        optional_ordinal_option<EvidenceSequenceTag>(args, "pressure-sequence");
    const auto point = optional_id_option<SpaceRefId>(args, "point");
    const auto named_fan_point = optional_id_option<SpaceRefId>(args, "fan-point");
    const auto named_pressure_point = optional_id_option<SpaceRefId>(args, "pressure-point");
    std::string problem;
    if (failed(source, problem) || failed(sequence, problem) || failed(named_fan_sequence, problem) ||
        failed(named_pressure_sequence, problem) || failed(point, problem) ||
        failed(named_fan_point, problem) || failed(named_pressure_point, problem)) {
      return usage_error("--verify: " + problem);
    }
    const EvidenceSequence shared = sequence.value().value_or(EvidenceSequence::from(0));
    verified_by = source.value();
    fan_sequence = named_fan_sequence.value().value_or(shared);
    pressure_sequence = named_pressure_sequence.value().value_or(shared);
    fan_point = named_fan_point.value().value_or(
        point.value().value_or(delivery_point(request.value().device)));
    pressure_point = named_pressure_point.value().value_or(
        point.value().value_or(differential_point(request.value().device,
                                                   request.value().relationship)));
  }

  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  const auto view = engine.device(request.value().device);
  if (!view.ok()) {
    return refuse_after_close(engine, view.status(), args.json);
  }

  SyntheticAirflowAdapter adapter(synthetic_descriptor());
  const AdapterDescriptor descriptor = adapter.describe();
  // The plant starts where the engine last observed the device, so the command
  // under test is the only thing that moves it.
  seed_adapter(adapter, engine, view.value(), observed_plant(view.value()),
               request.value().relationship, mode.value());
  announce_adapter();

  const Result<AttemptRecord> issued = engine.issue(request.value(), adapter);
  if (!issued.ok()) {
    return refuse_after_close(engine, issued.status(), args.json);
  }
  Emitter out(args.json);
  report_adapter(out, descriptor, adapter_mode_name(mode.value()));
  out.note("adapter=SYNTHETIC (no hardware is driven)");
  report_attempt(out, issued.value(), false);
  if (is_definite_refusal(issued.value().disposition)) {
    out.note("the adapter stated " + std::string(to_string(issued.value().disposition)) +
             "; no effect was established");
  }
  if (args.verify) {
    const VerificationRequest verification{.attempt = issued.value().id,
                                            .adapter = &adapter,
                                            .source = *verified_by,
                                            .fan_point = *fan_point,
                                            .pressure_point = *pressure_point,
                                            .fan_sequence = *fan_sequence,
                                            .pressure_sequence = *pressure_sequence,
                                            .at = request.value().requested_at,
                                            .relationship = request.value().relationship};
    const Result<VerifiedEffect> effect = engine.verify(verification);
    if (!effect.ok()) {
      return refuse_after_close(engine, effect.status(), args.json);
    }
    report_effect(out, effect.value());
    if (effect.value().state != EffectState::effective &&
        verification.at == issued.value().accepted_at) {
      out.note(
          "the verification ran at the instant the command was accepted; evidence about a command "
          "must be accepted strictly after it, so advance the clock and run verify at a later "
          "--tick to establish the effect");
    }
  }
  return finish_verb(engine, out, args.json);
}

int verb_verify(const Args& args) {
  const auto attempt = ordinal_option<AttemptIdTag>(args, "attempt");
  const auto source = id_option<SourceId>(args, "source");
  const auto tick = tick_option(args, "tick");
  const auto relationship = optional_id_option<PressureRelationshipId>(args, "relationship");
  const auto fan_sequence = optional_ordinal_option<EvidenceSequenceTag>(args, "fan-sequence");
  const auto pressure_sequence = optional_ordinal_option<EvidenceSequenceTag>(args, "pressure-sequence");
  const auto fan_point = optional_id_option<SpaceRefId>(args, "fan-point");
  const auto pressure_point = optional_id_option<SpaceRefId>(args, "pressure-point");
  std::string problem;
  if (failed(attempt, problem) || failed(source, problem) || failed(tick, problem) ||
      failed(relationship, problem) || failed(fan_sequence, problem) ||
      failed(pressure_sequence, problem) || failed(fan_point, problem) ||
      failed(pressure_point, problem)) {
    return usage_error(problem);
  }
  // A refusing adapter is not accepted here: verify never executes a command, so
  // there is nothing for a refusal to be about.
  const bool with_adapter = args.has("adapter");
  AdapterMode mode = AdapterMode::synthetic;
  if (with_adapter) {
    const auto parsed = adapter_mode_option(args, false);
    if (!parsed.ok()) {
      return usage_error(parsed.message());
    }
    mode = parsed.value();
  }

  auto opened = open_engine(args, OpenMode::open_or_create);
  if (!opened.ok()) {
    return refuse(opened.status(), args.json);
  }
  AirflowControlEngine& engine = opened.value();
  const auto record = engine.attempt(attempt.value());
  if (!record.ok()) {
    return refuse_after_close(engine, record.status(), args.json);
  }
  const auto view = engine.device(record.value().device);
  if (!view.ok()) {
    return refuse_after_close(engine, view.status(), args.json);
  }

  std::optional<SyntheticAirflowAdapter> adapter;
  AirflowAdapter* adapter_pointer = nullptr;
  if (with_adapter) {
    adapter.emplace(synthetic_descriptor());
    // acknowledge_only is exactly the mode in which the simulated device took
    // the command and never moved: its plant still reports where the engine
    // last observed it. The plain synthetic adapter is seeded where the command
    // put the device, which is what a device that acted reports.
    const PlantSeed seed = mode == AdapterMode::acknowledge_only
                               ? observed_plant(view.value())
                               : delivered_plant(view.value(), record.value().setpoint);
    seed_adapter(*adapter, engine, view.value(), seed, relationship.value(), mode);
    adapter_pointer = &*adapter;
  }
  // The label is written for every run of this verb, in both modes: whatever
  // the answer is, it was not produced by hardware.
  announce_adapter();
  if (!with_adapter) {
    std::cerr << "adapter=none consulted: only observations the engine already accepted are "
                 "considered\n";
  }

  const VerificationRequest request{
      .attempt = attempt.value(),
      .adapter = adapter_pointer,
      .source = source.value(),
      .fan_point = fan_point.value().value_or(delivery_point(record.value().device)),
      .pressure_point = pressure_point.value().value_or(
          differential_point(record.value().device, relationship.value())),
      .fan_sequence = fan_sequence.value().value_or(EvidenceSequence::from(0)),
      .pressure_sequence = pressure_sequence.value().value_or(EvidenceSequence::from(0)),
      .at = tick.value(),
      .relationship = relationship.value()};
  const Result<VerifiedEffect> effect = engine.verify(request);
  if (!effect.ok()) {
    return refuse_after_close(engine, effect.status(), args.json);
  }
  Emitter out(args.json);
  if (with_adapter) {
    report_adapter(out, adapter->describe(), adapter_mode_name(mode));
  } else {
    out.string("adapter", "none");
    out.boolean("synthetic", true);
  }
  out.note("adapter=SYNTHETIC (no hardware is driven)");
  report_effect(out, effect.value());
  if (effect.value().state != EffectState::effective) {
    out.note("the effect was not established: " + effect.value().message);
  }
  return finish_verb(engine, out, args.json);
}

}  // namespace

int main(int argc, char** argv) {
  Args args;
  std::string problem;
  if (!parse_arguments(argc, argv, args, problem)) {
    return usage_error(problem);
  }
  if (args.version || args.verb == "version") {
    std::cout << "airflow-control " << kVersionString << " (store format " << kStoreFormatVersion
              << ")\n";
    return kExitOk;
  }
  if (args.help || args.verb == "help") {
    std::cout << kUsage;
    return kExitOk;
  }
  if (args.verb.empty()) {
    std::cout << kUsage;
    return kExitUsage;
  }
  const std::string& verb = args.verb;
  if (verb == "init") {
    return verb_init(args);
  }
  if (verb == "epoch") {
    return verb_epoch(args);
  }
  if (verb == "tick") {
    return verb_tick(args);
  }
  if (verb == "device") {
    return verb_device(args);
  }
  if (verb == "lifecycle") {
    return verb_lifecycle(args);
  }
  if (verb == "policy") {
    return verb_policy(args);
  }
  if (verb == "relationship") {
    return verb_relationship(args);
  }
  if (verb == "containment") {
    return verb_containment(args);
  }
  if (verb == "obligation") {
    return verb_obligation(args);
  }
  if (verb == "interlock") {
    return verb_interlock(args);
  }
  if (verb == "grant") {
    return verb_grant(args);
  }
  if (verb == "override") {
    return verb_override(args);
  }
  if (verb == "permit") {
    return verb_permit(args);
  }
  if (verb == "observe") {
    return verb_observe(args);
  }
  if (verb == "evaluate") {
    return verb_evaluate(args);
  }
  if (verb == "issue") {
    return verb_issue(args);
  }
  if (verb == "verify") {
    return verb_verify(args);
  }
  if (verb == "resolve") {
    return verb_resolve(args);
  }
  if (verb == "attempts" || verb == "attempt") {
    return verb_attempts(args);
  }
  if (verb == "state") {
    return verb_state(args);
  }
  if (verb == "digest") {
    return verb_digest(args);
  }
  if (verb == "history") {
    return verb_history(args);
  }
  if (verb == "store-audit") {
    return verb_store_audit(args);
  }
  return unknown_verb(verb);
}
