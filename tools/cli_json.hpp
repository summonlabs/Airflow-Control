#pragma once

// A tiny deterministic JSON writer for the airflow-control administration tool.
//
// Everything this tool prints goes through here, so there is exactly one place
// where a string is escaped and exactly one place where a number becomes text.
// Nothing consults the locale: no floating-point value is ever formatted, an
// integer is rendered by std::to_string, and a string is escaped byte by byte.
//
// Objects preserve insertion order because the writer stores its members in a
// vector. Nothing iterates an unordered container while output is produced, so
// two runs over the same state print the same bytes.

#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace airflow_cli {

/// Escapes a string for inclusion in a JSON document.
///
/// The mandatory escapes (quote and reverse solidus), the four short control
/// escapes, and every other C0 control character as \u00XX. Bytes above 0x1F
/// are copied unchanged: the runtime validates every text field it stores as
/// UTF-8, and inventing an escape for a well-formed multi-byte sequence would
/// change the value rather than protect it.
[[nodiscard]] std::string json_escape(std::string_view text);

/// An ordered JSON object.
///
/// Each member's value is rendered when it is added, so the object cannot hold
/// a value that does not belong to the member it was added under.
class JsonObject {
 public:
  void add_string(std::string key, std::string_view value);
  void add_uinteger(std::string key, std::uint64_t value);
  void add_integer(std::string key, std::int64_t value);
  void add_boolean(std::string key, bool value);
  void add_null(std::string key);
  void add_object(std::string key, const JsonObject& value);
  void add_strings(std::string key, const std::vector<std::string>& values);
  void add_objects(std::string key, const std::vector<JsonObject>& values);
  /// Appends every member of another object under a prefix.
  ///
  /// This is what keeps a listing one flat object: the tool's JSON output
  /// contains exactly one brace pair, so a caller that parses a single object
  /// never has to walk a nested array to find the rows.
  void add_prefixed(const std::string& prefix, const JsonObject& value);

  [[nodiscard]] std::string str() const;
  [[nodiscard]] bool empty() const noexcept { return members_.empty(); }

 private:
  std::vector<std::pair<std::string, std::string>> members_;
};

/// One record of a listing.
///
/// The same field calls fill the JSON object and the human "key=value" line, so
/// the two renderings of one record cannot drift apart: a field that a machine
/// reads is a field a human sees.
class Row {
 public:
  void string(std::string key, std::string_view value);
  void uinteger(std::string key, std::uint64_t value);
  void integer(std::string key, std::int64_t value);
  void boolean(std::string key, bool value);
  void null(std::string key);
  void strings(std::string key, const std::vector<std::string>& values);
  /// A quantity carried as its exact integer and rendered for a human. The JSON
  /// member is the integer, because a machine must never have to parse
  /// "50.00 %" back into basis points.
  void quantity(std::string key, std::int64_t exact, std::string rendered);

  [[nodiscard]] const JsonObject& object() const noexcept { return object_; }
  [[nodiscard]] const std::string& human() const noexcept { return human_; }

 private:
  JsonObject object_;
  std::string human_;
};

/// The result of exactly one verb, rendered either as one compact JSON object
/// or as human-readable lines.
///
/// Nothing reaches standard output until finish() is called, so a verb that
/// turns out to have been refused can be reported as a refusal without a
/// partial success line having already been printed.
class Emitter {
 public:
  explicit Emitter(bool json) noexcept : json_(json) {}

  [[nodiscard]] bool json() const noexcept { return json_; }

  void string(std::string key, std::string_view value);
  void uinteger(std::string key, std::uint64_t value);
  void integer(std::string key, std::int64_t value);
  void boolean(std::string key, bool value);
  void null(std::string key);
  void strings(std::string key, const std::vector<std::string>& values);
  void object(std::string key, const JsonObject& value);
  /// A list of records: one flat "<key>-<index>-<field>" member per field in
  /// JSON mode, one human line per record otherwise. The listing stays inside
  /// the one object the run prints rather than becoming a nested array.
  void rows(std::string key, const std::vector<Row>& values);
  void quantity(std::string key, std::int64_t exact, std::string rendered);
  /// A line only the human rendering carries: a reminder that an adapter is
  /// synthetic, or an explanation of an outcome that did not establish an
  /// effect. It is never part of the JSON object, because a JSON run prints
  /// exactly one object and nothing else.
  void note(std::string_view text);
  void finish(std::ostream& out) const;

 private:
  bool json_;
  JsonObject object_;
  std::string human_;
};

}  // namespace airflow_cli
