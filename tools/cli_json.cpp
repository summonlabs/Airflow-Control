#include "cli_json.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace airflow_cli {
namespace {

/// Renders a value that has already been validated as a JSON literal.
std::string quoted(std::string_view text) { return "\"" + json_escape(text) + "\""; }

/// Joins rendered items into a JSON array.
std::string array_of(const std::vector<std::string>& items) {
  std::string out = "[";
  bool first = true;
  for (const std::string& item : items) {
    if (!first) {
      out.push_back(',');
    }
    first = false;
    out.append(item);
  }
  out.push_back(']');
  return out;
}

}  // namespace

std::string json_escape(std::string_view text) {
  static constexpr char kHexDigits[] = "0123456789abcdef";
  std::string out;
  out.reserve(text.size());
  for (const char character : text) {
    const auto code = static_cast<unsigned char>(character);
    switch (character) {
      case '"':
        out.append("\\\"");
        break;
      case '\\':
        out.append("\\\\");
        break;
      case '\b':
        out.append("\\b");
        break;
      case '\f':
        out.append("\\f");
        break;
      case '\n':
        out.append("\\n");
        break;
      case '\r':
        out.append("\\r");
        break;
      case '\t':
        out.append("\\t");
        break;
      default:
        if (code < 0x20U) {
          out.append("\\u00");
          out.push_back(kHexDigits[(code >> 4U) & 0x0FU]);
          out.push_back(kHexDigits[code & 0x0FU]);
        } else {
          out.push_back(character);
        }
        break;
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// JsonObject
// ---------------------------------------------------------------------------

void JsonObject::add_string(std::string key, std::string_view value) {
  members_.emplace_back(std::move(key), quoted(value));
}

void JsonObject::add_uinteger(std::string key, std::uint64_t value) {
  members_.emplace_back(std::move(key), std::to_string(value));
}

void JsonObject::add_integer(std::string key, std::int64_t value) {
  members_.emplace_back(std::move(key), std::to_string(value));
}

void JsonObject::add_boolean(std::string key, bool value) {
  members_.emplace_back(std::move(key), value ? "true" : "false");
}

void JsonObject::add_null(std::string key) { members_.emplace_back(std::move(key), "null"); }

void JsonObject::add_object(std::string key, const JsonObject& value) {
  members_.emplace_back(std::move(key), value.str());
}

void JsonObject::add_strings(std::string key, const std::vector<std::string>& values) {
  std::vector<std::string> rendered;
  rendered.reserve(values.size());
  for (const std::string& value : values) {
    rendered.push_back(quoted(value));
  }
  members_.emplace_back(std::move(key), array_of(rendered));
}

void JsonObject::add_objects(std::string key, const std::vector<JsonObject>& values) {
  std::vector<std::string> rendered;
  rendered.reserve(values.size());
  for (const JsonObject& value : values) {
    rendered.push_back(value.str());
  }
  members_.emplace_back(std::move(key), array_of(rendered));
}

void JsonObject::add_prefixed(const std::string& prefix, const JsonObject& value) {
  for (const std::pair<std::string, std::string>& member : value.members_) {
    members_.emplace_back(prefix + member.first, member.second);
  }
}

std::string JsonObject::str() const {
  std::string out = "{";
  bool first = true;
  for (const std::pair<std::string, std::string>& member : members_) {
    if (!first) {
      out.push_back(',');
    }
    first = false;
    out.append(quoted(member.first));
    out.push_back(':');
    out.append(member.second);
  }
  out.push_back('}');
  return out;
}

// ---------------------------------------------------------------------------
// Row
// ---------------------------------------------------------------------------

void Row::string(std::string key, std::string_view value) {
  human_.append(key);
  human_.push_back('=');
  human_.append(value);
  human_.push_back(' ');
  object_.add_string(std::move(key), value);
}

void Row::uinteger(std::string key, std::uint64_t value) {
  human_.append(key);
  human_.push_back('=');
  human_.append(std::to_string(value));
  human_.push_back(' ');
  object_.add_uinteger(std::move(key), value);
}

void Row::integer(std::string key, std::int64_t value) {
  human_.append(key);
  human_.push_back('=');
  human_.append(std::to_string(value));
  human_.push_back(' ');
  object_.add_integer(std::move(key), value);
}

void Row::boolean(std::string key, bool value) {
  human_.append(key);
  human_.push_back('=');
  human_.append(value ? "true" : "false");
  human_.push_back(' ');
  object_.add_boolean(std::move(key), value);
}

void Row::null(std::string key) {
  human_.append(key);
  human_.append("=null ");
  object_.add_null(std::move(key));
}

void Row::strings(std::string key, const std::vector<std::string>& values) {
  human_.append(key);
  human_.push_back('=');
  bool first = true;
  for (const std::string& value : values) {
    if (!first) {
      human_.push_back(',');
    }
    first = false;
    human_.append(value);
  }
  human_.push_back(' ');
  object_.add_strings(std::move(key), values);
}

void Row::quantity(std::string key, std::int64_t exact, std::string rendered) {
  human_.append(key);
  human_.push_back('=');
  human_.append(rendered);
  human_.push_back(' ');
  object_.add_integer(std::move(key), exact);
}

// ---------------------------------------------------------------------------
// Emitter
// ---------------------------------------------------------------------------

void Emitter::string(std::string key, std::string_view value) {
  if (json_) {
    object_.add_string(std::move(key), value);
    return;
  }
  human_.append(key);
  human_.push_back('=');
  human_.append(value);
  human_.push_back('\n');
}

void Emitter::uinteger(std::string key, std::uint64_t value) {
  if (json_) {
    object_.add_uinteger(std::move(key), value);
    return;
  }
  human_.append(key);
  human_.push_back('=');
  human_.append(std::to_string(value));
  human_.push_back('\n');
}

void Emitter::integer(std::string key, std::int64_t value) {
  if (json_) {
    object_.add_integer(std::move(key), value);
    return;
  }
  human_.append(key);
  human_.push_back('=');
  human_.append(std::to_string(value));
  human_.push_back('\n');
}

void Emitter::boolean(std::string key, bool value) {
  if (json_) {
    object_.add_boolean(std::move(key), value);
    return;
  }
  human_.append(key);
  human_.push_back('=');
  human_.append(value ? "true" : "false");
  human_.push_back('\n');
}

void Emitter::null(std::string key) {
  if (json_) {
    object_.add_null(std::move(key));
    return;
  }
  human_.append(key);
  human_.append("=null\n");
}

void Emitter::strings(std::string key, const std::vector<std::string>& values) {
  if (json_) {
    object_.add_strings(std::move(key), values);
    return;
  }
  human_.append(key);
  human_.push_back('=');
  bool first = true;
  for (const std::string& value : values) {
    if (!first) {
      human_.push_back(',');
    }
    first = false;
    human_.append(value);
  }
  human_.push_back('\n');
}

void Emitter::object(std::string key, const JsonObject& value) {
  if (json_) {
    object_.add_object(std::move(key), value);
    return;
  }
  human_.append(key);
  human_.push_back('=');
  human_.append(value.str());
  human_.push_back('\n');
}

void Emitter::rows(std::string key, const std::vector<Row>& values) {
  if (json_) {
    for (std::size_t index = 0; index < values.size(); ++index) {
      object_.add_prefixed(key + "-" + std::to_string(index) + "-", values[index].object());
    }
    return;
  }
  for (const Row& row : values) {
    human_.append(row.human());
    human_.push_back('\n');
  }
}

void Emitter::quantity(std::string key, std::int64_t exact, std::string rendered) {
  if (json_) {
    object_.add_integer(std::move(key), exact);
    return;
  }
  human_.append(key);
  human_.push_back('=');
  human_.append(rendered);
  human_.push_back('\n');
}

void Emitter::note(std::string_view text) {
  if (json_) {
    return;
  }
  human_.append(text);
  human_.push_back('\n');
}

void Emitter::finish(std::ostream& out) const {
  if (json_) {
    out << object_.str() << "\n";
    return;
  }
  out << human_;
}

}  // namespace airflow_cli
