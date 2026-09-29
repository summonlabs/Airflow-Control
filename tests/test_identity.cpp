#include "test_harness.hpp"

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

// Model-layer proof suite: identities, bounded text, ordinals, the logical
// clock, the fixed-point quantities, and the status token table. Everything here
// is a pure function of its arguments, so a failure is a statement about the
// contract rather than about a scenario.
//
// Nothing in this file reaches into the library's internals: every name below is
// a public header name.

namespace {

using namespace airflow_control;  // NOLINT(google-build-using-namespace)

/// A token comparison that compares text rather than the address of a string
/// literal, so a check cannot pass because two identical literals were folded.
std::string_view token(StatusCode code) { return to_string(code); }

std::string repeated(char value, std::size_t count) { return std::string(count, value); }

/// An identifier type with a smaller bound than the default, to prove the bound
/// is a parameter of the type rather than a global constant.
struct TinyTag {};
using TinyId = BasicId<TinyTag, 8>;

}  // namespace

AIRFLOW_TEST(identifier_validation_is_closed) {
  // An identifier is printable ASCII from a closed set, and never a path.
  CHECK_STATUS(validate_identifier("", kMaxIdentifierLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_identifier(".", kMaxIdentifierLength), StatusCode::path_invalid);
  CHECK_STATUS(validate_identifier("..", kMaxIdentifierLength), StatusCode::path_invalid);

  CHECK_STATUS(validate_identifier("dev/1", kMaxIdentifierLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_identifier("dev\\1", kMaxIdentifierLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_identifier("..\\dev", kMaxIdentifierLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_identifier("a b", kMaxIdentifierLength), StatusCode::invalid_argument);
  // The accepted character set is alphanumeric plus '-', '_', and '.' only. A
  // colon is refused everywhere in an identifier, so a drive-letter form such as
  // "c:temp" cannot be an identifier and cannot be reinterpreted as a path by a
  // filesystem or a shell.
  CHECK_STATUS(validate_identifier("c:temp", kMaxIdentifierLength),
               StatusCode::invalid_argument);
  CHECK_STATUS(validate_identifier("dev:1", kMaxIdentifierLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_identifier("a:", kMaxIdentifierLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_identifier(":", kMaxIdentifierLength), StatusCode::invalid_argument);

  // Control characters are refused rather than escaped. The NUL case is built
  // from an explicit length because a C string literal would truncate at it.
  const std::string with_nul("a\0b", 3);
  CHECK_STATUS(validate_identifier(with_nul, kMaxIdentifierLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_identifier(std::string("a\tb"), kMaxIdentifierLength),
               StatusCode::invalid_argument);
  CHECK_STATUS(validate_identifier(std::string("a\nb"), kMaxIdentifierLength),
               StatusCode::invalid_argument);
  CHECK_STATUS(validate_identifier(std::string("a\x7F" "b"), kMaxIdentifierLength),
               StatusCode::invalid_argument);

  // Non-ASCII bytes are refused, not replaced or transcoded: two different byte
  // strings must never be able to name the same durable identity.
  CHECK_STATUS(validate_identifier("caf\xC3\xA9", kMaxIdentifierLength),
               StatusCode::invalid_argument);
  CHECK_STATUS(validate_identifier(std::string("a\x80z"), kMaxIdentifierLength),
               StatusCode::invalid_argument);

  // The first character must be alphanumeric; a leading digit is allowed and a
  // leading punctuation character is not.
  CHECK(validate_identifier("9dev", kMaxIdentifierLength).ok());
  CHECK(validate_identifier("0", kMaxIdentifierLength).ok());
  CHECK_STATUS(validate_identifier("-dev", kMaxIdentifierLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_identifier("_dev", kMaxIdentifierLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_identifier(".dev", kMaxIdentifierLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_identifier(":dev", kMaxIdentifierLength), StatusCode::invalid_argument);

  // A trailing dot is refused so that a name can never collide with a shortened
  // form a filesystem would produce.
  CHECK_STATUS(validate_identifier("dev.", kMaxIdentifierLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_identifier("dev..", kMaxIdentifierLength), StatusCode::invalid_argument);

  // Interior dots, dashes, and underscores are ordinary characters.
  CHECK(validate_identifier("dev.1", kMaxIdentifierLength).ok());
  CHECK(validate_identifier("dev-1_x", kMaxIdentifierLength).ok());
  CHECK(validate_identifier("dev-1.x_y", kMaxIdentifierLength).ok());
  CHECK_EQ(validate_identifier("dev-1", kMaxIdentifierLength).value(), std::string("dev-1"));
}

AIRFLOW_TEST(identifier_bound_is_enforced) {
  // The bound is applied before the character rules, so an over-long identifier
  // reports the bound rather than the first character it dislikes.
  CHECK(validate_identifier(repeated('a', kMaxIdentifierLength), kMaxIdentifierLength).ok());
  CHECK_EQ(validate_identifier(repeated('a', kMaxIdentifierLength), kMaxIdentifierLength)
               .value()
               .size(),
           kMaxIdentifierLength);
  CHECK_STATUS(validate_identifier(repeated('a', kMaxIdentifierLength + 1), kMaxIdentifierLength),
               StatusCode::bounds_exceeded);

  // A typed identifier carries its own bound.
  CHECK(TinyId::parse("12345678").ok());
  CHECK_STATUS(TinyId::parse("123456789"), StatusCode::bounds_exceeded);
  CHECK_STATUS(TinyId::parse(""), StatusCode::invalid_argument);

  const Result<AirflowDeviceId> id = AirflowDeviceId::parse("dev-1");
  REQUIRE(id.ok());
  CHECK_EQ(id.value().str(), std::string("dev-1"));
  CHECK_EQ(id.value().length(), std::size_t{5});
  CHECK_EQ(id.value().hash(), AirflowDeviceId::parse("dev-1").value().hash());
  CHECK(id.value() == AirflowDeviceId::parse("dev-1").value());
  CHECK(!(id.value() == AirflowDeviceId::parse("dev-2").value()));
  CHECK(id.value() < AirflowDeviceId::parse("dev-2").value());
}

AIRFLOW_TEST(text_validation_accepts_utf8_and_refuses_controls) {
  // Valid UTF-8 is accepted, including multi-byte code points outside the C0 and
  // C1 control ranges.
  CHECK(validate_text("", kMaxTextLength).ok());
  CHECK_EQ(validate_text("caf\xC3\xA9", kMaxTextLength).value(), std::string("caf\xC3\xA9"));
  CHECK(validate_text("\xC2\xA0", kMaxTextLength).ok());   // U+00A0, a space
  CHECK(validate_text("\xE2\x82\xAC", kMaxTextLength).ok());  // U+20AC, a currency sign
  CHECK(validate_text("\xF0\x9F\x98\x80", kMaxTextLength).ok());  // U+1F600

  // Overlong encodings are refused: a permissive decoder would make two byte
  // strings decode to the same text.
  CHECK_STATUS(validate_text("\xC0\xAF", kMaxTextLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_text("\xE0\x80\xAF", kMaxTextLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_text("\xF0\x80\x80\xAF", kMaxTextLength), StatusCode::invalid_argument);

  // Surrogate code points are not text.
  CHECK_STATUS(validate_text("\xED\xA0\x80", kMaxTextLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_text("\xED\xBF\xBF", kMaxTextLength), StatusCode::invalid_argument);

  // Beyond U+10FFFF, truncated sequences, and a lone continuation byte.
  CHECK_STATUS(validate_text("\xF4\x90\x80\x80", kMaxTextLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_text("\xE2\x82", kMaxTextLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_text("\xF0\x9F\x98", kMaxTextLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_text("\x80", kMaxTextLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_text("\xFF", kMaxTextLength), StatusCode::invalid_argument);

  // C0, C1, and DEL are refused.
  CHECK_STATUS(validate_text(std::string("\0", 1), kMaxTextLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_text("\x01", kMaxTextLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_text("\t", kMaxTextLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_text("\n", kMaxTextLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_text("\x1F", kMaxTextLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_text("\x7F", kMaxTextLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_text("\xC2\x85", kMaxTextLength), StatusCode::invalid_argument);
  CHECK_STATUS(validate_text("\xC2\x9F", kMaxTextLength), StatusCode::invalid_argument);

  // The bound is in bytes and is enforced exactly at the edge.
  CHECK(validate_text(repeated('a', kMaxTextLength), kMaxTextLength).ok());
  CHECK_STATUS(validate_text(repeated('a', kMaxTextLength + 1), kMaxTextLength),
               StatusCode::bounds_exceeded);
}

AIRFLOW_TEST(ordinals_and_the_logical_clock_are_checked) {
  CHECK(Ordinal<AttemptIdTag>::from(0).is_zero());
  CHECK(!Ordinal<AttemptIdTag>::from(1).is_zero());
  CHECK_EQ(Ordinal<AttemptIdTag>::from(1).next().value(), Ordinal<AttemptIdTag>::from(2));
  CHECK_EQ(Ordinal<AttemptIdTag>::from(1).to_string(), std::string("1"));

  const AttemptId maximum = AttemptId::from(std::numeric_limits<std::uint64_t>::max());
  CHECK_STATUS(maximum.next(), StatusCode::overflow);
  CHECK_EQ(AttemptId::from(std::numeric_limits<std::uint64_t>::max() - 1).next().value(), maximum);

  // tick_add is checked arithmetic, so a clock cannot wrap.
  CHECK_EQ(tick_add(LogicalTick::from(0), 0).value(), LogicalTick::from(0));
  CHECK_EQ(tick_add(LogicalTick::from(1), 41).value(), LogicalTick::from(42));
  CHECK_EQ(tick_add(LogicalTick::from(std::numeric_limits<std::uint64_t>::max()), 0).value(),
           LogicalTick::from(std::numeric_limits<std::uint64_t>::max()));
  CHECK_STATUS(tick_add(LogicalTick::from(std::numeric_limits<std::uint64_t>::max()), 1),
               StatusCode::overflow);
  CHECK_STATUS(tick_add(LogicalTick::from(std::numeric_limits<std::uint64_t>::max() - 1), 2),
               StatusCode::overflow);
  CHECK_EQ(tick_add(LogicalTick::from(std::numeric_limits<std::uint64_t>::max() - 1), 1).value(),
           LogicalTick::from(std::numeric_limits<std::uint64_t>::max()));

  CHECK_EQ(tick_age(LogicalTick::from(10), LogicalTick::from(4)).value(), std::uint64_t{6});
  CHECK_EQ(tick_age(LogicalTick::from(5), LogicalTick::from(5)).value(), std::uint64_t{0});
  CHECK_EQ(tick_age(LogicalTick::from(0), LogicalTick::from(0)).value(), std::uint64_t{0});
  // An instant later than "now" is a future reading, not a negative age. The
  // engine reports it as a future evidence instant rather than as a shape fault.
  CHECK_STATUS(tick_age(LogicalTick::from(4), LogicalTick::from(5)), StatusCode::evidence_future);
  CHECK_STATUS(tick_age(LogicalTick::from(0), LogicalTick::from(1)), StatusCode::evidence_future);

  CHECK(LogicalTick::from(3) < LogicalTick::from(4));
  CHECK(LogicalTick::from(3) == LogicalTick::from(3));
  CHECK_EQ(LogicalTick::from(3).to_string(), std::string("3"));
  CHECK_EQ(to_string(DeviceLifecycle::active), std::string_view("active"));
}

AIRFLOW_TEST(basis_point_bounds_are_closed) {
  CHECK(SetpointBasisPoints::create(0).ok());
  CHECK_EQ(SetpointBasisPoints::create(0).value().basis_points(), std::uint32_t{0});
  CHECK(SetpointBasisPoints::create(SetpointBasisPoints::kFull).ok());
  CHECK_EQ(SetpointBasisPoints::create(SetpointBasisPoints::kFull).value().basis_points(),
           std::uint32_t{10000});
  CHECK_STATUS(SetpointBasisPoints::create(SetpointBasisPoints::kFull + 1),
               StatusCode::out_of_range);
  CHECK_STATUS(SetpointBasisPoints::create(10001), StatusCode::out_of_range);
  CHECK_STATUS(SetpointBasisPoints::create(UINT32_MAX), StatusCode::out_of_range);

  CHECK(SlewBasisPoints::create(0).ok());
  CHECK(SlewBasisPoints::create(SlewBasisPoints::kFull).ok());
  CHECK_STATUS(SlewBasisPoints::create(SlewBasisPoints::kFull + 1), StatusCode::out_of_range);
  CHECK_STATUS(SlewBasisPoints::create(10001), StatusCode::out_of_range);
  CHECK_STATUS(SlewBasisPoints::create(UINT32_MAX), StatusCode::out_of_range);

  // kFull is the value both constructors accept at their edge, read back
  // through the constructor rather than compared as a compile-time constant.
  CHECK_EQ(SetpointBasisPoints::create(SetpointBasisPoints::kFull).value().basis_points(),
           std::uint32_t{10000});
  CHECK_EQ(SlewBasisPoints::create(SlewBasisPoints::kFull).value().basis_points(),
           std::uint32_t{10000});

  // The two types are materially different quantities and do not compare across.
  CHECK(SetpointBasisPoints::create(5000).value() == SetpointBasisPoints::create(5000).value());
  CHECK(SetpointBasisPoints::create(5000).value() < SetpointBasisPoints::create(5001).value());
}

AIRFLOW_TEST(quantities_render_exactly) {
  CHECK_EQ(Airflow::from_cubic_metres_per_hour(5).to_string(), std::string("5 m3/h"));
  CHECK_EQ(Airflow::from_cubic_metres_per_hour(-1234).to_string(), std::string("-1234 m3/h"));
  CHECK_EQ(Airflow::from_cubic_metres_per_hour(0).to_string(), std::string("0 m3/h"));
  CHECK_EQ(Pressure::from_millipascals(0).to_string(), std::string("0.000 Pa"));
  CHECK_EQ(Pressure::from_millipascals(1).to_string(), std::string("0.001 Pa"));
  CHECK_EQ(Pressure::from_millipascals(-20000).to_string(), std::string("-20.000 Pa"));
  CHECK_EQ(Pressure::from_millipascals(-1).to_string(), std::string("-0.001 Pa"));
  CHECK_EQ(MilliCelsius::from_millidegrees(-1234).to_string(), std::string("-1.234 C"));
  CHECK_EQ(MilliCelsius::from_millidegrees(1000000).to_string(), std::string("1000.000 C"));
  CHECK_EQ(SetpointBasisPoints::create(0).value().to_string(), std::string("0.00 %"));
  CHECK_EQ(SetpointBasisPoints::create(1).value().to_string(), std::string("0.01 %"));
  CHECK_EQ(SetpointBasisPoints::create(9999).value().to_string(), std::string("99.99 %"));
  CHECK_EQ(SetpointBasisPoints::create(10000).value().to_string(), std::string("100.00 %"));
  CHECK_EQ(SlewBasisPoints::create(1000).value().to_string(), std::string("10.00 %"));

  // The most negative value is rendered without overflow: negating it in signed
  // arithmetic would be undefined.
  CHECK_EQ(Airflow::from_cubic_metres_per_hour(std::numeric_limits<std::int64_t>::min())
               .to_string(),
           std::string("-9223372036854775808 m3/h"));
  // Pressure carries three fractional digits, so its most negative value
  // renders as a fixed-point decimal of the same exact magnitude.
  CHECK_EQ(Pressure::from_millipascals(std::numeric_limits<std::int64_t>::min()).to_string(),
           std::string("-9223372036854775.808 Pa"));

  // render_fixed renders the exact fixed-point decimal: no unit, no padding, and
  // no trailing separator.
  CHECK_EQ(render_fixed(1234, 3), std::string("1.234"));
  CHECK_EQ(render_fixed(1234, 3).size(), std::size_t{5});
  CHECK_EQ(render_fixed(-1234, 3), std::string("-1.234"));
  CHECK_EQ(render_fixed(5, 0), std::string("5"));
  CHECK_EQ(render_fixed(5, 0).size(), std::size_t{1});
  CHECK_EQ(render_fixed(-5, 2), std::string("-0.05"));
  CHECK_EQ(render_fixed(0, 3), std::string("0.000"));
  CHECK_EQ(render_fixed(0, 0), std::string("0"));
  CHECK_NE(render_fixed(1234, 3), render_fixed(1235, 3));
}

AIRFLOW_TEST(status_tokens_round_trip_and_are_unique) {
  const auto last = static_cast<std::uint32_t>(StatusCode::internal_error);
  CHECK_EQ(token(StatusCode::ok), std::string_view("ok"));
  CHECK_EQ(parse_status_code("ok").value(), StatusCode::ok);
  CHECK_EQ(token(StatusCode::attempt_unresolved), std::string_view("attempt_unresolved"));
  CHECK_EQ(token(StatusCode::evidence_generation_mismatch),
           std::string_view("evidence_generation_mismatch"));
  CHECK_EQ(token(StatusCode::adapter_fenced), std::string_view("adapter_fenced"));

  for (std::uint32_t raw = 0; raw <= last; ++raw) {
    const auto code = static_cast<StatusCode>(raw);
    const std::string_view rendered = token(code);
    CHECK(!rendered.empty());
    const std::optional<StatusCode> parsed = parse_status_code(rendered);
    CHECK(parsed.has_value());
    CHECK_EQ(parsed.value(), code);
    CHECK_EQ(is_success(code), code == StatusCode::ok);
  }

  // Every code has exactly one token, and no token names two codes.
  for (std::uint32_t left = 0; left <= last; ++left) {
    for (std::uint32_t right = left + 1; right <= last; ++right) {
      CHECK(token(static_cast<StatusCode>(left)) != token(static_cast<StatusCode>(right)));
    }
  }

  CHECK(!parse_status_code("").has_value());
  CHECK(!parse_status_code("OK").has_value());
  CHECK(!parse_status_code("not_a_status").has_value());
  CHECK(!parse_status_code("ok ").has_value());
  CHECK(is_success(StatusCode::ok));
  CHECK(!is_success(StatusCode::not_found));
}

AIRFLOW_TEST(status_renders_code_message_and_first_refusals) {
  const Status success = Status::success();
  CHECK(success.ok());
  CHECK_EQ(success.to_string(), std::string("ok: "));

  Status failure = Status::failure(StatusCode::not_found, "device is not registered");
  CHECK(!failure.ok());
  CHECK_EQ(failure.code(), StatusCode::not_found);
  CHECK_EQ(failure.message(), std::string("device is not registered"));
  CHECK_EQ(failure.to_string(), std::string("not_found: device is not registered"));

  // ok trace entries are notes and are not rendered; a non-ok entry is the
  // refusal a reader is meant to see.
  failure.note("identity", "identity resolved");
  failure.add_check("lifecycle", StatusCode::lifecycle_forbidden, "device lifecycle is retired");
  CHECK_EQ(failure.to_string(),
           std::string("not_found: device is not registered\n"
                       "  at lifecycle: lifecycle_forbidden (device lifecycle is retired)"));
  CHECK_EQ(failure.trace().size(), std::size_t{2});
  CHECK_EQ(failure.trace().front().outcome, StatusCode::ok);
}
