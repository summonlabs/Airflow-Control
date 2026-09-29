#include "test_harness.hpp"

#include <cstdint>
#include <string>
#include <string_view>

// Model-layer proof suite: pressure bands, containment precedence, obligations,
// envelopes, intents, action sets, delivery comparison, and the supersession
// rule. These are the pure functions the engine's decisions are built from, so
// they are proven directly and exhaustively rather than sampled through a
// scenario.

namespace {

using namespace airflow_control;  // NOLINT(google-build-using-namespace)

Pressure mp(std::int64_t value) { return Pressure::from_millipascals(value); }

SetpointBasisPoints percent(std::uint32_t value) {
  return SetpointBasisPoints::create(value).value();
}

SlewBasisPoints slew(std::uint32_t value) { return SlewBasisPoints::create(value).value(); }

OperatingEnvelope legal_envelope() {
  return OperatingEnvelope{
      .min_fan_percent = percent(2000),
      .max_fan_percent = percent(9000),
      .default_fan_percent = percent(5000),
      .min_airflow = Airflow::from_cubic_metres_per_hour(2000),
      .max_airflow = Airflow::from_cubic_metres_per_hour(9000),
      .max_step = slew(1000),
      .source = SourceId::parse("authority").value(),
      .evidence_generation = EvidenceGeneration::from(1),
  };
}

SetpointRequest fan(std::uint32_t basis_points) {
  return SetpointRequest{SetpointPercent{percent(basis_points)}};
}

SetpointRequest airflow(std::int64_t cubic_metres_per_hour) {
  return SetpointRequest{SetpointAirflow{Airflow::from_cubic_metres_per_hour(cubic_metres_per_hour)}};
}

}  // namespace

AIRFLOW_TEST(pressure_band_create_accepts_exactly_the_legal_bands) {
  // A legal band of each polarity.
  const Result<PressureBand> positive =
      PressureBand::create(PressurePolarity::positive, mp(1000), mp(5000), mp(100));
  REQUIRE(positive.ok());
  CHECK_EQ(positive.value().polarity(), PressurePolarity::positive);
  CHECK_EQ(positive.value().lower(), mp(1000));
  CHECK_EQ(positive.value().upper(), mp(5000));
  CHECK_EQ(positive.value().tolerance(), mp(100));

  const Result<PressureBand> negative =
      PressureBand::create(PressurePolarity::negative, mp(-30000), mp(-10000), mp(2000));
  REQUIRE(negative.ok());
  CHECK_EQ(negative.value().to_string(),
           std::string("negative -30.000 Pa .. -10.000 Pa tolerance 2.000 Pa"));

  const Result<PressureBand> neutral =
      PressureBand::create(PressurePolarity::neutral, mp(-1000), mp(1000), mp(0));
  REQUIRE(neutral.ok());
  CHECK_EQ(neutral.value().polarity(), PressurePolarity::neutral);

  // The structural boundaries are inclusive: a degenerate band is legal, a
  // neutral band exactly at the structural bound is legal, and a tolerance of
  // exactly the structural maximum is legal.
  CHECK(PressureBand::create(PressurePolarity::positive, mp(1), mp(1), mp(0)).ok());
  CHECK(PressureBand::create(PressurePolarity::neutral, mp(-50000), mp(50000), mp(0)).ok());
  CHECK(PressureBand::create(PressurePolarity::positive, mp(1000), mp(5000), mp(10000)).ok());

  // An inverted interval is refused whatever the polarity.
  CHECK_STATUS(PressureBand::create(PressurePolarity::positive, mp(5000), mp(1000), mp(0)),
               StatusCode::pressure_band_invalid);
  CHECK_STATUS(PressureBand::create(PressurePolarity::neutral, mp(100), mp(-100), mp(0)),
               StatusCode::pressure_band_invalid);

  // A negative tolerance is meaningless, and one above the structural bound is
  // refused rather than clamped.
  CHECK_STATUS(PressureBand::create(PressurePolarity::positive, mp(1000), mp(5000), mp(-1)),
               StatusCode::pressure_band_invalid);
  CHECK_STATUS(PressureBand::create(PressurePolarity::positive, mp(1000), mp(5000), mp(10001)),
               StatusCode::pressure_band_invalid);

  // A positive relationship must lie strictly above zero.
  CHECK_STATUS(PressureBand::create(PressurePolarity::positive, mp(0), mp(5000), mp(0)),
               StatusCode::pressure_band_invalid);
  CHECK_STATUS(PressureBand::create(PressurePolarity::positive, mp(-1000), mp(5000), mp(0)),
               StatusCode::pressure_band_invalid);
  CHECK_STATUS(PressureBand::create(PressurePolarity::positive, mp(-5000), mp(-1000), mp(0)),
               StatusCode::pressure_band_invalid);

  // A negative relationship must lie strictly below zero.
  CHECK_STATUS(PressureBand::create(PressurePolarity::negative, mp(-5000), mp(0), mp(0)),
               StatusCode::pressure_band_invalid);
  CHECK_STATUS(PressureBand::create(PressurePolarity::negative, mp(-5000), mp(1000), mp(0)),
               StatusCode::pressure_band_invalid);

  // A neutral relationship must straddle zero and stay inside the structural
  // neutral bound.
  CHECK_STATUS(PressureBand::create(PressurePolarity::neutral, mp(1), mp(5), mp(0)),
               StatusCode::pressure_band_invalid);
  CHECK_STATUS(PressureBand::create(PressurePolarity::neutral, mp(-5), mp(-1), mp(0)),
               StatusCode::pressure_band_invalid);
  CHECK_STATUS(PressureBand::create(PressurePolarity::neutral, mp(-50001), mp(0), mp(0)),
               StatusCode::pressure_band_invalid);
  CHECK_STATUS(PressureBand::create(PressurePolarity::neutral, mp(0), mp(50001), mp(0)),
               StatusCode::pressure_band_invalid);

  // A band outside the representable pressure range is refused before anything
  // is stored.
  CHECK_STATUS(PressureBand::create(PressurePolarity::positive, mp(100000001), mp(100000002), mp(0)),
               StatusCode::pressure_band_invalid);
  CHECK_STATUS(PressureBand::create(PressurePolarity::negative, mp(-100000002), mp(-100000001), mp(0)),
               StatusCode::pressure_band_invalid);
}

AIRFLOW_TEST(pressure_band_membership_is_exact_at_the_endpoints) {
  const PressureBand negative =
      PressureBand::create(PressurePolarity::negative, mp(-30000), mp(-10000), mp(2000)).value();
  CHECK(negative.contains(mp(-30000)));
  CHECK(negative.contains(mp(-10000)));
  CHECK(negative.contains(mp(-20000)));
  CHECK(!negative.contains(mp(-30001)));
  CHECK(!negative.contains(mp(-9999)));
  CHECK(!negative.contains(mp(0)));
  CHECK(negative.sign_matches(mp(-30000)));
  CHECK(negative.sign_matches(mp(-1)));
  CHECK(!negative.sign_matches(mp(0)));
  CHECK(!negative.sign_matches(mp(1)));
  CHECK(negative.satisfied_by(mp(-30000)));
  CHECK(negative.satisfied_by(mp(-20000)));
  CHECK(negative.satisfied_by(mp(-10000)));
  CHECK(!negative.satisfied_by(mp(-30001)));
  CHECK(!negative.satisfied_by(mp(10000)));
  CHECK(!negative.satisfied_by(mp(0)));

  const PressureBand positive =
      PressureBand::create(PressurePolarity::positive, mp(1000), mp(5000), mp(0)).value();
  CHECK(positive.contains(mp(1000)));
  CHECK(positive.contains(mp(5000)));
  CHECK(!positive.contains(mp(999)));
  CHECK(!positive.contains(mp(5001)));
  CHECK(positive.satisfied_by(mp(1000)));
  CHECK(positive.satisfied_by(mp(5000)));
  CHECK(!positive.satisfied_by(mp(0)));
  CHECK(!positive.satisfied_by(mp(-1000)));

  // A neutral band contains values of either sign, but only a differential of
  // exactly zero exhibits the neutral polarity.
  const PressureBand neutral =
      PressureBand::create(PressurePolarity::neutral, mp(-1000), mp(1000), mp(0)).value();
  CHECK(neutral.contains(mp(-1000)));
  CHECK(neutral.contains(mp(0)));
  CHECK(neutral.contains(mp(1000)));
  CHECK(!neutral.contains(mp(-1001)));
  CHECK(!neutral.contains(mp(1001)));
  CHECK(neutral.sign_matches(mp(0)));
  CHECK(!neutral.sign_matches(mp(-1000)));
  CHECK(!neutral.sign_matches(mp(1000)));
  CHECK(neutral.satisfied_by(mp(0)));
  CHECK(!neutral.satisfied_by(mp(-1000)));
  CHECK(!neutral.satisfied_by(mp(1000)));
  CHECK(!neutral.satisfied_by(mp(-1)));
}

AIRFLOW_TEST(classification_at_the_three_signs) {
  CHECK_EQ(classify(mp(-1)), PressurePolarity::negative);
  CHECK_EQ(classify(mp(0)), PressurePolarity::neutral);
  CHECK_EQ(classify(mp(1)), PressurePolarity::positive);
  CHECK_EQ(classify(mp(-100000)), PressurePolarity::negative);
  CHECK_EQ(classify(mp(100000)), PressurePolarity::positive);

  const PressurePolarity polarities[] = {PressurePolarity::positive, PressurePolarity::negative,
                                         PressurePolarity::neutral};
  for (const PressurePolarity polarity : polarities) {
    const std::string_view rendered = to_string(polarity);
    CHECK(!rendered.empty());
    const std::optional<PressurePolarity> parsed = parse_pressure_polarity(rendered);
    REQUIRE(parsed.has_value());
    CHECK(parsed.value() == polarity);
  }
  CHECK(std::string_view(to_string(PressurePolarity::positive)) !=
        std::string_view(to_string(PressurePolarity::negative)));
  CHECK(!parse_pressure_polarity("Positive").has_value());
  CHECK(!parse_pressure_polarity("").has_value());
}

AIRFLOW_TEST(containment_rank_is_a_policy_order_not_the_enum_order) {
  // Listed in increasing safety precedence, which is deliberately not the
  // numeric order of the enumeration.
  const ContainmentState states[] = {ContainmentState::intact, ContainmentState::open_for_service,
                                     ContainmentState::unknown, ContainmentState::breached};
  for (std::size_t left = 0; left < 4; ++left) {
    for (std::size_t right = left + 1; right < 4; ++right) {
      // Distinct ranks: the order is strict and total.
      CHECK(containment_rank(states[left]) != containment_rank(states[right]));
      CHECK(containment_rank(states[left]) < containment_rank(states[right]));
    }
  }
  CHECK_EQ(containment_rank(ContainmentState::intact), std::uint32_t{0});
  CHECK_EQ(containment_rank(ContainmentState::open_for_service), std::uint32_t{1});
  CHECK_EQ(containment_rank(ContainmentState::unknown), std::uint32_t{2});
  CHECK_EQ(containment_rank(ContainmentState::breached), std::uint32_t{3});

  // The enum numbers breached below unknown; the safety order puts the breach
  // above the unknown. A caller that sorted by enum value would get this
  // backwards, which is why the rank function exists.
  // (Read through non-const locals so the comparison is a runtime one.)
  std::uint32_t breached_number = static_cast<std::uint32_t>(ContainmentState::breached);
  std::uint32_t unknown_number = static_cast<std::uint32_t>(ContainmentState::unknown);
  CHECK(breached_number < unknown_number);
  CHECK(containment_rank(ContainmentState::breached) >
        containment_rank(ContainmentState::unknown));
  CHECK(containment_rank(ContainmentState::open_for_service) >
        containment_rank(ContainmentState::intact));

  CHECK(is_known(ContainmentState::intact));
  CHECK(is_known(ContainmentState::open_for_service));
  CHECK(is_known(ContainmentState::breached));
  CHECK(!is_known(ContainmentState::unknown));
}

AIRFLOW_TEST(containment_and_obligation_tokens_round_trip) {
  const ContainmentState states[] = {ContainmentState::intact, ContainmentState::open_for_service,
                                     ContainmentState::breached, ContainmentState::unknown};
  for (const ContainmentState state : states) {
    const std::string_view rendered = to_string(state);
    CHECK(!rendered.empty());
    const std::optional<ContainmentState> parsed = parse_containment_state(rendered);
    REQUIRE(parsed.has_value());
    CHECK(parsed.value() == state);
  }
  const ContainmentKind kinds[] = {
      ContainmentKind::aisle_containment, ContainmentKind::rack_chimney,
      ContainmentKind::blanking_panel,    ContainmentKind::subfloor_barrier,
      ContainmentKind::door,              ContainmentKind::other,
  };
  for (const ContainmentKind kind : kinds) {
    const std::string_view rendered = to_string(kind);
    CHECK(!rendered.empty());
    const std::optional<ContainmentKind> parsed = parse_containment_kind(rendered);
    REQUIRE(parsed.has_value());
    CHECK(parsed.value() == kind);
  }
  CHECK_EQ(to_string(ContainmentKind::blanking_panel), std::string_view("blanking_panel"));

  const ObligationScope scopes[] = {ObligationScope::room, ObligationScope::row,
                                    ObligationScope::rack};
  for (const ObligationScope scope : scopes) {
    const std::string_view rendered = to_string(scope);
    CHECK(!rendered.empty());
    REQUIRE(parse_obligation_scope(rendered).has_value());
    CHECK(parse_obligation_scope(rendered).value() == scope);
  }
  const ObligationClass classes[] = {ObligationClass::protected_obligation,
                                     ObligationClass::advisory};
  for (const ObligationClass klass : classes) {
    const std::string_view rendered = to_string(klass);
    CHECK(!rendered.empty());
    REQUIRE(parse_obligation_class(rendered).has_value());
    CHECK(parse_obligation_class(rendered).value() == klass);
  }
  const ObligationBinding bindings[] = {ObligationBinding::metered_scope,
                                        ObligationBinding::device_sum};
  for (const ObligationBinding binding : bindings) {
    const std::string_view rendered = to_string(binding);
    CHECK(!rendered.empty());
    REQUIRE(parse_obligation_binding(rendered).has_value());
    CHECK(parse_obligation_binding(rendered).value() == binding);
  }
  CHECK_EQ(to_string(ObligationClass::protected_obligation), std::string_view("protected"));
  CHECK_EQ(to_string(ObligationBinding::metered_scope), std::string_view("metered"));
  CHECK_EQ(to_string(ObligationBinding::device_sum), std::string_view("device_sum"));

  // An out-of-range numeric value renders as an explicit non-token and does not
  // parse back into a real state.
  CHECK_EQ(to_string(static_cast<ContainmentState>(4)), std::string_view("unknown"));
  CHECK(!parse_containment_state("4").has_value());
  CHECK_EQ(to_string(static_cast<ContainmentKind>(6)), std::string_view("other"));
  CHECK(!parse_containment_kind("6").has_value());
  CHECK_EQ(to_string(static_cast<ObligationScope>(3)), std::string_view("unknown"));
  CHECK(!parse_obligation_scope("").has_value());
  CHECK(!parse_obligation_class("protected_obligation").has_value());
}

AIRFLOW_TEST(envelope_validation_refuses_invalid_envelopes) {
  CHECK(validate_envelope(legal_envelope()).ok());

  // The default may sit exactly on either edge of the interval.
  OperatingEnvelope at_min = legal_envelope();
  at_min.default_fan_percent = at_min.min_fan_percent;
  CHECK(validate_envelope(at_min).ok());
  OperatingEnvelope at_max = legal_envelope();
  at_max.default_fan_percent = at_max.max_fan_percent;
  CHECK(validate_envelope(at_max).ok());

  OperatingEnvelope inverted_percent = legal_envelope();
  inverted_percent.min_fan_percent = percent(9000);
  inverted_percent.max_fan_percent = percent(2000);
  inverted_percent.default_fan_percent = percent(2000);
  CHECK_STATUS(validate_envelope(inverted_percent), StatusCode::limit_invalid);

  OperatingEnvelope default_below = legal_envelope();
  default_below.default_fan_percent = percent(1999);
  CHECK_STATUS(validate_envelope(default_below), StatusCode::limit_invalid);

  OperatingEnvelope default_above = legal_envelope();
  default_above.default_fan_percent = percent(9001);
  CHECK_STATUS(validate_envelope(default_above), StatusCode::limit_invalid);

  OperatingEnvelope inverted_airflow = legal_envelope();
  inverted_airflow.min_airflow = Airflow::from_cubic_metres_per_hour(9000);
  inverted_airflow.max_airflow = Airflow::from_cubic_metres_per_hour(2000);
  CHECK_STATUS(validate_envelope(inverted_airflow), StatusCode::limit_invalid);

  OperatingEnvelope huge_airflow = legal_envelope();
  huge_airflow.max_airflow = Airflow::from_cubic_metres_per_hour(
      PhysicalBounds::max_abs_airflow_cubic_metres_per_hour + 1);
  CHECK_STATUS(validate_envelope(huge_airflow), StatusCode::limit_invalid);

  OperatingEnvelope negative_huge_airflow = legal_envelope();
  negative_huge_airflow.min_airflow = Airflow::from_cubic_metres_per_hour(
      -PhysicalBounds::max_abs_airflow_cubic_metres_per_hour - 1);
  CHECK_STATUS(validate_envelope(negative_huge_airflow), StatusCode::limit_invalid);

  OperatingEnvelope at_representable_edge = legal_envelope();
  at_representable_edge.min_airflow = Airflow::from_cubic_metres_per_hour(
      -PhysicalBounds::max_abs_airflow_cubic_metres_per_hour);
  at_representable_edge.max_airflow = Airflow::from_cubic_metres_per_hour(
      PhysicalBounds::max_abs_airflow_cubic_metres_per_hour);
  CHECK(validate_envelope(at_representable_edge).ok());

  // A negative airflow bound is representable, so a fan policy that allows
  // reverse flow is not refused by shape validation.
  OperatingEnvelope reverse_flow = legal_envelope();
  reverse_flow.min_airflow = Airflow::from_cubic_metres_per_hour(-500);
  CHECK(validate_envelope(reverse_flow).ok());
}

AIRFLOW_TEST(intent_classification_covers_every_intent) {
  struct Expected {
    ControlIntent intent;
    RequestClass klass;
  };
  const Expected table[] = {
      {ControlIntent::hold_setpoint, RequestClass::protective},
      {ControlIntent::raise_airflow, RequestClass::protective},
      {ControlIntent::lower_airflow, RequestClass::optimization},
      {ControlIntent::restore_pressure_relationship, RequestClass::protective},
      {ControlIntent::rebalance_rows, RequestClass::optimization},
      {ControlIntent::trim_for_efficiency, RequestClass::optimization},
      {ControlIntent::emergency_purge, RequestClass::safety_directed},
      {ControlIntent::release_to_policy, RequestClass::protective},
  };
  std::size_t optimization = 0;
  std::size_t protective = 0;
  std::size_t safety_directed = 0;
  for (const Expected& row : table) {
    CHECK_EQ(classify(row.intent), row.klass);
    const std::string_view rendered = to_string(row.intent);
    CHECK(!rendered.empty());
    const std::optional<ControlIntent> parsed = parse_control_intent(rendered);
    REQUIRE(parsed.has_value());
    CHECK(parsed.value() == row.intent);
    switch (classify(row.intent)) {
      case RequestClass::optimization:
        ++optimization;
        break;
      case RequestClass::protective:
        ++protective;
        break;
      case RequestClass::safety_directed:
        ++safety_directed;
        break;
    }
  }
  // Every intent is classified, and every class is used.
  CHECK_EQ(optimization, std::size_t{3});
  CHECK_EQ(protective, std::size_t{4});
  CHECK_EQ(safety_directed, std::size_t{1});
  CHECK(std::string_view(to_string(RequestClass::optimization)) == "optimization");
  CHECK(std::string_view(to_string(RequestClass::protective)) == "protective");
  CHECK(std::string_view(to_string(RequestClass::safety_directed)) == "safety_directed");
  CHECK(!parse_control_intent("hold").has_value());
  CHECK(!parse_control_intent("").has_value());
}

AIRFLOW_TEST(intent_predicates_cover_every_intent) {
  const ControlIntent intents[] = {
      ControlIntent::hold_setpoint,          ControlIntent::raise_airflow,
      ControlIntent::lower_airflow,          ControlIntent::restore_pressure_relationship,
      ControlIntent::rebalance_rows,         ControlIntent::trim_for_efficiency,
      ControlIntent::emergency_purge,        ControlIntent::release_to_policy,
  };
  for (const ControlIntent intent : intents) {
    const bool optimization = classify(intent) == RequestClass::optimization;
    CHECK_EQ(requires_containment_proof(intent), optimization);
    CHECK_EQ(requires_pressure_proof(intent),
             intent == ControlIntent::restore_pressure_relationship);
    CHECK_EQ(requires_safety_permit(intent), classify(intent) == RequestClass::safety_directed);
    // Releasing to policy names no setpoint; every other intent commands one.
    CHECK_EQ(changes_setpoint(intent), !(intent == ControlIntent::release_to_policy));
  }
  // The predicates are exactly the documented sets, stated once more as sets so
  // a new intent cannot be added without deciding all four.
  CHECK(requires_pressure_proof(ControlIntent::restore_pressure_relationship));
  CHECK(requires_containment_proof(ControlIntent::lower_airflow));
  CHECK(requires_containment_proof(ControlIntent::rebalance_rows));
  CHECK(requires_containment_proof(ControlIntent::trim_for_efficiency));
  CHECK(!requires_containment_proof(ControlIntent::hold_setpoint));
  CHECK(!requires_containment_proof(ControlIntent::emergency_purge));
  CHECK(requires_safety_permit(ControlIntent::emergency_purge));
  CHECK(!requires_safety_permit(ControlIntent::raise_airflow));
}

AIRFLOW_TEST(required_action_covers_every_intent) {
  struct Expected {
    ControlIntent intent;
    ControlAction action;
  };
  const Expected table[] = {
      {ControlIntent::hold_setpoint, ControlAction::apply_setpoint},
      {ControlIntent::raise_airflow, ControlAction::raise_airflow},
      {ControlIntent::lower_airflow, ControlAction::lower_airflow},
      {ControlIntent::restore_pressure_relationship, ControlAction::change_pressure_target},
      {ControlIntent::rebalance_rows, ControlAction::apply_setpoint},
      {ControlIntent::trim_for_efficiency, ControlAction::lower_airflow},
      {ControlIntent::emergency_purge, ControlAction::emergency_purge},
      {ControlIntent::release_to_policy, ControlAction::release_hold},
  };
  for (const Expected& row : table) {
    CHECK_EQ(required_action(row.intent), row.action);
  }
  // Every action has a stable token that parses back to it, including the ones
  // no intent requires.
  for (std::uint32_t raw = 0; raw < kControlActionCount; ++raw) {
    const auto action = static_cast<ControlAction>(raw);
    const std::string_view rendered = to_string(action);
    CHECK(!rendered.empty());
    const std::optional<ControlAction> parsed = parse_control_action(rendered);
    REQUIRE(parsed.has_value());
    CHECK(parsed.value() == action);
  }
  CHECK_EQ(to_string(ControlAction::set_lifecycle), std::string_view("set_lifecycle"));
  CHECK(!parse_control_action("Set_Lifecycle").has_value());
  CHECK(!parse_control_action("").has_value());
}

AIRFLOW_TEST(action_set_masks_and_membership) {
  const std::uint32_t all_bits = (1u << kControlActionCount) - 1u;

  const ActionSet none = ActionSet::none();
  CHECK(none.empty());
  CHECK_EQ(none.mask(), std::uint32_t{0});
  CHECK(!none.contains(ControlAction::apply_setpoint));
  CHECK_EQ(none.to_string(), std::string(""));

  const ActionSet all = ActionSet::all();
  CHECK(!all.empty());
  CHECK_EQ(all.mask(), all_bits);
  for (std::uint32_t raw = 0; raw < kControlActionCount; ++raw) {
    CHECK(all.contains(static_cast<ControlAction>(raw)));
  }
  // Membership is bounded by the declared actions: an out-of-range value is
  // never a member, whatever the mask holds.
  CHECK(!all.contains(static_cast<ControlAction>(kControlActionCount)));
  CHECK(!all.contains(static_cast<ControlAction>(99)));
  CHECK(!none.contains(static_cast<ControlAction>(99)));

  // from_mask refuses a mask with any bit outside the declared actions, so a
  // set can never claim an authority this build does not implement.
  CHECK(ActionSet::from_mask(0).ok());
  CHECK(ActionSet::from_mask(all_bits).ok());
  CHECK(ActionSet::from_mask(1u << (kControlActionCount - 1)).ok());
  CHECK_STATUS(ActionSet::from_mask(1u << kControlActionCount), StatusCode::invalid_argument);
  CHECK_STATUS(ActionSet::from_mask(all_bits | (1u << kControlActionCount)),
               StatusCode::invalid_argument);
  CHECK_STATUS(ActionSet::from_mask(0xFFFFFFFFu), StatusCode::invalid_argument);
  CHECK_EQ(ActionSet::from_mask(0).value(), none);
  CHECK_EQ(ActionSet::from_mask(all_bits).value(), all);

  const ActionSet pair =
      ActionSet::of({ControlAction::apply_setpoint, ControlAction::lower_airflow});
  CHECK_EQ(pair.mask(), (1u << 0) | (1u << 2));
  CHECK(pair.contains(ControlAction::apply_setpoint));
  CHECK(pair.contains(ControlAction::lower_airflow));
  CHECK(!pair.contains(ControlAction::raise_airflow));
  CHECK(!pair.contains(ControlAction::change_pressure_target));
  CHECK(!pair.contains(ControlAction::set_policy));
  CHECK_EQ(pair.to_string(), std::string("apply_setpoint,lower_airflow"));
  CHECK_EQ(ActionSet::of({ControlAction::emergency_purge, ControlAction::apply_setpoint}).to_string(),
           std::string("apply_setpoint,emergency_purge"));
}

AIRFLOW_TEST(delivery_comparison_orders_like_kinds_only) {
  CHECK_EQ(compare_delivery(fan(5000), fan(5000)), DeliveryComparison::equal);
  CHECK_EQ(compare_delivery(fan(6000), fan(5000)), DeliveryComparison::increases);
  CHECK_EQ(compare_delivery(fan(4000), fan(5000)), DeliveryComparison::decreases);
  CHECK_EQ(compare_delivery(fan(10000), fan(0)), DeliveryComparison::increases);
  CHECK_EQ(compare_delivery(fan(0), fan(10000)), DeliveryComparison::decreases);

  CHECK_EQ(compare_delivery(airflow(100), airflow(100)), DeliveryComparison::equal);
  CHECK_EQ(compare_delivery(airflow(200), airflow(100)), DeliveryComparison::increases);
  CHECK_EQ(compare_delivery(airflow(50), airflow(100)), DeliveryComparison::decreases);
  CHECK_EQ(compare_delivery(airflow(-100), airflow(-50)), DeliveryComparison::decreases);

  // A percent request and an airflow request are not ordered by this runtime:
  // the fan curve that would relate them belongs to the device.
  CHECK_EQ(compare_delivery(fan(9000), airflow(1)), DeliveryComparison::incomparable);
  CHECK_EQ(compare_delivery(airflow(1), fan(9000)), DeliveryComparison::incomparable);
  CHECK_EQ(compare_delivery(fan(0), airflow(1000000)), DeliveryComparison::incomparable);
  CHECK_EQ(compare_delivery(airflow(0), fan(10000)), DeliveryComparison::incomparable);
}

AIRFLOW_TEST(supersession_rule_table) {
  const RequestClass classes[] = {RequestClass::optimization, RequestClass::protective,
                                  RequestClass::safety_directed};
  const DeliveryComparison comparisons[] = {
      DeliveryComparison::equal, DeliveryComparison::increases, DeliveryComparison::decreases,
      DeliveryComparison::incomparable,
  };
  for (const RequestClass klass : classes) {
    for (const DeliveryComparison comparison : comparisons) {
      for (const bool permit : {false, true}) {
        const bool emergency = klass == RequestClass::safety_directed && permit;
        const bool at_least_equal = comparison == DeliveryComparison::equal ||
                                    comparison == DeliveryComparison::increases;
        CHECK_EQ(supersession_permitted(klass, permit, comparison), emergency || at_least_equal);
      }
    }
  }

  // The safety-permit override is the only way a reducing or incomparable
  // command may take over an unresolved attempt.
  CHECK(supersession_permitted(RequestClass::safety_directed, true, DeliveryComparison::decreases));
  CHECK(supersession_permitted(RequestClass::safety_directed, true,
                               DeliveryComparison::incomparable));
  CHECK(supersession_permitted(RequestClass::safety_directed, true, DeliveryComparison::equal));
  CHECK(!supersession_permitted(RequestClass::safety_directed, false, DeliveryComparison::decreases));
  CHECK(!supersession_permitted(RequestClass::optimization, true, DeliveryComparison::decreases));
  CHECK(!supersession_permitted(RequestClass::protective, true, DeliveryComparison::decreases));
  CHECK(!supersession_permitted(RequestClass::protective, false, DeliveryComparison::incomparable));
  CHECK(supersession_permitted(RequestClass::optimization, false, DeliveryComparison::increases));
  CHECK(supersession_permitted(RequestClass::protective, false, DeliveryComparison::equal));
  CHECK(supersession_permitted(RequestClass::optimization, false, DeliveryComparison::equal));
}
