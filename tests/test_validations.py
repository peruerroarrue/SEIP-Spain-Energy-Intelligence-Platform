import json
from datetime import datetime, timezone

from seip.quality.validations import ESIOS_VALIDATION_RULES, Rule, apply_rules, is_night_hour_utc


def _raw(indicator_id, value, datetime_utc, geo_id=8741, geo_name="Península"):
    # Mirrors seip.transform.bronze_to_silver.parse_esios_bronze_record's output shape.
    return {
        "indicator_id": indicator_id,
        "value": value,
        "datetime_utc": datetime.fromisoformat(datetime_utc.replace("Z", "+00:00")),
        "geo_id": geo_id,
        "geo_name": geo_name,
    }


# --- generic rule engine -----------------------------------------------------


def test_apply_rules_returns_names_of_violated_rules_only():
    always_violates = Rule("always", python_check=lambda r: True, spark_condition=lambda: None)
    never_violates = Rule("never", python_check=lambda r: False, spark_condition=lambda: None)

    assert apply_rules({}, [always_violates, never_violates]) == ["always"]


def test_apply_rules_empty_when_no_rules_violated():
    never_violates = Rule("never", python_check=lambda r: False, spark_condition=lambda: None)
    assert apply_rules({}, [never_violates]) == []


def test_apply_rules_preserves_rule_order():
    rule_a = Rule("a", python_check=lambda r: True, spark_condition=lambda: None)
    rule_b = Rule("b", python_check=lambda r: True, spark_condition=lambda: None)
    assert apply_rules({}, [rule_a, rule_b]) == ["a", "b"]


# --- ESIOS rules (moved out of bronze_to_silver.py, same behavior) ---------


def test_is_night_hour_utc_night():
    # 23:00 UTC + 2h offset -> 01:00 local, clearly night.
    assert is_night_hour_utc(datetime(2026, 7, 30, 23, 0, 0)) is True


def test_is_night_hour_utc_daytime():
    # 11:00 UTC + 2h offset -> 13:00 local, clearly daytime.
    assert is_night_hour_utc(datetime(2026, 7, 30, 11, 0, 0)) is False


def test_esios_rules_flag_price_out_of_range():
    record = _raw(1001, -5.0, "2026-07-30T14:00:00Z")
    assert apply_rules(record, ESIOS_VALIDATION_RULES) == ["value_out_of_range_0.0_700.0"]


def test_esios_rules_no_flag_for_price_in_range():
    record = _raw(600, 120.0, "2026-07-30T14:00:00Z")
    assert apply_rules(record, ESIOS_VALIDATION_RULES) == []


def test_esios_rules_flag_night_solar_generation():
    record = _raw(1295, 15.0, "2026-07-30T23:00:00Z")
    assert apply_rules(record, ESIOS_VALIDATION_RULES) == ["night_solar_generation_above_threshold"]


def test_esios_rules_no_flag_for_daytime_solar():
    record = _raw(1295, 25000.0, "2026-07-30T11:00:00Z")
    assert apply_rules(record, ESIOS_VALIDATION_RULES) == []


def test_esios_rules_no_flag_for_low_night_solar():
    record = _raw(1295, 2.0, "2026-07-30T23:00:00Z")
    assert apply_rules(record, ESIOS_VALIDATION_RULES) == []


def test_esios_rules_no_rules_for_wind_indicator():
    record = _raw(551, 999999.0, "2026-07-30T23:00:00Z")
    assert apply_rules(record, ESIOS_VALIDATION_RULES) == []


def test_esios_rules_can_both_fire_independently():
    # Sanity check that the two rules don't interfere with each other: a
    # price violation for indicator 1001 must never touch the solar rule.
    price_record = _raw(1001, 999.0, "2026-07-30T12:00:00Z")
    assert apply_rules(price_record, ESIOS_VALIDATION_RULES) == ["value_out_of_range_0.0_700.0"]
