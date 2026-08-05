"""Reusable data-quality validation rules.

A small, generic rule engine — not Great Expectations, not literal
`@dlt.expect` (see DECISIONS.md for why neither was used). Each `Rule` pairs
a plain-Python check (fast to unit test, the readable "spec") with an
equivalent Spark boolean expression (what actually runs against a
DataFrame/stream) for the same condition — same pure+Spark-native pattern
used throughout this project.

Violations are flagged, never used to drop a row — Silver/Gold keep every
record and decide what to do with flagged data; this module never deletes
anything.

This used to live inline inside transform/bronze_to_silver.py. Extracted
here so the rule engine is reusable (e.g. for REData rules later) instead
of being ESIOS-specific plumbing buried in one transform file, and so it's
ready to be wrapped in `@dlt.expect` decorators once this project actually
runs on Databricks — see DECISIONS.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Iterable

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


@dataclass(frozen=True)
class Rule:
    """A named condition. `python_check`/`spark_condition` must agree: both
    return True exactly when the record/row VIOLATES the rule.
    """

    name: str
    python_check: Callable[[dict], bool]
    spark_condition: Callable[[], "Column"]


def apply_rules(record: dict, rules: Iterable[Rule]) -> list[str]:
    """Plain-Python: names of every rule this record violates (empty if none)."""
    return [rule.name for rule in rules if rule.python_check(record)]


def rules_flags_column(rules: Iterable[Rule]) -> "Column":
    """Spark-native: one array<string> column with every violated rule's name."""
    from pyspark.sql import functions as F

    flags = [F.when(rule.spark_condition(), F.lit(rule.name)) for rule in rules]
    return F.array_except(F.array(*flags), F.array(F.lit(None).cast("string")))


def add_validation_flags(df: "DataFrame", rules: Iterable[Rule]) -> "DataFrame":
    """Convenience wrapper: add a `validation_flags` column for the given rules."""
    return df.withColumn("validation_flags", rules_flags_column(rules))


# --- ESIOS rules -------------------------------------------------------------
# Spec 4.3: "alerta si PVPC < 0 o > 700 €/MWh, si potencia solar nocturna >
# 10 MW, etc." Only these two explicit rules are implemented — the spec's
# "etc." leaves room for more, deliberately left as a TODO rather than
# guessed at. No equivalent range rules are specified for REData, so none
# are invented for it either.

PRICE_RANGE = (0.0, 700.0)
PRICE_INDICATOR_IDS = (1001, 600)  # PVPC, SPOT
SOLAR_INDICATOR_ID = 1295
NIGHT_SOLAR_MAX_MW = 10.0


def is_night_hour_utc(datetime_utc: datetime, local_utc_offset_hours: int = 2) -> bool:
    """Rough local-hour check for the sun-down window, used only for the solar sanity check.

    Uses a fixed CEST-ish offset rather than a full timezone/DST library —
    good enough for a sanity alert, not for anything that needs to be exact.
    """
    local_hour = (datetime_utc.hour + local_utc_offset_hours) % 24
    return local_hour < 6 or local_hour >= 22


def _price_out_of_range(record: dict) -> bool:
    low, high = PRICE_RANGE
    return record["indicator_id"] in PRICE_INDICATOR_IDS and not (low <= record["value"] <= high)


def _price_out_of_range_spark() -> "Column":
    from pyspark.sql import functions as F

    low, high = PRICE_RANGE
    return F.col("indicator_id").isin(*PRICE_INDICATOR_IDS) & ~F.col("value").between(low, high)


def _night_solar_above_threshold(record: dict) -> bool:
    return (
        record["indicator_id"] == SOLAR_INDICATOR_ID
        and is_night_hour_utc(record["datetime_utc"])
        and record["value"] > NIGHT_SOLAR_MAX_MW
    )


def _night_solar_above_threshold_spark() -> "Column":
    from pyspark.sql import functions as F

    local_hour = (F.hour("datetime_utc") + 2) % 24
    is_night = (local_hour < 6) | (local_hour >= 22)
    return (F.col("indicator_id") == SOLAR_INDICATOR_ID) & is_night & (F.col("value") > NIGHT_SOLAR_MAX_MW)


ESIOS_VALIDATION_RULES: tuple[Rule, ...] = (
    Rule(
        name=f"value_out_of_range_{PRICE_RANGE[0]}_{PRICE_RANGE[1]}",
        python_check=_price_out_of_range,
        spark_condition=_price_out_of_range_spark,
    ),
    Rule(
        name="night_solar_generation_above_threshold",
        python_check=_night_solar_above_threshold,
        spark_condition=_night_solar_above_threshold_spark,
    ),
)
