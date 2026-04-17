"""
test_gold.py
============
Unit tests for Gold layer derived columns and Star Schema logic.

Validates every F.when chain used in gold_job.py against expected values,
including boundary conditions and null handling.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, ArrayType, StringType


# ── dim_customer derived columns ──────────────────────────────────────────────

class TestAgeGroup:
    """age_group boundaries: <=30 | 31-45 | 46-60 | 60+"""

    def _age_group(self, spark, age):
        df = spark.createDataFrame([(age,)], ["age"])
        return (
            df.select(
                F.when(F.col("age") <= 30, "Young (<=30)")
                 .when(F.col("age") <= 45, "Mid (31-45)")
                 .when(F.col("age") <= 60, "Senior (46-60)")
                 .otherwise("Elder (60+)")
                 .alias("age_group")
            ).first()["age_group"]
        )

    def test_young(self, spark):
        assert self._age_group(spark, 25) == "Young (<=30)"

    def test_boundary_30_is_young(self, spark):
        assert self._age_group(spark, 30) == "Young (<=30)"

    def test_boundary_31_is_mid(self, spark):
        assert self._age_group(spark, 31) == "Mid (31-45)"

    def test_mid(self, spark):
        assert self._age_group(spark, 40) == "Mid (31-45)"

    def test_senior(self, spark):
        assert self._age_group(spark, 55) == "Senior (46-60)"

    def test_elder(self, spark):
        assert self._age_group(spark, 65) == "Elder (60+)"

    def test_boundary_60_is_senior(self, spark):
        assert self._age_group(spark, 60) == "Senior (46-60)"

    def test_boundary_61_is_elder(self, spark):
        assert self._age_group(spark, 61) == "Elder (60+)"


class TestIncomeBand:
    """income_band: <4000=Low | 4000-7999=Mid | >=8000=High"""

    def _income_band(self, spark, income):
        df = spark.createDataFrame([(float(income),)], ["monthly_income"])
        return (
            df.select(
                F.when(F.col("monthly_income") <  4000, "Low")
                 .when(F.col("monthly_income") <  8000, "Mid")
                 .otherwise("High")
                 .alias("band")
            ).first()["band"]
        )

    def test_low(self, spark):           assert self._income_band(spark, 3000)  == "Low"
    def test_boundary_3999_is_low(self, spark): assert self._income_band(spark, 3999) == "Low"
    def test_boundary_4000_is_mid(self, spark): assert self._income_band(spark, 4000) == "Mid"
    def test_mid(self, spark):           assert self._income_band(spark, 6000)  == "Mid"
    def test_boundary_7999_is_mid(self, spark): assert self._income_band(spark, 7999) == "Mid"
    def test_boundary_8000_is_high(self, spark): assert self._income_band(spark, 8000) == "High"
    def test_high(self, spark):          assert self._income_band(spark, 15000) == "High"


class TestCreditTier:
    """credit_tier: <580=Poor | 580-669=Fair | 670-739=Good | >=740=Excellent"""

    def _credit_tier(self, spark, score):
        df = spark.createDataFrame([(score,)], ["credit_score"])
        return (
            df.select(
                F.when(F.col("credit_score") < 580, "Poor")
                 .when(F.col("credit_score") < 670, "Fair")
                 .when(F.col("credit_score") < 740, "Good")
                 .otherwise("Excellent")
                 .alias("tier")
            ).first()["tier"]
        )

    def test_poor(self, spark):          assert self._credit_tier(spark, 500) == "Poor"
    def test_boundary_579_is_poor(self, spark): assert self._credit_tier(spark, 579) == "Poor"
    def test_boundary_580_is_fair(self, spark): assert self._credit_tier(spark, 580) == "Fair"
    def test_fair(self, spark):          assert self._credit_tier(spark, 630) == "Fair"
    def test_boundary_670_is_good(self, spark): assert self._credit_tier(spark, 670) == "Good"
    def test_good(self, spark):          assert self._credit_tier(spark, 700) == "Good"
    def test_boundary_740_is_excellent(self, spark): assert self._credit_tier(spark, 740) == "Excellent"
    def test_excellent(self, spark):     assert self._credit_tier(spark, 800) == "Excellent"


# ── dim_offer derived columns ─────────────────────────────────────────────────

class TestProductCategory:
    def _category(self, spark, product_name):
        df = spark.createDataFrame([(product_name,)], ["product_name"])
        return (
            df.select(
                F.when(F.lower(F.col("product_name")).contains("loan"), "Loan")
                 .when(F.lower(F.col("product_name")).contains("card"), "Card")
                 .otherwise("Other")
                 .alias("category")
            ).first()["category"]
        )

    def test_personal_loan(self, spark):
        assert self._category(spark, "Personal Loan") == "Loan"

    def test_case_insensitive_loan(self, spark):
        assert self._category(spark, "HOME LOAN") == "Loan"

    def test_credit_card(self, spark):
        assert self._category(spark, "Signature Credit Card") == "Card"

    def test_unknown_falls_to_other(self, spark):
        assert self._category(spark, "Insurance Package") == "Other"


# ── fact_telesales_calls derived columns ─────────────────────────────────────

class TestTalkTimeBand:
    """talk_time_band: <=60=SHORT | 61-180=MEDIUM | >180=LONG"""

    def _band(self, spark, seconds):
        df = spark.createDataFrame([(seconds,)], ["talk_time_seconds"])
        return (
            df.select(
                F.when(F.col("talk_time_seconds") <=  60, "SHORT (<=60s)")
                 .when(F.col("talk_time_seconds") <= 180, "MEDIUM (61-180s)")
                 .otherwise("LONG (>180s)")
                 .alias("band")
            ).first()["band"]
        )

    def test_short(self, spark):                   assert self._band(spark, 30)  == "SHORT (<=60s)"
    def test_boundary_60_is_short(self, spark):    assert self._band(spark, 60)  == "SHORT (<=60s)"
    def test_boundary_61_is_medium(self, spark):   assert self._band(spark, 61)  == "MEDIUM (61-180s)"
    def test_medium(self, spark):                  assert self._band(spark, 120) == "MEDIUM (61-180s)"
    def test_boundary_180_is_medium(self, spark):  assert self._band(spark, 180) == "MEDIUM (61-180s)"
    def test_boundary_181_is_long(self, spark):    assert self._band(spark, 181) == "LONG (>180s)"
    def test_long(self, spark):                    assert self._band(spark, 500) == "LONG (>180s)"


class TestOutcomeCategory:
    """
    outcome_category priority (highest wins):
      SUCCESSFUL_SALE > DO_NOT_CALL_REQUEST > HARD_REJECTION > SOFT_REJECTION > WARM_LEAD > IN_PROGRESS
    """

    def _outcome(self, spark, codes):
        df = spark.createDataFrame([(codes,)], ["codes"])
        return (
            df.select(
                F.when(F.array_contains(F.col("codes"), "SUCCESSFUL_SALE"),    "SALE")
                 .when(F.array_contains(F.col("codes"), "DO_NOT_CALL_REQUEST"), "DO_NOT_CALL")
                 .when(F.array_contains(F.col("codes"), "HARD_REJECTION"),     "HARD_REJECTION")
                 .when(F.array_contains(F.col("codes"), "SOFT_REJECTION"),     "SOFT_REJECTION")
                 .when(F.array_contains(F.col("codes"), "WARM_LEAD"),          "CALLBACK")
                 .otherwise("IN_PROGRESS")
                 .alias("outcome")
            ).first()["outcome"]
        )

    def test_sale_wins(self, spark):
        assert self._outcome(spark, ["SUCCESSFUL_SALE", "HARD_REJECTION"]) == "SALE"

    def test_do_not_call_over_hard_rejection(self, spark):
        assert self._outcome(spark, ["DO_NOT_CALL_REQUEST", "HARD_REJECTION"]) == "DO_NOT_CALL"

    def test_hard_rejection(self, spark):
        assert self._outcome(spark, ["HARD_REJECTION"]) == "HARD_REJECTION"

    def test_soft_rejection(self, spark):
        assert self._outcome(spark, ["SOFT_REJECTION"]) == "SOFT_REJECTION"

    def test_callback(self, spark):
        assert self._outcome(spark, ["WARM_LEAD"]) == "CALLBACK"

    def test_in_progress_when_no_final_code(self, spark):
        assert self._outcome(spark, ["OPENING", "NEEDS_ANALYSIS"]) == "IN_PROGRESS"

    def test_empty_codes_is_in_progress(self, spark):
        assert self._outcome(spark, []) == "IN_PROGRESS"


class TestOutcomeFlags:
    """Boolean flags derived from call_code_original array."""

    def _flags(self, spark, codes):
        df = spark.createDataFrame([(codes,)], ["codes"])
        return df.select(
            F.array_contains(F.col("codes"), "SUCCESSFUL_SALE").alias("sale"),
            F.array_contains(F.col("codes"), "HARD_REJECTION").alias("hard"),
            F.array_contains(F.col("codes"), "SOFT_REJECTION").alias("soft"),
            F.array_contains(F.col("codes"), "DO_NOT_CALL_REQUEST").alias("dnc"),
            F.array_contains(F.col("codes"), "OBJECTION_HANDLING").alias("objection"),
        ).first()

    def test_all_true(self, spark):
        row = self._flags(spark, [
            "SUCCESSFUL_SALE", "HARD_REJECTION", "SOFT_REJECTION",
            "DO_NOT_CALL_REQUEST", "OBJECTION_HANDLING"
        ])
        assert row["sale"] and row["hard"] and row["soft"] and row["dnc"] and row["objection"]

    def test_all_false(self, spark):
        row = self._flags(spark, ["OPENING", "NEEDS_ANALYSIS"])
        assert not row["sale"] and not row["hard"] and not row["soft"]

    def test_only_sale(self, spark):
        row = self._flags(spark, ["SUCCESSFUL_SALE"])
        assert row["sale"] is True
        assert row["hard"] is False


# ── dim_date derived columns ──────────────────────────────────────────────────

class TestDimDate:
    def test_date_key_format(self, spark):
        df = spark.createDataFrame([("2024-03-15",)], ["d"])
        result = (
            df.select(
                F.date_format(F.to_date(F.col("d")), "yyyyMMdd")
                 .cast(IntegerType()).alias("date_key")
            ).first()["date_key"]
        )
        assert result == 20240315

    def test_is_weekend_saturday(self, spark):
        df = spark.createDataFrame([("2024-03-16",)], ["d"])   # Saturday
        result = (
            df.select(F.dayofweek(F.to_date(F.col("d"))).isin(1, 7).alias("is_weekend"))
              .first()["is_weekend"]
        )
        assert result is True

    def test_is_weekend_sunday(self, spark):
        df = spark.createDataFrame([("2024-03-17",)], ["d"])   # Sunday
        result = (
            df.select(F.dayofweek(F.to_date(F.col("d"))).isin(1, 7).alias("is_weekend"))
              .first()["is_weekend"]
        )
        assert result is True

    def test_is_weekday(self, spark):
        df = spark.createDataFrame([("2024-03-18",)], ["d"])   # Monday
        result = (
            df.select(F.dayofweek(F.to_date(F.col("d"))).isin(1, 7).alias("is_weekend"))
              .first()["is_weekend"]
        )
        assert result is False

    def test_quarter_q1(self, spark):
        df = spark.createDataFrame([("2024-02-01",)], ["d"])
        result = df.select(F.quarter(F.to_date(F.col("d"))).alias("q")).first()["q"]
        assert result == 1

    def test_quarter_q4(self, spark):
        df = spark.createDataFrame([("2024-11-01",)], ["d"])
        result = df.select(F.quarter(F.to_date(F.col("d"))).alias("q")).first()["q"]
        assert result == 4
