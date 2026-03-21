"""
Feature definitions for the driver stats dataset.

Feature Views:
  1. driver_efficiency   — conversion and acceptance rates (driver quality)
  2. driver_activity     — trip volume and time-based activity metrics
  3. driver_scoring (on-demand) — real-time composite score from efficiency + activity
"""

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, RequestSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64
import pandas as pd


# ============================================================
# Entity
# ============================================================

driver = Entity(
    name="driver",
    join_keys=["driver_id"],
    description="Unique driver identifier",
)

# ============================================================
# Data source
# ============================================================

driver_stats_source = FileSource(
    name="driver_stats_source",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# ============================================================
# Feature View 1: Driver Efficiency
# ============================================================

driver_efficiency_fv = FeatureView(
    name="driver_efficiency",
    entities=[driver],
    ttl=timedelta(days=365 * 3),
    schema=[
        Field(name="conv_rate", dtype=Float32, description="Conversion rate"),
        Field(name="acc_rate", dtype=Float32, description="Acceptance rate"),
    ],
    online=True,
    source=driver_stats_source,
    tags={"team": "driver_performance", "domain": "efficiency"},
)

# ============================================================
# Feature View 2: Driver Activity
# ============================================================

driver_activity_fv = FeatureView(
    name="driver_activity",
    entities=[driver],
    ttl=timedelta(days=365 * 3),
    schema=[
        Field(name="avg_daily_trips", dtype=Int64, description="Average daily trips"),
    ],
    online=True,
    source=driver_stats_source,
    tags={"team": "driver_performance", "domain": "activity"},
)

# ============================================================
# On-Demand Feature View: Driver Scoring
# ============================================================

# Request source for real-time input (e.g. current surge multiplier)
scoring_request = RequestSource(
    name="scoring_request",
    schema=[
        Field(name="surge_multiplier", dtype=Float64),
    ],
)


@on_demand_feature_view(
    sources=[driver_efficiency_fv, driver_activity_fv, scoring_request],
    schema=[
        Field(name="weighted_rating", dtype=Float64),
        Field(name="trip_score", dtype=Float64),
        Field(name="surge_adjusted_score", dtype=Float64),
    ],
)
def driver_scoring(inputs: pd.DataFrame) -> pd.DataFrame:
    """
    Compute real-time driver scoring based on efficiency + activity.

    - weighted_rating: 60% conversion + 40% acceptance
    - trip_score: weighted_rating scaled by log of trip volume
    - surge_adjusted_score: trip_score adjusted by current surge multiplier
    """
    import numpy as np

    df = pd.DataFrame()
    conv = inputs["conv_rate"].fillna(0.0)
    acc = inputs["acc_rate"].fillna(0.0)
    trips = inputs["avg_daily_trips"].fillna(0)

    df["weighted_rating"] = conv * 0.6 + acc * 0.4
    df["trip_score"] = df["weighted_rating"] * np.log1p(trips.astype(float))
    df["surge_adjusted_score"] = df["trip_score"] * inputs["surge_multiplier"]
    return df
