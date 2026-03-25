"""Tests for feature engineering module."""

from datetime import datetime

import pytest

from app.features import extract_features


class TestExtractFeatures:
    """Test feature extraction matches PySpark pipeline behavior."""

    def test_returns_five_features(self):
        features = extract_features(100.0, "2026-03-22 14:30:00")
        assert len(features) == 5

    def test_tx_amount_passthrough(self):
        features = extract_features(150.0, "2026-03-22 14:30:00")
        assert features[0] == 150.0

    def test_tx_amount_is_float(self):
        features = extract_features(100, "2026-03-22 14:30:00")
        assert isinstance(features[0], float)

    def test_hour_of_day(self):
        features = extract_features(100.0, "2026-03-22 14:30:00")
        assert features[1] == 14.0

    def test_hour_midnight(self):
        features = extract_features(100.0, "2026-03-22 00:15:00")
        assert features[1] == 0.0

    def test_hour_end_of_day(self):
        features = extract_features(100.0, "2026-03-22 23:59:00")
        assert features[1] == 23.0

    def test_day_of_week_sunday(self):
        # 2026-03-22 is a Sunday
        features = extract_features(100.0, "2026-03-22 12:00:00")
        # PySpark: Sunday = 1
        assert features[2] == 1.0

    def test_day_of_week_monday(self):
        # 2026-03-23 is a Monday
        features = extract_features(100.0, "2026-03-23 12:00:00")
        # PySpark: Monday = 2
        assert features[2] == 2.0

    def test_day_of_week_saturday(self):
        # 2026-03-28 is a Saturday
        features = extract_features(100.0, "2026-03-28 12:00:00")
        # PySpark: Saturday = 7
        assert features[2] == 7.0

    def test_day_of_week_wednesday(self):
        # 2026-03-25 is a Wednesday
        features = extract_features(100.0, "2026-03-25 12:00:00")
        # PySpark: Wednesday = 4
        assert features[2] == 4.0

    def test_is_weekend_sunday(self):
        # 2026-03-22 is a Sunday (PySpark dow=1)
        features = extract_features(100.0, "2026-03-22 12:00:00")
        assert features[3] == 1.0

    def test_is_weekend_saturday(self):
        # 2026-03-28 is a Saturday (PySpark dow=7)
        features = extract_features(100.0, "2026-03-28 12:00:00")
        assert features[3] == 1.0

    def test_is_weekday(self):
        # 2026-03-23 is a Monday
        features = extract_features(100.0, "2026-03-23 12:00:00")
        assert features[3] == 0.0

    def test_is_night_early_morning(self):
        features = extract_features(100.0, "2026-03-22 03:00:00")
        assert features[4] == 1.0

    def test_is_night_boundary_zero(self):
        features = extract_features(100.0, "2026-03-22 00:00:00")
        assert features[4] == 1.0

    def test_is_night_boundary_six(self):
        features = extract_features(100.0, "2026-03-22 06:00:00")
        assert features[4] == 1.0

    def test_is_not_night_seven(self):
        features = extract_features(100.0, "2026-03-22 07:00:00")
        assert features[4] == 0.0

    def test_is_not_night_afternoon(self):
        features = extract_features(100.0, "2026-03-22 14:00:00")
        assert features[4] == 0.0

    def test_accepts_datetime_object(self):
        dt = datetime(2026, 3, 22, 14, 30, 0)
        features = extract_features(100.0, dt)
        assert features[1] == 14.0

    def test_feature_order(self):
        # 2026-03-22 is a Sunday, 03:00 is night
        features = extract_features(250.0, "2026-03-22 03:00:00")
        assert features == [250.0, 3.0, 1.0, 1.0, 1.0]

    def test_weekday_daytime(self):
        # 2026-03-24 is a Tuesday, 10:00 is not night
        features = extract_features(50.0, "2026-03-24 10:00:00")
        # tx_amount=50.0, hour=10, dow=3 (Tuesday in PySpark), weekend=0, night=0
        assert features == [50.0, 10.0, 3.0, 0.0, 0.0]
