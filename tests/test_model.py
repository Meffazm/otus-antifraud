"""Tests for model loading and prediction."""

import json
import math
import tempfile
from pathlib import Path

import numpy as np
import pytest

from app.model import AntifraudModel


@pytest.fixture
def model_path(tmp_path):
    """Create a temporary model JSON file with known coefficients."""
    model_data = {
        "feature_names": ["tx_amount", "hour_of_day", "day_of_week", "is_weekend", "is_night"],
        "coefficients": [0.5, -0.1, 0.05, 0.2, 0.3],
        "intercept": -2.0,
    }
    path = tmp_path / "model.json"
    path.write_text(json.dumps(model_data))
    return path


@pytest.fixture
def model(model_path):
    """Load the test model."""
    return AntifraudModel(model_path)


class TestModelLoading:
    """Test model loading from JSON."""

    def test_loads_feature_names(self, model):
        assert model.feature_names == [
            "tx_amount", "hour_of_day", "day_of_week", "is_weekend", "is_night"
        ]

    def test_loads_coefficients(self, model):
        np.testing.assert_array_almost_equal(
            model.coefficients, [0.5, -0.1, 0.05, 0.2, 0.3]
        )

    def test_loads_intercept(self, model):
        assert model.intercept == -2.0

    def test_raises_on_missing_file(self):
        with pytest.raises(FileNotFoundError):
            AntifraudModel("/nonexistent/model.json")

    def test_raises_on_invalid_json(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("not json")
        with pytest.raises(json.JSONDecodeError):
            AntifraudModel(path)


class TestPredictProba:
    """Test probability prediction (sigmoid of linear combination)."""

    def test_known_features(self, model):
        # features: [100.0, 14.0, 1.0, 1.0, 0.0]
        # z = 0.5*100 + (-0.1)*14 + 0.05*1 + 0.2*1 + 0.3*0 + (-2.0)
        # z = 50 - 1.4 + 0.05 + 0.2 + 0 - 2.0 = 46.85
        # sigmoid(46.85) ~ 1.0
        prob = model.predict_proba([100.0, 14.0, 1.0, 1.0, 0.0])
        assert prob == pytest.approx(1.0, abs=1e-6)

    def test_zero_features(self, model):
        # z = 0 + 0 + 0 + 0 + 0 + (-2.0) = -2.0
        # sigmoid(-2.0) = 1 / (1 + exp(2.0)) ~ 0.1192
        prob = model.predict_proba([0.0, 0.0, 0.0, 0.0, 0.0])
        expected = 1.0 / (1.0 + math.exp(2.0))
        assert prob == pytest.approx(expected, abs=1e-6)

    def test_returns_float(self, model):
        prob = model.predict_proba([1.0, 1.0, 1.0, 1.0, 1.0])
        assert isinstance(prob, float)

    def test_probability_in_range(self, model):
        prob = model.predict_proba([50.0, 12.0, 3.0, 0.0, 0.0])
        assert 0.0 <= prob <= 1.0

    def test_small_amount_low_probability(self, model):
        # Small tx_amount with large intercept offset => lower probability
        # z = 0.5*1 + (-0.1)*12 + 0.05*3 + 0.2*0 + 0.3*0 + (-2.0)
        # z = 0.5 - 1.2 + 0.15 + 0 + 0 - 2.0 = -2.55
        prob = model.predict_proba([1.0, 12.0, 3.0, 0.0, 0.0])
        expected = 1.0 / (1.0 + math.exp(2.55))
        assert prob == pytest.approx(expected, abs=1e-6)
        assert prob < 0.5


class TestPredict:
    """Test binary prediction with threshold at 0.5."""

    def test_high_amount_predicts_fraud(self, model):
        # Large tx_amount => large z => probability > 0.5
        prediction, prob = model.predict([100.0, 14.0, 1.0, 1.0, 0.0])
        assert prediction == 1
        assert prob > 0.5

    def test_low_amount_predicts_legit(self, model):
        # Small tx_amount => z < 0 => probability < 0.5
        prediction, prob = model.predict([1.0, 12.0, 3.0, 0.0, 0.0])
        assert prediction == 0
        assert prob < 0.5

    def test_returns_tuple(self, model):
        result = model.predict([1.0, 1.0, 1.0, 1.0, 1.0])
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_prediction_is_int(self, model):
        prediction, _ = model.predict([1.0, 1.0, 1.0, 1.0, 1.0])
        assert isinstance(prediction, int)

    def test_probability_is_float(self, model):
        _, prob = model.predict([1.0, 1.0, 1.0, 1.0, 1.0])
        assert isinstance(prob, float)
