"""Tests for FastAPI endpoints."""

import math

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)


class TestHealthEndpoint:
    """Test GET /health endpoint."""

    def test_health_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_ok(self, client):
        response = client.get("/health")
        assert response.json() == {"status": "ok"}


class TestPredictEndpoint:
    """Test POST /predict endpoint."""

    def test_predict_returns_200(self, client):
        payload = {
            "transaction_id": 12345,
            "tx_amount": 150.0,
            "tx_datetime": "2026-03-22 14:30:00",
        }
        response = client.post("/predict", json=payload)
        assert response.status_code == 200

    def test_predict_returns_transaction_id(self, client):
        payload = {
            "transaction_id": 12345,
            "tx_amount": 150.0,
            "tx_datetime": "2026-03-22 14:30:00",
        }
        response = client.post("/predict", json=payload)
        data = response.json()
        assert data["transaction_id"] == 12345

    def test_predict_returns_prediction_field(self, client):
        payload = {
            "tx_amount": 150.0,
            "tx_datetime": "2026-03-22 14:30:00",
        }
        response = client.post("/predict", json=payload)
        data = response.json()
        assert "prediction" in data
        assert data["prediction"] in (0, 1)

    def test_predict_returns_fraud_probability(self, client):
        payload = {
            "tx_amount": 150.0,
            "tx_datetime": "2026-03-22 14:30:00",
        }
        response = client.post("/predict", json=payload)
        data = response.json()
        assert "fraud_probability" in data
        assert 0.0 <= data["fraud_probability"] <= 1.0

    def test_predict_transaction_id_optional(self, client):
        payload = {
            "tx_amount": 100.0,
            "tx_datetime": "2026-03-22 14:30:00",
        }
        response = client.post("/predict", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["transaction_id"] is None

    def test_predict_high_amount_is_fraud(self, client):
        # Large amount should trigger fraud prediction with dummy model
        payload = {
            "tx_amount": 1000.0,
            "tx_datetime": "2026-03-22 14:30:00",
        }
        response = client.post("/predict", json=payload)
        data = response.json()
        assert data["prediction"] == 1

    def test_predict_low_amount_is_legit(self, client):
        # Small amount should be predicted as legitimate
        payload = {
            "tx_amount": 1.0,
            "tx_datetime": "2026-03-23 10:00:00",
        }
        response = client.post("/predict", json=payload)
        data = response.json()
        assert data["prediction"] == 0

    def test_predict_missing_tx_amount(self, client):
        payload = {
            "tx_datetime": "2026-03-22 14:30:00",
        }
        response = client.post("/predict", json=payload)
        assert response.status_code == 422

    def test_predict_missing_tx_datetime(self, client):
        payload = {
            "tx_amount": 100.0,
        }
        response = client.post("/predict", json=payload)
        assert response.status_code == 422

    def test_predict_known_values(self, client):
        # Verify exact probability for a known input
        # 2026-03-22 is Sunday, 14:30 is not night
        # features: [150.0, 14.0, 1.0, 1.0, 0.0]
        # z = 0.5*150 + (-0.1)*14 + 0.05*1 + 0.2*1 + 0.3*0 + (-2.0)
        # z = 75 - 1.4 + 0.05 + 0.2 + 0 - 2.0 = 71.85
        # sigmoid(71.85) ~ 1.0
        payload = {
            "transaction_id": 1,
            "tx_amount": 150.0,
            "tx_datetime": "2026-03-22 14:30:00",
        }
        response = client.post("/predict", json=payload)
        data = response.json()
        assert data["prediction"] == 1
        assert data["fraud_probability"] > 0.99
