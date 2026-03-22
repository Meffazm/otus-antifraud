"""FastAPI application for anti-fraud transaction prediction."""

import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel
from starlette_exporter import PrometheusMiddleware, handle_metrics
from prometheus_client import Counter

from app.features import extract_features
from app.model import AntifraudModel

MODEL_PATH = os.getenv(
    "MODEL_PATH",
    str(Path(__file__).resolve().parent.parent / "model" / "model.json"),
)

app = FastAPI(title="Anti-Fraud Prediction Service")
app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics", handle_metrics)

PREDICTIONS_TOTAL = Counter("predictions_total", "Total predictions served")
FRAUD_PREDICTIONS_TOTAL = Counter("fraud_predictions_total", "Fraud predictions (prediction=1)")

model = AntifraudModel(MODEL_PATH)


class TransactionRequest(BaseModel):
    """Incoming transaction for fraud prediction."""

    transaction_id: Optional[int] = None
    tx_amount: float
    tx_datetime: str


class PredictionResponse(BaseModel):
    """Fraud prediction result."""

    transaction_id: Optional[int] = None
    prediction: int
    fraud_probability: float


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok"}


@app.post("/predict", response_model=PredictionResponse)
def predict(request: TransactionRequest):
    """Predict whether a transaction is fraudulent."""
    features = extract_features(request.tx_amount, request.tx_datetime)
    prediction, probability = model.predict(features)
    PREDICTIONS_TOTAL.inc()
    if prediction == 1:
        FRAUD_PREDICTIONS_TOTAL.inc()
    return PredictionResponse(
        transaction_id=request.transaction_id,
        prediction=prediction,
        fraud_probability=round(probability, 6),
    )
