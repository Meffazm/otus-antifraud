"""Model loading and prediction for anti-fraud detection.

Loads LogisticRegression coefficients from a JSON file and performs
inference using numpy (sigmoid of linear combination).
"""

import json
from pathlib import Path

import numpy as np


class AntifraudModel:
    """Logistic regression model loaded from JSON coefficients."""

    def __init__(self, model_path: str | Path):
        """Load model from a JSON file.

        Expected JSON format:
            {
                "feature_names": ["f1", "f2", ...],
                "coefficients": [c1, c2, ...],
                "intercept": b
            }
        """
        with open(model_path) as f:
            data = json.load(f)

        self.feature_names: list[str] = data["feature_names"]
        self.coefficients: np.ndarray = np.array(data["coefficients"])
        self.intercept: float = float(data["intercept"])

    def predict_proba(self, features: list[float]) -> float:
        """Compute fraud probability using sigmoid(z).

        Args:
            features: feature vector in the same order as feature_names.

        Returns:
            Fraud probability (0.0 to 1.0).
        """
        features_array = np.array(features)
        z = np.dot(self.coefficients, features_array) + self.intercept
        probability = 1.0 / (1.0 + np.exp(-z))
        return float(probability)

    def predict(self, features: list[float]) -> tuple[int, float]:
        """Predict fraud label and probability.

        Returns:
            Tuple of (prediction, probability) where prediction is
            1 if probability > 0.5, else 0.
        """
        probability = self.predict_proba(features)
        prediction = 1 if probability > 0.5 else 0
        return prediction, probability
