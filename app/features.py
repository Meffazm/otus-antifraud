"""Feature engineering for anti-fraud transaction detection.

Replicates the PySpark feature pipeline in pure Python.
Features: tx_amount, hour_of_day, day_of_week, is_weekend, is_night.
"""

from datetime import datetime
from typing import Union


def extract_features(tx_amount: float, tx_datetime: Union[str, datetime]) -> list[float]:
    """Extract feature vector from transaction data.

    Matches PySpark pipeline feature engineering exactly:
    - hour_of_day: hour extracted from tx_datetime (0-23)
    - day_of_week: PySpark convention (1=Sunday, 2=Monday, ..., 7=Saturday)
    - is_weekend: 1.0 if Sunday (1) or Saturday (7), else 0.0
    - is_night: 1.0 if hour is between 0 and 6 inclusive, else 0.0

    Returns feature vector in order:
        [tx_amount, hour_of_day, day_of_week, is_weekend, is_night]
    """
    if isinstance(tx_datetime, str):
        dt = datetime.strptime(tx_datetime, "%Y-%m-%d %H:%M:%S")
    else:
        dt = tx_datetime

    hour_of_day = float(dt.hour)

    # PySpark dayofweek: 1=Sunday, 2=Monday, ..., 7=Saturday
    # Python isoweekday: 1=Monday, ..., 7=Sunday
    day_of_week = float((dt.isoweekday() % 7) + 1)

    is_weekend = 1.0 if day_of_week in (1.0, 7.0) else 0.0
    is_night = 1.0 if 0 <= dt.hour <= 6 else 0.0

    return [float(tx_amount), hour_of_day, day_of_week, is_weekend, is_night]
