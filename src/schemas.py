"""Pydantic data models for OHLCV candle validation."""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class ValidationStatus(str, Enum):
    VALID = "valid"
    INVALID = "invalid"
    WARNING = "warning"


class OHLCVBar(BaseModel):
    """Single OHLCV candlestick bar with built-in invariant enforcement."""

    timestamp: datetime = Field(..., description="Bar open time (UTC)")
    open: float = Field(..., gt=0)
    high: float = Field(..., gt=0)
    low: float = Field(..., gt=0)
    close: float = Field(..., gt=0)
    volume: float = Field(..., ge=0)
    symbol: str = Field(..., description="Trading pair, e.g. 'BTC/USDT'")
    interval: str = Field(default="1m")

    @field_validator("timestamp")
    @classmethod
    def ensure_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v

    @model_validator(mode="after")
    def check_ohlc_invariants(self) -> "OHLCVBar":
        """Enforce: H >= max(O,C), L <= min(O,C), H >= L."""
        if self.high < self.low:
            raise ValueError(f"high ({self.high}) < low ({self.low})")
        if self.high < self.open:
            raise ValueError(f"high ({self.high}) < open ({self.open})")
        if self.high < self.close:
            raise ValueError(f"high ({self.high}) < close ({self.close})")
        if self.low > self.open:
            raise ValueError(f"low ({self.low}) > open ({self.open})")
        if self.low > self.close:
            raise ValueError(f"low ({self.low}) > close ({self.close})")
        return self


class ValidationResult(BaseModel):
    """Wraps an OHLCVBar with validation outcome and diagnostics."""

    bar: OHLCVBar
    status: ValidationStatus
    checks_passed: list[str] = Field(default_factory=list)
    checks_failed: list[str] = Field(default_factory=list)
    anomaly_score: Optional[float] = Field(default=None)
    validated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class DataQualityReport(BaseModel):
    """Aggregated data health metrics for a validation session."""

    total_bars: int = 0
    valid_bars: int = 0
    invalid_bars: int = 0
    warning_bars: int = 0
    anomalies_detected: list[str] = Field(default_factory=list)

    @property
    def validity_rate(self) -> float:
        if self.total_bars == 0:
            return 0.0
        return (self.valid_bars / self.total_bars) * 100
