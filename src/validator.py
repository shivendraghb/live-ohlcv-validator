"""Four-layer OHLCV validation engine with rolling statistical analysis."""

import logging
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np

from src.schemas import (
    DataQualityReport,
    OHLCVBar,
    ValidationResult,
    ValidationStatus,
)

logger = logging.getLogger(__name__)


class OHLCVValidator:
    """
    Real-time OHLCV validator with four check layers:
    structural, logical, temporal, and statistical (rolling z-score).
    """

    def __init__(
        self,
        symbol: str,
        interval: str = "1m",
        window_size: int = 100,
        zscore_threshold: float = 3.0,
        max_gap_multiplier: float = 2.0,
    ):
        self.symbol = symbol
        self.interval = interval
        self.window_size = window_size
        self.zscore_threshold = zscore_threshold
        self.max_gap_multiplier = max_gap_multiplier

        self._price_history: deque[float] = deque(maxlen=window_size)
        self._volume_history: deque[float] = deque(maxlen=window_size)
        self._last_timestamp: Optional[datetime] = None
        self._bars_processed: int = 0
        self._report = DataQualityReport()
        self._expected_interval = self._parse_interval(interval)

        logger.info(
            f"Validator initialized | {symbol} @ {interval} | "
            f"window={window_size} | z-threshold={zscore_threshold}"
        )

    @staticmethod
    def _parse_interval(interval: str) -> timedelta:
        """Convert interval string ('1m', '5m', '1h', '1d') to timedelta."""
        multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        unit = interval[-1]
        value = int(interval[:-1])
        if unit not in multipliers:
            raise ValueError(
                f"Unknown interval unit '{unit}' in '{interval}'. "
                f"Use one of: {list(multipliers.keys())}"
            )
        return timedelta(seconds=value * multipliers[unit])

    def validate_bar(self, bar: OHLCVBar) -> ValidationResult:
        """Run all validation layers on a single OHLCV bar."""
        checks_passed: list[str] = []
        checks_failed: list[str] = []
        anomaly_score: Optional[float] = None

        structural_ok, structural_msg = self._check_structural(bar)
        if structural_ok:
            checks_passed.append("structural")
        else:
            checks_failed.append(f"structural: {structural_msg}")

        logical_ok, logical_msg = self._check_logical(bar)
        if logical_ok:
            checks_passed.append("logical_invariants")
        else:
            checks_failed.append(f"logical: {logical_msg}")

        temporal_ok, temporal_msg = self._check_temporal(bar)
        if temporal_ok:
            checks_passed.append("temporal_consistency")
        else:
            checks_failed.append(f"temporal: {temporal_msg}")

        stat_ok, stat_msg, z_score = self._check_statistical(bar)
        anomaly_score = z_score
        if stat_ok:
            checks_passed.append("statistical_sanity")
        else:
            checks_failed.append(f"statistical: {stat_msg}")

        if any("structural" in f or "logical" in f for f in checks_failed):
            status = ValidationStatus.INVALID
        elif checks_failed:
            status = ValidationStatus.WARNING
        else:
            status = ValidationStatus.VALID

        # Update state AFTER validation to prevent data leakage
        self._update_state(bar)
        self._update_report(status, checks_failed)

        result = ValidationResult(
            bar=bar,
            status=status,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
            anomaly_score=anomaly_score,
        )

        if status == ValidationStatus.INVALID:
            logger.warning(f"INVALID | {bar.timestamp} | {checks_failed}")
        elif status == ValidationStatus.WARNING:
            logger.info(f"WARNING | {bar.timestamp} | {checks_failed}")

        return result

    def _check_structural(self, bar: OHLCVBar) -> tuple[bool, str]:
        """Verify symbol match and reject future timestamps."""
        if bar.symbol != self.symbol:
            return False, f"Expected '{self.symbol}', got '{bar.symbol}'"

        now = datetime.now(timezone.utc)
        if bar.timestamp > now + timedelta(seconds=120):
            return False, (
                f"Timestamp {bar.timestamp} is in the future (UTC: {now})"
            )
        return True, "OK"

    def _check_logical(self, bar: OHLCVBar) -> tuple[bool, str]:
        """Defense-in-depth check for OHLC mathematical invariants."""
        errors = []
        if bar.high < bar.low:
            errors.append(f"high({bar.high}) < low({bar.low})")
        if bar.high < max(bar.open, bar.close):
            errors.append(
                f"high({bar.high}) < max(open,close)({max(bar.open, bar.close)})"
            )
        if bar.low > min(bar.open, bar.close):
            errors.append(
                f"low({bar.low}) > min(open,close)({min(bar.open, bar.close)})"
            )
        if errors:
            return False, "; ".join(errors)
        return True, "OK"

    def _check_temporal(self, bar: OHLCVBar) -> tuple[bool, str]:
        """Verify monotonic timestamps and detect time gaps."""
        if self._last_timestamp is None:
            return True, "OK (first bar)"

        if bar.timestamp <= self._last_timestamp:
            return False, (
                f"Non-monotonic: {bar.timestamp} <= {self._last_timestamp}"
            )

        actual_gap = bar.timestamp - self._last_timestamp
        max_allowed_gap = self._expected_interval * self.max_gap_multiplier
        if actual_gap > max_allowed_gap:
            missed = actual_gap / self._expected_interval
            return False, (
                f"Time gap detected: {actual_gap} "
                f"(~{missed:.1f} bars missing). "
                f"Expected interval: {self._expected_interval}"
            )
        return True, "OK"

    def _check_statistical(
        self, bar: OHLCVBar
    ) -> tuple[bool, str, Optional[float]]:
        """Rolling z-score anomaly detection on price and volume."""
        min_history = 20
        if len(self._price_history) < min_history:
            return True, f"OK (warmup: {len(self._price_history)}/{min_history})", None

        prices = np.array(self._price_history)
        mean_p, std_p = np.mean(prices), np.std(prices)
        if std_p == 0:
            return True, "OK (zero variance)", 0.0

        z_score = abs(bar.close - mean_p) / std_p
        if z_score > self.zscore_threshold:
            return False, (
                f"Price anomaly: close={bar.close}, mean={mean_p:.2f}, "
                f"std={std_p:.2f}, z-score={z_score:.2f} > threshold={self.zscore_threshold}"
            ), z_score

        volumes = np.array(self._volume_history)
        mean_v, std_v = np.mean(volumes), np.std(volumes)
        if std_v > 0:
            vol_z = abs(bar.volume - mean_v) / std_v
            if vol_z > self.zscore_threshold:
                return False, (
                    f"Volume anomaly: vol={bar.volume}, mean={mean_v:.2f}, z={vol_z:.2f}"
                ), z_score

        return True, "OK", z_score

    def _update_state(self, bar: OHLCVBar) -> None:
        self._price_history.append(bar.close)
        self._volume_history.append(bar.volume)
        self._last_timestamp = bar.timestamp
        self._bars_processed += 1

    def _update_report(self, status: ValidationStatus, failures: list[str]) -> None:
        self._report.total_bars += 1
        if status == ValidationStatus.VALID:
            self._report.valid_bars += 1
        elif status == ValidationStatus.INVALID:
            self._report.invalid_bars += 1
        elif status == ValidationStatus.WARNING:
            self._report.warning_bars += 1
        for f in failures:
            self._report.anomalies_detected.append(f"[Bar #{self._bars_processed}] {f}")

    def get_report(self) -> DataQualityReport:
        return self._report

    def reset(self) -> None:
        """Reset all validation state."""
        self._price_history.clear()
        self._volume_history.clear()
        self._last_timestamp = None
        self._bars_processed = 0
        self._report = DataQualityReport()
        logger.info(f"Validator reset for {self.symbol}")
