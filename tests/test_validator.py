"""Comprehensive test suite for the OHLCV validation engine."""

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from src.schemas import OHLCVBar, ValidationStatus, DataQualityReport
from src.validator import OHLCVValidator


@pytest.fixture
def validator() -> OHLCVValidator:
    return OHLCVValidator(symbol="BTC/USDT", interval="1m", window_size=100, zscore_threshold=3.0)


@pytest.fixture
def sample_bar() -> OHLCVBar:
    return OHLCVBar(
        timestamp=datetime.now(timezone.utc) - timedelta(hours=1),
        open=60000.0, high=60100.0, low=59900.0, close=60050.0,
        volume=123.456, symbol="BTC/USDT", interval="1m",
    )


def make_bar(minute_offset: int = 0, timestamp=None, close: float = 60050.0,
             volume: float = 123.456, symbol: str = "BTC/USDT", **kwargs) -> OHLCVBar:
    if timestamp is None:
        base = datetime.now(timezone.utc) - timedelta(hours=1)
        timestamp = base + timedelta(minutes=minute_offset)
    return OHLCVBar(
        timestamp=timestamp, open=kwargs.get("open_", 60000.0),
        high=kwargs.get("high", 60100.0), low=kwargs.get("low", 59900.0),
        close=close, volume=volume, symbol=symbol, interval="1m",
    )


class TestOHLCVBarSchema:
    def test_valid_bar_creation(self, sample_bar):
        assert sample_bar.close == 60050.0

    def test_negative_price_rejected(self):
        with pytest.raises(ValidationError):
            OHLCVBar(timestamp=datetime.now(timezone.utc), open=-100.0,
                     high=60100.0, low=59900.0, close=60050.0, volume=123.456, symbol="BTC/USDT")

    def test_zero_price_rejected(self):
        with pytest.raises(ValidationError):
            OHLCVBar(timestamp=datetime.now(timezone.utc), open=0.0,
                     high=60100.0, low=59900.0, close=60050.0, volume=123.456, symbol="BTC/USDT")

    def test_negative_volume_rejected(self):
        with pytest.raises(ValidationError):
            OHLCVBar(timestamp=datetime.now(timezone.utc), open=60000.0,
                     high=60100.0, low=59900.0, close=60050.0, volume=-1.0, symbol="BTC/USDT")

    def test_zero_volume_accepted(self):
        bar = make_bar(volume=0.0)
        assert bar.volume == 0.0

    def test_high_less_than_low_rejected(self):
        with pytest.raises(ValidationError):
            OHLCVBar(timestamp=datetime.now(timezone.utc), open=60000.0,
                     high=59000.0, low=61000.0, close=60050.0, volume=123.456, symbol="BTC/USDT")

    def test_naive_timestamp_gets_utc(self):
        bar = OHLCVBar(timestamp=datetime(2026, 1, 1, 12, 0, 0), open=60000.0,
                       high=60100.0, low=59900.0, close=60050.0, volume=123.456, symbol="BTC/USDT")
        assert bar.timestamp.tzinfo == timezone.utc


class TestStructuralValidation:
    def test_valid_bar_passes(self, validator, sample_bar):
        result = validator.validate_bar(sample_bar)
        assert "structural" in result.checks_passed

    def test_wrong_symbol_detected(self, validator):
        result = validator.validate_bar(make_bar(symbol="ETH/USDT"))
        assert any("structural" in f for f in result.checks_failed)

    def test_future_timestamp_detected(self, validator):
        future_bar = make_bar(timestamp=datetime.now(timezone.utc) + timedelta(hours=1))
        result = validator.validate_bar(future_bar)
        assert any("structural" in f for f in result.checks_failed)


class TestLogicalValidation:
    def test_valid_ohlc_passes(self, validator, sample_bar):
        result = validator.validate_bar(sample_bar)
        assert "logical_invariants" in result.checks_passed

    def test_high_equals_close_is_valid(self, validator):
        bar = make_bar(close=60100.0, high=60100.0)
        result = validator.validate_bar(bar)
        assert "logical_invariants" in result.checks_passed

    def test_doji_candle_is_valid(self, validator):
        bar = OHLCVBar(timestamp=datetime.now(timezone.utc) - timedelta(hours=1),
                       open=60000.0, high=60000.0, low=60000.0, close=60000.0,
                       volume=5.0, symbol="BTC/USDT")
        result = validator.validate_bar(bar)
        assert "logical_invariants" in result.checks_passed


class TestTemporalValidation:
    def test_first_bar_always_passes(self, validator):
        result = validator.validate_bar(make_bar(minute_offset=0))
        assert "temporal_consistency" in result.checks_passed

    def test_sequential_bars_pass(self, validator):
        validator.validate_bar(make_bar(minute_offset=0))
        result = validator.validate_bar(make_bar(minute_offset=1))
        assert "temporal_consistency" in result.checks_passed

    def test_duplicate_timestamp_detected(self, validator):
        t = datetime.now(timezone.utc) - timedelta(hours=1)
        validator.validate_bar(make_bar(timestamp=t))
        result = validator.validate_bar(make_bar(timestamp=t))
        assert any("temporal" in f for f in result.checks_failed)

    def test_backwards_timestamp_detected(self, validator):
        validator.validate_bar(make_bar(minute_offset=5))
        result = validator.validate_bar(make_bar(minute_offset=0))
        assert any("temporal" in f for f in result.checks_failed)

    def test_large_gap_detected(self, validator):
        validator.validate_bar(make_bar(minute_offset=0))
        result = validator.validate_bar(make_bar(minute_offset=10))
        assert any("temporal" in f for f in result.checks_failed)

    def test_small_gap_allowed(self, validator):
        bar1 = make_bar(minute_offset=0)
        base = datetime.now(timezone.utc) - timedelta(hours=1)
        bar2 = make_bar(timestamp=base + timedelta(seconds=90))
        validator.validate_bar(bar1)
        result = validator.validate_bar(bar2)
        assert "temporal_consistency" in result.checks_passed


class TestStatisticalValidation:
    def test_warmup_period_skips_stats(self, validator):
        result = validator.validate_bar(make_bar(minute_offset=0))
        assert "statistical_sanity" in result.checks_passed

    def test_normal_price_after_warmup(self, validator):
        for i in range(25):
            validator.validate_bar(make_bar(minute_offset=i, close=60000.0 + i))
        result = validator.validate_bar(make_bar(minute_offset=25, close=60025.0))
        assert "statistical_sanity" in result.checks_passed

    def test_extreme_price_spike_detected(self, validator):
        for i in range(25):
            validator.validate_bar(make_bar(minute_offset=i, close=60000.0 + i * 2))
        result = validator.validate_bar(make_bar(minute_offset=25, close=99999.0, high=99999.0))
        assert any("statistical" in f for f in result.checks_failed)


class TestStatusDetermination:
    def test_all_checks_pass_gives_valid(self, validator, sample_bar):
        assert validator.validate_bar(sample_bar).status == ValidationStatus.VALID

    def test_structural_failure_gives_invalid(self, validator):
        assert validator.validate_bar(make_bar(symbol="ETH/USDT")).status == ValidationStatus.INVALID

    def test_temporal_warning_gives_warning(self, validator):
        validator.validate_bar(make_bar(minute_offset=0))
        result = validator.validate_bar(make_bar(minute_offset=10))
        assert result.status == ValidationStatus.WARNING


class TestQualityReport:
    def test_report_counts(self, validator):
        for i in range(5):
            validator.validate_bar(make_bar(minute_offset=i))
        assert validator.get_report().total_bars == 5

    def test_report_tracks_anomalies(self, validator):
        validator.validate_bar(make_bar(symbol="ETH/USDT"))
        assert len(validator.get_report().anomalies_detected) > 0

    def test_validity_rate(self):
        report = DataQualityReport(total_bars=10, valid_bars=8)
        assert report.validity_rate == 80.0

    def test_zero_division_safe(self):
        assert DataQualityReport().validity_rate == 0.0


class TestValidatorReset:
    def test_reset_clears_state(self, validator, sample_bar):
        validator.validate_bar(sample_bar)
        validator.reset()
        assert validator.get_report().total_bars == 0


class TestIntervalParsing:
    @pytest.mark.parametrize("interval,expected", [
        ("1m", 60), ("5m", 300), ("1h", 3600), ("1d", 86400), ("30s", 30), ("15m", 900),
    ])
    def test_valid_intervals(self, interval, expected):
        assert OHLCVValidator._parse_interval(interval).total_seconds() == expected

    def test_invalid_interval_raises(self):
        with pytest.raises(ValueError):
            OHLCVValidator._parse_interval("1x")
