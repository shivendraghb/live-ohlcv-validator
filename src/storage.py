"""Buffered data persistence — Parquet for clean bars, CSV for anomalies."""

import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.schemas import DataQualityReport, OHLCVBar, ValidationResult, ValidationStatus

logger = logging.getLogger(__name__)


class DataStorage:
    """
    Repository for validated OHLCV data and anomaly logs.
    Clean bars -> Parquet | Anomalies -> CSV | Reports -> JSON
    """

    def __init__(self, base_dir: str = "data", flush_threshold: int = 5):
        self.base_dir = Path(base_dir)
        self.clean_dir = self.base_dir / "clean"
        self.raw_dir = self.base_dir / "raw"
        self.reports_dir = self.base_dir / "reports"

        for d in [self.clean_dir, self.raw_dir, self.reports_dir]:
            d.mkdir(parents=True, exist_ok=True)

        self._clean_buffer: list[dict] = []
        self._anomaly_buffer: list[dict] = []
        self._flush_threshold = flush_threshold

        logger.info(f"Storage initialized at {self.base_dir.absolute()}")

    def save_result(self, result: ValidationResult) -> None:
        """Route validation result to clean storage or anomaly log."""
        bar_dict = self._bar_to_dict(result.bar)

        if result.status == ValidationStatus.VALID:
            self._clean_buffer.append(bar_dict)
            if len(self._clean_buffer) >= self._flush_threshold:
                self._flush_clean_data(result.bar.symbol)
        else:
            anomaly_dict = {
                **bar_dict,
                "status": result.status.value,
                "checks_failed": "; ".join(result.checks_failed),
                "anomaly_score": result.anomaly_score,
                "validated_at": result.validated_at.isoformat(),
            }
            self._anomaly_buffer.append(anomaly_dict)
            if len(self._anomaly_buffer) >= self._flush_threshold:
                self._flush_anomaly_log(result.bar.symbol)

    def _bar_to_dict(self, bar: OHLCVBar) -> dict:
        return {
            "timestamp": bar.timestamp.isoformat(),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "symbol": bar.symbol,
            "interval": bar.interval,
        }

    def _flush_clean_data(self, symbol: str) -> None:
        """Write buffered clean bars to Parquet."""
        if not self._clean_buffer:
            return

        new_df = pd.DataFrame(self._clean_buffer)
        filepath = self.clean_dir / f"{symbol.replace('/', '_')}_{self._clean_buffer[0].get('interval', '1m')}.parquet"

        try:
            if filepath.exists():
                existing_df = pd.read_parquet(filepath)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=["timestamp"], keep="last")
            else:
                combined_df = new_df

            combined_df.to_parquet(filepath, index=False, engine="pyarrow")
            logger.info(f"Flushed {len(self._clean_buffer)} bars to {filepath.name} (total: {len(combined_df)})")
        except Exception as e:
            logger.error(f"Failed to flush clean data: {e}")
        finally:
            self._clean_buffer.clear()

    def _flush_anomaly_log(self, symbol: str) -> None:
        """Append anomaly records to CSV."""
        if not self._anomaly_buffer:
            return

        filepath = self.raw_dir / f"anomalies_{symbol.replace('/', '_')}.csv"
        try:
            file_exists = filepath.exists()
            with open(filepath, mode="a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self._anomaly_buffer[0].keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerows(self._anomaly_buffer)
            logger.info(f"Logged {len(self._anomaly_buffer)} anomalies to {filepath.name}")
        except Exception as e:
            logger.error(f"Failed to flush anomaly log: {e}")
        finally:
            self._anomaly_buffer.clear()

    def save_quality_report(self, report: DataQualityReport, symbol: str) -> None:
        """Save quality report snapshot as JSON."""
        filepath = self.reports_dir / f"quality_{symbol.replace('/', '_')}.json"
        try:
            report_dict = report.model_dump()
            report_dict["validity_rate"] = report.validity_rate
            report_dict["generated_at"] = datetime.now(timezone.utc).isoformat()
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(report_dict, f, indent=2, default=str)
            logger.info(f"Quality report saved to {filepath.name}")
        except Exception as e:
            logger.error(f"Failed to save quality report: {e}")

    def flush_all(self, symbol: str) -> None:
        """Force-flush all buffers (call on shutdown)."""
        self._flush_clean_data(symbol)
        self._flush_anomaly_log(symbol)
        logger.info("All buffers flushed.")

    def get_clean_data(self, symbol: str, interval: str = "1m") -> Optional[pd.DataFrame]:
        filepath = self.clean_dir / f"{symbol.replace('/', '_')}_{interval}.parquet"
        if not filepath.exists():
            return None
        try:
            return pd.read_parquet(filepath)
        except Exception as e:
            logger.error(f"Failed to read clean data: {e}")
            return None

    def get_anomaly_log(self, symbol: str) -> Optional[pd.DataFrame]:
        filepath = self.raw_dir / f"anomalies_{symbol.replace('/', '_')}.csv"
        if not filepath.exists():
            return None
        try:
            return pd.read_csv(filepath)
        except Exception as e:
            logger.error(f"Failed to read anomaly log: {e}")
            return None
