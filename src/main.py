"""Pipeline orchestrator — connects ingestion, validation, and storage."""

import asyncio
import logging
import sys
from datetime import datetime, timezone

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from src.ingestion import BinanceIngestionClient
from src.validator import OHLCVValidator
from src.storage import DataStorage
from src.schemas import ValidationStatus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-20s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data/validation.log", mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

SYMBOL = "BTC/USDT"
INTERVAL = "1m"
Z_SCORE_THRESHOLD = 3.0
WINDOW_SIZE = 100


def print_banner() -> None:
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║           LIVE OHLCV VALIDATION FRAMEWORK                    ║
    ║           ─────────────────────────────────                  ║
    ║           Real-time market data quality engine               ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)
    logger.info(f"Starting validation for {SYMBOL} @ {INTERVAL}")
    logger.info(f"Z-Score threshold: {Z_SCORE_THRESHOLD} | Window: {WINDOW_SIZE}")


def print_bar_summary(result, bar_count: int) -> None:
    bar = result.bar
    icons = {ValidationStatus.VALID: "✅", ValidationStatus.INVALID: "❌", ValidationStatus.WARNING: "⚠️ "}
    icon = icons.get(result.status, "?")
    z = f"z={result.anomaly_score:.2f}" if result.anomaly_score is not None else "z=N/A"

    print(
        f"  {icon} Bar #{bar_count:>5} | {bar.timestamp.strftime('%H:%M:%S')} | "
        f"O={bar.open:>10,.2f}  H={bar.high:>10,.2f}  "
        f"L={bar.low:>10,.2f}  C={bar.close:>10,.2f} | "
        f"Vol={bar.volume:>12,.4f} | {z} | "
        f"Checks: {len(result.checks_passed)}✓ {len(result.checks_failed)}✗"
    )
    for f in result.checks_failed:
        print(f"       └─ {f}")


def print_report(report, symbol: str) -> None:
    print("\n" + "=" * 64)
    print(f"  📊 DATA QUALITY REPORT — {symbol}")
    print("=" * 64)
    print(f"  Total bars processed:  {report.total_bars}")
    print(f"  ✅ Valid:              {report.valid_bars}")
    print(f"  ❌ Invalid:            {report.invalid_bars}")
    print(f"  ⚠️  Warnings:           {report.warning_bars}")
    print(f"  📈 Validity rate:      {report.validity_rate:.1f}%")
    if report.anomalies_detected:
        print(f"\n  Recent anomalies ({min(5, len(report.anomalies_detected))}):")
        for a in report.anomalies_detected[-5:]:
            print(f"    • {a}")
    print("=" * 64 + "\n")


async def run_pipeline() -> None:
    ingestion = BinanceIngestionClient(symbol=SYMBOL, interval=INTERVAL)
    validator = OHLCVValidator(
        symbol=ingestion.display_symbol, interval=INTERVAL,
        window_size=WINDOW_SIZE, zscore_threshold=Z_SCORE_THRESHOLD,
    )
    storage = DataStorage(base_dir="data")
    bar_count = 0

    try:
        print(f"\n  🔌 Connecting to Binance WebSocket...")
        print(f"  📡 Streaming {ingestion.display_symbol} @ {INTERVAL}")
        print(f"  ⏳ Waiting for first closed candle (up to {INTERVAL})...\n")

        async for bar in ingestion.stream():
            bar_count += 1
            result = validator.validate_bar(bar)
            print_bar_summary(result, bar_count)
            storage.save_result(result)

            if bar_count % 10 == 0:
                report = validator.get_report()
                print_report(report, ingestion.display_symbol)
                storage.save_quality_report(report, ingestion.display_symbol)

    except KeyboardInterrupt:
        logger.info("Shutdown requested (Ctrl+C)")
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
    finally:
        ingestion.stop()
        storage.flush_all(ingestion.display_symbol)
        final_report = validator.get_report()
        print_report(final_report, ingestion.display_symbol)
        storage.save_quality_report(final_report, ingestion.display_symbol)
        logger.info(
            f"Pipeline stopped. {bar_count} bars | "
            f"Validity: {final_report.validity_rate:.1f}%"
        )


if __name__ == "__main__":
    print_banner()
    try:
        asyncio.run(run_pipeline())
    except KeyboardInterrupt:
        print("\n  👋 Goodbye!\n")
