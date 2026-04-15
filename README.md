# Live OHLCV Validation Framework

A production-grade, real-time market data validation engine that ingests live cryptocurrency OHLCV (Open, High, Low, Close, Volume) data via WebSocket streams and runs it through a multi-layered validation pipeline before storage.

Built as a portfolio project demonstrating quantitative finance data engineering, async programming, and software testing practices.

![Python](https://img.shields.io/badge/Python-3.11+-blue?style=flat-square&logo=python)
![Tests](https://img.shields.io/badge/Tests-37%20passing-brightgreen?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)

---

## Architecture

```
[Binance WebSocket] → [Ingestion Engine] → [Validation Pipeline] → [Storage Layer]
                                                    │                      │
                                                    ▼                      ▼
                                              [Console Output]      [Parquet / CSV]
                                                                         │
                                                                         ▼
                                                                  [Streamlit Dashboard]
```

### Validation Pipeline (4 Layers)

| Layer | Check | What It Catches |
|-------|-------|----------------|
| **Structural** | Schema, types, symbol match, future timestamps | Malformed data, wrong streams, clock skew |
| **Logical** | `High ≥ max(O,C)`, `Low ≤ min(O,C)`, `H ≥ L` | Corrupt candlestick data |
| **Temporal** | Monotonic timestamps, gap detection | Duplicates, missing bars, out-of-order data |
| **Statistical** | Rolling Z-score on price and volume | Flash crashes, wash trading, data glitches |

---

## Tech Stack

| Technology | Purpose | Why This Choice |
|-----------|---------|----------------|
| **Python 3.11+** | Core language | Industry standard for quant finance |
| **Pydantic v2** | Runtime data validation | Type-safe schemas with custom validators |
| **WebSockets** | Real-time data streaming | Push-based, low-latency market data delivery |
| **Pandas + NumPy** | Data manipulation & statistics | Vectorized operations for rolling z-scores |
| **PyArrow (Parquet)** | Data storage | 10-50x faster and smaller than CSV for time-series |
| **Pytest** | Testing framework | Fixtures, parametrize, clean assertion syntax |
| **Streamlit** | Monitoring dashboard | Python-native web UI, zero frontend code |
| **asyncio** | Async I/O | Non-blocking WebSocket handling |

---

## Project Structure

```
quant_ohlcv_validator/
├── src/
│   ├── __init__.py        # Package marker
│   ├── schemas.py         # Pydantic OHLCV data models + validators
│   ├── validator.py       # Core 4-layer validation engine
│   ├── ingestion.py       # Async Binance WebSocket client
│   ├── storage.py         # Parquet/CSV persistence + anomaly logging
│   └── main.py            # Pipeline orchestrator (entry point)
├── tests/
│   ├── __init__.py
│   └── test_validator.py  # 37 unit tests across 9 test groups
├── data/
│   ├── clean/             # Validated OHLCV bars (Parquet)
│   ├── raw/               # Anomaly logs (CSV)
│   └── reports/           # Quality report snapshots (JSON)
├── dashboard.py           # Streamlit monitoring UI
├── requirements.txt       # Annotated dependencies
└── README.md              # This file
```

---

## Quick Start

### 1. Clone & Setup

```bash
cd quant_ohlcv_validator
python -m venv venv
.\venv\Scripts\activate        # Windows
# source venv/bin/activate     # macOS/Linux
pip install -r requirements.txt
```

### 2. Run Tests

```bash
python -m pytest tests/ -v
```

Expected output: **37 passed** across 9 test groups covering schema validation, structural checks, logical invariants, temporal consistency, statistical anomaly detection, status determination, quality reports, reset functionality, and interval parsing.

### 3. Start Live Pipeline

```bash
python -m src.main
```

This connects to Binance's free public WebSocket and begins validating live BTC/USDT 1-minute candles in real-time. You'll see output like:

```
  ✅ Bar #    1 | 14:30:00 | O= 60,123.45  H= 60,200.00  L= 60,050.00  C= 60,180.00 | Vol=    12.4567 | z=N/A  | Checks: 4✓ 0✗
  ✅ Bar #    2 | 14:31:00 | O= 60,180.00  H= 60,250.00  L= 60,100.00  C= 60,210.00 | Vol=     8.2341 | z=0.42 | Checks: 4✓ 0✗
```

### 4. Launch Dashboard (Optional)

In a separate terminal:

```bash
streamlit run dashboard.py
```

Opens at `http://localhost:8501` with live quality metrics, price charts, and anomaly logs.

---

## Key Design Decisions

### Why Pydantic (not dataclasses)?
Runtime type validation at the data boundary. When receiving data from external APIs, types are never guaranteed. Pydantic catches `string-where-float-should-be` errors before they corrupt the pipeline.

### Why Parquet (not CSV/SQLite)?
Columnar format optimized for analytics. Reading only the `close` column from 1M rows reads only that column from disk. CSV reads everything. Parquet is 10-50x smaller with built-in compression.

### Why WebSockets (not REST polling)?
Push-based, zero-waste delivery. REST polling wastes 99% of requests asking "any new data?" when the answer is "no." WebSockets maintain a persistent connection and push data instantly.

### Why deque for rolling windows?
Fixed-size ring buffer with O(1) append and automatic eviction. No manual index management, no memory leaks. The standard data structure for sliding window computations.

### Why update state AFTER validation?
Prevents data leakage — the current bar doesn't influence its own z-score calculation. Same principle as preventing look-ahead bias in backtesting.

---

## Testing Philosophy

Tests are organized into 9 groups covering every validation layer:

1. **Schema Validation** — Pydantic rejects bad types and impossible values
2. **Structural Checks** — Symbol mismatch, future timestamps
3. **Logical Invariants** — OHLC mathematical rules, edge cases (doji candles)
4. **Temporal Consistency** — Monotonicity, duplicates, gaps, jitter tolerance
5. **Statistical Anomaly** — Warmup period, normal ranges, spike detection
6. **Status Determination** — VALID/INVALID/WARNING logic
7. **Quality Reports** — Counter accuracy, validity rate edge cases
8. **Validator Reset** — Clean state restoration
9. **Interval Parsing** — Parametrized across multiple timeframes

---

## License

MIT — Free for personal and commercial use.
