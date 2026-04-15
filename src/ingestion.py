"""Async Binance WebSocket client for live OHLCV kline ingestion."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from src.schemas import OHLCVBar

logger = logging.getLogger(__name__)

BINANCE_WS_BASE = "wss://stream.binance.com:9443/ws"
MAX_RECONNECT_ATTEMPTS = 10
RECONNECT_DELAY_SECONDS = 5


class BinanceIngestionClient:
    """
    Async WebSocket client for Binance kline streams.
    Provides a clean async generator interface with auto-reconnect.
    Only emits closed (finalized) candles.
    """

    def __init__(self, symbol: str = "btcusdt", interval: str = "1m"):
        self._ws_symbol = symbol.lower().replace("/", "")
        self.display_symbol = (
            symbol.upper() if "/" in symbol
            else self._format_display(symbol)
        )
        self.interval = interval
        self._ws_url = f"{BINANCE_WS_BASE}/{self._ws_symbol}@kline_{interval}"
        self._running = False
        self._bars_received = 0

        logger.info(f"Ingestion client created: {self.display_symbol} | {self._ws_url}")

    @staticmethod
    def _format_display(symbol: str) -> str:
        """Convert 'btcusdt' -> 'BTC/USDT'."""
        symbol = symbol.upper()
        for quote in ["USDT", "BUSD", "USDC", "BTC", "ETH", "BNB"]:
            if symbol.endswith(quote) and len(symbol) > len(quote):
                return f"{symbol[:-len(quote)]}/{quote}"
        return symbol

    def _parse_kline_message(self, raw_msg: str) -> Optional[OHLCVBar]:
        """Parse a Binance kline WebSocket message. Returns None for non-closed candles."""
        try:
            data = json.loads(raw_msg)
            if data.get("e") != "kline":
                return None

            kline = data["k"]
            if not kline.get("x", False):
                return None

            bar = OHLCVBar(
                timestamp=datetime.fromtimestamp(kline["t"] / 1000, tz=timezone.utc),
                open=float(kline["o"]),
                high=float(kline["h"]),
                low=float(kline["l"]),
                close=float(kline["c"]),
                volume=float(kline["v"]),
                symbol=self.display_symbol,
                interval=self.interval,
            )
            self._bars_received += 1
            return bar

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Parse error: {e} | raw: {raw_msg[:200]}")
            return None

    async def stream(self) -> AsyncGenerator[OHLCVBar, None]:
        """Async generator yielding closed OHLCVBar objects with auto-reconnect."""
        self._running = True
        reconnect_attempts = 0

        while self._running and reconnect_attempts < MAX_RECONNECT_ATTEMPTS:
            try:
                logger.info(f"Connecting to {self._ws_url}...")
                async with websockets.connect(
                    self._ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    logger.info(f"Connected to Binance WebSocket for {self.display_symbol}")
                    reconnect_attempts = 0

                    async for message in ws:
                        if not self._running:
                            break
                        bar = self._parse_kline_message(message)
                        if bar is not None:
                            yield bar

            except ConnectionClosed as e:
                logger.warning(f"Connection closed: {e}. Reconnecting...")
            except Exception as e:
                logger.error(f"WebSocket error: {e}")

            reconnect_attempts += 1
            delay = min(RECONNECT_DELAY_SECONDS * (2 ** (reconnect_attempts - 1)), 60)
            logger.info(f"Reconnect {reconnect_attempts}/{MAX_RECONNECT_ATTEMPTS} in {delay}s...")
            await asyncio.sleep(delay)

        if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
            logger.critical(f"Max reconnect attempts ({MAX_RECONNECT_ATTEMPTS}) reached.")

    def stop(self) -> None:
        self._running = False
        logger.info("Ingestion stop requested.")

    @property
    def bars_received(self) -> int:
        return self._bars_received
