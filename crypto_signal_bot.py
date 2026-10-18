"""Binance Spot Early Momentum Scanner — v4.0 (Full Upgrade)

Binance Spot is the ONLY signal universe. Binance Futures is used only as
confirmation/market intelligence (OI, funding, liquidation, taker pressure).
DeFiLlama / CoinGecko used for market-cap, sector tags and light on-chain checks.

Core Score = 100 (early-momentum weighted) + bonuses / penalties:
  Relative volume spike         18
  Price acceleration            10
  Spot taker-buy pressure       12
  Timeframes 5m/15m/30m/1h/4h   3/5/7/7/3 (25 total)
  Compression/breakout           8
  Market-cap turnover            8
  Resistance room                7
  EMA structure                  5
  Futures OI                     3
  DeFiLlama/on-chain             2
  Funding                        1
  Liquidation context            1
  Order-book imbalance         +0–6 bonus (new)
  Early-pump combo             +0–8 bonus (new)
  OI/Funding divergence        penalty / soft-block (new)
  BTC regime                   multiplier 0.70–1.00 (stronger)

Visibility:
  <45  -> invisible
  45-49 -> EARLY RADAR
  50-59 -> WATCH
  60-80 -> BUY WATCH
  81-90 -> STRONG SIGNAL
  91+   -> STRONG+

Any late / overheated / near-resistance / hard-divergence setup is blocked.

v4.0 upgrades:
  - Scan-level kline cache (big speed win)
  - Smarter Stage-1 + pre-rank by relative volume & acceleration
  - Order-book imbalance (spot depth)
  - OI + Funding divergence filter
  - Early-pump combo detector
  - Stronger BTC regime + optional hard filter
  - Sector / category tags (CoinGecko)
  - Enhanced stats + simple outcome summary
  - Concurrent evaluation with controlled workers
"""

from __future__ import annotations

import json
import math
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
TG_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT: str = os.getenv("TELEGRAM_CHAT_ID", "")
SQUARE_KEY: str = os.getenv("BINANCE_SQUARE_OPENAPI_KEY", "").strip()
POST_TO_SQUARE: bool = os.getenv("POST_TO_SQUARE", "false").lower() == "true"
SQUARE_MIN_SCORE: float = float(os.getenv("SQUARE_MIN_SCORE", "60"))

STATE_FILE: str = os.getenv("STATE_FILE", "state_binance.json")
SCAN_EVERY_MIN: int = int(os.getenv("SCAN_EVERY_MIN", "5"))
TICK_SECONDS: int = int(os.getenv("TICK_SECONDS", "60"))
MIN_24H_VOL: float = float(os.getenv("BINANCE_MIN_24H_VOLUME_USD", "100000"))
STAGE1_TOP: int = int(os.getenv("STAGE1_TOP", "120"))
PRE_RANK_N: int = int(os.getenv("PRE_RANK_N", "80"))          # after quick volume/accel
DEEP_N: int = int(os.getenv("DEEP_SCAN_COUNT", "45"))
SIGNAL_MIN: float = float(os.getenv("SIGNAL_MIN_SCORE", "45"))
COOLDOWN_MIN: int = int(os.getenv("ALERT_COOLDOWN_MIN", "720"))
ENABLE_DEFILLAMA: bool = os.getenv("ENABLE_DEFILLAMA", "true").lower() == "true"
ENABLE_FUTURES: bool = os.getenv("ENABLE_FUTURES_CONFIRMATION", "true").lower() == "true"
ENABLE_ORDERBOOK: bool = os.getenv("ENABLE_ORDERBOOK", "true").lower() == "true"
ENABLE_SECTOR: bool = os.getenv("ENABLE_SECTOR", "true").lower() == "true"
BTC_HARD_FILTER: bool = os.getenv("BTC_HARD_FILTER", "true").lower() == "true"

EVAL_WORKERS: int = max(1, min(12, int(os.getenv("EVAL_WORKERS", "8"))))
EVAL_DELAY_SEC: float = float(os.getenv("EVAL_DELAY_SEC", "0.07"))
REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "15"))
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "2"))
DEPTH_LIMIT: int = int(os.getenv("DEPTH_LIMIT", "20"))

BINANCE_SPOT: List[str] = [
    "https://data-api.binance.vision",
    "https://api-gcp.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
    "https://api.binance.com",
]
FAPI_BASE: str = "https://fapi.binance.com"

STABLES = {

...
