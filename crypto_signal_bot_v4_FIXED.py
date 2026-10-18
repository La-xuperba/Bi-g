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
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD", "USDP", "DAI",
    "EUR", "TRY", "BRL", "USDE", "USDD", "PYUSD", "USTC",
}

INTERVALS = {"5m": "5m", "15m": "15m", "30m": "30m", "1h": "1h", "4h": "4h"}

# Core weights (sum = 100). Bonuses are added on top then clipped.
W: Dict[str, float] = {
    "5m": 3, "15m": 5, "30m": 7, "1h": 7, "4h": 3,
    "volume": 18, "turnover": 8, "oi": 3, "funding": 1,
    "liq": 1, "pressure": 12, "llama": 2, "res": 7,
    "accel": 10, "compression": 8, "ema": 5,
}
assert abs(sum(W.values()) - 100) < 1e-6

# ---------------------------------------------------------------------------
# Shared session + pacing
# ---------------------------------------------------------------------------
S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0 Binance-Early-Radar/4.0"})
_request_lock = threading.Lock()
_last_request_ts = 0.0


def log(msg: Any) -> None:
    text = str(msg)
    if TG_TOKEN:
        text = text.replace(TG_TOKEN, "***")
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {text}", flush=True)


def get_json(
    url: str,
    params: Optional[Dict] = None,
    retries: int = MAX_RETRIES,
    timeout: int = REQUEST_TIMEOUT,
) -> Optional[Any]:
    global _last_request_ts
    for attempt in range(retries + 1):
        try:
            with _request_lock:
                elapsed = time.time() - _last_request_ts
                if elapsed < 0.025:
                    time.sleep(0.025 - elapsed)
                _last_request_ts = time.time()
            r = S.get(url, params=params, timeout=timeout)
            if r.status_code == 451:
                return None
            if r.status_code == 429:
                time.sleep(1.2 + attempt * 1.5)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries:
                log(f"request failed {url.split('?')[0]}: {e}")
            else:
                time.sleep(0.4 + attempt * 0.35)
    return None


# ---------------------------------------------------------------------------
# Telegram / Square
# ---------------------------------------------------------------------------
def post_tg(text: str) -> bool:
    if not TG_TOKEN or not TG_CHAT:
        print(text)
        return False
    ok = True
    for i in range(0, len(text), 3900):
        try:
            r = S.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data={
                    "chat_id": TG_CHAT,
                    "text": text[i : i + 3900],
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            ok &= r.ok
        except Exception as e:
            log(f"Telegram error: {e}")
            ok = False
    return ok


def post_square(text: str) -> bool:
    if not POST_TO_SQUARE:
        return False
    if not SQUARE_KEY:
        log("Binance Square posting enabled but BINANCE_SQUARE_OPENAPI_KEY is missing")
        return False
    url = "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"
    headers = {
        "X-Square-OpenAPI-Key": SQUARE_KEY,
        "Content-Type": "application/json",
        "clienttype": "binanceSkill",
        "User-Agent": "Mozilla/5.0 Binance-Gems-Spot-Radar",
    }
    try:
        r = S.post(url, headers=headers, json={"bodyTextOnly": text}, timeout=20)
        try:
            data = r.json()
        except Exception:
            data = {}
        if r.ok and str(data.get("code", "")) == "000000":
            post_id = (data.get("data") or {}).get("id")
            log(
                "Binance Square post published"
                + (f": https://www.binance.com/square/post/{post_id}" if post_id else "")
            )
            return True
        log(
            f"Binance Square failed: HTTP {r.status_code}, "
            f"code={data.get('code', '?')}, msg={data.get('message') or data.get('msg')}"
        )
    except Exception as e:
        log(f"Binance Square request failed: {e}")
    return False


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------
def usd(x: Any) -> str:
    x = float(x or 0)
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(x) >= div:
            return f"${x / div:.2f}{suffix}"
    return f"${x:.2f}"


def trade_levels(it: Dict) -> Optional[Dict]:
    entry = float(it.get("price") or 0)
    d = it.get("detail", {})
    atr = float(d.get("atr15") or 0)
    if entry <= 0 or atr <= 0:
        return None
    risk = max(atr * 1.5, entry * 0.005)
    sl = max(entry - risk, entry * 0.5)
    room = d.get("room")
    targets = [entry + risk, entry + risk * 1.5, entry + risk * 2]
    if room is not None and 0 < float(room) < 10:
        resistance = entry * (1 + float(room) / 100)
        cap = resistance * 0.995
        targets = [x for x in targets if x < cap]
    return {
        "entry": entry,
        "sl": sl,
        "tps": targets,
        "risk_pct": (entry - sl) / entry * 100,
    }


def square_text(it: Dict, serial: Optional[int] = None) -> str:
    d = it.get("detail", {})
    score = it.get("score", 0)
    levels = trade_levels(it)
    level_text = "Entry: wait for confirmation\nTP/SL: unavailable\n"
    if levels:
        level_text = f"Entry ref: ${levels['entry']:.8g}\n"
        level_text += "".join(f"🎯 TP{i}: ${tp:.8g}\n" for i, tp in enumerate(levels["tps"], 1))
        level_text += f"🛑 SL: ${levels['sl']:.8g}\n"
        if not levels["tps"]:
            level_text += "Targets withheld: nearby resistance.\n"
    room = d.get("room")
    setup = "Early momentum watch; confirm volume + clean breakout."
    if room is not None and room < 2:
        setup = "Resistance very close — wait for confirmed breakout or skip."
    early = "🔥 EARLY PUMP COMBO detected\n" if it.get("early_pump") else ""
    return (
        f"💎 BINANCE GEMS | SPOT MOMENTUM ALERT\n"
        f"Signal #{serial if serial is not None else '—'}\n\n"
        f"{early}"
        f"${it['base']} — {it['stage']}\n"
        f"Score: {score:.0f}/100\n"
        f"Price: ${it['price']:.8g}\n"
        f"24h Change: {it['change24']:+.1f}% | 24h Volume: {usd(it['volume24'])}\n\n"
        f"{level_text}\n"
        f"Setup: {setup}\n\n"
        f"Levels are ATR-based estimates, not guarantees.\n"
        f"#BinanceGems #SpotTrading #${it['base']}"
    )


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
def load_state() -> Dict:
    try:
        with open(STATE_FILE) as f:
            st = json.load(f)
    except Exception:
        st = {}
    st.setdefault("last_scan", 0)
    st.setdefault("alerts", {})
    st.setdefault("oi", {})
    st.setdefault("cg", {})
    st.setdefault("market_caps", {})
    st.setdefault("sectors", {})
    st.setdefault("signal_seq", 0)
    st.setdefault("open_signals", {})
    st.setdefault("stats", {
        "sent": 0, "tp1": 0, "sl": 0, "ambiguous": 0,
        "early_pump_sent": 0, "blocked_divergence": 0,
    })
    return st


def save_state(st: Dict) -> None:
    now = time.time()
    st["alerts"] = {k: v for k, v in st["alerts"].items() if now - v.get("ts", 0) < 3 * 86400}
    cutoff = now - 30 * 86400
    st["open_signals"] = {
        k: v
        for k, v in st.get("open_signals", {}).items()
        if v.get("status") == "OPEN" or v.get("closed_at", v.get("ts", 0)) > cutoff
    }
    with open(STATE_FILE, "w") as f:
        json.dump(st, f)


# ---------------------------------------------------------------------------
# Technical helpers
# ---------------------------------------------------------------------------
def ema(values: List[float], n: int) -> List[float]:
    if not values:
        return []
    a = 2 / (n + 1)
    out = [values[0]]
    for x in values[1:]:
        out.append(a * x + (1 - a) * out[-1])
    return out


def rsi(values: List[float], n: int = 14) -> float:
    if len(values) < n + 2:
        return 50.0
    gains, losses = [], []
    for a, b in zip(values, values[1:]):
        d = b - a
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains[:n]) / n
    avg_loss = sum(losses[:n]) / n
    for i in range(n, len(gains)):
        avg_gain = (avg_gain * (n - 1) + gains[i]) / n
        avg_loss = (avg_loss * (n - 1) + losses[i]) / n
    if avg_loss == 0:
        return 100.0
    return 100.0 - 100.0 / (1 + avg_gain / avg_loss)


def sma(values: List[float], n: int) -> Optional[float]:
    if len(values) < n:
        return None
    return sum(values[-n:]) / n


def rows(kl: Any) -> List[Dict]:
    if not isinstance(kl, list):
        return []
    now_ms = time.time() * 1000
    out = []
    for k in kl:
        if len(k) < 8 or float(k[6]) > now_ms:
            continue
        out.append({
            "t": int(k[0]),
            "o": float(k[1]),
            "h": float(k[2]),
            "l": float(k[3]),
            "c": float(k[4]),
            "v": float(k[5]),
            "q": float(k[7]),
            "tb": float(k[9]) if len(k) > 9 else 0.0,
        })
    return out


def linear_change(closes: List[float], n: int) -> float:
    if len(closes) <= n or not closes[-1 - n]:
        return 0.0
    return (closes[-1] / closes[-1 - n] - 1) * 100


# ---------------------------------------------------------------------------
# Kline cache (scan-level)
# ---------------------------------------------------------------------------
class KlineCache:
    """Thread-safe in-memory cache for one scan cycle."""

    def __init__(self) -> None:
        self._data: Dict[Tuple[str, str], List[Dict]] = {}
        self._lock = threading.Lock()

    def get(self, sym: str, tf: str) -> Optional[List[Dict]]:
        with self._lock:
            return self._data.get((sym, tf))

    def set(self, sym: str, tf: str, candles: List[Dict]) -> None:
        with self._lock:
            self._data[(sym, tf)] = candles

    def __len__(self) -> int:
        return len(self._data)


# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------
def spot_klines(
    sym: str,
    tf: str,
    limit: int = 120,
    cache: Optional[KlineCache] = None,
) -> List[Dict]:
    if cache is not None:
        hit = cache.get(sym, tf)
        if hit is not None:
            return hit
    for base in BINANCE_SPOT:
        data = get_json(
            f"{base}/api/v3/klines",
            {"symbol": sym, "interval": tf, "limit": limit},
        )
        if isinstance(data, list) and len(data) > 20:
            candles = rows(data)
            if cache is not None:
                cache.set(sym, tf, candles)
            return candles
    if cache is not None:
        cache.set(sym, tf, [])
    return []


def spot_depth(sym: str, limit: int = DEPTH_LIMIT) -> Optional[Dict]:
    """Top-of-book depth for imbalance calculation."""
    for base in BINANCE_SPOT:
        data = get_json(
            f"{base}/api/v3/depth",
            {"symbol": sym, "limit": limit},
            retries=1,
            timeout=8,
        )
        if isinstance(data, dict) and data.get("bids") and data.get("asks"):
            return data
    return None


def orderbook_imbalance(sym: str) -> Tuple[float, float, str]:
    """
    Returns (score_bonus 0–6, imbalance_ratio, note).
    imbalance_ratio = bid_qty / (bid_qty + ask_qty) on top N levels.
    >0.55 = mild buy pressure, >0.62 = strong.
    """
    if not ENABLE_ORDERBOOK:
        return 0.0, 0.5, "orderbook disabled"
    depth = spot_depth(sym)
    if not depth:
        return 0.0, 0.5, "orderbook unavailable"
    try:
        bid_qty = sum(float(b[1]) for b in depth["bids"][:DEPTH_LIMIT])
        ask_qty = sum(float(a[1]) for a in depth["asks"][:DEPTH_LIMIT])
        total = bid_qty + ask_qty
        if total <= 0:
            return 0.0, 0.5, "orderbook empty"
        ratio = bid_qty / total
        if ratio >= 0.68:
            return 6.0, ratio, f"orderbook strong bid ({ratio:.0%})"
        if ratio >= 0.60:
            return 4.0, ratio, f"orderbook bid-heavy ({ratio:.0%})"
        if ratio >= 0.55:
            return 2.0, ratio, f"orderbook mild bid ({ratio:.0%})"
        if ratio <= 0.38:
            return 0.0, ratio, f"orderbook ask-heavy ({ratio:.0%}) — caution"
        return 0.0, ratio, f"orderbook balanced ({ratio:.0%})"
    except Exception:
        return 0.0, 0.5, "orderbook parse error"


def spot_exchange() -> List[Dict]:
    for base in BINANCE_SPOT:
        data = get_json(f"{base}/api/v3/exchangeInfo", timeout=20)
        if data and data.get("symbols"):
            return data["symbols"]
    return []


def universe() -> List[str]:
    syms = []
    for x in spot_exchange():
        if x.get("status") != "TRADING" or x.get("quoteAsset") != "USDT":
            continue
        base = x.get("baseAsset", "").upper()
        if base in STABLES or x.get("isSpotTradingAllowed") is False:
            continue
        syms.append(x["symbol"])
    return syms


def ticker24() -> Dict[str, Dict]:
    for base in BINANCE_SPOT:
        data = get_json(f"{base}/api/v3/ticker/24hr", timeout=25)
        if isinstance(data, list):
            return {x["symbol"]: x for x in data}
    return {}


def futures_json(path: str, params: Dict) -> Optional[Any]:
    if not ENABLE_FUTURES:
        return None
    return get_json(f"{FAPI_BASE}{path}", params, retries=0)


def okx_futures_data(base: str) -> Tuple[float, Optional[float]]:
    inst = f"{base}-USDT-SWAP"
    oi = get_json(
        "https://www.okx.com/api/v5/public/open-interest",
        {"instType": "SWAP", "instId": inst},
        retries=0,
    )
    fr = get_json(
        "https://www.okx.com/api/v5/public/funding-rate",
        {"instId": inst},
        retries=0,
    )
    cur = 0.0
    funding: Optional[float] = None
    try:
        d = (oi or {}).get("data") or []
        cur = float(d[0].get("oiUsd") or 0) if d else 0.0
    except (TypeError, ValueError, IndexError):
        pass
    try:
        d = (fr or {}).get("data") or []
        funding = float(d[0].get("fundingRate")) if d else None
    except (TypeError, ValueError, IndexError):
        pass
    return cur, funding


def cg_id(symbol: str, st: Dict) -> Optional[str]:
    s = symbol[:-4].lower()
    cache = st["cg"]
    now = time.time()
    if s in cache and "verified" in cache[s] and now - cache[s].get("ts", 0) < 30 * 86400:
        return cache[s].get("id")
    data = get_json("https://api.coingecko.com/api/v3/search", {"query": s}, retries=0)
    cid = None
    try:
        coins = data.get("coins", []) if data else []
        exact = [x for x in coins if str(x.get("symbol", "")).lower() == s]
        cid = exact[0].get("id") if len(exact) == 1 else None
    except Exception:
        pass
    cache[s] = {"id": cid, "ts": now, "verified": bool(cid)}
    return cid


def refresh_market_caps_and_sectors(
    symbols: List[str], st: Dict
) -> Tuple[Dict[str, float], Dict[str, List[str]]]:
    """Batch market caps + categories from CoinGecko."""
    now = time.time()
    cache = st.setdefault("market_caps", {})
    sec_cache = st.setdefault("sectors", {})
    ids: List[str] = []
    symbol_to_id: Dict[str, str] = {}

    for sym in symbols:
        base = sym[:-4]
        cid = cg_id(sym, st)
        if cid:
            symbol_to_id[base] = cid
            item = cache.get(cid, {})
            if now - item.get("ts", 0) > 6 * 3600:
                ids.append(cid)

    ids = list(dict.fromkeys(ids))
    for i in range(0, len(ids), 80):
        batch = ids[i : i + 80]
        data = get_json(
            "https://api.coingecko.com/api/v3/coins/markets",
            {
                "vs_currency": "usd",
                "ids": ",".join(batch),
                "per_page": len(batch),
                "page": 1,
            },
            retries=0,
            timeout=12,
        )
        if isinstance(data, list):
            for coin in data:
                cid = coin.get("id")
                mc = coin.get("market_cap")
                if cid and mc:
                    cache[cid] = {
                        "market_cap": float(mc),
                        "ts": now,
                        "symbol": coin.get("symbol", ""),
                    }

    # Lightweight sector fetch for a subset (avoid rate limits)
    if ENABLE_SECTOR:
        for base, cid in list(symbol_to_id.items())[:DEEP_N]:
            if cid in sec_cache and now - sec_cache[cid].get("ts", 0) < 7 * 86400:
                continue
            detail = get_json(
                f"https://api.coingecko.com/api/v3/coins/{cid}",
                {"localization": "false", "tickers": "false",
                 "market_data": "false", "community_data": "false",
                 "developer_data": "false"},
                retries=0,
                timeout=10,
            )
            cats: List[str] = []
            try:
                cats = list(detail.get("categories") or []) if detail else []
            except Exception:
                pass
            sec_cache[cid] = {"categories": cats[:6], "ts": now}
            time.sleep(0.15)  # CoinGecko soft limit

    caps: Dict[str, float] = {}
    sectors: Dict[str, List[str]] = {}
    for base, cid in symbol_to_id.items():
        item = cache.get(cid, {})
        if item.get("market_cap") and (now - item.get("ts", 0) < 7 * 86400):
            caps[base] = float(item["market_cap"])
        sec_item = sec_cache.get(cid, {})
        if sec_item.get("categories"):
            sectors[base] = sec_item["categories"]
    return caps, sectors


# ---------------------------------------------------------------------------
# Scoring components
# ---------------------------------------------------------------------------
def tf_score(k: List[Dict], tf: str) -> Tuple[float, str]:
    if len(k) < 35:
        return 0.0, "unavailable"
    c = [x["c"] for x in k]
    e9 = ema(c, 9)[-1]
    e21 = ema(c, 21)[-1]
    r = rsi(c)
    ch = linear_change(c, 3 if tf == "5m" else 4)
    if c[-1] > e9 > e21 and 48 <= r <= 72:
        return W[tf], f"{tf} structure positive"
    if c[-1] > e21 and ch > 0:
        return W[tf] * 0.65, f"{tf} improving"
    if c[-1] > e21:
        return W[tf] * 0.4, f"{tf} above EMA21"
    if ch > 0 and r >= 45:
        return W[tf] * 0.25, f"{tf} attempting turn"
    return 0.0, f"{tf} not confirmed"


def volume_score(
    k: List[Dict], market_cap: Optional[float] = None, volume24: float = 0.0
) -> Tuple[float, float, Optional[float], str]:
    if len(k) < 35:
        return 0.0, 0.0, None, "unavailable"
    v = [x["v"] for x in k]
    recent = v[-3:]
    base = sum(v[-27:-3]) / 24 if sum(v[-27:-3]) else 0.0
    ratio = (sum(recent) / 3) / base if base else 0.0
    last_ratio = v[-1] / max(sum(v[-21:-1]) / 20, 1)
    ratio_points = min(max(ratio - 1, 0) / 2, 1) * 11
    last_points = min(max(last_ratio - 1, 0) / 2, 1) * 7
    score = min(W["volume"], ratio_points + last_points)
    turnover = (float(volume24 or 0) / float(market_cap)) if market_cap and market_cap > 0 else None
    desc = f"relative volume {ratio:.1f}x baseline; latest candle {last_ratio:.1f}x baseline"
    if turnover is not None:
        desc += f"; 24h volume/market cap {turnover * 100:.2f}%"
    else:
        desc += "; market cap unavailable"
    return score, ratio, turnover, desc


def turnover_score(market_cap: Optional[float], volume24: float) -> Tuple[float, str]:
    if not market_cap or market_cap <= 0 or not volume24:
        return 0.0, "market-cap turnover unavailable (no points)"
    turnover = float(volume24) / float(market_cap)
    if turnover >= 0.50:
        points = 8
    elif turnover >= 0.20:
        points = 7
    elif turnover >= 0.10:
        points = 6
    elif turnover >= 0.05:
        points = 5
    elif turnover >= 0.02:
        points = 3
    elif turnover >= 0.005:
        points = 1
    else:
        points = 0
    return float(points), f"24h volume/market cap {turnover * 100:.2f}% (turnover, not net inflow)"


def pressure_score(k: List[Dict]) -> Tuple[float, str]:
    if len(k) < 25:
        return 0.0, "unavailable"
    b = sum(x["tb"] for x in k[-12:])
    q = sum(x["q"] for x in k[-12:])
    p = b / q if q else 0.5
    s = min(max(p - 0.5, 0) / 0.15, 1) * W["pressure"]
    return s, f"buy pressure {p * 100:.0f}%"


def accel_score(k: List[Dict]) -> Tuple[float, str]:
    if len(k) < 30:
        return 0.0, "unavailable"
    c = [x["c"] for x in k]
    a = linear_change(c, 3)
    b = linear_change(c, 12)
    s = 0.0
    if a > 0:
        s += 3
    if a > b / 4 and a > 0:
        s += 3
    if 0 < a < 8:
        s += 2
    if 0.5 <= a < 5:
        s += 2
    return min(W["accel"], s), f"price acceleration {a:+.2f}%"


def compression_score(k: List[Dict]) -> Tuple[float, str]:
    if len(k) < 40:
        return 0.0, "unavailable"
    c = [x["c"] for x in k]
    recent = c[-20:]
    mean = sum(recent) / 20
    sd = math.sqrt(sum((x - mean) ** 2 for x in recent) / 20)
    bw = sd / mean if mean else 0.0
    old = []
    for i in range(20, 35):
        w = c[i - 20 : i]
        m = sum(w) / len(w)
        old.append(math.sqrt(sum((x - m) ** 2 for x in w) / len(w)) / m if m else 0.0)
    low = sorted(old)[len(old) // 3] if old else bw
    if bw <= low * 1.05:
        return W["compression"], "compressed"
    if c[-1] > max(c[-12:-1]):
        return W["compression"], "breakout after compression"
    return 0.0, "no compression edge"


def ema_structure(k: List[Dict]) -> Tuple[float, str]:
    if len(k) < 60:
        return 0.0, "unavailable"
    c = [x["c"] for x in k]
    e20 = ema(c, 20)[-1]
    e50 = ema(c, 50)[-1]
    if c[-1] > e20 > e50:
        return W["ema"], "price > EMA20 > EMA50"
    if c[-1] > e20:
        return 3.0, "price above EMA20"
    return 0.0, "EMA structure weak"


def resistance(k: List[Dict]) -> Tuple[float, Optional[float], str]:
    if len(k) < 40:
        return 0.0, None, "unavailable"
    c = [x["c"] for x in k]
    price = c[-1]
    highs = [x["h"] for x in k[-60:-5]]
    res = max(highs) if highs else price
    room = (res - price) / price * 100 if price else 0.0
    if room >= 10:
        return W["res"], room, "good resistance room"
    if room >= 7:
        return 5.0, room, "reasonable resistance room"
    if room >= 3:
        return 2.0, room, "limited resistance room"
    return 0.0, room, "near resistance"


def indicator_confluence(k: List[Dict]) -> Tuple[int, List[str], Dict]:
    if len(k) < 110:
        return 0, ["indicator confluence unavailable (need 110 candles)"], {}
    c = [x["c"] for x in k]
    h = [x["h"] for x in k]
    l = [x["l"] for x in k]
    p = c[-1]
    points = 0
    notes: List[str] = []
    info: Dict[str, Any] = {}

    rv = rsi(c, 14)
    info["rsi"] = rv
    if 52 <= rv <= 68:
        points += 2
        notes.append(f"RSI bullish/healthy ({rv:.0f})")
    elif 45 <= rv < 52:
        points += 1
        notes.append(f"RSI recovering ({rv:.0f})")
    elif rv > 75:
        notes.append(f"RSI overbought ({rv:.0f})")
    else:
        notes.append(f"RSI {rv:.0f}")

    e10 = ema(c, 10)[-1]
    e20 = ema(c, 20)[-1]
    e55 = ema(c, 55)[-1]
    info.update({"ema10": e10, "ema20": e20, "ema55": e55})
    if p > e10 > e20 > e55:
        points += 3
        notes.append("EMA10/20/55 bullish alignment")
    elif p > e20 > e55:
        points += 2
        notes.append("EMA20/55 bullish structure")
    elif p > e10:
        points += 1
        notes.append("price above EMA10")
    else:
        notes.append("EMA10/20/55 not aligned bullish")

    m50 = sma(c, 50)
    m100 = sma(c, 100)
    info.update({"ma50": m50, "ma100": m100})
    if m50 and m100 and p > m50 > m100:
        points += 2
        notes.append("MA50 above MA100; price above both")
    elif m50 and p > m50:
        points += 1
        notes.append("price above MA50")
    else:
        notes.append("MA50/100 trend not bullish")

    look = 60
    hi = max(h[-look:])
    lo = min(l[-look:])
    span = hi - lo
    if span > 0:
        fibs = {"0.382": hi - span * 0.382, "0.5": hi - span * 0.5, "0.618": hi - span * 0.618}
        near = min(fibs.items(), key=lambda z: abs(p - z[1]) / p if p else 999)
        dist = abs(p - near[1]) / p * 100 if p else 999
        info["fib_nearest"] = {"level": near[0], "price": near[1], "distance_pct": dist}
        if dist <= 1.0 and p >= lo + span * 0.25:
            points += 2
            notes.append(f"near Fib {near[0]} retracement ({near[1]:.8g})")
        elif dist <= 2.0:
            points += 1
            notes.append(f"approaching Fib {near[0]} ({near[1]:.8g})")
        else:
            notes.append("no nearby Fibonacci level")

    prior_hi = max(h[-21:-1])
    prior_lo = min(l[-21:-1])
    last = k[-1]
    swept_low = last["l"] < prior_lo and last["c"] > prior_lo
    swept_high = last["h"] > prior_hi and last["c"] < prior_hi
    info["liquidity_sweep"] = "sell-side sweep" if swept_low else "buy-side sweep" if swept_high else None
    if swept_low:
        points += 2
        notes.append("sell-side liquidity sweep and reclaim")
    elif swept_high:
        notes.append("buy-side liquidity sweep (potential rejection)")
    else:
        notes.append("no fresh 20-candle liquidity sweep")

    demand = min(l[-30:-2])
    supply = max(h[-30:-2])
    info["demand_zone"] = demand
    info["supply_zone"] = supply
    demand_dist = (p - demand) / p * 100 if p else 999
    supply_dist = (supply - p) / p * 100 if p else -999
    if 0 <= demand_dist <= 1.5 and p >= last["o"]:
        points += 1
        notes.append(f"near demand zone ({demand:.8g})")
    elif 0 <= supply_dist <= 1.5:
        notes.append(f"near supply/resistance zone ({supply:.8g})")
    else:
        notes.append("not at detected supply/demand zone")

    fvg = None
    for i in range(len(k) - 1, max(1, len(k) - 13), -1):
        if l[i] > h[i - 2]:
            fvg = ("bullish", h[i - 2], l[i])
            break
        if h[i] < l[i - 2]:
            fvg = ("bearish", h[i], l[i - 2])
            break
    info["fvg"] = {"type": fvg[0], "low": fvg[1], "high": fvg[2]} if fvg else None
    if fvg and fvg[0] == "bullish" and fvg[1] <= p <= fvg[2]:
        points += 1
        notes.append("price inside bullish FVG")
    elif fvg:
        notes.append(f"{fvg[0]} FVG detected ({fvg[1]:.8g}-{fvg[2]:.8g})")
    else:
        notes.append("no recent FVG detected")

    rng = max(h[-20:-5]) - min(l[-20:-5])
    base = max(p, 1e-12)
    tight = rng / base < 0.04
    manip = last["l"] < min(l[-20:-5]) and last["c"] > min(l[-20:-5])
    if tight and manip and last["c"] > last["o"]:
        points += 1
        notes.append("AMD-like accumulation → sell-side manipulation → bullish recovery")
    elif tight:
        notes.append("AMD-like range/accumulation watch")
    else:
        notes.append("no clear AMD sequence")

    q = sum(x["q"] for x in k[-12:])
    buy = sum(x["tb"] for x in k[-12:])
    ratio = buy / q if q else 0.5
    info["taker_buy_ratio"] = ratio
    if ratio >= 0.58:
        points += 1
        notes.append(f"order-flow proxy buyer-dominant ({ratio:.0%} taker-buy)")
    else:
        notes.append(f"order-flow proxy {ratio:.0%} taker-buy")

    return min(15, points), notes, info


def futures_data(sym: str, st: Dict, price_change_24h: float = 0.0) -> Dict:
    """Includes OI/Funding divergence detection."""
    out: Dict[str, Any] = {
        "oi": 0.0,
        "funding": None,
        "oi_change_pct": None,
        "notes": [],
        "scores": {"oi": 0.0, "funding": 0.0, "liq": 0.0},
        "divergence": None,  # "bearish" | "bullish" | None
        "divergence_penalty": 0.0,
    }
    base = sym[:-4]
    oi = futures_json("/fapi/v1/openInterest", {"symbol": sym})
    try:
        cur = float(oi["openInterest"])
        oi_source = "binance"
    except (TypeError, ValueError, KeyError):
        cur = 0.0
        oi_source = "okx"

    fr = futures_json("/fapi/v1/fundingRate", {"symbol": sym, "limit": 1})
    try:
        funding = float(fr[-1]["fundingRate"]) if isinstance(fr, list) and fr else None
    except (TypeError, ValueError, KeyError, IndexError):
        funding = None

    if not cur or funding is None:
        okx_oi, okx_funding = okx_futures_data(base)
        if not cur and okx_oi:
            cur = okx_oi
            oi_source = "okx"
        if funding is None:
            funding = okx_funding
        if cur or funding is not None:
            out["notes"].append("Futures confirmation via OKX fallback")

    oi_ch = None
    if cur:
        prev = st["oi"].get(base)
        out["oi"] = cur
        if prev and prev.get("v") and prev.get("source", "binance") == oi_source:
            oi_ch = (cur / prev["v"] - 1) * 100
            out["oi_change_pct"] = oi_ch
            if oi_ch > 8:
                out["scores"]["oi"] = W["oi"]
                out["notes"].append(f"OI rising {oi_ch:+.1f}%")
            elif oi_ch > 3:
                out["scores"]["oi"] = 2.0
                out["notes"].append(f"OI rising {oi_ch:+.1f}%")
            elif oi_ch > 0:
                out["scores"]["oi"] = 1.0
            elif oi_ch < -5:
                out["notes"].append(f"OI falling {oi_ch:+.1f}%")
        st["oi"][base] = {"v": cur, "ts": time.time(), "source": oi_source}

    if funding is not None:
        out["funding"] = funding
        if -0.0005 <= funding <= 0.0005:
            out["scores"]["funding"] = W["funding"]
            out["notes"].append("funding neutral/healthy")
        elif funding < 0:
            out["scores"]["funding"] = 1.0
            out["notes"].append("funding negative")
        else:
            out["scores"]["funding"] = 0.0
            out["notes"].append("funding crowded positive")
    else:
        out["notes"].append("funding data unavailable; no points assigned")

    # --- Divergence logic (new) ---
    # Price up strongly + OI falling → likely short-covering / weak pump
    # Price up + very high positive funding → crowded, higher reversal risk
    if oi_ch is not None and price_change_24h > 8 and oi_ch < -4:
        out["divergence"] = "bearish"
        out["divergence_penalty"] = 8.0
        out["notes"].append(
            f"⚠ OI/Price divergence: price +{price_change_24h:.1f}% but OI {oi_ch:+.1f}% (weak pump risk)"
        )
    elif funding is not None and funding > 0.001 and price_change_24h > 12:
        out["divergence"] = "bearish"
        out["divergence_penalty"] = 5.0
        out["notes"].append(
            f"⚠ Crowded longs: funding {funding:.4f} with +{price_change_24h:.1f}% move"
        )
    elif oi_ch is not None and oi_ch > 5 and price_change_24h > 3:
        out["divergence"] = "bullish"
        out["notes"].append("OI rising with price — healthy participation")

    fo = futures_json("/fapi/v1/allForceOrders", {"symbol": sym, "limit": 100})
    if isinstance(fo, list) and fo:
        cutoff = int((time.time() - 900) * 1000)
        short = long = 0.0
        for x in fo:
            if int(x.get("time", 0)) < cutoff:
                continue
            q = float(x.get("origQty", 0)) * float(x.get("price", 0))
            side = x.get("side", "")
            if side == "SELL":
                short += q
            elif side == "BUY":
                long += q
        if short + long > 0:
            if short > long * 1.25:
                out["scores"]["liq"] = W["liq"]
                out["notes"].append(f"short liquidation heavy {usd(short)}")
            elif long > short * 1.25:
                out["notes"].append(f"long liquidation heavy {usd(long)}")

    return out


def llama_score(symbol: str, st: Dict) -> Tuple[float, str]:
    if not ENABLE_DEFILLAMA:
        return 0.0, "disabled"
    cid = cg_id(symbol, st)
    if not cid:
        return 0.0, "no DeFiLlama mapping"
    data = get_json(f"https://coins.llama.fi/prices/current/coingecko:{cid}", retries=0)
    try:
        p = data["coins"][f"coingecko:{cid}"]
        conf = p.get("confidence")
        if conf is not None and float(conf) >= 0.9:
            return W["llama"], f"DeFiLlama price verified (confidence {float(conf):.0%})"
        return 1.0, f"DeFiLlama price available for {cid}"
    except Exception:
        return 0.0, "DeFiLlama data unavailable"


def early_pump_combo(
    vol_ratio: float,
    compression_pts: float,
    pressure_pts: float,
    oi_rising: bool,
    room: Optional[float],
    accel_pts: float,
) -> Tuple[float, bool, str]:
    """
    Detect classic early-pump structure.
    Returns (bonus 0–8, is_early_pump, note).
    """
    score = 0.0
    reasons = []
    if compression_pts >= W["compression"] * 0.9:
        score += 2.0
        reasons.append("compressed")
    if vol_ratio >= 1.8:
        score += 2.0
        reasons.append(f"vol {vol_ratio:.1f}x")
    elif vol_ratio >= 1.4:
        score += 1.0
        reasons.append(f"vol {vol_ratio:.1f}x")
    if pressure_pts >= W["pressure"] * 0.6:
        score += 1.5
        reasons.append("buy pressure")
    if oi_rising:
        score += 1.5
        reasons.append("OI rising")
    if room is not None and room >= 5:
        score += 1.0
        reasons.append(f"room {room:.1f}%")
    if accel_pts >= 6:
        score += 1.0
        reasons.append("acceleration")

    bonus = min(8.0, score)
    is_early = bonus >= 5.5
    note = "Early-pump combo: " + ", ".join(reasons) if reasons else "no early-pump combo"
    return bonus, is_early, note


def btc_regime() -> Tuple[float, str, bool]:
    """
    Returns (multiplier, note, is_hard_bear).
    Stronger than v3.1: hard bear can optionally block new signals.
    """
    k = spot_klines("BTCUSDT", "4h", 100)
    if len(k) < 60:
        return 1.0, "BTC regime unavailable", False
    c = [x["c"] for x in k]
    r = rsi(c)
    e50 = ema(c, 50)[-1]
    e20 = ema(c, 20)[-1]
    ch12 = linear_change(c, 12)

    if c[-1] < e50 and r < 42 and ch12 < -3:
        return 0.70, "BTC hard-bear regime", True
    if c[-1] < e50 and r < 48:
        return 0.80, "BTC bearish regime", False
    if c[-1] > e20 > e50 and r >= 52:
        return 1.00, "BTC supportive regime", False
    if c[-1] > e50 and r >= 48:
        return 0.95, "BTC mildly supportive", False
    return 0.88, "BTC neutral regime", False


# ---------------------------------------------------------------------------
# Core evaluation
# ---------------------------------------------------------------------------
def evaluate(
    sym: str,
    t: Dict,
    st: Dict,
    market_caps: Optional[Dict[str, float]] = None,
    sectors: Optional[Dict[str, List[str]]] = None,
    regime_mult: float = 1.0,
    cache: Optional[KlineCache] = None,
) -> Optional[Dict]:
    ks = {
        tf: spot_klines(sym, tf, 120 if tf in ("5m", "15m", "30m") else 100, cache=cache)
        for tf in INTERVALS
    }
    if len(ks["15m"]) < 60 or len(ks["5m"]) < 35:
        return None

    score = 0.0
    notes: List[str] = []
    blocked: List[str] = []
    detail: Dict[str, Any] = {}

    for tf in ("5m", "15m", "30m", "1h", "4h"):
        s, n = tf_score(ks[tf], tf)
        score += s
        notes.append(n)

    market_cap = (market_caps or {}).get(sym[:-4])
    volume24 = float(t.get("quoteVolume", 0) or 0)
    ch24 = float(t.get("priceChangePercent", 0))

    sv, vr, turnover, vn = volume_score(ks["15m"], market_cap, volume24)
    score += sv
    notes.append(vn)
    detail["volume_ratio"] = vr
    detail["market_cap"] = market_cap
    detail["mcap_turnover"] = turnover

    sturn, tn = turnover_score(market_cap, volume24)
    score += sturn
    notes.append(tn)

    sa, an = accel_score(ks["5m"])
    score += sa
    notes.append(an)

    sc, cn = compression_score(ks["30m"])
    score += sc
    notes.append(cn)

    se, en = ema_structure(ks["1h"])
    score += se
    notes.append(en)

    sp, pn = pressure_score(ks["5m"])
    score += sp
    notes.append(pn)

    sr, room, rn = resistance(ks["1h"])
    score += sr
    notes.append(rn)
    detail["room"] = room

    fd = futures_data(sym, st, price_change_24h=ch24)
    score += sum(fd["scores"].values())
    notes.extend(fd["notes"])
    if fd.get("divergence_penalty"):
        score = max(0.0, score - fd["divergence_penalty"])
        if fd["divergence"] == "bearish" and fd["divergence_penalty"] >= 7:
            blocked.append("oi_price_divergence")

    sl, ln = llama_score(sym, st)
    score += sl
    notes.append(ln)

    # Order-book imbalance bonus
    ob_bonus, ob_ratio, ob_note = orderbook_imbalance(sym)
    score += ob_bonus
    notes.append(ob_note)
    detail["orderbook_ratio"] = ob_ratio
    detail["orderbook_bonus"] = ob_bonus

    # Early-pump combo bonus
    oi_rising = any("OI rising" in n for n in fd["notes"])
    ep_bonus, is_early, ep_note = early_pump_combo(
        vol_ratio=vr or 0.0,
        compression_pts=sc,
        pressure_pts=sp,
        oi_rising=oi_rising,
        room=room,
        accel_pts=sa,
    )
    score += ep_bonus
    notes.append(ep_note)
    detail["early_pump_bonus"] = ep_bonus

    # Sector tag (display only)
    cats = (sectors or {}).get(sym[:-4], [])
    detail["sectors"] = cats[:4]

    # Diagnostic confluence (not scored)
    ind_score, ind_notes, ind_info = indicator_confluence(ks["15m"])
    notes.extend(ind_notes)
    detail["indicators"] = ind_info
    detail["indicator_score"] = ind_score

    # ATR
    true_ranges = []
    for j in range(1, min(len(ks["15m"]), 15)):
        cur = ks["15m"][-j]
        prev = ks["15m"][-j - 1]
        true_ranges.append(
            max(cur["h"] - cur["l"], abs(cur["h"] - prev["c"]), abs(cur["l"] - prev["c"]))
        )
    detail["atr15"] = (sum(true_ranges) / len(true_ranges)) if true_ranges else 0.0

    c5 = [x["c"] for x in ks["5m"]]
    c1 = [x["c"] for x in ks["1h"]]
    r1 = rsi(c1)

    late = ch24 > 45 or r1 > 84 or (room is not None and room < 1.2)
    overheated = r1 > 88 or linear_change(c5, 12) > 12
    if late:
        blocked.append("late/extended move")
    if overheated:
        blocked.append("overheated")
    if room is not None and room < 1.2:
        blocked.append("near resistance")

    if linear_change(c1, 3) < -3:
        score = max(0.0, score - 4)
        notes.append("1h momentum weak (-4 caution points)")

    # BTC regime
    if regime_mult != 1.0:
        score *= regime_mult
        notes.append(f"BTC regime multiplier ×{regime_mult:.2f}")

    score = min(100.0, max(0.0, score))

    if blocked:
        stage = "BLOCKED"
    elif score < 50:
        stage = "EARLY RADAR"
    elif score < 60:
        stage = "WATCH"
    elif score <= 80:
        stage = "BUY WATCH"
    elif score <= 90:
        stage = "STRONG SIGNAL"
    else:
        stage = "STRONG+"

    # Upgrade stage label if early-pump and otherwise solid
    if is_early and stage in ("WATCH", "BUY WATCH") and not blocked:
        stage = "BUY WATCH" if stage == "WATCH" else stage

    return {
        "symbol": sym,
        "base": sym[:-4],
        "score": score,
        "stage": stage,
        "blocked": blocked,
        "notes": notes,
        "detail": detail,
        "price": float(t.get("lastPrice", 0)),
        "change24": ch24,
        "volume24": float(t.get("quoteVolume", 0)),
        "market_cap": market_cap,
        "mcap_turnover": turnover,
        "funding": fd["funding"],
        "oi": fd["oi"],
        "url": f"https://www.binance.com/en/trade/{sym}",
        "rsi1": r1,
        "regime_mult": regime_mult,
        "early_pump": is_early,
        "sectors": cats[:4],
        "divergence": fd.get("divergence"),
    }


# ---------------------------------------------------------------------------
# Text builders
# ---------------------------------------------------------------------------
def human_reason(it: Dict) -> str:
    n = " ".join(it["notes"]).lower()
    openings = [
        "This one caught my attention because the move is still relatively early.",
        "There is a change in activity here that is worth watching.",
        "The interesting part is not just the price move — it is the activity behind it.",
        "I would keep this one on the radar rather than chase it.",
    ]
    opening = openings[int(it["score"]) % len(openings)]
    bits = []
    if it.get("early_pump"):
        bits.append(
            "This shows a classic early-pump structure: compression + rising volume + "
            "buy pressure, with room still left above."
        )
    if "volume" in n:
        bits.append("Volume is picking up while price has not gone fully vertical yet.")
    if "oi rising" in n:
        bits.append("Futures open interest is rising — more participants are engaging.")
    if "funding neutral" in n:
        bits.append("Funding remains relatively healthy; not obviously overcrowded.")
    if "orderbook" in n and ("bid" in n or "strong bid" in n):
        bits.append("Spot order book is bid-heavy, supporting short-term upside pressure.")
    if "resistance room" in n or "good resistance" in n:
        bits.append("There is still breathing room before the next major resistance.")
    if "compressed" in n:
        bits.append("The 30-minute structure is compressed — a clean expansion would be interesting.")
    if it.get("divergence") == "bearish":
        bits.append(
            "Caution: there is an OI/price or funding divergence that weakens conviction."
        )
    if not bits:
        bits.append("The higher-timeframe structure is the main reason this remains on the radar.")
    close = (
        "Next confirmation I want: volume stays elevated, short-term structure holds, "
        "and price clears nearby resistance cleanly."
    )
    return (
        opening + "\n\n" + " ".join(bits) + "\n\n" + close
        + "\n\n⚠️ Momentum watch only — not financial advice. Do not chase high scores blindly."
    )


def signal_text(it: Dict, serial: Optional[int] = None) -> str:
    d = it["detail"]
    score = it["score"]
    entry = it["price"]
    levels = trade_levels(it)
    sl = levels["sl"] if levels else None
    tps = levels["tps"] if levels else []
    mc = it.get("market_cap")
    turnover = it.get("mcap_turnover")
    cap_line = f"🏦 Market Cap: {usd(mc) if mc else 'Unavailable'}\n"
    turnover_line = (
        f"📐 24h Volume / Market Cap: {turnover * 100:.2f}%\n"
        if turnover is not None
        else "📐 Volume / Market Cap: unavailable\n"
    )
    ind = d.get("indicators", {})
    fib = ind.get("fib_nearest")
    fvg = ind.get("fvg")

    ind_lines = (
        f"📟 RSI(14): {ind.get('rsi', 0):.1f}\n"
        f"📉 EMA 10/20/55: {ind.get('ema10', 0):.8g} / {ind.get('ema20', 0):.8g} / {ind.get('ema55', 0):.8g}\n"
        f"📊 MA 50/100: {ind.get('ma50', 0):.8g} / {ind.get('ma100', 0):.8g}\n"
    )
    if fib:
        ind_lines += f"🧮 Fibonacci: {fib['level']} @ {fib['price']:.8g} ({fib['distance_pct']:.2f}% away)\n"
    else:
        ind_lines += "🧮 Fibonacci: unavailable\n"
    ind_lines += f"🧹 Liquidity Sweep: {ind.get('liquidity_sweep') or 'none detected'}\n"
    ind_lines += f"🧱 Demand/Supply: {ind.get('demand_zone', 0):.8g} / {ind.get('supply_zone', 0):.8g}\n"
    if fvg:
        ind_lines += f"🟦 FVG: {fvg['type']} {fvg['low']:.8g}-{fvg['high']:.8g}\n"
    else:
        ind_lines += "🟦 FVG: none detected\n"
    ind_lines += f"🧭 Order-flow proxy: {ind.get('taker_buy_ratio', 0.5):.0%} taker-buy\n"
    ind_lines += f"📖 Orderbook imbalance: {d.get('orderbook_ratio', 0.5):.0%} bid "
    ind_lines += f"(+{d.get('orderbook_bonus', 0):.0f} pts)\n"

    regime_note = ""
    if it.get("regime_mult", 1.0) != 1.0:
        regime_note = f"🌍 BTC Regime Mult: ×{it['regime_mult']:.2f}\n"
    early_note = "🔥 EARLY PUMP COMBO\n" if it.get("early_pump") else ""
    sector_note = ""
    if it.get("sectors"):
        sector_note = f"🏷️ Sector: {', '.join(it['sectors'][:3])}\n"
    div_note = ""
    if it.get("divergence") == "bearish":
        div_note = "⚠️ Divergence warning active\n"

    return (
        f"🚨 BINANCE GEMS | SIGNAL #{serial if serial is not None else '—'}\n"
        f"{early_note}{it['stage']} — ${it['base']}\n\n"
        f"🔥 Score: {score:.0f}/100\n"
        f"🧮 Technical confluence (diagnostic): {d.get('indicator_score', 0)}/15\n"
        f"{regime_note}{div_note}{sector_note}"
        f"📈 Spot Momentum: building\n\n"
        f"💰 Price: ${entry:.8g}\n📊 24H: {it['change24']:+.1f}%\n💵 24H Volume: {usd(it['volume24'])}\n"
        f"{cap_line}{turnover_line}\n{ind_lines}\n"
        f"🎯 Entry Watch: ${entry:.8g}\n"
        + (
            f"🛑 Stop Loss: ${sl:.8g}\n"
            + "".join(f"🎯 TP{i}: ${tp:.8g}\n" for i, tp in enumerate(tps, 1))
            if levels
            else "🛑 TP/SL: unavailable (insufficient data)\n"
        )
        + "\n"
        f"🧠 Why I'm Watching\n{human_reason(it)}\n\n"
        f"🔗 Binance Spot: {it['url']}"
    )


# ---------------------------------------------------------------------------
# Stage-1 + smart pre-rank + deep scan
# ---------------------------------------------------------------------------
def stage1() -> List[Tuple[str, Dict, float]]:
    allowed = set(universe())
    ticks = ticker24()
    arr = []
    for sym, t in ticks.items():
        if sym not in allowed:
            continue
        q = float(t.get("quoteVolume", 0))
        ch = float(t.get("priceChangePercent", 0))
        if q < MIN_24H_VOL or ch < -35:
            continue
        arr.append((sym, t, q))

    arr.sort(key=lambda z: (z[2], abs(float(z[1].get("priceChangePercent", 0)))), reverse=True)
    movers = sorted(arr, key=lambda z: float(z[1].get("priceChangePercent", 0)), reverse=True)[
        : max(40, STAGE1_TOP // 2)
    ]
    recovering = sorted(
        (x for x in arr if -8 <= float(x[1].get("priceChangePercent", 0)) <= 15),
        key=lambda z: (float(z[1].get("priceChangePercent", 0)), z[2]),
        reverse=True,
    )[: max(30, STAGE1_TOP // 3)]

    merged = {x[0]: x for x in arr[:STAGE1_TOP]}
    for x in movers + recovering:
        merged.setdefault(x[0], x)
    return list(merged.values())


def quick_momentum_score(sym: str, t: Dict, cache: KlineCache) -> float:
    """Cheap 15m relative-volume + 5m acceleration for pre-ranking."""
    k15 = spot_klines(sym, "15m", 60, cache=cache)
    k5 = spot_klines(sym, "5m", 40, cache=cache)
    score = 0.0
    if len(k15) >= 35:
        v = [x["v"] for x in k15]
        base = sum(v[-27:-3]) / 24 if sum(v[-27:-3]) else 1
        ratio = (sum(v[-3:]) / 3) / base
        score += min(ratio, 4.0) * 10
    if len(k5) >= 20:
        c = [x["c"] for x in k5]
        a = linear_change(c, 3)
        if 0 < a < 6:
            score += a * 3
        elif a >= 6:
            score += 12
    ch = float(t.get("priceChangePercent", 0))
    score += max(min(ch, 15), -5) * 0.4
    score += math.log10(max(float(t.get("quoteVolume", 1)), 1)) * 2
    return score


def scan(st: Dict, regime_mult: float = 1.0, hard_bear: bool = False) -> List[Dict]:
    if hard_bear and BTC_HARD_FILTER:
        log("BTC hard-bear: only allowing high-conviction setups (stricter threshold)")
        # continue but SIGNAL_MIN effectively higher later

    cand = stage1()
    log(f"Stage 1: {len(cand)} candidates (floor {usd(MIN_24H_VOL)})")

    cache = KlineCache()

    # Pre-rank with quick momentum (uses cache)
    log(f"Pre-ranking top {min(len(cand), PRE_RANK_N)} by relative volume + acceleration …")
    pre = cand[: max(PRE_RANK_N, DEEP_N + 20)]
    scored_pre = []
    with ThreadPoolExecutor(max_workers=min(EVAL_WORKERS, 10)) as pool:
        futs = {pool.submit(quick_momentum_score, sym, t, cache): (sym, t, q) for sym, t, q in pre}
        for fut in as_completed(futs):
            item = futs[fut]
            try:
                qs = fut.result()
                scored_pre.append((qs, item))
            except Exception as e:
                log(f"pre-rank {item[0]}: {e}")
    scored_pre.sort(key=lambda x: x[0], reverse=True)
    shortlist = [item for _, item in scored_pre[:DEEP_N]]

    caps, sectors = refresh_market_caps_and_sectors([x[0] for x in shortlist], st)

    def priority(item: Tuple) -> Tuple:
        sym, t, q = item
        mc = caps.get(sym[:-4])
        turnover = (q / mc) if mc else 0.0
        ch = float(t.get("priceChangePercent", 0))
        return (min(turnover, 0.75) * 100 + max(min(ch, 20), -10) * 0.5, q)

    shortlist = sorted(shortlist, key=priority, reverse=True)

    min_score = SIGNAL_MIN + (8 if hard_bear and BTC_HARD_FILTER else 0)
    items: List[Dict] = []
    lock = threading.Lock()

    def _eval_one(pair: Tuple) -> Optional[Dict]:
        sym, t, _ = pair
        try:
            time.sleep(EVAL_DELAY_SEC)
            it = evaluate(
                sym, t, st,
                market_caps=caps,
                sectors=sectors,
                regime_mult=regime_mult,
                cache=cache,
            )
            if it and it["score"] >= min_score and not it["blocked"]:
                return it
            if it and it.get("divergence") == "bearish":
                st.setdefault("stats", {})["blocked_divergence"] = (
                    st["stats"].get("blocked_divergence", 0) + 1
                )
        except Exception as e:
            log(f"eval {sym}: {e}")
        return None

    log(f"Deep scan: {len(shortlist)} symbols | cache={len(cache)} | workers={EVAL_WORKERS}")
    with ThreadPoolExecutor(max_workers=EVAL_WORKERS) as pool:
        futures = {pool.submit(_eval_one, item): item[0] for item in shortlist}
        for fut in as_completed(futures):
            result = fut.result()
            if result:
                with lock:
                    items.append(result)

    items.sort(key=lambda x: (x.get("early_pump", False), x["score"]), reverse=True)
    log(f"Kline cache hits this scan: {len(cache)} entries")
    return items


# ---------------------------------------------------------------------------
# Monitoring & dispatch
# ---------------------------------------------------------------------------
def monitor_open_signals(st: Dict) -> None:
    opened = st.setdefault("open_signals", {})
    stats = st.setdefault("stats", {
        "sent": 0, "tp1": 0, "sl": 0, "ambiguous": 0,
        "early_pump_sent": 0, "blocked_divergence": 0,
    })

    for key, sig in list(opened.items()):
        if sig.get("status") != "OPEN":
            continue
        try:
            data = get_json(
                BINANCE_SPOT[0] + "/api/v3/ticker/price",
                {"symbol": sig["symbol"]},
                retries=0,
                timeout=8,
            )
            if not isinstance(data, dict) or not data.get("price"):
                continue
            price = float(data["price"])
            sig["last_price"] = price
            sig["last_checked"] = time.time()
            sl = float(sig.get("sl") or 0)
            tp1 = float((sig.get("tps") or [0])[0] or 0)
            hit_sl = sl > 0 and price <= sl
            hit_tp = tp1 > 0 and price >= tp1

            if hit_sl and hit_tp:
                sig["status"] = "AMBIGUOUS"
                sig["result"] = "TP1/SL ordering unclear from polling"
                stats["ambiguous"] = stats.get("ambiguous", 0) + 1
                post_tg(
                    f"📋 BINANCE GEMS — SIGNAL #{sig['serial']} UPDATE\n"
                    f"${sig['base']} result unclear (TP1/SL both touched in sample). "
                    f"Last: ${price:.8g}"
                )
            elif hit_sl:
                sig["status"] = "SL_HIT"
                sig["result"] = "SL"
                sig["closed_at"] = time.time()
                stats["sl"] = stats.get("sl", 0) + 1
                post_tg(
                    f"🛑 BINANCE GEMS — SIGNAL #{sig['serial']} RESULT\n"
                    f"${sig['base']} hit SL.\n"
                    f"Entry: ${sig['entry']:.8g} | SL: ${sl:.8g} | Last: ${price:.8g}"
                )
            elif hit_tp:
                sig["status"] = "TP1_HIT"
                sig["result"] = "TP1"
                sig["closed_at"] = time.time()
                stats["tp1"] = stats.get("tp1", 0) + 1
                post_tg(
                    f"🎯 BINANCE GEMS — SIGNAL #{sig['serial']} UPDATE\n"
                    f"${sig['base']} reached TP1.\n"
                    f"Entry: ${sig['entry']:.8g} | TP1: ${tp1:.8g} | Last: ${price:.8g}"
                )
        except Exception as e:
            log(f"outcome monitor {key}: {e}")


def send_signals(st: Dict, items: List[Dict]) -> int:
    now = time.time()
    sent = 0
    stats = st.setdefault("stats", {
        "sent": 0, "tp1": 0, "sl": 0, "ambiguous": 0,
        "early_pump_sent": 0, "blocked_divergence": 0,
    })

    for it in items:
        key = it["symbol"]
        old = st["alerts"].get(key)
        if old:
            age = now - old.get("ts", 0)
            major_upgrade = age >= 3 * 3600 and it["score"] >= old.get("score", 0) + 15
            if age < COOLDOWN_MIN * 60 and not major_upgrade:
                continue

        st["signal_seq"] = int(st.get("signal_seq", 0)) + 1
        serial = st["signal_seq"]
        text = signal_text(it, serial)
        post_tg(text)

        levels = trade_levels(it)
        if levels and levels.get("tps"):
            st.setdefault("open_signals", {})[key] = {
                "serial": serial,
                "symbol": key,
                "base": it["base"],
                "entry": levels["entry"],
                "sl": levels["sl"],
                "tps": levels["tps"],
                "ts": now,
                "status": "OPEN",
                "last_price": it["price"],
                "early_pump": it.get("early_pump", False),
            }

        if POST_TO_SQUARE and it["score"] >= SQUARE_MIN_SCORE:
            post_square(square_text(it, serial))

        st["alerts"][key] = {
            "ts": now,
            "score": it["score"],
            "stage": it["stage"],
            "serial": serial,
        }
        stats["sent"] = stats.get("sent", 0) + 1
        if it.get("early_pump"):
            stats["early_pump_sent"] = stats.get("early_pump_sent", 0) + 1
        sent += 1
    return sent


def print_stats(st: Dict) -> None:
    stats = st.get("stats", {})
    open_n = sum(1 for v in st.get("open_signals", {}).values() if v.get("status") == "OPEN")
    closed = [v for v in st.get("open_signals", {}).values() if v.get("status") != "OPEN"]
    tp1 = stats.get("tp1", 0)
    sl = stats.get("sl", 0)
    total_resolved = tp1 + sl
    win_rate = (tp1 / total_resolved * 100) if total_resolved else 0.0
    print("=== BINANCE GEMS STATS ===")
    print(json.dumps(stats, indent=2))
    print(f"Open signals     : {open_n}")
    print(f"Resolved (TP1/SL): {total_resolved}")
    print(f"TP1 hit rate     : {win_rate:.1f}% (price-monitor only, not live fills)")
    print(f"Early-pump sent  : {stats.get('early_pump_sent', 0)}")
    print(f"Blocked by div.  : {stats.get('blocked_divergence', 0)}")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def tick(force: bool = False) -> None:
    st = load_state()
    now = time.time()
    if not force and now - st["last_scan"] < SCAN_EVERY_MIN * 60 - 5:
        return

    regime_mult, regime_note, hard_bear = btc_regime()
    log(regime_note)

    monitor_open_signals(st)
    items = scan(st, regime_mult=regime_mult, hard_bear=hard_bear)
    n = send_signals(st, items)

    st["last_scan"] = now
    save_state(st)
    log(f"Eligible: {len(items)} | sent: {n} | stats={st.get('stats')}")


def watch(minutes: float, force_first: bool = False) -> None:
    end = time.time() + minutes * 60
    first = True
    while time.time() < end:
        try:
            tick(force=force_first and first)
        except Exception as e:
            log(f"tick error: {e}")
        first = False
        time.sleep(TICK_SECONDS)


if __name__ == "__main__":
    if "--test-telegram" in sys.argv:
        post_tg("✅ Binance Spot bot connected (v4.0).")
    elif "--once" in sys.argv:
        tick(True)
    elif "--watch" in sys.argv:
        idx = sys.argv.index("--watch")
        mins = float(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else 60
        watch(mins, "--force" in sys.argv)
    elif "--tick" in sys.argv:
        tick("--force" in sys.argv)
    elif "--stats" in sys.argv:
        print_stats(load_state())
    else:
        log("Starting continuous loop (v4.0) …")
        while True:
            try:
                tick()
            except Exception as e:
                log(f"loop error: {e}")
            time.sleep(TICK_SECONDS)
