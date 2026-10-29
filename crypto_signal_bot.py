"""BINANCE GEMS — Binance Spot USDT early-momentum scanner.
CoinGecko is used as optional market-cap context; Binance remains the trading universe.
"""
import logging
import os
import random
import re
import time
from pathlib import Path

import pandas as pd
import requests

LOG = logging.getLogger("binance_gems")
BINANCE_URLS = [
    "https://api.binance.com",
    "https://data-api.binance.vision",
]
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
SQUARE_URL = "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"
TIMEOUT = 12

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "").strip()
CMC_API_KEY = os.getenv("COINMARKETCAP_API_KEY", "").strip()
SQUARE_KEY = os.getenv("BINANCE_SQUARE_OPENAPI_KEY", "").strip()
POST_TO_SQUARE = os.getenv("POST_TO_SQUARE", "false").lower() == "true"
MIN_SCORE = int(os.getenv("MIN_SCORE", "45"))
SQUARE_MIN_SCORE = int(os.getenv("SQUARE_MIN_SCORE", str(MIN_SCORE)))
MAX_PAIRS = int(os.getenv("MAX_PAIRS", "500"))
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", "720"))
STATE_FILE = Path(os.getenv("STATE_FILE", "state_binance.json"))
DRAFT_FILE = Path("square_drafts.txt")

STABLE_BASES = {
    "USDC", "FDUSD", "TUSD", "USDP", "DAI", "USDD", "BUSD", "USDE",
    "PYUSD", "EUR", "TRY", "BRL", "BIDR", "IDRT", "UAH", "RUB",
}
LEVERAGED_ENDINGS = ("UP", "DOWN", "BULL", "BEAR")
# A maintained blocklist: Binance listings and token classifications can change.
TOKENIZED_STOCK_BASES = {
    "AAPL", "AMZN", "COIN", "CRCL", "GOOGL", "META", "MSFT",
    "MSTR", "NVDA", "QQQ", "SPY", "TSLA", "HOOD",
}

session = requests.Session()
session.headers.update({"User-Agent": "BINANCE-GEMS-Spot-Radar/1.0"})


def request_json(url, *, params=None, headers=None, timeout=TIMEOUT):
    response = session.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def binance_get(path, params=None):
    last_error = None
    for root in BINANCE_URLS:
        try:
            return request_json(root + path, params=params)
        except Exception as exc:
            last_error = exc
            LOG.debug("Binance endpoint failed (%s): %s", root, exc)
    raise RuntimeError(f"Binance endpoints unavailable: {last_error}")


def fmt_price(value):
    value = float(value)
    if value >= 1000:
        return f"{value:,.2f}"
    if value >= 1:
        return f"{value:.4f}".rstrip("0").rstrip(".")
    if value >= 0.01:
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return f"{value:.10f}".rstrip("0").rstrip(".")


def load_state():
    import json
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8")) if STATE_FILE.exists() else {}
    except (OSError, ValueError):
        return {}


def save_state(state):
    import json
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def get_coingecko_market_caps():
    """Load CoinGecko market-cap context in a small number of bulk requests.
    Demo keys use x-cg-demo-api-key. Keyless public access is also supported.
    CoinGecko symbol matching is inherently ambiguous, so only unique symbols are used.
    """
    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
    candidates = {}
    try:
        # 250 assets per request; only a few pages to keep within free-tier limits.
        for page in (1, 2, 3, 4):
            data = request_json(
                f"{COINGECKO_BASE}/coins/markets",
                params={
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": 250,
                    "page": page,
                    "sparkline": "false",
                },
                headers=headers,
            )
            if not data:
                break
            for coin in data:
                symbol = str(coin.get("symbol", "")).upper()
                cap = coin.get("market_cap")
                if symbol and isinstance(cap, (int, float)) and cap > 0:
                    candidates.setdefault(symbol, []).append(float(cap))
        # Only map symbols with one market-cap candidate to avoid wrong asset matches.
        return {sym: values[0] for sym, values in candidates.items() if len(values) == 1}
    except Exception as exc:
        LOG.warning("CoinGecko market-cap context unavailable; fallback scoring will be used: %s", exc)
        return {}


def get_universe():
    info = binance_get("/api/v3/exchangeInfo")
    tickers = binance_get("/api/v3/ticker/24hr")
    ticker_map = {x["symbol"]: x for x in tickers}
    valid = {}
    for item in info.get("symbols", []):
        symbol = item.get("symbol", "")
        base = str(item.get("baseAsset", "")).upper()
        if item.get("status") != "TRADING" or item.get("quoteAsset") != "USDT":
            continue
        if not item.get("isSpotTradingAllowed", False):
            continue
        if base in STABLE_BASES or base in TOKENIZED_STOCK_BASES:
            continue
        if base.endswith(LEVERAGED_ENDINGS):
            continue
        ticker = ticker_map.get(symbol)
        if not ticker:
            continue
        try:
            price = float(ticker["lastPrice"])
            change = float(ticker["priceChangePercent"])
            quote_volume = float(ticker["quoteVolume"])
        except (KeyError, TypeError, ValueError):
            continue
        if price <= 0 or change > 35:
            continue
        valid[symbol] = {
            "symbol": symbol, "base": base, "price": price,
            "change": change, "quote_volume": quote_volume,
        }
    # Keep broad coverage, prioritizing the most active spot markets.
    ranked = sorted(valid.values(), key=lambda x: x["quote_volume"], reverse=True)
    return ranked[:max(50, MAX_PAIRS)]


def candles(symbol, interval, limit=80):
    rows = binance_get("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    cols = ["open_time", "open", "high", "low", "close", "volume", "close_time",
            "quote_volume", "trades", "taker_base", "taker_quote", "ignore"]
    df = pd.DataFrame(rows, columns=cols)
    for col in ("open", "high", "low", "close", "volume", "quote_volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def score_coin(coin, market_caps):
    try:
        c15 = candles(coin["symbol"], "15m", 65)
        c1h = candles(coin["symbol"], "1h", 60)
        if len(c15) < 35 or len(c1h) < 30:
            return None
        # Ignore the currently forming candle.
        c15 = c15.iloc[:-1].copy()
        c1h = c1h.iloc[:-1].copy()
        close = c15["close"]
        vol = c15["quote_volume"]
        ema20 = close.ewm(span=20, adjust=False).mean()
        ema50_1h = c1h["close"].ewm(span=50, adjust=False).mean()
        ema20_1h = c1h["close"].ewm(span=20, adjust=False).mean()
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean().replace(0, 1e-9)
        rsi = 100 - (100 / (1 + gain / loss))
        current = float(close.iloc[-1])
        rsi_now = float(rsi.iloc[-1])
        volume_avg = float(vol.iloc[-21:-1].mean())
        volume_ratio = float(vol.iloc[-1] / volume_avg) if volume_avg > 0 else 0
        ema20_now = float(ema20.iloc[-1])
        trend_up = float(ema20_1h.iloc[-1]) > float(ema50_1h.iloc[-1])
        ema_slope = float(ema20.iloc[-1] / ema20.iloc[-5] - 1) if ema20.iloc[-5] else 0
        candle = c15.iloc[-1]
        body = abs(float(candle["close"]) - float(candle["open"]))
        candle_range = max(float(candle["high"] - candle["low"]), 1e-12)
        close_position = (float(candle["close"]) - float(candle["low"])) / candle_range

        technical = 0
        reasons = []
        if current > ema20_now:
            technical += 12; reasons.append("above EMA20")
        if trend_up:
            technical += 12; reasons.append("1h EMA trend bullish")
        if 48 <= rsi_now <= 68:
            technical += 8; reasons.append(f"RSI {rsi_now:.0f}")
        if ema_slope > 0:
            technical += 4; reasons.append("EMA slope positive")
        if close_position >= 0.65 and body / candle_range >= 0.25:
            technical += 4; reasons.append("strong candle close")

        data_score = 0
        if volume_ratio >= 1.2:
            data_score += 12; reasons.append(f"15m volume {volume_ratio:.2f}x")
        if volume_ratio >= 1.8:
            data_score += 6
        if coin["quote_volume"] > 0:
            data_score += 4
        mcap = market_caps.get(coin["base"])
        turnover_pct = (coin["quote_volume"] / mcap * 100) if mcap else None
        if turnover_pct is not None:
            target = 15 if mcap < 1_000_000 else 8 if mcap < 10_000_000 else 5 if mcap < 100_000_000 else 3 if mcap < 500_000_000 else 2
            if turnover_pct >= target:
                data_score += 8
                reasons.append(f"24h turnover/mcap {turnover_pct:.2f}%")
        # Binance-only scanner has no reliable global sentiment feed; don't fabricate it.
        sentiment_score = 0
        score = min(100, technical + data_score + sentiment_score)
        if score < MIN_SCORE:
            return None
        return {
            **coin, "close": current, "score": score,
            "volume_ratio": volume_ratio, "rsi": rsi_now,
            "mcap": mcap, "turnover_pct": turnover_pct,
            "reasons": ", ".join(reasons) if reasons else "activity fallback",
        }
    except Exception as exc:
        LOG.debug("Could not score %s: %s", coin["symbol"], exc)
        return None


def telegram_send(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        LOG.warning("Telegram credentials missing; skipping Telegram post.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = session.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=TIMEOUT)
        resp.raise_for_status()
        return bool(resp.json().get("ok"))
    except Exception as exc:
        LOG.error("Telegram post failed: %s", exc)
        return False


def signal_text(row, serial):
    turnover = (f"🏦 24h turnover/mcap: {row['turnover_pct']:.2f}%"
                if row["turnover_pct"] is not None else "🏦 Market cap: unavailable (CoinGecko fallback)")
    return "\n".join([
        f"💎 BINANCE GEMS | EARLY SPOT SIGNAL #{serial}",
        f"🪙 {row['base']} ({row['symbol']})",
        f"⭐ Score: {row['score']}/100 | BUY WATCH",
        f"💵 Price: {fmt_price(row['close'])} USDT",
        f"📈 24h change: {row['change']:+.2f}% | 15m volume: {row['volume_ratio']:.2f}× average",
        turnover,
        f"🔎 {row['reasons']}",
        "⚠️ Spot watch only. No guaranteed pump; manage risk.",
    ])


def square_text(row):
    return (
        f"💎 BINANCE GEMS | EARLY SPOT RADAR\n"
        f"${row['base']} ({row['symbol']}) — Score: {row['score']}/100\n"
        f"Price: {fmt_price(row['close'])} USDT | 24h: {row['change']:+.2f}%\n"
        f"15m volume: {row['volume_ratio']:.2f}× average\n"
        f"Watching for confirmation, not chasing a candle. Spot watch only; no guaranteed pump."
    )


def square_post(posts):
    eligible = [(r, square_text(r)) for r in posts if r["score"] >= SQUARE_MIN_SCORE]
    if not eligible:
        return
    if POST_TO_SQUARE and SQUARE_KEY:
        headers = {
            "X-Square-OpenAPI-Key": SQUARE_KEY,
            "Content-Type": "application/json",
            "clienttype": "binanceSkill",
        }
        for i, (row, body) in enumerate(eligible):
            try:
                resp = session.post(SQUARE_URL, json={"bodyTextOnly": body}, headers=headers, timeout=TIMEOUT)
                resp.raise_for_status()
                payload = resp.json()
                if str(payload.get("code", "")) != "000000":
                    raise RuntimeError(str(payload.get("message", payload.get("msg", "Square API rejected post"))))
                LOG.info("Square accepted post for %s", row["symbol"])
            except Exception as exc:
                LOG.error("Square post failed for %s: %s", row["symbol"], exc)
                with DRAFT_FILE.open("a", encoding="utf-8") as f:
                    f.write(f"\n--- {row['symbol']} ---\n{body}\n")
            if i < len(eligible) - 1:
                time.sleep(random.randint(10, 20))
    else:
        with DRAFT_FILE.open("a", encoding="utf-8") as f:
            for row, body in eligible:
                f.write(f"\n--- {row['symbol']} ---\n{body}\n")
        LOG.warning("Square posting disabled or API key missing; drafts saved.")


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s | %(levelname)s | %(message)s")
    state = load_state()
    now = time.time()
    LOG.info("Loading Binance Spot USDT universe...")
    universe = get_universe()
    LOG.info("Eligible Binance Spot pairs: %d", len(universe))
    caps = get_coingecko_market_caps()
    LOG.info("Unique CoinGecko market-cap symbols: %d", len(caps))
    scored = []
    for i, coin in enumerate(universe, 1):
        row = score_coin(coin, caps)
        if row:
            scored.append(row)
        if i % 50 == 0:
            LOG.info("Scanned %d/%d pairs; candidates=%d", i, len(universe), len(scored))
    scored.sort(key=lambda x: x["score"], reverse=True)
    square_rows = []
    sent = 0
    for row in scored:
        key = f"signal|{row['symbol']}"
        if now - float(state.get(key, 0) or 0) < SIGNAL_COOLDOWN_MINUTES * 60:
            continue
        serial = int(state.get("signal_serial", 0)) + 1
        if telegram_send(signal_text(row, serial)):
            state[key] = time.time()
            state["signal_serial"] = serial
            save_state(state)
            sent += 1
            square_rows.append(row)
            time.sleep(random.randint(2, 5))
    square_post(square_rows)
    LOG.info("Scan complete: scored=%d, Telegram signals sent=%d", len(scored), sent)


if __name__ == "__main__":
    main()
