"""
BINANCE GEMS — Spot Screener + Short Square Drafts + Original Charts
Python 3.11 | pip install -r requirements.txt

Uses public Binance Spot market data and Telegram Bot API.
Square publishing is NOT automated unless you configure a documented,
authorized endpoint that explicitly supports the payload you use.
By default, creates short drafts and matching original chart PNG files.
"""
import os
import time
import math
import random
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE_URL = os.getenv("BINANCE_BASE_URL", "https://data-api.binance.vision").rstrip("/")
BINANCE_URLS = list(dict.fromkeys([BASE_URL, "https://data-api.binance.vision", "https://api.binance.com"]))
QUOTE_ASSET = "USDT"
MIN_PRICE = 0.01
MAX_24H_CHANGE_PCT = 15.0
TREND_INTERVAL = "1h"
TRIGGER_INTERVAL = "15m"
TREND_LIMIT = 120
TRIGGER_LIMIT = 100
BTC_RSI_MIN = 48
COIN_RSI_MIN = 52
COIN_RSI_MAX = 67
MIN_VOLUME_RATIO = 1.5
PREVIOUS_VOLUME_RATIO = 1.2
MIN_SCORE = int(os.getenv("MIN_SCORE", "45"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
REQUEST_TIMEOUT = 12
SQUARE_POST_DELAY_SECONDS = int(os.getenv("SQUARE_POST_DELAY_SECONDS", "45"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
# Kept only for an authorized endpoint explicitly configured by the user.
SQUARE_API_URL = os.getenv("BINANCE_SQUARE_API_URL", "").strip()
SQUARE_API_TOKEN = os.getenv("BINANCE_SQUARE_API_TOKEN", "").strip()
CHART_DIR = Path("square_charts")
DRAFT_PATH = Path("square_drafts.txt")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("binance_gems")


def binance_get(endpoint, params=None):
    last_error = None
    for base in BINANCE_URLS:
        try:
            response = requests.get(base + endpoint, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            log.warning("Binance endpoint failed (%s): %s", base, exc)
    raise last_error


def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def atr(df, period=14):
    previous = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - previous).abs(),
        (df["low"] - previous).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


def macd_histogram(close):
    macd = ema(close, 12) - ema(close, 26)
    return macd - ema(macd, 9)


def get_klines(symbol, interval, limit):
    raw = binance_get("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    cols = ["open_time", "open", "high", "low", "close", "volume", "close_time",
            "quote_volume", "number_of_trades", "taker_buy_base_volume",
            "taker_buy_quote_volume", "ignore"]
    df = pd.DataFrame(raw, columns=cols)
    for col in ["open", "high", "low", "close", "volume", "quote_volume",
                "number_of_trades", "taker_buy_base_volume", "taker_buy_quote_volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df


def build_universe():
    info = binance_get("/api/v3/exchangeInfo")
    tickers = binance_get("/api/v3/ticker/24hr")
    by_symbol = {t["symbol"]: t for t in tickers}
    stable_bases = {"USDC", "FDUSD", "TUSD", "USDP", "DAI", "USDD", "BUSD",
                    "EUR", "TRY", "USDE", "PYUSD"}
    excluded_suffixes = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")
    result = []
    for item in info["symbols"]:
        symbol, base = item["symbol"], item["baseAsset"]
        if item.get("status") != "TRADING" or not item.get("isSpotTradingAllowed", False):
            continue
        if item.get("quoteAsset") != QUOTE_ASSET or base in stable_bases:
            continue
        if any(symbol.endswith(suffix) for suffix in excluded_suffixes):
            continue
        ticker = by_symbol.get(symbol)
        if not ticker:
            continue
        price = float(ticker["lastPrice"])
        change = float(ticker["priceChangePercent"])
        if price < MIN_PRICE or change >= MAX_24H_CHANGE_PCT:
            continue
        result.append({
            "symbol": symbol, "base_asset": base, "last_price": price,
            "quote_volume_24h": float(ticker["quoteVolume"]),
            "price_change_24h_pct": change,
        })
    return result


def get_btc_return_6h():
    df = get_klines("BTCUSDT", "1h", TREND_LIMIT)
    df["ema20"], df["ema50"] = ema(df["close"], 20), ema(df["close"], 50)
    df["rsi14"] = rsi(df["close"])
    row = df.iloc[-2]
    return float(df["close"].pct_change(6).iloc[-2] * 100), bool(
        row["close"] > row["ema50"] and row["ema20"] > row["ema50"] and row["rsi14"] >= BTC_RSI_MIN
    )


def screen_symbol(item, btc_return):
    trend_df = get_klines(item["symbol"], TREND_INTERVAL, TREND_LIMIT)
    trigger_df = get_klines(item["symbol"], TRIGGER_INTERVAL, TRIGGER_LIMIT)
    trend_df["ema20"] = ema(trend_df["close"], 20)
    trend_df["ema50"] = ema(trend_df["close"], 50)
    trend_df["return_6h"] = trend_df["close"].pct_change(6) * 100

    trigger_df["rsi14"] = rsi(trigger_df["close"])
    trigger_df["atr14"] = atr(trigger_df)
    trigger_df["macd_histogram"] = macd_histogram(trigger_df["close"])
    trigger_df["volume_sma20"] = trigger_df["volume"].rolling(20).mean()
    trigger_df["volume_ratio"] = trigger_df["volume"] / trigger_df["volume_sma20"]
    trigger_df["previous_high_20"] = trigger_df["high"].shift(1).rolling(20).max()
    trigger_df["body"] = (trigger_df["close"] - trigger_df["open"]).abs()
    trigger_df["upper_wick"] = trigger_df["high"] - trigger_df[["open", "close"]].max(axis=1)
    trigger_df["wick_ratio"] = trigger_df["upper_wick"] / trigger_df["body"].replace(0, np.nan)

    trend, trigger, previous = trend_df.iloc[-2], trigger_df.iloc[-2], trigger_df.iloc[-3]
    trend_ok = trend["close"] > trend["ema20"] > trend["ema50"]
    volume_ratio = float(trigger["volume_ratio"])
    previous_volume = float(previous["volume_ratio"])
    breakout_ok = trigger["close"] > trigger["previous_high_20"]
    momentum_ok = COIN_RSI_MIN <= trigger["rsi14"] <= COIN_RSI_MAX and trigger["macd_histogram"] > 0
    relative_strength = float(trend["return_6h"] - btc_return)
    distance = float(trend["close"] / trend["ema20"])
    score = 0
    reasons = []
    if trend_ok: score += 25; reasons.append("1h trend aligned")
    if volume_ratio >= MIN_VOLUME_RATIO: score += 20; reasons.append(f"15m volume {volume_ratio:.2f}x")
    if breakout_ok: score += 15; reasons.append("15m breakout")
    if momentum_ok: score += 15; reasons.append("RSI/MACD aligned")
    if relative_strength > 0: score += 15; reasons.append("outperforming BTC")
    if 1.01 <= distance <= 1.06: score += 10; reasons.append("EMA20 extension acceptable")

    green = trigger["close"] > trigger["open"]
    body_atr = float(trigger["body"] / trigger["atr14"]) if trigger["atr14"] > 0 else 0
    wick_ok = pd.notna(trigger["wick_ratio"]) and trigger["wick_ratio"] <= 1.5
    active = volume_ratio >= MIN_VOLUME_RATIO or previous_volume >= PREVIOUS_VOLUME_RATIO
    candle_ok = green and body_atr >= 0.25 and wick_ok
    eligible = score >= MIN_SCORE and (trend_ok or momentum_ok) and active and candle_ok and relative_strength > -1.0

    return {
        **item, "close": float(trigger["close"]), "score": int(score), "eligible": bool(eligible),
        "volume_ratio_15m": volume_ratio, "previous_volume_ratio_15m": previous_volume,
        "rsi_15m": float(trigger["rsi14"]), "relative_strength_vs_btc_pct": relative_strength,
        "reasons": " | ".join(reasons) if reasons else "Activity/momentum scan",
        "candle_close_time": trigger["close_time"],
    }


def scan():
    log.info("Loading BTC regime and Binance Spot universe...")
    btc_return, btc_enabled = get_btc_return_6h()
    if not btc_enabled:
        log.warning("BTC regime weak; scan continues with caution.")
    universe = build_universe()
    log.info("%d symbols in eligible Binance Spot universe", len(universe))
    results, errors = [], []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(screen_symbol, item, btc_return): item["symbol"] for item in universe}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                errors.append((futures[future], str(exc)))
    if errors:
        log.warning("%d symbols failed; examples: %s", len(errors), errors[:5])
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results).sort_values(
        ["eligible", "score", "relative_strength_vs_btc_pct", "volume_ratio_15m"],
        ascending=[False, False, False, False]
    )
    stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%d_%H%M%S")
    df.to_csv(f"pre_pump_ranking_{stamp}.csv", index=False)
    return df[df["eligible"]].copy()


def fmt_price(value):
    value = float(value)
    if value >= 1000: return f"{value:,.2f}"
    if value >= 1: return f"{value:.4f}".rstrip("0").rstrip(".")
    if value >= 0.01: return f"{value:.5f}".rstrip("0").rstrip(".")
    return f"{value:.8f}".rstrip("0").rstrip(".")


def make_telegram_message(selected):
    now = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M UTC")
    lines = ["💎 BINANCE GEMS | PRE-PUMP RADAR", f"🕒 Scan: {now}",
             f"📊 Qualified candidates: {len(selected)}"]
    if selected.empty:
        lines += ["", "No coin met the current eligibility checks this scan."]
    else:
        for i, (_, row) in enumerate(selected.iterrows(), 1):
            lines += [
                "", f"{i}. ${row['base_asset']} — Score {row['score']}/100",
                f"💵 Price: {fmt_price(row['close'])} USDT",
                f"📈 24h: {row['price_change_24h_pct']:+.2f}% | Vol: ${row['quote_volume_24h']/1e6:.2f}M",
                f"⚡ 15m volume: {row['volume_ratio_15m']:.2f}x | RSI: {row['rsi_15m']:.1f}",
                f"🔎 {row['reasons']}", "⚠️ Spot watch only; verify your own entry and risk."
            ]
    return "\n".join(lines)


def create_chart(symbol, interval="15m"):
    df = get_klines(symbol, interval, 48)
    if df.empty:
        raise ValueError("No candle data")
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    path = CHART_DIR / f"{symbol}_{interval}.png"
    fig, ax = plt.subplots(figsize=(10, 5), dpi=160)
    for i, row in df.iterrows():
        color = "#00C087" if row["close"] >= row["open"] else "#F6465D"
        ax.vlines(i, row["low"], row["high"], color=color, linewidth=1)
        bottom = min(row["open"], row["close"])
        height = max(abs(row["close"] - row["open"]), abs(row["close"]) * 0.00001)
        ax.bar(i, height, bottom=bottom, width=0.62, color=color, edgecolor=color)
    ax.set_title(f"{symbol} | Binance Spot | {interval} candles")
    ax.set_ylabel("Price (USDT)")
    ax.grid(alpha=0.2)
    ticks = list(range(0, len(df), 6))
    ax.set_xticks(ticks)
    ax.set_xticklabels([df["open_time"].iloc[i].strftime("%H:%M") for i in ticks], rotation=25)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return str(path)


def make_square_posts(selected):
    posts = []
    # Several short styles; choose according to actual measured conditions.
    for _, row in selected.iterrows():
        coin = row["base_asset"]
        change = float(row["price_change_24h_pct"])
        vol = float(row["volume_ratio_15m"])
        score = int(row["score"])
        if vol >= 2.5:
            text = f"⚡ ${coin} volume is heating up!\n{vol:.1f}× its average 👀\nWorth watching?"
        elif row["relative_strength_vs_btc_pct"] >= 1:
            text = f"👀 ${coin} is outperforming BTC.\nMomentum is picking up.\nOn your watchlist?"
        elif change >= 3:
            text = f"🚀 ${coin} is moving higher!\n24h: {change:+.1f}%\nCan buyers keep it going?"
        elif score >= 70:
            text = f"💎 ${coin} is showing strong signals.\nMomentum is developing.\nKeep an eye on it."
        else:
            text = f"📈 ${coin} is getting active.\nWatching for confirmation 👀"
        try:
            chart_path = create_chart(row["symbol"], "15m")
        except Exception as exc:
            log.warning("Chart failed for %s: %s", row["symbol"], exc)
            chart_path = None
        posts.append({"symbol": row["symbol"], "text": text, "chart_path": chart_path})
    return posts


def post_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram secrets missing; skipping Telegram.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chunks, current = [], ""
    for line in message.splitlines():
        candidate = f"{current}\n{line}".strip() if current else line
        if len(candidate) > 3800 and current:
            chunks.append(current)
            current = line
        else:
            current = candidate
    if current: chunks.append(current)
    for chunk in chunks:
        response = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID, "text": chunk, "disable_web_page_preview": True
        }, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        if not response.json().get("ok"):
            raise RuntimeError("Telegram API returned an unsuccessful response")
    return True


def save_square_drafts(posts):
    """Save drafts and charts; publishing images needs an authorized image-upload API."""
    with DRAFT_PATH.open("w", encoding="utf-8") as f:
        for post in posts:
            f.write(f"--- {post['symbol']} ---\n{post['text']}\n")
            f.write(f"Chart file: {post['chart_path'] or 'CHART_FAILED'}\n\n")
    log.info("Saved %d Square drafts to %s", len(posts), DRAFT_PATH)
    if SQUARE_API_URL:
        log.warning(
            "BINANCE_SQUARE_API_URL is set, but this script will not send text-only "
            "requests because image upload support has not been verified. Drafts saved instead."
        )
    else:
        log.info("No authorized Square endpoint configured; manual review/posting required.")


def main():
    selected = scan()
    telegram_message = make_telegram_message(selected)
    post_telegram(telegram_message)
    posts = make_square_posts(selected)
    save_square_drafts(posts)
    if not selected.empty:
        print("\nSELECTED COINS")
        print(selected[["symbol", "score", "close", "volume_ratio_15m",
                        "price_change_24h_pct"]].to_string(index=False))
    else:
        print("No eligible coins this scan.")


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as exc:
        log.exception("Network/API error: %s", exc)
        raise
    except Exception as exc:
        log.exception("Unexpected error: %s", exc)
        raise
