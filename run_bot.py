"""Orchestrator: preserve original early-pump eligibility and add independent flash alerts."""
import json, logging, os, random, time
from pathlib import Path
import pandas as pd
import crypto_signal_bot as core
import flash_pump_bot as flash

STATE_PATH = Path(os.getenv("STATE_FILE", "state_binance.json"))
SIGNAL_COOLDOWN = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", "720")) * 60
POST_DELAY_MIN = max(10, min(20, int(os.getenv("POST_DELAY_MIN_SECONDS", "10"))))
POST_DELAY_MAX = max(POST_DELAY_MIN, min(20, int(os.getenv("POST_DELAY_MAX_SECONDS", "20"))))


def load_state():
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8")) if STATE_PATH.exists() else {}
    except (OSError, ValueError):
        return {}


def save_state(state):
    STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def signal_message(row, serial):
    market_cap_line = (
        f"🏦 Market-cap turnover: {float(row['volume_to_market_cap_pct']):.2f}%"
        if pd.notna(row.get('volume_to_market_cap_pct'))
        else "🏦 Market-cap ratio unavailable; using Binance activity fallback"
    )
    return "\n".join([
        f"💎 BINANCE GEMS | EARLY SPOT SIGNAL #{serial}",
        f"🪙 {row['base_asset']} ({row['symbol']})",
        f"⭐ Score: {int(row['score'])}/100 | BUY WATCH",
        f"💵 Price: {core.fmt_price(row['close'])} USDT",
        f"📈 24h: {float(row['price_change_24h_pct']):+.2f}% | 15m volume: {float(row['volume_ratio_15m']):.2f}× average",
        market_cap_line,
        f"🔎 {row.get('reasons', 'Early momentum criteria passed')}",
        "⚠️ Spot watch only. No guaranteed pump; manage risk.",
    ])


def square_signal_post(row):
    """Build a concise Square post whose angle reflects this coin's actual setup."""
    symbol = str(row["base_asset"])
    change = float(row["price_change_24h_pct"])
    ratio = float(row["volume_ratio_15m"])
    score = int(row["score"])
    price = core.fmt_price(row["close"])
    reasons = str(row.get("reasons", "") or "").lower()

    # Select the narrative from measurable conditions rather than changing only the ticker.
    if change >= 8:
        opening = f"👀 ${symbol} has already moved {change:+.2f}% in 24h, so I’m not treating this as a chase entry."
        detail = f"Radar score: {score}/100 • Price: {price} USDT."
        close = "I’d want a clean pause or fresh confirmation before considering a spot entry."
    elif ratio >= 2.0 or "volume" in reasons or "turnover" in reasons:
        opening = f"📊 ${symbol} caught my eye because spot activity is running at {ratio:.2f}× its recent average."
        detail = f"Score: {score}/100 • 24h change: {change:+.2f}% • Price: {price} USDT."
        close = "The next thing to watch is whether that activity holds through candle close."
    elif any(term in reasons for term in ("ema", "trend", "slope", "support")):
        opening = f"🔎 ${symbol} is on my radar for its technical structure, not just the ticker move."
        detail = f"Score: {score}/100 • Price: {price} USDT • 15m volume: {ratio:.2f}× average."
        close = "I’m watching whether price can respect the trend area and confirm."
    elif any(term in reasons for term in ("candle", "close", "breakout", "momentum")):
        opening = f"🕯️ ${symbol} has a candle setup worth watching on the spot chart."
        detail = f"Score: {score}/100 • 24h: {change:+.2f}% • Price: {price} USDT."
        close = "A strong close matters more than a quick wick, so confirmation comes first."
    else:
        opening = f"💎 I’m keeping ${symbol} on the BINANCE GEMS spot watchlist for now."
        detail = f"Score: {score}/100 • Price: {price} USDT • 15m volume: {ratio:.2f}× average."
        close = f"24h change is {change:+.2f}%; I’ll wait for confirmation instead of guessing."

    return (str(row["symbol"]), "\n".join([opening, detail, close, "Spot watch only — no guaranteed pump."]))


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s | %(levelname)s | %(message)s")
    state = load_state()
    now = time.time()
    selected = core.scan()  # only original eligibility-passing candidates are regular signals
    square_posts = []
    if selected is None or selected.empty:
        logging.info("No regular early-pump candidates passed this scan.")
    else:
        for _, row in selected.iterrows():
            symbol = str(row['symbol'])
            key = f"signal|{symbol}"
            if now - float(state.get(key, 0) or 0) < SIGNAL_COOLDOWN:
                logging.info("Skipping repeated regular signal during cooldown: %s", symbol)
                continue
            serial = int(state.get("signal_serial", 0)) + 1
            message = signal_message(row, serial)
            if core.post_telegram(message):
                state[key] = time.time()
                state["signal_serial"] = serial
                save_state(state)
            # Collect one separate Square draft per eligible coin; never bulk-post coins together.
            square_posts.append(square_signal_post(row))
            time.sleep(random.randint(POST_DELAY_MIN, POST_DELAY_MAX))

    # Square integration is only attempted if user has configured an authorized gateway.
    core.post_square_or_save_drafts(square_posts)

    # Independent monitor: alerts do not need a score or regular-signal eligibility.
    # If the same coin also passed core.scan(), it can legitimately get both message types.
    flash.run()

if __name__ == "__main__":
    main()
