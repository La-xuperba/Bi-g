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
    symbol = row['base_asset']
    change = float(row['price_change_24h_pct'])
    ratio = float(row['volume_ratio_15m'])
    variants = [
        f"💎 ${symbol} is on the BINANCE GEMS spot radar.\nScore: {int(row['score'])}/100 | Price: {core.fmt_price(row['close'])} USDT\n15m volume is running at {ratio:.2f}× its average, with 24h change at {change:+.2f}%.\nWatching for confirmation rather than chasing a candle.",
        f"Early momentum watch: ${symbol}.\nCurrent score {int(row['score'])}/100 at {core.fmt_price(row['close'])} USDT.\nVolume activity is {ratio:.2f}× average; 24h change: {change:+.2f}%.\nA watchlist candidate, not a promise of a pump.",
        f"👀 Keeping ${symbol} on the spot watchlist.\nPrice: {core.fmt_price(row['close'])} USDT | Radar score: {int(row['score'])}/100\n15m activity: {ratio:.2f}× average. 24h: {change:+.2f}%.\nThe setup still needs confirmation—no blind chasing.",
    ]
    return (row['symbol'], random.choice(variants))


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
