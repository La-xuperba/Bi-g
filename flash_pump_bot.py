"""BINANCE GEMS flash-pump monitor (Binance Spot + Gate.io Spot).

Runs alongside the existing early-pump scanner. Flash alerts are never signals.
GitHub Actions is periodic, not continuous; short-lived moves may be missed.
"""
import json, logging, os, random, time
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

LOG = logging.getLogger("binance_gems.flash")
TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))
THRESHOLD = float(os.getenv("FLASH_PUMP_THRESHOLD_PCT", "5"))
COOLDOWN = int(os.getenv("FLASH_PUMP_COOLDOWN_MINUTES", "60")) * 60
STATE_PATH = Path(os.getenv("FLASH_PUMP_STATE_FILE", "flash_pump_state.json"))
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "").strip()
BINANCE_BASES = ["https://data-api.binance.vision", "https://api.binance.com"]
GATE_BASE = "https://api.gateio.ws/api/v4"
WINDOWS = ((5, "5m"), (15, "15m"), (30, "30m"))
# API budget control: scan these many most-active USDT pairs per exchange each run.
# No price floor is applied. Ranking by quote turnover is a practical scheduled-run compromise.
MAX_PAIRS = int(os.getenv("FLASH_PUMP_MAX_PAIRS", "250"))


def _get(url, params=None):
    r = requests.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def load_state():
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8")) if STATE_PATH.exists() else {}
    except (OSError, ValueError):
        return {}


def save_state(state):
    STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def pct_change(open_price, last_price):
    if open_price is None or last_price is None or float(open_price) <= 0:
        return None
    return (float(last_price) / float(open_price) - 1.0) * 100.0


def _binance_get(path, params=None):
    error = None
    for base in BINANCE_BASES:
        try:
            return _get(base + path, params)
        except requests.RequestException as exc:
            error = exc
    raise error


def get_binance_pairs():
    tickers = _binance_get("/api/v3/ticker/24hr")
    info = _binance_get("/api/v3/exchangeInfo")
    valid = {s["symbol"] for s in info.get("symbols", [])
             if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT"
             and s.get("isSpotTradingAllowed", False)}
    ranked = []
    for t in tickers:
        symbol = t.get("symbol", "")
        if symbol in valid:
            try:
                ranked.append((symbol, float(t.get("quoteVolume", 0) or 0)))
            except (TypeError, ValueError):
                pass
    ranked.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in ranked[:MAX_PAIRS]]


def check_binance_symbol(symbol):
    """Alert only when a single, fully closed 5m or 15m candle rose >= threshold.

    The currently-forming candle is always excluded. 24h ticker change is not
    used as a trigger, so a coin up 12% in 24h but only 1% on its latest candle
    will not generate a flash alert.
    """
    alerts = []
    for interval in ("5m", "15m"):
        rows = _binance_get(
            "/api/v3/klines",
            {"symbol": symbol, "interval": interval, "limit": 3},
        )
        # Binance kline format: [open time, open, high, low, close, ...].
        # The final row is still forming; rows[-2] is the latest closed candle.
        if len(rows) < 2:
            continue
        candle = rows[-2]
        opening = float(candle[1])
        closing = float(candle[4])
        change = pct_change(opening, closing)
        if change is not None and change >= THRESHOLD:
            alerts.append({
                "exchange": "Binance Spot",
                "pair": symbol,
                "window": interval,
                "minutes": int(interval[:-1]),
                "change": change,
                "price": closing,
                "candle_open_time": int(candle[0]),
            })
    return alerts

def scan_flash_pumps():
    """Scan the most active Binance Spot USDT pairs for closed-candle pumps."""
    try:
        pairs = get_binance_pairs()
    except Exception as exc:
        LOG.error("Could not load Binance Spot pair universe: %s", exc)
        return []

    found = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(check_binance_symbol, symbol): symbol for symbol in pairs}
        for future in as_completed(futures):
            try:
                found.extend(future.result())
            except Exception as exc:
                LOG.debug("Flash check failed for %s: %s", futures[future], exc)
    return found


def format_flash_post(alert):
    pair = alert["pair"].replace("_", "/")
    price = alert["price"]
    if price >= 1000: price_text = f"{price:,.2f}"
    elif price >= 1: price_text = f"{price:.4f}".rstrip("0").rstrip(".")
    elif price >= .01: price_text = f"{price:.6f}".rstrip("0").rstrip(".")
    else: price_text = f"{price:.10f}".rstrip("0").rstrip(".")
    variants = [
        f"⚡ FLASH PUMP ALERT — {alert['exchange']}\n{pair} closed its {alert['window']} candle +{alert['change']:.2f}% from open to close.\nClose: {price_text} USDT. Fast moves can reverse quickly; check liquidity before acting.",
        f"🚨 FAST MOVE DETECTED | {alert['exchange']}\nThe closed {alert['window']} candle for {pair} gained +{alert['change']:.2f}% from open to close.\nClose: {price_text} USDT. This is a movement alert, not a buy signal.",
        f"📈 FLASH WATCH — {alert['exchange']}\n{pair}'s latest closed {alert['window']} candle rose +{alert['change']:.2f}%.\nClose: {price_text} USDT. Watch volatility and avoid chasing blindly.",
        f"⚡ MARKET ALERT | {alert['exchange']}\n{pair} printed a +{alert['change']:.2f}% closed {alert['window']} candle.\nClose: {price_text} USDT. Alert only — no signal score assigned.",
    ]
    return random.choice(variants)


def telegram_post(text):
    if not TG_TOKEN or not TG_CHAT:
        LOG.warning("Telegram credentials missing; cannot publish alert.")
        return False
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    for attempt in range(5):
        try:
            r = requests.post(url, json={"chat_id": TG_CHAT, "text": text,
                                         "disable_web_page_preview": True}, timeout=TIMEOUT)
            if r.status_code == 429:
                wait = int(r.json().get("parameters", {}).get("retry_after", 2))
                time.sleep(min(wait, 60)); continue
            r.raise_for_status()
            payload = r.json()
            if payload.get("ok"):
                return True
            raise RuntimeError("Telegram API returned ok=false")
        except requests.RequestException as exc:
            if attempt == 4: raise
            time.sleep(min(2 ** attempt, 20))
    return False


def run():
    state = load_state()
    now = time.time()
    alerts = scan_flash_pumps()
    fresh = []
    for alert in alerts:
        candle_key = f"seen|{alert['exchange']}|{alert['pair']}|{alert['window']}|{alert.get('candle_open_time', 0)}"
        cooldown_key = f"cooldown|{alert['exchange']}|{alert['pair']}|{alert['window']}"
        already_sent_candle = float(state.get(candle_key, 0) or 0) > 0
        last_sent = float(state.get(cooldown_key, 0) or 0)
        if not already_sent_candle and now - last_sent >= COOLDOWN:
            alert["_key"] = candle_key
            alert["_cooldown_key"] = cooldown_key
            fresh.append(alert)
    fresh.sort(key=lambda a: a["change"], reverse=True)
    LOG.info("Flash scan found %d threshold events; %d are new after cooldown.", len(alerts), len(fresh))
    for idx, alert in enumerate(fresh):
        post = format_flash_post(alert)
        if telegram_post(post):
            state[alert["_key"]] = now
            state[alert["_cooldown_key"]] = now
            save_state(state)
            LOG.info("Flash alert sent: %s %s %s +%.2f%%", alert['exchange'], alert['pair'], alert['window'], alert['change'])
            if idx < len(fresh) - 1:
                delay = random.randint(10, 20)
                LOG.info("Waiting %ds before next alert post.", delay)
                time.sleep(delay)

if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s | %(levelname)s | %(message)s")
    run()
