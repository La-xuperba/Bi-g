"""BINANCE GEMS flash-pump monitor — Binance Spot USDT only."""
import json
import logging
import os
import time
from pathlib import Path

import requests

LOG = logging.getLogger("binance_gems.flash")
ROOTS = ["https://api.binance.com", "https://data-api.binance.vision"]
STATE_FILE = Path(os.getenv("FLASH_PUMP_STATE_FILE", "flash_pump_state.json"))
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
THRESHOLD = float(os.getenv("FLASH_PUMP_THRESHOLD_PCT", "5"))
COOLDOWN = int(os.getenv("FLASH_PUMP_COOLDOWN_MINUTES", "60"))
MAX_PAIRS = int(os.getenv("FLASH_PUMP_MAX_PAIRS", "250"))
BLOCK = {"USDC", "FDUSD", "TUSD", "USDP", "DAI", "USDD", "BUSD", "USDE", "PYUSD",
         "AAPL", "AMZN", "COIN", "CRCL", "GOOGL", "META", "MSFT", "MSTR", "NVDA",
         "QQQ", "SPY", "TSLA", "HOOD"}
session = requests.Session()


def get_json(path):
    last = None
    for root in ROOTS:
        try:
            r = session.get(root + path, timeout=12)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last = exc
    raise RuntimeError(f"Binance API unavailable: {last}")


def load_state():
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8")) if STATE_FILE.exists() else {}
    except Exception:
        return {}


def send(text):
    if not BOT_TOKEN or not CHAT_ID:
        LOG.warning("Telegram credentials missing; flash alert skipped.")
        return False
    try:
        r = session.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                         json={"chat_id": CHAT_ID, "text": text}, timeout=12)
        r.raise_for_status()
        return bool(r.json().get("ok"))
    except Exception as exc:
        LOG.error("Flash alert Telegram failed: %s", exc)
        return False


def run():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s | %(levelname)s | %(message)s")
    state = load_state()
    info = get_json("/api/v3/exchangeInfo")
    tickers = get_json("/api/v3/ticker/24hr")
    valid = {
        s["symbol"] for s in info.get("symbols", [])
        if s.get("status") == "TRADING"
        and s.get("quoteAsset") == "USDT"
        and s.get("isSpotTradingAllowed", False)
        and s.get("baseAsset", "").upper() not in BLOCK
        and not s.get("baseAsset", "").upper().endswith(("UP", "DOWN", "BULL", "BEAR"))
    }
    pairs = [x for x in tickers if x.get("symbol") in valid]
    pairs.sort(key=lambda x: float(x.get("quoteVolume", 0) or 0), reverse=True)
    now = time.time()
    sent = 0
    for item in pairs[:MAX_PAIRS]:
        symbol = item["symbol"]
        try:
            change = float(item["priceChangePercent"])
            quote_volume = float(item["quoteVolume"])
        except (KeyError, TypeError, ValueError):
            continue
        if change < THRESHOLD:
            continue
        key = f"flash|{symbol}"
        if now - float(state.get(key, 0) or 0) < COOLDOWN * 60:
            continue
        base = symbol[:-4] if symbol.endswith("USDT") else symbol
        text = (f"⚡ BINANCE GEMS | FLASH SPOT ALERT\n"
                f"🪙 {base} ({symbol})\n"
                f"📈 24h move: {change:+.2f}%\n"
                f"💵 Price: {item.get('lastPrice')} USDT\n"
                f"💧 24h quote volume: ${quote_volume:,.0f}\n"
                f"⚠️ Momentum alert only — do your own risk checks.")
        if send(text):
            state[key] = time.time()
            sent += 1
            time.sleep(2)
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    LOG.info("Flash scan complete; alerts sent=%d", sent)


if __name__ == "__main__":
    run()
