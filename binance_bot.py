"""
Binance Momentum Scanner (only coins listed on Binance, USDT spot pairs)

Free public endpoints only (no API key): Binance public market data, OKX public data
(funding rate / open interest), CoinGecko trending.

What it does
  - Scans ALL Binance USDT spot pairs (2-stage: cheap filter -> deep analysis)
  - 15m + 1h + 4h technical analysis, Bollinger squeeze, support/resistance
  - Binance taker-buy ratio (real buy pressure), volume spike, relative strength vs BTC
  - Funding rate + open interest (perpetuals)
  - BTC market regime filter, CoinGecko trending bonus
  - Alerts only for NEW coins or big score jumps
  - Performance tracking of every pick (1h / 6h / 24h) -> /stats
  - Telegram commands: /scan  /coin <SYMBOL>  /stats  /help

NOT a guarantee that any coin will pump. It is a momentum + risk score.

Run:
  python binance_bot.py --test-telegram
  python binance_bot.py --once          # one forced scan + full report
  python binance_bot.py --tick          # one cycle (GitHub Actions, every 15 min)
  python binance_bot.py --tick --force
  python binance_bot.py --watch 12      # keep running 12 min (GitHub Actions near-real-time mode)
  python binance_bot.py                 # loop forever (Termux / PC)
"""
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------- Settings (from .env / environment) ----------------
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
SCAN_EVERY_MIN = int(os.getenv("SCAN_EVERY_MIN", "30"))
TICK_SECONDS = int(os.getenv("TICK_SECONDS", "60"))
TOP_N = int(os.getenv("TOP_N", "6"))
MIN_QV = float(os.getenv("MIN_QUOTE_VOLUME", "5000000"))      # min 24h USDT volume
STAGE1_COUNT = int(os.getenv("STAGE1_COUNT", "40"))            # coins pre-checked by volume
GAINERS_COUNT = int(os.getenv("GAINERS_COUNT", "15"))          # extra top gainers pre-checked
DEEP_COUNT = int(os.getenv("DEEP_COUNT", "12"))                # coins fully analyzed
MAX_CHANGE_24H = float(os.getenv("MAX_CHANGE_24H", "60"))
ALERT_MIN_SCORE = float(os.getenv("ALERT_MIN_SCORE", "55"))
ALERT_JUMP = float(os.getenv("ALERT_SCORE_JUMP", "10"))
ALERT_COOLDOWN_H = float(os.getenv("ALERT_COOLDOWN_HOURS", "8"))
ALERT_STAGES = {x.strip().upper() for x in os.getenv("ALERT_STAGES", "EARLY,RUNNING").split(",") if x.strip()}
ENABLE_LISTING = os.getenv("ENABLE_NEW_LISTING_ALERTS", "true").lower() == "true"
DIGEST_EVERY_MIN = int(os.getenv("DIGEST_EVERY_MIN", "60"))      # send passed coins every N minutes
DIGEST_MIN_SCORE = float(os.getenv("DIGEST_MIN_SCORE", "50"))
DIGEST_SEND_EMPTY = os.getenv("DIGEST_SEND_EMPTY", "true").lower() == "true"
STATE_FILE = os.getenv("STATE_FILE", "state_binance.json")

# Binance Square auto-post (optional). Needs a Square OpenAPI key (NOT your trading API key).
POST_SQUARE = os.getenv("POST_TO_SQUARE", "false").lower() == "true"
SQUARE_KEY = os.getenv("BINANCE_SQUARE_OPENAPI_KEY", "")
SQUARE_MIN_SCORE = float(os.getenv("SQUARE_MIN_SCORE", "65"))
SQUARE_EVERY_H = float(os.getenv("SQUARE_EVERY_HOURS", "4"))
SQUARE_URL = "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"

# Score weights - tune these after /stats shows which signals work.
W = {
    "vol_spike": 25, "taker_buy": 15, "price_ok": 10, "rs_btc": 5,
    "rsi": 10, "macd": 8, "trend_1h": 7, "breakout": 5,
    "trend_4h": 8, "trend_15m": 5, "squeeze": 3, "bb_expand": 4,
    "sr_room": 5, "trending": 5,
}




BINANCE = ["https://data-api.binance.vision", "https://api.binance.com"]


STABLES = {"USDT", "USDC", "FDUSD", "TUSD", "DAI", "BUSD", "USDP", "USDE", "EUR", "TRY", "BRL", "WBTC", "WBETH"}


S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0 (binance-momentum-bot)"})


CHECKS = (("1h", 3600), ("6h", 21600), ("24h", 86400))


DISCLAIMER = ("⚠️ Not financial advice. The score reflects momentum and a risk filter only, "
              "not a guarantee of a pump. Always DYOR.")


def log(msg):
    msg = str(msg)
    for secret in (TG_TOKEN, SQUARE_KEY):
        if secret:
            msg = msg.replace(secret, "***")
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def get_json(url, params=None, retries=2, timeout=20):
    for i in range(retries + 1):
        try:
            r = S.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                time.sleep(5 * (i + 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa
            if i == retries:
                log(f"request failed: {url.split('?')[0]} -> {e}")
                return None
            time.sleep(2)
    return None


def _f(x, default=0.0):
    try:
        return float(x)
    except Exception:  # noqa
        return default


def usd(x):
    x = x or 0
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(x) >= div:
            return f"${x / div:.2f}{suf}"
    return f"${x:.2f}"


def load_state():
    try:
        with open(STATE_FILE) as f:
            st = json.load(f)
    except Exception:  # noqa
        st = {}
    for k, v in (("tg_offset", 0), ("last_scan", 0), ("alerted", {}), ("picks", []), ("closed", []),
                 ("oi", {}), ("last_summary", 0), ("n_scans", 0), ("n_alerts", 0), ("last_tweet", 0)):
        st.setdefault(k, v)
    return st


def save_state(st):
    now = time.time()
    st["alerted"] = {k: v for k, v in st["alerted"].items() if now - v["ts"] < 3 * 86400}
    st["closed"] = st["closed"][-500:]
    st["picks"] = st["picks"][-200:]
    with open(STATE_FILE, "w") as f:
        json.dump(st, f)


def send_telegram(text):
    if not TG_TOKEN or not TG_CHAT:
        log("No Telegram token/chat id set, printing to console:")
        print(text)
        return False
    ok = True
    for i in range(0, len(text), 4000):
        try:
            r = S.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                       data={"chat_id": TG_CHAT, "text": text[i:i + 4000], "disable_web_page_preview": True},
                       timeout=20)
            if not r.ok:
                log(f"Telegram error: {r.status_code} {r.text[:200]}")
                ok = False
        except Exception as e:  # noqa
            log(f"Telegram error: {e}")
            ok = False
    return ok


def send_blocks(blocks):
    msg = ""
    for b in blocks:
        if msg and len(msg) + len(b) + 2 > 3900:
            send_telegram(msg)
            msg = ""
        msg += ("\n\n" if msg else "") + b
    if msg:
        send_telegram(msg)


def tg_updates(offset):
    if not TG_TOKEN:
        return []
    d = get_json(f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates",
                 params={"offset": offset, "timeout": 0, "allowed_updates": json.dumps(["message"])})
    return d.get("result", []) if isinstance(d, dict) else []


def handle_commands(st):
    cmds = {"scan": False, "coins": [], "stats": False, "help": False}
    for u in tg_updates(st["tg_offset"]):
        st["tg_offset"] = u["update_id"] + 1
        m = u.get("message") or {}
        if str((m.get("chat") or {}).get("id")) != str(TG_CHAT):
            continue  # ignore everyone except the owner
        text = (m.get("text") or "").strip()
        if not text.startswith("/"):
            continue
        parts = text.split()
        cmd = parts[0].split("@")[0].lower()
        if cmd == "/scan":
            cmds["scan"] = True
        elif cmd == "/coin" and len(parts) > 1:
            cmds["coins"].append(parts[1])
        elif cmd == "/stats":
            cmds["stats"] = True
        elif cmd in ("/help", "/start"):
            cmds["help"] = True
    return cmds


def col(rows, i):
    return [r[i] for r in rows]


def ewm(values, alpha):
    out = [values[0]]
    for v in values[1:]:
        out.append(alpha * v + (1 - alpha) * out[-1])
    return out


def ema(values, n):
    return ewm(values, 2 / (n + 1))


def rsi(values, n=14):
    gains, losses = [0.0], [0.0]
    for a, b in zip(values, values[1:]):
        d = b - a
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    up, dn = ewm(gains, 1 / n)[-1], ewm(losses, 1 / n)[-1]
    return 100 - 100 / (1 + up / (dn if dn else 1e-12))


def macd_hist(values):
    m = [a - b for a, b in zip(ema(values, 12), ema(values, 26))]
    return [x - y for x, y in zip(m, ema(m, 9))]


def bollinger(c, n=20, k=2):
    out = []
    for i in range(n - 1, len(c)):
        w = c[i - n + 1:i + 1]
        m = sum(w) / n
        sd = math.sqrt(sum((x - m) ** 2 for x in w) / n)
        out.append((m, m + k * sd, m - k * sd, (2 * k * sd) / m if m else 0))
    return out


def squeeze_info(c):
    """(in_squeeze_now, broke_up_after_squeeze)"""
    bb = bollinger(c)
    if len(bb) < 30:
        return False, False
    bws = [x[3] for x in bb][-100:]
    thr = sorted(bws)[int(len(bws) * 0.2)]
    now_sq = bws[-1] <= thr
    recent_sq = any(b <= thr for b in bws[-7:-1])
    return now_sq, (c[-1] > bb[-1][1] and recent_sq)


def sr_levels(h, l, c, w=3):
    """nearest swing support below and resistance above the current price"""
    price = c[-1]
    res = [h[i] for i in range(w, len(h) - w) if h[i] == max(h[i - w:i + w + 1]) and h[i] > price]
    sup = [l[i] for i in range(w, len(l) - w) if l[i] == min(l[i - w:i + w + 1]) and l[i] < price]
    return (max(sup) if sup else None), (min(res) if res else None)


def to_4h(rows):
    out = {}
    for r in rows:
        k = int(r[0]) // 14400
        if k not in out:
            out[k] = [r[0], r[1], r[2], r[3], r[4], r[5]]
        else:
            b = out[k]
            b[2], b[3], b[4], b[5] = max(b[2], r[2]), min(b[3], r[3]), r[4], b[5] + r[5]
    return [out[k] for k in sorted(out)]


def entry_stage(c24, ratio, info, late_c24=30, early_c24=12):
    """EARLY = volume accelerating but price has barely moved. LATE = the pump already happened."""
    r = info.get("rsi", 50)
    if info.get("at_res") or info.get("rejected") or info.get("extended") or c24 > late_c24 or r > 75:
        return "LATE"
    if c24 <= early_c24 and ratio >= 2.5 and r < 66:
        return "EARLY"
    return "RUNNING"


def resistance_state(hi, c):
    """(at_resistance_level, rejected_level): price sitting under a level it touched recently,
    or a recent wick that hit a level and closed well below it."""
    n, price = len(c), c[-1]
    # only levels that formed BEFORE the last 6 candles (a running high in an uptrend is not resistance)
    levels = [max(hi[-30:-6])]
    if n >= 80:
        levels.append(max(hi[-78:-6]))
    w = 2
    levels += [hi[i] for i in range(w, n - 6) if hi[i] == max(hi[i - w:i + w + 1])]
    recent_hi = max(hi[-6:])
    at_res = rejected = None
    for L in sorted(set(levels)):
        if L <= 0:
            continue
        confirmed_break = price > L and c[-2] > L
        if 0.975 * L <= price <= 1.005 * L and recent_hi >= 0.995 * L and not confirmed_break:
            at_res = L
        for k in (1, 2, 3):
            if hi[-k] >= 0.995 * L and c[-k] <= 0.985 * L:
                rejected = L
    return at_res, rejected


def tech_analysis(h1, h4, m15):
    """returns (score 0-40, notes, warns, tags, info)"""
    s, notes, warns, tags, info = 0.0, [], [], [], {}
    if h1 and len(h1) >= 50:
        c, hi, lo = col(h1, 4), col(h1, 2), col(h1, 3)
        r = rsi(c)
        info["rsi"] = r
        if 50 <= r <= 68:
            s += W["rsi"]; tags.append("rsi_ok"); notes.append(f"1h RSI {r:.0f} (healthy momentum)")
        elif 40 <= r < 50:
            s += 4
        elif r > 75:
            s -= 6; warns.append(f"1h RSI {r:.0f} (overbought)")
        h = macd_hist(c)
        if h[-1] > 0 and h[-1] > h[-2]:
            s += W["macd"]; info["macd"] = "up"; tags.append("macd_up"); notes.append("1h MACD bullish and rising")
        elif h[-1] > 0:
            s += W["macd"] / 2; info["macd"] = "+"
        else:
            info["macd"] = "down"
        e20, e50 = ema(c, 20)[-1], ema(c, 50)[-1]
        if c[-1] > e20 > e50:
            s += W["trend_1h"]; tags.append("trend_1h"); notes.append("1h: price > EMA20 > EMA50")
        if c[-1] > max(hi[-25:-1]):
            s += W["breakout"]; info["breakout"] = True; tags.append("breakout"); notes.append("Broke above 24h high")
        sq, bu = squeeze_info(c)
        if bu:
            s += W["bb_expand"]; tags.append("bb_breakout"); notes.append("Bollinger breakout after a squeeze")
        elif sq:
            s += W["squeeze"]; info["squeeze"] = True; tags.append("squeeze"); notes.append("Bollinger squeeze (volatility compressed)")
        sup, res = sr_levels(hi, lo, c)
        price = c[-1]
        info["sup"], info["res"], info["price"] = sup, res, price
        at_res, rejected = resistance_state(hi, c)
        if at_res:
            s -= 10; info["at_res"] = True
            warns.append(f"At/near resistance {at_res:.6g} (touched recently) - poor entry")
        if rejected:
            s -= 8; info["rejected"] = True
            warns.append(f"Rejected at resistance {rejected:.6g} (wick back down)")
        if price / e20 - 1 > 0.10:
            s -= 6; info["extended"] = True
            warns.append(f"Extended: {(price / e20 - 1) * 100:.0f}% above 1h EMA20 (chasing risk)")
        if res and (res - price) / price < 0.02:
            s -= 5; warns.append(f"Resistance very close (+{(res - price) / price * 100:.1f}%)")
        elif (res is None or (res - price) / price > 0.08) and not (at_res or rejected):
            s += W["sr_room"]; tags.append("room_to_run"); notes.append("Room to run (no resistance within 8%)")
    if h4 and len(h4) >= 25:
        c4 = col(h4, 4)
        e = ema(c4, 20)
        if c4[-1] > e[-1] and e[-1] > e[-3]:
            s += W["trend_4h"]; info["t4"] = "up"; tags.append("trend_4h"); notes.append("4h trend up (above rising EMA20)")
        elif c4[-1] > e[-1]:
            s += 4; info["t4"] = "+"
        else:
            info["t4"] = "down"
    if m15 and len(m15) >= 40:
        c15 = col(m15, 4)
        e = ema(c15, 20)[-1]
        if c15[-1] > e and macd_hist(c15)[-1] > 0:
            s += W["trend_15m"]; info["t15"] = "up"; tags.append("trend_15m")
        else:
            info["t15"] = "down"
        if rsi(c15) > 80:
            s -= 3; warns.append("15m RSI very high (short-term overheated)")
    return max(0.0, min(s, 40.0)), notes, warns, tags, info


def tech_line(info):
    parts = []
    if "rsi" in info:
        parts.append(f"RSI {info['rsi']:.0f}")
    if "macd" in info:
        parts.append(f"MACD {info['macd']}")
    if "t4" in info:
        parts.append(f"4h {info['t4']}")
    if "t15" in info:
        parts.append(f"15m {info['t15']}")
    if info.get("squeeze"):
        parts.append("Squeeze")
    if info.get("breakout"):
        parts.append("Breakout")
    return " | ".join(parts)


def sr_line(info):
    price, sup, res = info.get("price"), info.get("sup"), info.get("res")
    if not price or not (sup or res):
        return ""
    a = f"Support {sup:.6g} ({(sup - price) / price * 100:+.1f}%)" if sup else ""
    b = f"Resistance {res:.6g} ({(res - price) / price * 100:+.1f}%)" if res else ""
    return " | ".join(x for x in (a, b) if x)


def binance_klines(sym, interval, limit):
    for base in BINANCE:
        d = get_json(f"{base}/api/v3/klines", params={"symbol": sym, "interval": interval, "limit": limit}, retries=1)
        if isinstance(d, list) and d:
            return d
    return None


def btc_regime():
    closes = None
    kl = binance_klines("BTCUSDT", "4h", 100)
    if kl:
        closes = [_f(k[4]) for k in kl]
    else:
        d = get_json("https://www.okx.com/api/v5/market/candles",
                     params={"instId": "BTC-USDT", "bar": "4H", "limit": "100"}, retries=1)
        try:
            closes = [_f(r[4]) for r in reversed(d["data"])]
        except Exception:  # noqa
            closes = None
    if not closes or len(closes) < 60:
        return {"name": "UNKNOWN", "factor": 1.0, "note": "BTC regime: unavailable (no filter applied)"}
    e50, r = ema(closes, 50)[-1], rsi(closes)
    ch = (closes[-1] / closes[-7] - 1) * 100
    above = closes[-1] > e50
    if not above and (r < 45 or ch < -3):
        name, f = "BEAR", 0.8
    elif above and r >= 50:
        name, f = "BULL", 1.0
    else:
        name, f = "NEUTRAL", 0.92
    note = (f"BTC regime: {name} (scores x{f:.2f}) | 4h RSI {r:.0f}, 24h {ch:+.1f}%, "
            f"{'above' if above else 'below'} EMA50")
    return {"name": name, "factor": f, "note": note}


def coingecko_trending():
    d = get_json("https://api.coingecko.com/api/v3/search/trending", retries=1)
    syms, cats = set(), []
    try:
        for c in d.get("coins", []):
            syms.add(str(c["item"]["symbol"]).upper())
        for c in d.get("categories", [])[:4]:
            if c.get("name"):
                cats.append(c["name"])
    except Exception:  # noqa
        pass
    return syms, cats


def okx_funding_oi(base):
    inst = f"{base}-USDT-SWAP"
    fr = oi = None
    f = get_json("https://www.okx.com/api/v5/public/funding-rate", params={"instId": inst}, retries=0)
    try:
        fr = float(f["data"][0]["fundingRate"])
    except Exception:  # noqa
        pass
    o = get_json("https://www.okx.com/api/v5/public/open-interest",
                 params={"instType": "SWAP", "instId": inst}, retries=0)
    try:
        oi = float(o["data"][0]["oiUsd"])
    except Exception:  # noqa
        pass
    return fr, oi


def record_picks(st, items):
    now = time.time()
    recent = st["picks"] + st["closed"][-100:]
    for it in items:
        if it["score"] < ALERT_MIN_SCORE or not it["price"]:
            continue
        if any(p["key"] == it["key"] and now - p["ts"] < 6 * 3600 for p in recent):
            continue
        st["picks"].append({"key": it["key"], "kind": it["kind"], "symbol": it["symbol"], "chain": it["chain"],
                            "addr": it["addr"], "ts": now, "price0": it["price"], "score": round(it["score"], 1),
                            "tags": it["tags"], "r": {}})


def fetch_prices(picks):
    out, by = {}, {}
    for p in picks:
        if p["kind"] == "dex":
            by.setdefault(p["chain"], []).append(p["addr"])
    for ch, addrs in by.items():
        addrs = list(dict.fromkeys(addrs))
        for i in range(0, len(addrs), 30):
            data = get_json(f"https://api.dexscreener.com/tokens/v1/{ch}/{','.join(addrs[i:i + 30])}")
            best = {}
            for pr in (data if isinstance(data, list) else []):
                try:
                    a = pr["baseToken"]["address"].lower()
                    liq = (pr.get("liquidity") or {}).get("usd") or 0
                    if a not in best or liq > best[a][1]:
                        best[a] = (_f(pr.get("priceUsd")), liq)
                except KeyError:
                    continue
            for a, (px, _) in best.items():
                out[f"{ch}:{a}"] = px
    if any(p["kind"] == "cex" for p in picks):
        for base in BINANCE:
            d = get_json(f"{base}/api/v3/ticker/price", retries=1)
            if isinstance(d, list):
                for t in d:
                    if str(t.get("symbol", "")).endswith("USDT"):
                        out[f"cex:{t['symbol'][:-4]}"] = _f(t.get("price"))
                break
    return out


def update_perf(st):
    now = time.time()
    due = [p for p in st["picks"] if any(k not in p["r"] and now - p["ts"] >= s for k, s in CHECKS)]
    if due:
        prices = fetch_prices(due)
        for p in due:
            px = prices.get(p["key"])
            if not px or not p["price0"]:
                continue
            for k, s in CHECKS:
                if k not in p["r"] and now - p["ts"] >= s:
                    p["r"][k] = round((px / p["price0"] - 1) * 100, 2)
    keep = []
    for p in st["picks"]:
        if "24h" in p["r"] or now - p["ts"] > 36 * 3600:
            st["closed"].append(p)
        else:
            keep.append(p)
    st["picks"] = keep


def perf_report(st):
    picks = st["closed"] + st["picks"]
    if not picks:
        return ("📈 Performance: no tracked picks yet. A pick is tracked when the bot sends an alert "
                "or report with score >= " + f"{ALERT_MIN_SCORE:.0f}.")
    lines = [f"📈 Performance ({len(picks)} picks tracked)"]
    for h, _ in CHECKS:
        v = sorted(p["r"][h] for p in picks if h in p["r"])
        if v:
            win = sum(1 for x in v if x > 0) / len(v) * 100
            lines.append(f"{h}: avg {sum(v) / len(v):+.1f}% | median {v[len(v) // 2]:+.1f}% | win {win:.0f}% (n={len(v)})")

    def ret(p):
        return p["r"].get("6h", p["r"].get("1h"))

    lines.append("\nBy score (6h return):")
    for name, lo, hi in (("70+", 70, 101), ("55-69", 55, 70), ("<55", 0, 55)):
        v = [ret(p) for p in picks if lo <= p["score"] < hi and ret(p) is not None]
        if v:
            lines.append(f"  {name}: avg {sum(v) / len(v):+.1f}% (n={len(v)})")
    by_tag = {}
    for p in picks:
        r = ret(p)
        if r is None:
            continue
        for t in p["tags"]:
            by_tag.setdefault(t, []).append(r)
    rows = sorted(((sum(v) / len(v), t, len(v)) for t, v in by_tag.items() if len(v) >= 3), reverse=True)
    if rows:
        lines.append("\nBest signals: " + ", ".join(f"{t} {a:+.1f}% (n={n})" for a, t, n in rows[:3]))
        lines.append("Worst signals: " + ", ".join(f"{t} {a:+.1f}% (n={n})" for a, t, n in rows[-3:]))
    if len(picks) < 30:
        lines.append("\n⚠️ Small sample (<30 picks): do not trust these numbers yet.")
    return "\n".join(lines)


def pick_alerts(st, items):
    now, out = time.time(), []
    for it in items:
        if it["score"] < ALERT_MIN_SCORE or it.get("stage", "RUNNING") not in ALERT_STAGES:
            continue
        a = st["alerted"].get(it["key"])
        if a is None or now - a["ts"] > ALERT_COOLDOWN_H * 3600:
            out.append((it, "🆕 NEW"))
        elif it["score"] >= a["score"] + ALERT_JUMP:
            out.append((it, f"📈 SCORE UP (+{it['score'] - a['score']:.0f})"))
    return out


def mark_alerted(st, items):
    now = time.time()
    for it in items:
        st["alerted"][it["key"]] = {"ts": now, "score": it["score"]}


def fmt_item(it, tag=""):
    lines = [f"{it['title']} - Score {it['score']:.0f}/100 {tag}".strip()]
    lines += it["lines"]
    lines += [f"✅ {n}" for n in it["notes"][:5]]
    lines += [f"ℹ️ {n}" for n in it["infos"][:3]]
    lines += [f"⚠️ {n}" for n in it["flags"][:6]]
    if it.get("url"):
        lines.append(it["url"])
    return "\n".join(lines)


HELP = ("🤖 Binance Momentum Bot - Commands\n"
        "/scan - run a full scan now\n"
        "/coin <SYMBOL> - analyze any Binance coin (e.g. /coin SOL)\n"
        "/stats - performance of past picks\n"
        "/help - this message\n\n"
        "Note: commands are answered at the next bot cycle (up to ~15 min on GitHub Actions).")


# ---------------- Binance data ----------------
def get_tickers():
    for base in BINANCE:
        d = get_json(f"{base}/api/v3/ticker/24hr", retries=1)
        if isinstance(d, list) and d:
            return d
    return None


def kl_rows(kl):
    """[ts, open, high, low, close, quoteVolume, takerBuyQuoteVolume]"""
    return [[_f(k[0]) / 1000, _f(k[1]), _f(k[2]), _f(k[3]), _f(k[4]), _f(k[7]), _f(k[10])] for k in kl]


def funding_oi(base):
    fr, oi = okx_funding_oi(base)
    if fr is None:  # fallback: Binance futures (may be blocked on some servers)
        d = get_json("https://fapi.binance.com/fapi/v1/premiumIndex", params={"symbol": f"{base}USDT"}, retries=0)
        try:
            fr = float(d["lastFundingRate"])
        except Exception:  # noqa
            pass
    return fr, oi


def universe(tickers):
    out = []
    for t in tickers:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        base = sym[:-4]
        if base in STABLES or not re.fullmatch(r"[A-Z0-9]{2,15}", base):
            continue
        if _f(t.get("quoteVolume")) < MIN_QV:
            continue
        out.append(t)
    return out


def stage1_candidates(uni):
    by_vol = sorted((t for t in uni if 1 <= _f(t.get("priceChangePercent")) <= MAX_CHANGE_24H),
                    key=lambda t: _f(t.get("quoteVolume")), reverse=True)[:STAGE1_COUNT]
    gainers = sorted((t for t in uni if 3 <= _f(t.get("priceChangePercent")) <= MAX_CHANGE_24H),
                     key=lambda t: _f(t.get("priceChangePercent")), reverse=True)[:GAINERS_COUNT]
    seen, out = set(), []
    for t in by_vol + gainers:
        if t["symbol"] not in seen:
            seen.add(t["symbol"])
            out.append(t)
    return out


def market_components(h1, c24, btc_chg, tags, notes, warns):
    """volume spike + taker buy ratio + price sanity + relative strength vs BTC"""
    ms = 0.0
    vols = col(h1, 5)
    base_vol = sum(vols[-26:-2]) / 24 if len(vols) >= 27 else 0
    ratio = vols[-2] / base_vol if base_vol else 0
    ms += min(ratio, 4) / 4 * W["vol_spike"]
    if ratio >= 2:
        tags.append("vol_spike")
        notes.append(f"Volume spike {ratio:.1f}x (last closed 1h vs 24h avg)")
    tot = sum(vols[-4:-1])
    taker = sum(col(h1, 6)[-4:-1]) / tot if tot else 0
    if taker > 0.5:
        ms += min((taker - 0.5) / 0.1, 1) * W["taker_buy"]
    if taker >= 0.56:
        tags.append("buy_pressure")
        notes.append(f"Taker buy ratio {taker * 100:.0f}% (last 3 closed 1h candles)")
    if 2 <= c24 <= 25:
        ms += W["price_ok"]
    elif c24 > 40:
        ms -= 10
        warns.append(f"Already +{c24:.0f}% in 24h (late entry risk)")
    if btc_chg is not None and c24 - btc_chg >= 3:
        ms += W["rs_btc"]
        tags.append("rs_btc")
        notes.append(f"Outperforming BTC by {c24 - btc_chg:.1f}% (24h)")
    return ms, ratio, taker


def quick_score(h1, c24, btc_chg):
    """cheap ranking score using only the 1h candles"""
    tags, notes, warns = [], [], []
    ms, _, _ = market_components(h1, c24, btc_chg, tags, notes, warns)
    ts = tech_analysis(h1, None, None)[0]
    return ms + ts


def evaluate(t, h1, ctx, st, btc_chg):
    sym = t["symbol"]
    base = sym[:-4]
    c24 = _f(t.get("priceChangePercent"))
    if h1 is None:
        k1 = binance_klines(sym, "1h", 200)
        if not k1 or len(k1) < 50:
            return None
        h1 = kl_rows(k1)
    k15, k4 = binance_klines(sym, "15m", 100), binance_klines(sym, "4h", 100)
    m15, h4 = (kl_rows(k15) if k15 else None), (kl_rows(k4) if k4 else None)
    ts, tn, tw, tt, info = tech_analysis(h1, h4, m15)
    notes, warns, tags = list(tn), list(tw), tt + ["binance"]
    ms, ratio1h, _ = market_components(h1, c24, btc_chg, tags, notes, warns)
    acc5 = 0.0
    k5 = binance_klines(sym, "5m", 60)
    if k5 and len(k5) >= 48:
        r5 = kl_rows(k5)
        v5 = col(r5, 5)
        base5 = sum(v5[-40:-4]) / 36
        acc5 = (sum(v5[-4:-1]) / 3) / base5 if base5 else 0
        chg30 = (r5[-2][4] / r5[-8][4] - 1) * 100
        if acc5 >= 3 and 0.3 <= chg30 <= 8:
            ms += 8
            tags.append("early_accel")
            notes.append(f"5m volume accelerating {acc5:.1f}x while price only +{chg30:.1f}% in 30m")
    fr, oi = funding_oi(base)
    fund_txt = oi_txt = ""
    if fr is not None:
        fund_txt = f"Funding {fr * 100:+.3f}%"
        if fr > 0.0005:
            ms -= 8
            warns.append(f"Funding {fr * 100:.3f}% (crowded longs, squeeze-down risk)")
        elif fr < 0 and c24 > 0:
            ms += 5
            tags.append("neg_funding_up")
            notes.append("Negative funding while price rises (short-squeeze potential)")
    if oi:
        oi_txt = f"OI {usd(oi)}"
        prev = st["oi"].get(base)
        if prev and prev.get("v"):
            chg = (oi / prev["v"] - 1) * 100
            oi_txt += f" ({chg:+.1f}% since last scan)"
            if chg > 5 and c24 > 0:
                ms += 5
                tags.append("oi_up")
                notes.append(f"Open interest +{chg:.1f}% with rising price")
        st["oi"][base] = {"v": oi, "ts": time.time()}
    bonus = 0
    if base in ctx["trend_syms"]:
        bonus = W["trending"]
        notes.append("Trending on CoinGecko")
        tags.append("cg_trending")
    score = max(0.0, min(100.0, (max(ms, 0) + ts + bonus) * ctx["regime"]["factor"]))
    stage = entry_stage(c24, max(ratio1h, acc5), info)
    tags.append(f"stage:{stage}")
    if stage == "LATE":
        score = min(score, ALERT_MIN_SCORE - 1)  # the pump already happened: never alert
    price = _f(t.get("lastPrice"))
    lines = [f"Price ${price:.6g} | 24h {c24:+.1f}% | Vol24h {usd(_f(t.get('quoteVolume')))}"]
    for e in (" | ".join(x for x in (fund_txt, oi_txt) if x), tech_line(info), sr_line(info)):
        if e:
            lines.append(e)
    return {"kind": "cex", "key": f"cex:{base}", "symbol": base, "chain": "cex", "addr": sym,
            "title": f"{base}/USDT [{stage}]", "stage": stage, "c24": c24, "score": score, "price": price, "lines": lines, "notes": notes,
            "flags": warns, "infos": [], "tags": tags, "url": f"https://www.binance.com/en/trade/{base}_USDT"}


def scan(ctx, st):
    tickers = get_tickers()
    if not tickers:
        log("Binance tickers unavailable")
        return []
    btc = next((t for t in tickers if t.get("symbol") == "BTCUSDT"), None)
    btc_chg = _f(btc.get("priceChangePercent")) if btc else None
    uni = universe(tickers)
    cands = stage1_candidates(uni)
    log(f"universe {len(uni)} | stage1 {len(cands)}")
    ranked = []
    for t in cands:
        k1 = binance_klines(t["symbol"], "1h", 200)
        if not k1 or len(k1) < 50:
            continue
        h1 = kl_rows(k1)
        try:
            q = quick_score(h1, _f(t.get("priceChangePercent")), btc_chg)
        except Exception as e:  # noqa
            log(f"quick score failed {t['symbol']}: {e}")
            continue
        ranked.append((q, t, h1))
        time.sleep(0.15)
    ranked.sort(key=lambda x: x[0], reverse=True)
    items = []
    for _, t, h1 in ranked[:DEEP_COUNT]:
        try:
            it = evaluate(t, h1, ctx, st, btc_chg)
        except Exception as e:  # noqa
            log(f"evaluate failed {t['symbol']}: {e}")
            it = None
        if it:
            items.append(it)
        time.sleep(0.2)
    items.sort(key=lambda x: x["score"], reverse=True)
    return items[:TOP_N]


def analyze_coin(q, ctx, st):
    sym = re.sub(r"[^A-Za-z0-9]", "", q).upper()
    if not sym:
        return "Usage: /coin <SYMBOL>  (example: /coin SOL)"
    if not sym.endswith("USDT"):
        sym += "USDT"
    t = btc = None
    for base in BINANCE:
        d = get_json(f"{base}/api/v3/ticker/24hr", params={"symbol": sym}, retries=0)
        if isinstance(d, dict) and d.get("symbol"):
            t = d
            b = get_json(f"{base}/api/v3/ticker/24hr", params={"symbol": "BTCUSDT"}, retries=0)
            btc = _f(b.get("priceChangePercent")) if isinstance(b, dict) else None
            break
    if not t:
        return f"{sym} was not found on Binance spot."
    it = evaluate(t, None, ctx, st, btc)
    if not it:
        return f"Not enough candle data for {sym}."
    return fmt_item(it) + f"\n\n{ctx['regime']['note']}\n\n{DISCLAIMER}"


# ---------------- Reports ----------------
def build_ctx():
    syms, cats = coingecko_trending()
    return {"regime": btc_regime(), "trend_syms": syms, "hot_cats": cats}


def header(ctx):
    now = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")
    out = [f"📊 Binance Momentum Scan - {now}", ctx["regime"]["note"]]
    if ctx["hot_cats"]:
        out.append("CoinGecko hot categories: " + ", ".join(ctx["hot_cats"]))
    return "\n".join(out)


def full_report(ctx, items):
    blocks = [header(ctx)]
    if not items:
        blocks.append("No coins passed the filters right now.")
    blocks += [f"{i}) " + fmt_item(it) for i, it in enumerate(items, 1)]
    blocks.append(DISCLAIMER)
    return blocks


# ---------------- Binance Square auto-post ----------------
def maybe_post_square(st, items):
    if not POST_SQUARE or not SQUARE_KEY:
        return
    if time.time() - st.get("last_square", 0) < SQUARE_EVERY_H * 3600:
        return
    top = [i for i in items if i["score"] >= SQUARE_MIN_SCORE][:3]
    if not top:
        return
    now = datetime.now(timezone.utc).strftime("%d %b %H:%M UTC")
    lines = [f"🔎 Binance momentum watchlist (auto scan, {now})"]
    for i in top:
        note = (i["notes"][0] if i["notes"] else "momentum building")[:70]
        lines.append(f"${i['symbol']} | score {i['score']:.0f}/100 | 24h {i.get('c24', 0):+.1f}% | {note}")
    lines.append("Data-driven momentum scan, not financial advice. DYOR.")
    text = "\n".join(lines)
    try:
        r = S.post(SQUARE_URL, json={"bodyTextOnly": text}, timeout=20,
                   headers={"X-Square-OpenAPI-Key": SQUARE_KEY, "Content-Type": "application/json",
                            "clienttype": "binanceSkill"})
        d = r.json()
    except Exception as e:  # noqa
        log(f"Square post failed: {e}")
        send_telegram("⚠️ Binance Square post failed (network error).")
        return
    code = str(d.get("code"))
    if code in ("000000", "0") or d.get("success") is True:
        post_id = (d.get("data") or {}).get("id") if isinstance(d.get("data"), dict) else None
        st["last_square"] = time.time()
        link = f"https://www.binance.com/square/post/{post_id}" if post_id else "(link not returned)"
        send_telegram(f"✅ Posted to Binance Square: {link}")
    else:
        msg = f"Square post rejected: code {code} {str(d.get('message') or d.get('msg') or '')[:120]}"
        log(msg)
        send_telegram("⚠️ " + msg)


def digest_pick(items):
    """coins that passed: good score and the pump has not already happened"""
    ok = [i for i in items if i["score"] >= DIGEST_MIN_SCORE and i.get("stage", "RUNNING") != "LATE"]
    return sorted(ok, key=lambda i: i["score"], reverse=True)[:TOP_N]


def digest_blocks(ctx, passed):
    blocks = [header(ctx)]
    if passed:
        blocks.append(f"⏰ Hourly signals: {len(passed)} coin(s) passed (score >= {DIGEST_MIN_SCORE:.0f}, not LATE)")
        blocks += [f"{i}) " + fmt_item(it) for i, it in enumerate(passed, 1)]
    else:
        blocks.append(f"⏰ Hourly scan: no coin passed this hour (score >= {DIGEST_MIN_SCORE:.0f}, not LATE). "
                      "Bot is running normally.")
    blocks.append(DISCLAIMER)
    return blocks


# ---------------- New Binance listings ----------------
def check_new_listings(st):
    """returns USDT symbols that were never seen before (first run only stores a baseline)"""
    d = None
    for base in BINANCE:
        d = get_json(f"{base}/api/v3/ticker/price", retries=1)
        if isinstance(d, list) and d:
            break
    if not isinstance(d, list) or not d:
        return []
    syms = {t["symbol"] for t in d if str(t.get("symbol", "")).endswith("USDT")}
    known = set(st.get("symbols") or [])
    st["symbols"] = sorted(known | syms)  # union: a coin returning from maintenance is not "new"
    return sorted(syms - known) if known else []


def announce_listing(st, sym):
    base, t = sym[:-4], None
    for host in BINANCE:
        d = get_json(f"{host}/api/v3/ticker/24hr", params={"symbol": sym}, retries=0)
        if isinstance(d, dict) and d.get("symbol"):
            t = d
            break
    price = _f((t or {}).get("lastPrice"))
    send_telegram(f"🆕 NEW BINANCE LISTING: {base}/USDT\n"
                  f"Price ${price:.6g} | change {_f((t or {}).get('priceChangePercent')):+.1f}% | "
                  f"Vol {usd(_f((t or {}).get('quoteVolume')))}\n"
                  "⚠️ New listings are extremely volatile (wide spreads, fast dumps). This is NOT a buy signal.\n"
                  f"https://www.binance.com/en/trade/{base}_USDT")
    if price:
        st["picks"].append({"key": f"cex:{base}", "kind": "cex", "symbol": base, "chain": "cex", "addr": sym,
                            "ts": time.time(), "price0": price, "score": -1,
                            "tags": ["new_listing", "stage:LISTING", "binance"], "r": {}})


# ---------------- Main cycle ----------------
def tick(force=False):
    st = load_state()
    ctx_cache = []

    def get_ctx():
        if not ctx_cache:
            ctx_cache.append(build_ctx())
        return ctx_cache[0]

    try:
        cmds = handle_commands(st)
        if cmds["help"]:
            send_telegram(HELP)
        if ENABLE_LISTING:
            for lsym in check_new_listings(st)[:3]:
                announce_listing(st, lsym)
        update_perf(st)
        if cmds["stats"]:
            send_telegram(perf_report(st))
        for q in cmds["coins"][:3]:
            try:
                send_telegram(analyze_coin(q, get_ctx(), st))
            except Exception as e:  # noqa
                log(f"/coin failed: {e}")
                send_telegram("Could not analyze that coin right now. Try again later.")
        now = time.time()
        full = force or cmds["scan"]
        if full or now - st["last_scan"] >= SCAN_EVERY_MIN * 60 - 120:
            ctx = get_ctx()
            items = scan(ctx, st)
            st["last_scan"], st["n_scans"] = now, st["n_scans"] + 1
            digest_due = now - st.get("last_digest", 0) >= DIGEST_EVERY_MIN * 60 - 120
            if full:
                st["last_digest"] = now
                send_blocks(full_report(ctx, items))
                record_picks(st, items)
                maybe_post_square(st, items)
                mark_alerted(st, [i for i in items if i["score"] >= ALERT_MIN_SCORE])
            elif digest_due:
                st["last_digest"] = now
                passed = digest_pick(items)
                if passed or DIGEST_SEND_EMPTY:
                    send_blocks(digest_blocks(ctx, passed))
                record_picks(st, passed)
                mark_alerted(st, passed)
                maybe_post_square(st, passed)
            else:
                alerts = pick_alerts(st, items)
                if alerts:
                    blocks = [header(ctx), "🚨 Momentum alert"]
                    blocks += [fmt_item(it, tag) for it, tag in alerts]
                    blocks.append(DISCLAIMER)
                    send_blocks(blocks)
                    record_picks(st, [it for it, _ in alerts])
                    mark_alerted(st, [it for it, _ in alerts])
                    st["n_alerts"] += len(alerts)
                    maybe_post_square(st, [it for it, _ in alerts])
                else:
                    log("no new alerts")
        if st["last_summary"] == 0:
            st["last_summary"] = now
        elif now - st["last_summary"] >= 86400:
            send_blocks([f"🟢 Daily summary: {st['n_scans']} scans, {st['n_alerts']} alerts in the last 24h.",
                         perf_report(st)])
            st["n_scans"] = st["n_alerts"] = 0
            st["last_summary"] = now
    finally:
        save_state(st)


def watch(minutes, force_first=False):
    """Keep running tick() every TICK_SECONDS for `minutes` (used by GitHub Actions for near-real-time)."""
    end = time.time() + minutes * 60
    first = True
    while True:
        try:
            tick(force=force_first and first)
        except Exception as e:  # noqa
            log(f"tick error: {e}")
        first = False
        if time.time() + TICK_SECONDS >= end:
            break
        time.sleep(TICK_SECONDS)


if __name__ == "__main__":
    if "--test-telegram" in sys.argv:
        print("OK" if send_telegram("✅ Binance bot connected! Telegram setup is working.") else "FAILED - check your token/chat id")
    elif "--once" in sys.argv:
        tick(force=True)
    elif "--watch" in sys.argv:
        mins = float(sys.argv[sys.argv.index("--watch") + 1])
        watch(mins, force_first="--force" in sys.argv)
    elif "--tick" in sys.argv:
        tick(force="--force" in sys.argv)
    else:
        while True:
            try:
                tick()
            except Exception as e:  # noqa
                log(f"error: {e}")
            time.sleep(TICK_SECONDS)
