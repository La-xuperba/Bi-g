"""Binance Spot Early Momentum Scanner

Binance Spot is the ONLY signal universe. Binance Futures is used only as
confirmation/market intelligence (OI, funding, liquidation, taker pressure).
DeFiLlama is queried per shortlisted coin when a usable CoinGecko mapping exists.

Score = 100, early-momentum weighted:
  Relative volume spike         18
  Price acceleration            10
  Spot taker-buy pressure       12
  Timeframes 5m/15m/30m/1h/4h   3/5/7/7/3 (25 total)
  Compression/breakout           8
  Market-cap turnover            8 (turnover, NOT net inflow)
  Resistance room                7
  EMA structure                  5
  Futures OI                     3
  DeFiLlama/on-chain             2
  Funding                        1
  Liquidation context            1
  Technical confluence is displayed as diagnostics and is NOT double-counted.

Category scoring:
  Technical 25 + Data 25 = 50 priority points; Sentiment 30 additional points.
  Raw total 80 is normalized to Overall Score /100 for consistent thresholds.
  Sentiment uses CoinGecko Trending attention (20) and global Fear & Greed (10).
  Unavailable sentiment sources earn zero points; these are proxies, not full social/news coverage.

Visibility:
  <SIGNAL_MIN_SCORE -> no public signal (default threshold 45)
  45-54 -> EARLY ENTRY candidate
  55-74 -> CONFIRMED MOMENTUM candidate
  75-84 -> HIGH-CONFIDENCE EARLY ENTRY
  85+ -> STRONG PRE-PUMP SIGNAL

Any late/overheated/near-resistance setup is blocked even with a high score.
"""
import json, math, os, re, sys, time, random
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
load_dotenv()

TG_TOKEN=os.getenv('TELEGRAM_BOT_TOKEN',''); TG_CHAT=os.getenv('TELEGRAM_CHAT_ID','')
SQUARE_KEY=os.getenv('BINANCE_SQUARE_OPENAPI_KEY','').strip()
POST_TO_SQUARE=os.getenv('POST_TO_SQUARE','false').lower()=='true'
SQUARE_MIN_SCORE=float(os.getenv('SQUARE_MIN_SCORE','75'))
STATE_FILE=os.getenv('STATE_FILE','state_binance.json')
SCAN_EVERY_MIN=int(os.getenv('SCAN_EVERY_MIN','5'))
TICK_SECONDS=int(os.getenv('TICK_SECONDS','60'))
MIN_24H_VOL=float(os.getenv('BINANCE_MIN_24H_VOLUME_USD','100000'))  # safety floor only; ranking emphasizes volume/market cap
STAGE1_TOP=int(os.getenv('STAGE1_TOP','100'))
DEEP_N=int(os.getenv('DEEP_SCAN_COUNT','50'))
SIGNAL_MIN=float(os.getenv('SIGNAL_MIN_SCORE','45'))
COOLDOWN_MIN=int(os.getenv('ALERT_COOLDOWN_MIN','720'))
ENABLE_DEFILLAMA=os.getenv('ENABLE_DEFILLAMA','true').lower()=='true'
ENABLE_FUTURES=os.getenv('ENABLE_FUTURES_CONFIRMATION','true').lower()=='true'
# Outcome updates (TP/SL result messages) are disabled by default; signal publishing remains enabled.
ENABLE_OUTCOME_MONITOR=os.getenv('ENABLE_OUTCOME_MONITOR','false').lower()=='true'

# Try Binance's public market-data mirror first: api.binance.com can return HTTP 451
# from some hosted runners/regions. These are public market-data endpoints only.
BINANCE_SPOT=['https://data-api.binance.vision','https://api-gcp.binance.com','https://api1.binance.com','https://api2.binance.com','https://api3.binance.com','https://api4.binance.com','https://api.binance.com']
FAPI=['https://fapi.binance.com']
STABLES={'USDT','USDC','FDUSD','TUSD','BUSD','USDP','DAI','EUR','TRY','BRL','USDE','USDD','PYUSD','USTC'}
INTERVALS={'5m':'5m','15m':'15m','30m':'30m','1h':'1h','4h':'4h'}

W={'5m':3,'15m':5,'30m':7,'1h':7,'4h':3,'volume':18,'turnover':8,'oi':3,'funding':1,
   'liq':1,'pressure':12,'llama':2,'res':7,'accel':10,'compression':8,'ema':5}
assert sum(W.values())==100

S=requests.Session(); S.headers.update({'User-Agent':'Mozilla/5.0 Binance-Early-Radar/3.0'})


def log(x):
    x=str(x).replace(TG_TOKEN,'***') if TG_TOKEN else str(x)
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {x}",flush=True)

def get_json(url,params=None,retries=1,timeout=15):
    for i in range(retries+1):
        try:
            r=S.get(url,params=params,timeout=timeout)
            if r.status_code==451:
                # Region/eligibility block: don't retry the same host; caller may try a fallback.
                return None
            if r.status_code==429:
                time.sleep(2+i*2); continue
            r.raise_for_status(); return r.json()
        except Exception as e:
            if i==retries: log(f"request failed {url.split('?')[0]}: {e}")
            else: time.sleep(.8)
    return None

def post_tg(text):
    if not TG_TOKEN or not TG_CHAT:
        print(text); return False
    ok=True
    for i in range(0,len(text),3900):
        try:
            r=S.post(f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',data={'chat_id':TG_CHAT,'text':text[i:i+3900],'disable_web_page_preview':True},timeout=15)
            ok &= r.ok
        except Exception as e: log(f'Telegram error: {e}'); ok=False
    return ok

def post_square(text):
    """Publish a text-only post through Binance Square's dedicated posting API key."""
    if not POST_TO_SQUARE:
        return False
    if not SQUARE_KEY:
        log('Binance Square posting enabled but BINANCE_SQUARE_OPENAPI_KEY is missing')
        return False
    url='https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add'
    headers={
        'X-Square-OpenAPI-Key': SQUARE_KEY,
        'Content-Type': 'application/json',
        'clienttype': 'binanceSkill',
        'User-Agent': 'Mozilla/5.0 Binance-Gems-Spot-Radar'
    }
    try:
        r=S.post(url,headers=headers,json={'bodyTextOnly':text},timeout=20)
        try: data=r.json()
        except Exception: data={}
        if r.ok and str(data.get('code',''))=='000000':
            post_id=(data.get('data') or {}).get('id')
            log('Binance Square post published'+(f': https://www.binance.com/square/post/{post_id}' if post_id else ' (post ID unavailable)'))
            return True
        # Do not log headers or credentials.
        log(f'Binance Square posting failed: HTTP {r.status_code}, code={data.get("code","unknown")}, message={data.get("message") or data.get("msg") or "no details"}')
    except Exception as e:
        log(f'Binance Square request failed: {e}')
    return False

def trade_levels(it):
    """ATR-based indicative levels; never use a fixed arbitrary target percentage."""
    entry=float(it.get('price') or 0)
    d=it.get('detail',{})
    atr=float(d.get('atr15') or 0)
    if entry<=0 or atr<=0:
        return None
    risk=max(atr*1.5, entry*0.005)
    sl=max(entry-risk, entry*0.5)
    room=d.get('room')
    targets=[entry+risk, entry+risk*1.5, entry+risk*2]
    # If known resistance is close, keep targets just below it rather than inventing upside.
    if room is not None and 0 < float(room) < 10:
        resistance=entry*(1+float(room)/100)
        cap=resistance*0.995
        targets=[x for x in targets if x < cap]
    return {'entry':entry,'sl':sl,'tps':targets,'risk_pct':(entry-sl)/entry*100}

def square_text(it, serial=None):
    """Human-written-style Binance Square post; intentionally no direct trading/chart URL."""
    d=it.get('detail', {})
    score=float(it.get('score', 0) or 0)
    levels=trade_levels(it)
    room=d.get('room')
    notes=' '.join(it.get('notes', [])).lower()

    if it.get('stage') == 'EARLY ENTRY':
        headline='EARLY SPOT WATCH'
        opener=(f"I've got ${it['base']} on my radar because the setup may still be developing, "
                "rather than being a move I'd want to chase after a big candle.")
    elif 'CONFIRMED' in str(it.get('stage', '')).upper():
        headline='SPOT MOMENTUM WATCH'
        opener=(f"${it['base']} is showing a more developed momentum setup. I'm watching to see "
                "whether buyers can keep the pressure up and hold the breakout area.")
    else:
        headline='SPOT SETUP WATCH'
        opener=(f"${it['base']} is worth watching, but I want price and volume to confirm the move "
                "before treating it as a clean setup.")

    observations=[]
    if 'volume' in notes or 'volume' in str(d.get('notes', '')).lower():
        observations.append('Volume activity is one of the main things supporting this watch.')
    if room is not None:
        try:
            if float(room) >= 5:
                observations.append(f"There appears to be about {float(room):.1f}% of room to the nearby resistance reference.")
            elif float(room) < 2:
                observations.append('Nearby resistance is close, so I would not chase a breakout without a strong close.')
        except (TypeError, ValueError):
            pass
    if not observations:
        observations.append('The next confirmation I want is sustained volume and a clean candle close, not just a quick wick.')

    lines=[
        f"💎 BINANCE GEMS | {headline}",
        f"Signal #{serial if serial is not None else '—'} · ${it['base']}",
        f"Score: {score:.0f}/100 | {it.get('stage', 'SPOT WATCH')}",
        '', opener,
        ' '.join(observations),
        f"Price reference: ${float(it.get('price') or 0):.8g}",
        f"24h change: {float(it.get('change24') or 0):+.1f}% · 24h volume: {usd(it.get('volume24'))}",
    ]
    if levels:
        lines.append('')
        lines.append(f"Risk reference — SL: ${levels['sl']:.8g}")
        if levels['tps']:
            lines.extend(f"Potential TP{i}: ${tp:.8g}" for i, tp in enumerate(levels['tps'], 1))
        else:
            lines.append('Targets omitted: nearby resistance leaves limited room.')
    else:
        lines.append('Entry/TP/SL: wait for enough structure and volatility data; no levels invented.')
    lines += [
        '',
        'My plan: no chasing. I want the setup to hold, volume to stay healthy, and resistance to clear cleanly. If those conditions fail, I skip it.',
        'This is a spot-market watchlist idea, not a guaranteed result. Manage risk and confirm the live chart before acting.',
        '',
        f"#BinanceGems #SpotTrading ${it['base']}"
    ]
    return '\n'.join(lines)

def load_state():
    try: st=json.load(open(STATE_FILE))
    except Exception: st={}
    st.setdefault('last_scan',0); st.setdefault('alerts',{}); st.setdefault('oi',{}); st.setdefault('cg',{}); st.setdefault('llama',{}); st.setdefault('market_caps',{}); st.setdefault('signal_seq',0); st.setdefault('open_signals',{})
    return st

def save_state(st):
    now=time.time(); st['alerts']={k:v for k,v in st['alerts'].items() if now-v.get('ts',0)<3*86400}
    with open(STATE_FILE,'w') as f: json.dump(st,f)

def usd(x):
    x=float(x or 0)
    for d,s in ((1e9,'B'),(1e6,'M'),(1e3,'K')):
        if abs(x)>=d:return f'${x/d:.2f}{s}'
    return f'${x:.2f}'

def pct(x): return f'{x:+.1f}%'

def ema(v,n):
    if not v:return []
    a=2/(n+1); out=[v[0]]
    for x in v[1:]:out.append(a*x+(1-a)*out[-1])
    return out

def rsi(v,n=14):
    if len(v)<n+2:return 50
    g=[];l=[]
    for a,b in zip(v,v[1:]):
        d=b-a;g.append(max(d,0));l.append(max(-d,0))
    ag=sum(g[:n])/n; al=sum(l[:n])/n
    for i in range(n,len(g)):ag=(ag*(n-1)+g[i])/n;al=(al*(n-1)+l[i])/n
    return 100 if al==0 else 100-100/(1+ag/al)

def rows(kl):
    # closed candles only; discard current candle
    if not isinstance(kl,list):return []
    now=time.time()*1000; out=[]
    for k in kl:
        if len(k)<8 or float(k[6])>now:continue
        out.append({'t':int(k[0]),'o':float(k[1]),'h':float(k[2]),'l':float(k[3]),'c':float(k[4]),'v':float(k[5]),'q':float(k[7]),'tb':float(k[9]) if len(k)>9 else 0})
    return out

def spot_klines(sym,tf,limit=120):
    for base in BINANCE_SPOT:
        d=get_json(f'{base}/api/v3/klines',{'symbol':sym,'interval':tf,'limit':limit})
        if isinstance(d,list) and len(d)>20:return rows(d)
    return []

def spot_exchange():
    for base in BINANCE_SPOT:
        d=get_json(f'{base}/api/v3/exchangeInfo',timeout=20)
        if d and d.get('symbols'):return d['symbols']
    return []

def universe():
    syms=[]
    for x in spot_exchange():
        if x.get('status')!='TRADING' or x.get('quoteAsset')!='USDT':continue
        b=x.get('baseAsset','').upper()
        if b in STABLES or x.get('isSpotTradingAllowed') is False:continue
        syms.append(x['symbol'])
    return syms

def ticker24():
    for base in BINANCE_SPOT:
        d=get_json(f'{base}/api/v3/ticker/24hr',timeout=25)
        if isinstance(d,list):return {x['symbol']:x for x in d}
    return {}

def stage1():
    allowed=set(universe()); ticks=ticker24(); arr=[]
    for sym,t in ticks.items():
        if sym not in allowed:continue
        q=float(t.get('quoteVolume',0)); ch=float(t.get('priceChangePercent',0));
        if q<MIN_24H_VOL:continue  # low absolute safety floor; not a $5M/$500K quality requirement
        # Positive or recovering coins get priority; don't exclude quiet accumulation.
        if ch<-35:continue
        arr.append((sym,t,q))
    arr.sort(key=lambda z:(z[2],abs(float(z[1].get('priceChangePercent',0)))),reverse=True)
    # Keep some early movers even if their absolute volume rank is lower.
    movers=sorted(arr,key=lambda z:float(z[1].get('priceChangePercent',0)),reverse=True)[:max(40,STAGE1_TOP//2)]
    # Include recovering/early movers as well as high-liquidity pairs; don't let 24h volume alone dominate.
    recovering=sorted((x for x in arr if -8 <= float(x[1].get('priceChangePercent',0)) <= 12),
                      key=lambda z:(float(z[1].get('priceChangePercent',0)), z[2]), reverse=True)[:max(30,STAGE1_TOP//3)]
    merged={x[0]:x for x in arr[:STAGE1_TOP]}
    for x in movers+recovering:merged.setdefault(x[0],x)
    return list(merged.values())

def linear_change(c,n):
    if len(c)<=n or not c[-1-n]:return 0
    return (c[-1]/c[-1-n]-1)*100

def tf_score(k,tf):
    if len(k)<35:return 0,'unavailable'
    c=[x['c'] for x in k]; e9=ema(c,9)[-1];e21=ema(c,21)[-1];r=rsi(c)
    ch=linear_change(c,3 if tf=='5m' else 4)
    if c[-1]>e9>e21 and 48<=r<=72:return W[tf],f'{tf} structure positive'
    if c[-1]>e21 and ch>0:return W[tf]*.65,f'{tf} improving'
    if c[-1]>e21:return W[tf]*.4,f'{tf} above EMA21'
    if ch>0 and r>=45:return W[tf]*.25,f'{tf} attempting turn'
    return 0,f'{tf} not confirmed'

def volume_score(k, market_cap=None, volume24=0):
    """Short-term relative volume score. Market-cap turnover is scored separately."""
    if len(k)<35:return 0,0,0,'unavailable'
    v=[x['v'] for x in k]; recent=v[-3:];base=sum(v[-27:-3])/24 if sum(v[-27:-3]) else 0
    ratio=(sum(recent)/3)/base if base else 0
    last_ratio=v[-1]/max(sum(v[-21:-1])/20,1)
    # Reward unusual relative activity, while clipping one-candle extremes.
    ratio_points=min(max(ratio-1,0)/2,1)*11
    last_points=min(max(last_ratio-1,0)/2,1)*7
    score=min(W['volume'],ratio_points+last_points)
    turnover=(float(volume24 or 0)/float(market_cap)) if market_cap and market_cap>0 else None
    desc=f'relative volume {ratio:.1f}x baseline; latest candle {last_ratio:.1f}x baseline'
    if turnover is not None: desc+=f'; 24h volume/market cap {turnover*100:.2f}%'
    else: desc+='; market cap unavailable'
    return score,ratio,turnover,desc

def turnover_score(market_cap, volume24):
    """Market-cap-normalized turnover; this is NOT verified net inflow."""
    if not market_cap or market_cap<=0 or not volume24:
        return 0, 'market-cap turnover unavailable (no points)'
    turnover=float(volume24)/float(market_cap)
    if turnover>=0.50: points=8
    elif turnover>=0.20: points=7
    elif turnover>=0.10: points=6
    elif turnover>=0.05: points=5
    elif turnover>=0.02: points=3
    elif turnover>=0.005: points=1
    else: points=0
    return points, f'24h volume/market cap {turnover*100:.2f}% (turnover, not net inflow)'

def pressure_score(k):
    if len(k)<25:return 0,'unavailable'
    b=sum(x['tb'] for x in k[-12:]); q=sum(x['q'] for x in k[-12:]);
    # quote volume approximates buy-side quote amount via taker-buy quote.
    p=b/q if q else .5
    s=min(max(p-.5,0)/.15,1)*W['pressure']
    return s,f'buy pressure {p*100:.0f}%'

def accel_score(k):
    if len(k)<30:return 0,'unavailable'
    c=[x['c'] for x in k]; a=linear_change(c,3);b=linear_change(c,12)
    s=0
    if a>0:s+=3
    if a>b/4 and a>0:s+=3
    if 0<a<8:s+=2
    if a>=0.5 and a<5:s+=2
    return min(W['accel'],s),f'price acceleration {a:+.2f}%'

def compression_score(k):
    if len(k)<40:return 0,'unavailable'
    c=[x['c'] for x in k]; recent=c[-20:]; mean=sum(recent)/20
    sd=math.sqrt(sum((x-mean)**2 for x in recent)/20);bw=sd/mean if mean else 0
    old=[]
    for i in range(20,35):
        w=c[i-20:i];m=sum(w)/len(w);old.append(math.sqrt(sum((x-m)**2 for x in w)/len(w))/m if m else 0)
    low=sorted(old)[len(old)//3] if old else bw
    if bw<=low*1.05:return W['compression'],'compressed'
    if c[-1]>max(c[-12:-1]):return W['compression'],'breakout after compression'
    return 0,'no compression edge'

def ema_structure(k):
    if len(k)<60:return 0,'unavailable'
    c=[x['c'] for x in k];e20=ema(c,20)[-1];e50=ema(c,50)[-1]
    if c[-1]>e20>e50:return W['ema'],'price > EMA20 > EMA50'
    if c[-1]>e20:return 3,'price above EMA20'
    return 0,'EMA structure weak'


def sma(values, n):
    if len(values) < n: return None
    return sum(values[-n:]) / n

def ema_ma_support_score(k):
    """Core 0-15 score: EMA20/MA50 support, closed-candle close, slope and volume.
    Uses closed candles only. Support points are deliberately not duplicated elsewhere.
    """
    if len(k) < 60:
        return 0, {"ema20": 0, "ma50": 0, "candle_close": 0, "slope": 0, "volume": 0}, "EMA20/MA50 support unavailable"

    c=[x["c"] for x in k]
    o=[x["o"] for x in k]
    h=[x["h"] for x in k]
    l=[x["l"] for x in k]
    v=[x["v"] for x in k]
    e20_series=ema(c,20)
    m50_series=[sma(c[:i+1],50) for i in range(len(c))]
    p=c[-1]; e20=e20_series[-1]; m50=m50_series[-1]

    # EMA20 / MA50: above the level is partial credit; a recent touch-and-reclaim is stronger.
    def support_points(level, idx):
        recent_low=min(l[-4:])
        if p > level and recent_low <= level*1.008 and p >= o[-1]:
            return 3
        if p > level:
            return 2
        if p >= level*0.995 and p >= o[-1]:
            return 1
        return 0

    ema_pts=support_points(e20,20)
    ma_pts=support_points(m50,50)

    # Strong closed candle above both levels; no credit for an unfinished candle.
    close_pts=0
    if c[-1] > o[-1] and p > e20 and p > m50:
        body=(c[-1]-o[-1])/max(h[-1]-l[-1],1e-12)
        close_pts=3 if body >= 0.55 else 2
    elif p > e20 and p > m50:
        close_pts=1

    # Both average lines rising across several closed candles.
    slope_pts=0
    if len(e20_series)>=5 and len(m50_series)>=5 and m50_series[-5] is not None:
        e20_rise=(e20_series[-1]/e20_series[-5]-1)*100 if e20_series[-5] else 0
        m50_rise=(m50_series[-1]/m50_series[-5]-1)*100 if m50_series[-5] else 0
        if e20_rise > 0 and m50_rise > 0: slope_pts=3
        elif e20_rise > 0 or m50_rise > 0: slope_pts=2
        elif e20_rise > -0.15 and m50_rise > -0.15: slope_pts=1

    # Latest closed candle volume vs previous 20-candle mean.
    base=sum(v[-21:-1])/20 if sum(v[-21:-1]) else 0
    ratio=v[-1]/base if base else 0
    if ratio >= 1.8 and c[-1] > o[-1]: vol_pts=3
    elif ratio >= 1.3 and c[-1] > o[-1]: vol_pts=2
    elif ratio >= 1.0 and c[-1] > o[-1]: vol_pts=1
    else: vol_pts=0

    parts={"ema20":ema_pts,"ma50":ma_pts,"candle_close":close_pts,"slope":slope_pts,"volume":vol_pts,
           "ema20_value":e20,"ma50_value":m50,"volume_ratio":ratio}
    total=min(15,ema_pts+ma_pts+close_pts+slope_pts+vol_pts)
    desc=(f"EMA20/MA50 support {ema_pts+ma_pts}/6; candle close {close_pts}/3; "
          f"slope {slope_pts}/3; volume confirmation {vol_pts}/3 ({ratio:.2f}x)")
    return total,parts,desc

def indicator_confluence(k):
    """Technical confluence on closed candles; heuristics, not guaranteed market structure labels."""
    if len(k) < 110:
        return 0, ['indicator confluence unavailable (need 110 candles)'], {}
    c=[x['c'] for x in k]; h=[x['h'] for x in k]; l=[x['l'] for x in k]
    o=[x['o'] for x in k]; v=[x['v'] for x in k]
    p=c[-1]; points=0; notes=[]; info={}
    # RSI (0-2): favor healthy momentum, avoid overbought entries.
    rv=rsi(c,14); info['rsi']=rv
    if 52 <= rv <= 68: points+=2; notes.append(f'RSI bullish/healthy ({rv:.0f})')
    elif 45 <= rv < 52: points+=1; notes.append(f'RSI recovering ({rv:.0f})')
    elif rv > 75: notes.append(f'RSI overbought ({rv:.0f})')
    else: notes.append(f'RSI {rv:.0f}')
    # EMA 10/20/55 alignment (0-3).
    e10=ema(c,10)[-1]; e20=ema(c,20)[-1]; e55=ema(c,55)[-1]
    info.update({'ema10':e10,'ema20':e20,'ema55':e55})
    if p>e10>e20>e55: notes.append('EMA10/20/55 bullish alignment (diagnostic; EMA support scored separately)')
    elif p>e20>e55: notes.append('EMA20/55 bullish structure (diagnostic; EMA support scored separately)')
    elif p>e10: notes.append('price above EMA10 (diagnostic only)')
    else: notes.append('EMA10/20/55 not aligned bullish')
    # Simple moving averages 50/100 (0-2).
    m50=sma(c,50); m100=sma(c,100); info.update({'ma50':m50,'ma100':m100})
    if m50 and m100 and p>m50>m100: notes.append('MA50 above MA100; price above both (diagnostic; MA50 support scored separately)')
    elif m50 and p>m50: notes.append('price above MA50 (diagnostic only)')
    else: notes.append('MA50/100 trend not bullish')
    # Fibonacci retracement: latest 60-bar swing range, bullish retracement levels.
    look=60; hi=max(h[-look:]); lo=min(l[-look:]); span=hi-lo
    fibs={}
    if span>0:
        fibs={'0.382':hi-span*.382,'0.5':hi-span*.5,'0.618':hi-span*.618}
        near=min(fibs.items(),key=lambda z:abs(p-z[1])/p if p else 999)
        dist=abs(p-near[1])/p*100 if p else 999
        info['fib_nearest']={'level':near[0],'price':near[1],'distance_pct':dist}
        if dist<=1.0 and p>=lo+span*.25: points+=2; notes.append(f'near Fib {near[0]} retracement ({near[1]:.8g})')
        elif dist<=2.0: points+=1; notes.append(f'approaching Fib {near[0]} ({near[1]:.8g})')
        else: notes.append('no nearby Fibonacci level')
    # Liquidity sweep: wick takes prior 20-bar high/low then closes back inside.
    prior_hi=max(h[-21:-1]); prior_lo=min(l[-21:-1]); last=k[-1]
    swept_low=last['l']<prior_lo and last['c']>prior_lo
    swept_high=last['h']>prior_hi and last['c']<prior_hi
    info['liquidity_sweep']='sell-side sweep' if swept_low else 'buy-side sweep' if swept_high else None
    if swept_low: points+=2; notes.append('sell-side liquidity sweep and reclaim')
    elif swept_high: notes.append('buy-side liquidity sweep (potential rejection)')
    else: notes.append('no fresh 20-candle liquidity sweep')
    # Supply/demand proxy: proximity to recent swing demand/supply, not institutional order data.
    demand=min(l[-30:-2]); supply=max(h[-30:-2])
    info['demand_zone']=demand; info['supply_zone']=supply
    demand_dist=(p-demand)/p*100 if p else 999; supply_dist=(supply-p)/p*100 if p else -999
    if 0 <= demand_dist <= 1.5 and p>=last['o']: points+=1; notes.append(f'near demand zone ({demand:.8g})')
    elif 0 <= supply_dist <= 1.5: notes.append(f'near supply/resistance zone ({supply:.8g})')
    else: notes.append('not at detected supply/demand zone')
    # Fair Value Gap (3-candle imbalance) on latest 12 candles.
    fvg=None
    for i in range(len(k)-1,max(1,len(k)-13),-1):
        if l[i] > h[i-2]: fvg=('bullish',h[i-2],l[i]); break
        if h[i] < l[i-2]: fvg=('bearish',h[i],l[i-2]); break
    info['fvg']={'type':fvg[0],'low':fvg[1],'high':fvg[2]} if fvg else None
    if fvg and fvg[0]=='bullish' and fvg[1] <= p <= fvg[2]: points+=1; notes.append('price inside bullish FVG')
    elif fvg: notes.append(f'{fvg[0]} FVG detected ({fvg[1]:.8g}-{fvg[2]:.8g})')
    else: notes.append('no recent FVG detected')
    # AMD heuristic: tight range, downside manipulation wick, close back into range and bullish close.
    rng=max(h[-20:-5])-min(l[-20:-5]); base=max(p,1e-12)
    tight=rng/base < .04
    manip=last['l']<min(l[-20:-5]) and last['c']>min(l[-20:-5])
    if tight and manip and last['c']>last['o']:
        points+=1; notes.append('AMD-like accumulation → sell-side manipulation → bullish recovery')
    elif tight: notes.append('AMD-like range/accumulation watch; manipulation/distribution unconfirmed')
    else: notes.append('no clear AMD sequence')
    # Order flow proxy from Binance spot taker-buy quote volume, last 12 candles.
    q=sum(x['q'] for x in k[-12:]); buy=sum(x['tb'] for x in k[-12:]); ratio=buy/q if q else .5
    info['taker_buy_ratio']=ratio
    if ratio>=.58: points+=1; notes.append(f'order-flow proxy buyer-dominant ({ratio:.0%} taker-buy)')
    else: notes.append(f'order-flow proxy {ratio:.0%} taker-buy')
    return min(15,points),notes,info

def resistance(k):
    if len(k)<40:return 0,None,'unavailable'
    c=[x['c'] for x in k];price=c[-1]; highs=[x['h'] for x in k[-60:-5]]
    res=max(highs) if highs else price
    room=(res-price)/price*100 if price else 0
    if room>=10:return W['res'],room,'good resistance room'
    if room>=7:return 5,room,'reasonable resistance room'
    if room>=3:return 2,room,'limited resistance room'
    return 0,room,'near resistance'

def futures_json(path,params):
    if not ENABLE_FUTURES:return None
    return get_json(f'https://fapi.binance.com{path}',params,retries=0)

def okx_futures_data(base):
    """Fallback market intelligence when Binance Futures is geo-blocked.
    Only public OKX endpoints are used; unavailable fields stay unavailable."""
    inst=f'{base}-USDT-SWAP'
    oi=get_json('https://www.okx.com/api/v5/public/open-interest',{'instType':'SWAP','instId':inst},retries=0)
    fr=get_json('https://www.okx.com/api/v5/public/funding-rate',{'instId':inst},retries=0)
    cur=0.0; funding=None
    try:
        d=(oi or {}).get('data') or []; cur=float(d[0].get('oiUsd') or 0) if d else 0.0
    except (TypeError,ValueError,IndexError): pass
    try:
        d=(fr or {}).get('data') or []; funding=float(d[0].get('fundingRate')) if d else None
    except (TypeError,ValueError,IndexError): pass
    return cur,funding

def futures_data(sym,k1,st):
    out={'oi':0,'funding':None,'liq':0,'pressure':0,'notes':[],'scores':{'oi':0,'funding':0,'liq':0}}
    base=sym[:-4]
    # Binance Futures first; if restricted (HTTP 451) or unavailable, fall back to OKX.
    oi=futures_json('/fapi/v1/openInterest',{'symbol':sym})
    try:cur=float(oi['openInterest']); oi_source='binance'
    except (TypeError,ValueError,KeyError):cur=0.0; oi_source='okx'
    fr=futures_json('/fapi/v1/fundingRate',{'symbol':sym,'limit':1})
    try:funding=float(fr[-1]['fundingRate']) if isinstance(fr,list) and fr else None
    except (TypeError,ValueError,KeyError,IndexError):funding=None
    if not cur or funding is None:
        okx_oi,okx_funding=okx_futures_data(base)
        if not cur and okx_oi:cur=okx_oi; oi_source='okx'
        if funding is None:funding=okx_funding
        if cur or funding is not None:out['notes'].append('Futures confirmation via OKX fallback')
    if cur:
        prev=st['oi'].get(base);out['oi']=cur
        # Never compare Binance contract units with OKX USD notional.
        if prev and prev.get('v') and prev.get('source','binance')==oi_source:
            ch=(cur/prev['v']-1)*100
            if ch>8:out['scores']['oi']=W['oi'];out['notes'].append(f'OI rising {ch:+.1f}%')
            elif ch>3:out['scores']['oi']=2;out['notes'].append(f'OI rising {ch:+.1f}%')
            elif ch>0:out['scores']['oi']=1
        st['oi'][base]={'v':cur,'ts':time.time(),'source':oi_source}
    if funding is not None:
        out['funding']=funding
        if -0.0005<=funding<=0.0005:out['scores']['funding']=W['funding'];out['notes'].append('funding neutral/healthy')
        elif funding<0:out['scores']['funding']=1;out['notes'].append('funding negative')
        else:out['scores']['funding']=0;out['notes'].append('funding crowded positive')
    else:
        out['notes'].append('funding data unavailable; no points assigned')
    # Liquidation data is not assumed. The Binance allForceOrders endpoint can be restricted
    # or require permissions; without a verified public response, leave its score at zero.
    fo=futures_json('/fapi/v1/allForceOrders',{'symbol':sym,'limit':100})
    if isinstance(fo,list) and fo:
        cutoff=int((time.time()-900)*1000);short=long=0.0
        for x in fo:
            if int(x.get('time',0))<cutoff:continue
            q=float(x.get('origQty',0))*float(x.get('price',0)); side=x.get('side','')
            if side=='SELL':short+=q
            elif side=='BUY':long+=q
        total=short+long
        if total:
            if short>long*1.25:out['scores']['liq']=W['liq'];out['notes'].append(f'short liquidation heavy {usd(short)}')
            elif long>short*1.25:out['scores']['liq']=0;out['notes'].append(f'long liquidation heavy {usd(long)}')
            else:out['scores']['liq']=0
    return out

def cg_id(symbol,st):
    s=symbol[:-4].lower(); cache=st['cg']; now=time.time()
    if s in cache and 'verified' in cache[s] and now-cache[s].get('ts',0)<30*86400:return cache[s].get('id')
    d=get_json('https://api.coingecko.com/api/v3/search',{'query':s},retries=0)
    cid=None
    try:
        coins=d.get('coins',[])
        exact=[x for x in coins if str(x.get('symbol','')).lower()==s]
        # Do not guess when symbols collide: a wrong market cap is worse than missing data.
        cid=exact[0].get('id') if len(exact)==1 else None
    except:pass
    cache[s]={'id':cid,'ts':now,'verified':bool(cid)};return cid

def refresh_market_caps(symbols, st):
    """Refresh CoinGecko market caps in batches; use cached values if API is unavailable."""
    now=time.time(); cache=st.setdefault('market_caps',{}); ids=[]; symbol_to_id={}
    for sym in symbols:
        base=sym[:-4]
        cid=cg_id(sym,st)
        if cid:
            symbol_to_id[base]=cid
            item=cache.get(cid,{})
            if now-item.get('ts',0)>6*3600:
                ids.append(cid)
    ids=list(dict.fromkeys(ids))
    # CoinGecko accepts a comma-separated list of IDs; avoid one market-data request per coin.
    for i in range(0,len(ids),80):
        batch=ids[i:i+80]
        d=get_json('https://api.coingecko.com/api/v3/coins/markets',
                   {'vs_currency':'usd','ids':','.join(batch),'per_page':len(batch),'page':1},retries=0,timeout=12)
        if isinstance(d,list):
            for coin in d:
                cid=coin.get('id'); mc=coin.get('market_cap')
                if cid and mc:
                    cache[cid]={'market_cap':float(mc),'ts':now,'symbol':coin.get('symbol','')}
    out={}
    for base,cid in symbol_to_id.items():
        item=cache.get(cid,{})
        if item.get('market_cap') and (now-item.get('ts',0)<7*86400):out[base]=float(item['market_cap'])
    return out

def llama_score(symbol,st):
    if not ENABLE_DEFILLAMA:return 0,'disabled'
    cid=cg_id(symbol,st)
    if not cid:return 0,'no DeFiLlama mapping'
    d=get_json(f'https://coins.llama.fi/prices/current/coingecko:{cid}',retries=0)
    try:
        p=d['coins'][f'coingecko:{cid}'];price=float(p.get('price',0));dec=float(p.get('decimals',0) or 0)
        # Price availability alone is not bullish. Use confidence/symbol coverage as a modest data-quality score.
        conf=p.get('confidence')
        if conf is not None and float(conf)>=0.9:return W['llama'],f'DeFiLlama price verified (confidence {float(conf):.0%})'
        return 3,f'DeFiLlama price available for {cid}'
    except:return 0,'DeFiLlama data unavailable'

def btc_regime():
    k=spot_klines('BTCUSDT','4h',100)
    if len(k)<60:return 1.0,'BTC regime unavailable'
    c=[x['c'] for x in k];r=rsi(c);e=ema(c,50)[-1]
    if c[-1]<e and r<45:return .85,'BTC bearish regime'
    if c[-1]>e and r>=50:return 1.0,'BTC supportive regime'
    return .93,'BTC neutral regime'

def sentiment_snapshot(st):
    """Fetch lightweight, public sentiment proxies once per scan.
    CoinGecko Trending is attention proxy; Alternative.me Fear & Greed is broad market mood.
    Missing sources earn zero points and are never treated as positive.
    """
    now=time.time(); cached=st.get('sentiment_snapshot') or {}
    if now-cached.get('ts',0)<15*60:
        return cached
    out={'ts':now,'trending_symbols':{},'fear_greed':None,'notes':[]}
    d=get_json('https://api.coingecko.com/api/v3/search/trending',retries=0,timeout=10)
    try:
        coins=(d or {}).get('coins',[])
        for item in coins:
            coin=item.get('item',{})
            symbol=str(coin.get('symbol','')).upper()
            rank=int(coin.get('market_cap_rank') or 999999)
            if not symbol: continue
            pts=20 if rank<=15 else 16 if rank<=30 else 12 if rank<=50 else 8 if rank<=100 else 4
            # Use best available rank/score when a symbol occurs more than once.
            out['trending_symbols'][symbol]=max(out['trending_symbols'].get(symbol,0),pts)
        out['notes'].append('CoinGecko trending attention available' if coins else 'CoinGecko trending unavailable')
    except Exception:
        out['notes'].append('CoinGecko trending unavailable')
    fg=get_json('https://api.alternative.me/fng/',{'limit':1},retries=0,timeout=8)
    try:
        out['fear_greed']=int(fg['data'][0]['value'])
        out['notes'].append('global Fear & Greed available')
    except Exception:
        out['notes'].append('global Fear & Greed unavailable')
    st['sentiment_snapshot']=out
    return out

def sentiment_score(base, snapshot):
    """Returns 0-30: 20 coin attention + 10 broad market mood.
    This is a proxy, not a claim that social/news sentiment is fully measured.
    """
    attention=int((snapshot.get('trending_symbols') or {}).get(base.upper(),0))
    fg=snapshot.get('fear_greed'); mood=0; mood_note='market mood unavailable'
    if fg is not None:
        # Neutral-to-positive mood is supportive; extreme greed gets less credit to avoid chasing.
        if 45 <= fg <= 70: mood=10
        elif 35 <= fg < 45 or 70 < fg <= 80: mood=7
        elif 25 <= fg < 35 or 80 < fg <= 90: mood=4
        elif 15 <= fg < 25 or fg > 90: mood=2
        else: mood=1
        mood_note=f'global Fear & Greed {fg}/100'
    attention_note=f'CoinGecko trending attention {attention}/20' if attention else 'not in CoinGecko trending list (0/20)'
    return attention+mood, {'attention':attention,'market_mood':mood,'fear_greed':fg,
                            'attention_note':attention_note,'mood_note':mood_note}

def evaluate(sym,t,st,market_caps=None,sentiment=None):
    ks={tf:spot_klines(sym,tf,120 if tf in ('5m','15m','30m') else 100) for tf in INTERVALS}
    if len(ks['15m'])<60 or len(ks['5m'])<35:return None
    notes=[];blocked=[];detail={}
    tf_points=0
    for tf in ('5m','15m','30m','1h','4h'):
        pts,n=tf_score(ks[tf],tf);tf_points+=pts;notes.append(n)
    market_cap=(market_caps or {}).get(sym[:-4]); volume24=float(t.get('quoteVolume',0) or 0)
    sv,vr,turnover,vn=volume_score(ks['15m'],market_cap,volume24);notes.append(vn)
    sturn,tn=turnover_score(market_cap,volume24);notes.append(tn)
    detail['volume_ratio']=vr;detail['market_cap']=market_cap;detail['mcap_turnover']=turnover
    sa,an=accel_score(ks['5m']);notes.append(an)
    sc,cn=compression_score(ks['30m']);notes.append(cn)
    support_pts,support_detail,support_note=ema_ma_support_score(ks['15m'])
    notes.append(support_note); detail['ema_ma_support']=support_detail; detail['ema_ma_support_score']=support_pts
    sp,pn=pressure_score(ks['5m']);notes.append(pn)
    sr,room,rn=resistance(ks['1h']);notes.append(rn);detail['room']=room
    fd=futures_data(sym,ks['1h'],st);notes+=fd['notes']
    sl,ln=llama_score(sym,st);notes.append(ln)
    ind_score,ind_notes,ind_info=indicator_confluence(ks['15m'])
    notes.extend(ind_notes); detail['indicators']=ind_info; detail['indicator_score']=ind_score

    # Core + bonus design:
    # Technical raw max = TF 25 + acceleration 10 + compression 8 + EMA20/MA50 support 15
    # + resistance room 7 + non-duplicated indicator bonus 15 = 80; normalized to Technical /25.
    technical_raw=max(0,min(80,tf_points+sa+sc+support_pts+sr+ind_score))
    technical=round(technical_raw/80*25)
    # Data raw max = relative volume 18 + turnover 8 + spot buy pressure 12 + futures 5 + DeFiLlama 2 = 45.
    futures_pts=sum(fd.get('scores',{}).values())
    data_raw=max(0,min(45,sv+sturn+sp+futures_pts+sl))
    data=round(data_raw/45*25)
    sentiment_total,sent_detail=sentiment_score(sym[:-4],sentiment or {})
    sentiment_total=max(0,min(30,sentiment_total))
    # Preserve a 0-100 public score: the 80 available raw points are normalized to 100.
    raw_total=technical+data+sentiment_total
    overall=round(raw_total/80*100)
    detail.update({'technical_score':technical,'technical_raw':technical_raw,'technical_max_raw':80,
                   'data_score':data,'data_raw':data_raw,'data_max_raw':45,
                   'sentiment_score':sentiment_total,'sentiment':sent_detail,
                   'raw_total':raw_total,'raw_max':80,
                   'score_breakdown':{'Technical':f'{technical}/25','Data':f'{data}/25','Sentiment':f'{sentiment_total}/30'}})
    c5=[x['c'] for x in ks['5m']];c1=[x['c'] for x in ks['1h']]
    true_ranges=[]
    for j in range(1,min(len(ks['15m']),15)):
        cur=ks['15m'][-j];prev=ks['15m'][-j-1]
        true_ranges.append(max(cur['h']-cur['l'],abs(cur['h']-prev['c']),abs(cur['l']-prev['c'])))
    detail['atr15']=(sum(true_ranges)/len(true_ranges)) if true_ranges else 0
    r1=rsi(c1);ch24=float(t.get('priceChangePercent',0))
    late=(ch24>45 or r1>84 or (room is not None and room<1.2))
    overheated=(r1>88 or linear_change(c5,12)>12)
    if late:blocked.append('late/extended move')
    if overheated:blocked.append('overheated')
    if room is not None and room<1.2:blocked.append('near resistance')
    if linear_change(c1,3)<-3:
        technical=max(0,technical-4)
        detail['technical_score']=technical
        detail['score_breakdown']={'Technical':f'{technical}/25','Data':f'{data}/25','Sentiment':f'{sentiment_total}/30'}
        notes.append('1h momentum weak (-4 Technical caution points)')
    # Normalize the 80 available category points to the public 0-100 score.
    overall=round((technical+data+sentiment_total)/80*100)
    stage=('EARLY ENTRY' if overall<55 else
           'CONFIRMED MOMENTUM' if overall<75 else
           'HIGH-CONFIDENCE EARLY ENTRY' if overall<85 else
           'STRONG PRE-PUMP SIGNAL')
    if blocked:stage='BLOCKED'
    return {'symbol':sym,'base':sym[:-4],'score':overall,'stage':stage,'blocked':blocked,'notes':notes,'detail':detail,
            'price':float(t.get('lastPrice',0)),'change24':ch24,'volume24':volume24,'market_cap':market_cap,
            'mcap_turnover':turnover,'funding':fd['funding'],'oi':fd['oi'],'url':f'https://www.binance.com/en/trade/{sym}','rsi1':r1}

def human_reason(it):
    n=' '.join(it['notes']).lower(); score=it['score'];
    openings=[
      'This one caught my attention because the move is still relatively early.',
      'There is a change in activity here that is worth watching.',
      'The interesting part is not just the price move — it is the activity behind it.',
      'I would keep this one on the radar rather than chase it.'
    ]
    opening=openings[int(it['score'])%len(openings)]
    bits=[]
    if 'volume' in n:bits.append('Volume is picking up, which is usually more interesting when price has not already gone vertical.')
    if 'oi rising' in n:bits.append('Futures open interest is also moving higher, so traders are becoming more active around the move.')
    if 'funding neutral' in n:bits.append('Funding is still relatively healthy, so the setup is not obviously overcrowded on the long side.')
    if 'resistance room' in n or 'good resistance' in n:bits.append('There is still some room before the next major resistance, which gives the setup breathing space.')
    if 'compressed' in n:bits.append('The 30-minute chart is compressed, so a clean expansion could make the setup more interesting.')
    det=it.get('detail',{}); sent=det.get('sentiment',{})
    bits.append(f"Category scores: Technical {det.get('technical_score',0)}/25, Data {det.get('data_score',0)}/25, Sentiment {det.get('sentiment_score',0)}/30.")
    if sent.get('attention',0)>0: bits.append('CoinGecko is currently showing attention for this coin, which supports the sentiment score.')
    else: bits.append('No strong CoinGecko trending attention was detected; the bot does not invent social/news points.')
    if not bits:bits.append('The higher-timeframe structure is the main reason this remains on the radar.')
    close=('What I want to see next is simple: volume should stay elevated, the short-term structure should hold, '
           'and price should clear nearby resistance cleanly. If that happens, the setup becomes more interesting.')
    return opening+'\n\n'+' '.join(bits)+'\n\n'+close+'\n\n⚠️ This is a momentum watch, not financial advice. Do not chase a move just because the score is high.'

def signal_text(it, serial=None):
    d=it['detail'];score=it['score'];
    entry=it['price']; room=d.get('room');
    levels=trade_levels(it)
    sl=levels['sl'] if levels else None
    tps=levels['tps'] if levels else []
    mc=it.get('market_cap'); turnover=it.get('mcap_turnover')
    cap_line=f"🏦 Market Cap: {usd(mc) if mc else 'Unavailable'}\n"
    turnover_line=f"📐 24h Volume / Market Cap: {turnover*100:.2f}%\n" if turnover is not None else "📐 Volume / Market Cap: unavailable\n"
    ind=d.get('indicators',{}); fib=ind.get('fib_nearest'); fvg=ind.get('fvg')
    ind_lines=(f"📟 RSI(14): {ind.get('rsi',0):.1f}\n"
               f"📉 EMA 10/20/55: {ind.get('ema10',0):.8g} / {ind.get('ema20',0):.8g} / {ind.get('ema55',0):.8g}\n"
               f"📊 MA 50/100: {ind.get('ma50',0):.8g} / {ind.get('ma100',0):.8g}\n"
               f"🧮 Fibonacci: {fib['level']} @ {fib['price']:.8g} ({fib['distance_pct']:.2f}% away)\n" if fib else
               f"📟 RSI(14): {ind.get('rsi',0):.1f}\n📉 EMA 10/20/55: {ind.get('ema10',0):.8g} / {ind.get('ema20',0):.8g} / {ind.get('ema55',0):.8g}\n📊 MA 50/100: {ind.get('ma50',0):.8g} / {ind.get('ma100',0):.8g}\n🧮 Fibonacci: unavailable\n")
    ind_lines += f"🧹 Liquidity Sweep: {ind.get('liquidity_sweep') or 'none detected'}\n"
    ind_lines += f"🧱 Demand/Supply: {ind.get('demand_zone',0):.8g} / {ind.get('supply_zone',0):.8g}\n"
    ind_lines += f"🟦 FVG: {fvg['type']} {fvg['low']:.8g}-{fvg['high']:.8g}\n" if fvg else "🟦 FVG: none detected\n"
    ind_lines += f"🧭 Order-flow proxy: {ind.get('taker_buy_ratio',0.5):.0%} taker-buy\n"
    return (f"🚨 BINANCE GEMS | SIGNAL #{serial if serial is not None else '—'}\n"
            f"{it['stage']} — ${it['base']}\n\n"
            f"🏆 OVERALL SCORE: {score:.0f}/100\n"
            f"📊 Technical: {d.get('technical_score',0)}/25\n"
            f"💹 Data: {d.get('data_score',0)}/25\n"
            f"🧠 Sentiment: {d.get('sentiment_score',0)}/30\n"
            f"🧮 Technical confluence detail: {d.get('indicator_score',0)}/15\n"
            f"📈 Spot Momentum: building\n\n"
            f"WHY SELECTED\n"
            f"• Technical: {d.get('technical_score',0)}/25 — multi-timeframe trend, EMA, momentum, compression and chart structure.\n"
            f"• Data: {d.get('data_score',0)}/25 — relative volume, market-cap turnover, spot buy pressure and available external data.\n"
            f"• Sentiment: {d.get('sentiment_score',0)}/30 — CoinGecko trending attention + global Fear & Greed mood; unavailable data gets no points.\n\n"
            f"💰 Price: ${entry:.8g}\n📊 24H: {it['change24']:+.1f}%\n💵 24H Volume: {usd(it['volume24'])}\n"
            f"{cap_line}{turnover_line}\n{ind_lines}\n"
            f"🎯 Entry Watch: ${entry:.8g}\n"
            + (f"🛑 Stop Loss: ${sl:.8g}\n" + ''.join(f"🎯 TP{i}: ${tp:.8g}\n" for i,tp in enumerate(tps,1)) if levels else "🛑 TP/SL: unavailable (insufficient data)\n") + "\n"
            f"🧠 Why I'm Watching\n{human_reason(it)}\n\n"
            f"")

def scan(st):
    cand=stage1()
    log(f'Stage 1: {len(cand)} Binance Spot candidates (absolute safety floor {usd(MIN_24H_VOL)}; quality ranked by relative volume and market-cap turnover)')
    # Mix absolute liquidity with strongest 24h movers so low-cap activity is not buried.
    cand.sort(key=lambda x:(x[2],float(x[1].get('priceChangePercent',0))),reverse=True)
    by_volume=sorted(cand,key=lambda x:x[2],reverse=True)[:max(1,DEEP_N//3)]
    by_change=sorted(cand,key=lambda x:float(x[1].get('priceChangePercent',0)),reverse=True)[:max(1,DEEP_N//3)]
    by_recovery=sorted((x for x in cand if -8 <= float(x[1].get('priceChangePercent',0)) <= 12),
                       key=lambda x:(float(x[1].get('priceChangePercent',0)),x[2]),reverse=True)[:max(1,DEEP_N//2)]
    picked={x[0]:x for x in by_volume}
    for x in by_change+by_recovery:picked.setdefault(x[0],x)
    shortlist=list(picked.values())[:DEEP_N]
    log(f'Stage 2: deep scan shortlist={len(shortlist)} (DEEP_SCAN_COUNT={DEEP_N})')
    caps=refresh_market_caps([x[0] for x in shortlist],st)
    sentiment=sentiment_snapshot(st)
    def priority(item):
        sym,t,q=item; mc=caps.get(sym[:-4]); turnover=(q/mc) if mc else 0
        ch=float(t.get('priceChangePercent',0))
        return (min(turnover,0.75)*100 + max(min(ch,20),-10)*0.5, q)
    shortlist=sorted(shortlist,key=priority,reverse=True)
    items=[]; evaluated=[]; errors=0; insufficient=0
    for sym,t,_ in shortlist:
        try:
            it=evaluate(sym,t,st,caps,sentiment)
            if it is None:
                insufficient+=1
                continue
            evaluated.append(it)
            if it['score']>=SIGNAL_MIN and not it['blocked']:
                items.append(it)
        except Exception as e:
            errors+=1
            log(f'eval {sym}: {e}')
        time.sleep(.12)
    items.sort(key=lambda x:x['score'],reverse=True)
    # Diagnostics are written to Actions logs every scan so a zero-signal run is explainable.
    bands={'under_45':0,'45_54':0,'55_64':0,'65_74':0,'75_84':0,'85_plus':0}
    blocked_reasons={}; eligible_below=0
    for it in evaluated:
        sc=float(it['score'])
        if sc<45: bands['under_45']+=1
        elif sc<55: bands['45_54']+=1
        elif sc<65: bands['55_64']+=1
        elif sc<75: bands['65_74']+=1
        elif sc<85: bands['75_84']+=1
        else: bands['85_plus']+=1
        if it['blocked']:
            for reason in it['blocked']:
                blocked_reasons[reason]=blocked_reasons.get(reason,0)+1
        elif sc<SIGNAL_MIN:
            eligible_below+=1
    log('=== BINANCE GEMS SCAN DIAGNOSTICS ===')
    log(f'Candidates: stage1={len(cand)}, deep_scanned={len(shortlist)}, evaluated={len(evaluated)}, insufficient_klines={insufficient}, eval_errors={errors}')
    log(f'Score bands: <45={bands["under_45"]}, 45-54={bands["45_54"]}, 55-64={bands["55_64"]}, 65-74={bands["65_74"]}, 75-84={bands["75_84"]}, 85+={bands["85_plus"]}')
    log(f'Configured signal threshold={SIGNAL_MIN}; blocked by risk filters={sum(blocked_reasons.values())}; below threshold but not blocked={eligible_below}; eligible={len(items)}')
    log('Risk filter reasons: '+(json.dumps(blocked_reasons,ensure_ascii=False,sort_keys=True) if blocked_reasons else 'none'))
    if evaluated:
        top=sorted(evaluated,key=lambda x:x['score'],reverse=True)[:5]
        log('Top evaluated: '+', '.join(f"{x['symbol']}={x['score']}/100 ({x['stage']}{'; blocked: '+','.join(x['blocked']) if x['blocked'] else ''})" for x in top))
        log('Category averages: Technical={:.1f}/25, Data={:.1f}/25, Sentiment={:.1f}/30 (category raw max 80, normalized to /100)'.format(
            sum(x['detail'].get('technical_score',0) for x in evaluated)/len(evaluated),
            sum(x['detail'].get('data_score',0) for x in evaluated)/len(evaluated),
            sum(x['detail'].get('sentiment_score',0) for x in evaluated)/len(evaluated)))
    log('=== END SCAN DIAGNOSTICS ===')
    return items

def monitor_open_signals(st):
    """Report first observed TP1/SL touch using current Binance spot price. Polling can miss intrarun wicks."""
    opened=st.setdefault('open_signals',{})
    for key, sig in list(opened.items()):
        if sig.get('status') != 'OPEN':
            continue
        try:
            data=get_json(BINANCE_SPOT[0]+'/api/v3/ticker/price', {'symbol': sig['symbol']}, retries=0, timeout=8)
            if not isinstance(data,dict) or not data.get('price'):
                continue
            price=float(data['price']); sig['last_price']=price; sig['last_checked']=time.time()
            sl=float(sig.get('sl') or 0); tp1=float((sig.get('tps') or [0])[0] or 0)
            # If both are crossed at the same observed quote, do not guess which came first.
            hit_sl=sl>0 and price<=sl; hit_tp=tp1>0 and price>=tp1
            if hit_sl and hit_tp:
                sig['status']='AMBIGUOUS'; sig['result']='TP1/SL ordering unclear from polling'
                post_tg(f"📋 BINANCE GEMS — SIGNAL #{sig['serial']} UPDATE\n${sig['base']} result unclear: TP1/SL ordering cannot be verified from sampled prices. Last observed: ${price:.8g}")
            elif hit_sl:
                sig['status']='SL_HIT'; sig['result']='SL'; sig['closed_at']=time.time()
                post_tg(f"🛑 BINANCE GEMS — SIGNAL #{sig['serial']} RESULT\n${sig['base']} reached the stop-loss level.\nEntry: ${sig['entry']:.8g} | SL: ${sl:.8g} | Last observed: ${price:.8g}\nThis is a price-monitor result, not proof of an executed trade.")
            elif hit_tp:
                sig['status']='TP1_HIT'; sig['result']='TP1'; sig['closed_at']=time.time()
                post_tg(f"🎯 BINANCE GEMS — SIGNAL #{sig['serial']} UPDATE\n${sig['base']} reached TP1.\nEntry: ${sig['entry']:.8g} | TP1: ${tp1:.8g} | Last observed: ${price:.8g}\nI’ll keep tracking for TP2/TP3 or SL; price monitoring does not confirm a trade execution.")
            # Keep state bounded.
        except Exception as e:
            log(f"outcome monitor {key}: {e}")
    # Remove old completed records after 30 days.
    cutoff=time.time()-30*86400
    st['open_signals']={k:v for k,v in opened.items() if v.get('status')=='OPEN' or v.get('closed_at',v.get('ts',0))>cutoff}

def send_signals(st,items):
    now=time.time(); sent=0
    for it in items:
        key=it['symbol']; old=st['alerts'].get(key)
        if old:
            age=now-old.get('ts',0)
            major_upgrade=(age>=3*3600 and it['score']>=old.get('score',0)+15)
            if age<COOLDOWN_MIN*60 and not major_upgrade: continue
        st['signal_seq']=int(st.get('signal_seq',0))+1
        serial=st['signal_seq']
        text=signal_text(it,serial)
        tg_ok=post_tg(text)
        levels=trade_levels(it)
        # Only track signals when TP/SL outcome updates are explicitly enabled.
        if ENABLE_OUTCOME_MONITOR and levels and levels.get('tps'):
            st.setdefault('open_signals',{})[key]={'serial':serial,'symbol':key,'base':it['base'],'entry':levels['entry'],'sl':levels['sl'],'tps':levels['tps'],'ts':now,'status':'OPEN','last_price':it['price']}
        if POST_TO_SQUARE and it['score'] >= SQUARE_MIN_SCORE:
            post_square(square_text(it,serial))
        st['alerts'][key]={'ts':now,'score':it['score'],'stage':it['stage'],'serial':serial}
        sent+=1
    return sent

def tick(force=False):
    st=load_state();now=time.time()
    if not force and now-st['last_scan']<SCAN_EVERY_MIN*60-5:return
    regime,f=btc_regime();log(regime)
    if ENABLE_OUTCOME_MONITOR:
        monitor_open_signals(st)
    items=scan(st)
    # No empty Telegram message. No summary. Only real eligible coins are sent.
    n=send_signals(st,items)
    st['last_scan']=now;save_state(st)
    log(f'Eligible signals: {len(items)}, sent: {n}')

def watch(minutes,force_first=False):
    end=time.time()+minutes*60;first=True
    while time.time()<end:
        try:tick(force=force_first and first)
        except Exception as e:log(f'tick error: {e}')
        first=False;time.sleep(TICK_SECONDS)

if __name__=='__main__':
    if '--test-telegram' in sys.argv:post_tg('✅ Binance Spot bot connected.')
    elif '--once' in sys.argv:tick(True)
    elif '--watch' in sys.argv:watch(float(sys.argv[sys.argv.index('--watch')+1]),'--force' in sys.argv)
    elif '--tick' in sys.argv:tick('--force' in sys.argv)
    else:
        while True:
            try:tick()
            except Exception as e:log(f'loop error: {e}')
            time.sleep(TICK_SECONDS)
