"""Binance Spot Early Momentum Scanner

Binance Spot is the ONLY signal universe. Binance Futures is used only as
confirmation/market intelligence (OI, funding, liquidation, taker pressure).
DeFiLlama is queried per shortlisted coin when a usable CoinGecko mapping exists.

Score = 100
  5m confirmation              2
  15m confirmation              3
  30m confirmation              5
  1h trend                     10
  4h trend                     10
  Volume spike/acceleration    13
  Futures OI                   10
  Funding                       5
  Liquidation                   5
  Buy/Sell pressure             8
  DeFiLlama                     5
  Resistance room               7
  Price acceleration            7
  Compression/breakout          5
  EMA/market structure           5
  New indicator confluence      15% of final score (existing score scaled to 85%)

Visibility:
  <45  -> invisible
  45-49 -> EARLY RADAR
  50-59 -> WATCH
  60-80 -> BUY WATCH
  81-90 -> STRONG SIGNAL
  91+   -> STRONG+

Any late/overheated/near-resistance setup is blocked even with a high score.
"""
import json, math, os, re, sys, time, random
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
load_dotenv()

TG_TOKEN=os.getenv('TELEGRAM_BOT_TOKEN',''); TG_CHAT=os.getenv('TELEGRAM_CHAT_ID','')
STATE_FILE=os.getenv('STATE_FILE','state_binance.json')
SCAN_EVERY_MIN=int(os.getenv('SCAN_EVERY_MIN','5'))
TICK_SECONDS=int(os.getenv('TICK_SECONDS','60'))
MIN_24H_VOL=float(os.getenv('BINANCE_MIN_24H_VOLUME_USD','500000'))
STAGE1_TOP=int(os.getenv('STAGE1_TOP','60'))
DEEP_N=int(os.getenv('DEEP_SCAN_COUNT','24'))
SIGNAL_MIN=float(os.getenv('SIGNAL_MIN_SCORE','45'))
COOLDOWN_MIN=int(os.getenv('ALERT_COOLDOWN_MIN','720'))
ENABLE_DEFILLAMA=os.getenv('ENABLE_DEFILLAMA','true').lower()=='true'
ENABLE_FUTURES=os.getenv('ENABLE_FUTURES_CONFIRMATION','true').lower()=='true'

# Try Binance's public market-data mirror first: api.binance.com can return HTTP 451
# from some hosted runners/regions. These are public market-data endpoints only.
BINANCE_SPOT=['https://data-api.binance.vision','https://api-gcp.binance.com','https://api1.binance.com','https://api2.binance.com','https://api3.binance.com','https://api4.binance.com','https://api.binance.com']
FAPI=['https://fapi.binance.com']
STABLES={'USDT','USDC','FDUSD','TUSD','BUSD','USDP','DAI','EUR','TRY','BRL','USDE','USDD','PYUSD','USTC'}
INTERVALS={'5m':'5m','15m':'15m','30m':'30m','1h':'1h','4h':'4h'}

W={'5m':2,'15m':3,'30m':5,'1h':10,'4h':10,'volume':13,'oi':10,'funding':5,
   'liq':5,'pressure':8,'llama':5,'res':7,'accel':7,'compression':5,'ema':5}
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

def load_state():
    try: st=json.load(open(STATE_FILE))
    except Exception: st={}
    st.setdefault('last_scan',0); st.setdefault('alerts',{}); st.setdefault('oi',{}); st.setdefault('cg',{}); st.setdefault('llama',{}); st.setdefault('market_caps',{})
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
        if q<MIN_24H_VOL:continue
        # Positive or recovering coins get priority; don't exclude quiet accumulation.
        if ch<-35:continue
        arr.append((sym,t,q))
    arr.sort(key=lambda z:(z[2],abs(float(z[1].get('priceChangePercent',0)))),reverse=True)
    # Keep some early movers even if their absolute volume rank is lower.
    movers=sorted(arr,key=lambda z:float(z[1].get('priceChangePercent',0)),reverse=True)[:30]
    merged={x[0]:x for x in arr[:STAGE1_TOP]}
    for x in movers:merged.setdefault(x[0],x)
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
    """Volume spike plus market-cap-normalized turnover; turnover is NOT net inflow."""
    if len(k)<35:return 0,0,0,'unavailable'
    v=[x['v'] for x in k]; recent=v[-3:];base=sum(v[-27:-3])/24 if sum(v[-27:-3]) else 0
    ratio=(sum(recent)/3)/base if base else 0
    accel=(v[-1]/max(v[-2],1))-1
    # Keep short-term activity as the core signal (max 10 points).
    core=min(max(ratio-1,0)/2,1)*6 + min(max(accel,0)/1,1)*4
    turnover=(float(volume24 or 0)/float(market_cap)) if market_cap and market_cap>0 else None
    # A high volume/market-cap ratio means unusually high turnover, not proven fresh inflow.
    bonus=0
    if turnover is not None:
        if turnover>=0.50: bonus=3
        elif turnover>=0.20: bonus=2.5
        elif turnover>=0.10: bonus=2
        elif turnover>=0.05: bonus=1.5
        elif turnover>=0.02: bonus=1
        elif turnover>=0.005: bonus=0.5
    score=min(W['volume'],core+bonus)
    desc=f'volume {ratio:.1f}x baseline'
    if turnover is not None: desc+=f'; 24h volume/market cap {turnover*100:.2f}%'
    else: desc+='; market cap unavailable'
    return score,ratio,turnover,desc

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
    if a>b/4 and a>0:s+=2
    if 0<a<8:s+=2
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
    if p>e10>e20>e55: points+=3; notes.append('EMA10/20/55 bullish alignment')
    elif p>e20>e55: points+=2; notes.append('EMA20/55 bullish structure')
    elif p>e10: points+=1; notes.append('price above EMA10')
    else: notes.append('EMA10/20/55 not aligned bullish')
    # Simple moving averages 50/100 (0-2).
    m50=sma(c,50); m100=sma(c,100); info.update({'ma50':m50,'ma100':m100})
    if m50 and m100 and p>m50>m100: points+=2; notes.append('MA50 above MA100; price above both')
    elif m50 and p>m50: points+=1; notes.append('price above MA50')
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
            elif ch>3:out['scores']['oi']=6;out['notes'].append(f'OI rising {ch:+.1f}%')
            elif ch>0:out['scores']['oi']=3
        st['oi'][base]={'v':cur,'ts':time.time(),'source':oi_source}
    if funding is not None:
        out['funding']=funding
        if -0.0005<=funding<=0.0005:out['scores']['funding']=W['funding'];out['notes'].append('funding neutral/healthy')
        elif funding<0:out['scores']['funding']=4;out['notes'].append('funding negative')
        else:out['scores']['funding']=1;out['notes'].append('funding crowded positive')
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
            elif long>short*1.25:out['scores']['liq']=2;out['notes'].append(f'long liquidation heavy {usd(long)}')
            else:out['scores']['liq']=3
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

def evaluate(sym,t,st,market_caps=None):
    ks={tf:spot_klines(sym,tf,120 if tf in ('5m','15m','30m') else 100) for tf in INTERVALS}
    if len(ks['1h'])<60 or len(ks['4h'])<50:return None
    score=0;notes=[];blocked=[];detail={}
    for tf in ('5m','15m','30m','1h','4h'):
        s,n=tf_score(ks[tf],tf);score+=s;notes.append(n)
    market_cap=(market_caps or {}).get(sym[:-4]); volume24=float(t.get('quoteVolume',0) or 0)
    sv,vr,turnover,vn=volume_score(ks['15m'],market_cap,volume24);score+=sv;notes.append(vn)
    detail['volume_ratio']=vr;detail['market_cap']=market_cap;detail['mcap_turnover']=turnover
    sa,an=accel_score(ks['5m']);score+=sa;notes.append(an)
    sc,cn=compression_score(ks['30m']);score+=sc;notes.append(cn)
    se,en=ema_structure(ks['1h']);score+=se;notes.append(en)
    sp,pn=pressure_score(ks['5m']);score+=sp;notes.append(pn)
    sr,room,rn=resistance(ks['1h']);score+=sr;notes.append(rn);detail['room']=room
    fd=futures_data(sym,ks['1h'],st);score+=sum(fd['scores'].values());notes+=fd['notes']
    sl,ln=llama_score(sym,st);score+=sl;notes.append(ln)
    # New indicator pack contributes 15% of the final score; existing model contributes 85%.
    ind_score,ind_notes,ind_info=indicator_confluence(ks['15m'])
    score=min(100,score*0.85+ind_score)
    notes.extend(ind_notes); detail['indicators']=ind_info; detail['indicator_score']=ind_score
    c5=[x['c'] for x in ks['5m']];c15=[x['c'] for x in ks['15m']];c1=[x['c'] for x in ks['1h']]
    r1=rsi(c1); ch24=float(t.get('priceChangePercent',0));
    late=(ch24>30 or r1>75 or (room is not None and room<2))
    overheated=(r1>78 or linear_change(c5,12)>8)
    if late:blocked.append('late/extended move')
    if overheated:blocked.append('overheated')
    if room is not None and room<2:blocked.append('near resistance')
    # A healthy early setup should not be deeply red on 1h.
    if linear_change(c1,3)<-3:blocked.append('1h momentum weak')
    stage='EARLY RADAR' if score<50 else 'WATCH' if score<60 else 'BUY WATCH' if score<=80 else 'STRONG SIGNAL' if score<=90 else 'STRONG+'
    if blocked:stage='BLOCKED'
    return {'symbol':sym,'base':sym[:-4],'score':score,'stage':stage,'blocked':blocked,'notes':notes,'detail':detail,'price':float(t.get('lastPrice',0)),'change24':ch24,'volume24':float(t.get('quoteVolume',0)),'market_cap':market_cap,'mcap_turnover':turnover,'funding':fd['funding'],'oi':fd['oi'],'url':f'https://www.binance.com/en/trade/{sym}','rsi1':r1}

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
    if not bits:bits.append('The higher-timeframe structure is the main reason this remains on the radar.')
    close=('What I want to see next is simple: volume should stay elevated, the short-term structure should hold, '
           'and price should clear nearby resistance cleanly. If that happens, the setup becomes more interesting.')
    return opening+'\n\n'+' '.join(bits)+'\n\n'+close+'\n\n⚠️ This is a momentum watch, not financial advice. Do not chase a move just because the score is high.'

def signal_text(it):
    d=it['detail'];score=it['score'];
    entry=it['price']; room=d.get('room');
    # conservative levels derived from price, not promises
    sl=entry*.97;tp1=entry*1.05;tp2=entry*1.09
    if room is not None and room>0:tp1=min(tp1,entry*(1+room/100*.8))
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
    return (f"🚨 {it['stage']} — ${it['base']}\n\n"
            f"🔥 Score: {score:.0f}/100\n"
            f"📈 Spot Momentum: building\n\n"
            f"💰 Price: ${entry:.8g}\n📊 24H: {it['change24']:+.1f}%\n💵 24H Volume: {usd(it['volume24'])}\n"
            f"{cap_line}{turnover_line}\n{ind_lines}\n"
            f"🎯 Entry Watch: ${entry:.8g}\n🛑 Invalid Below: ${sl:.8g}\n🎯 TP1 Watch: ${tp1:.8g}\n🎯 TP2 Watch: ${tp2:.8g}\n\n"
            f"🧠 Why I'm Watching\n{human_reason(it)}\n\n"
            f"🔗 Binance Spot: {it['url']}")

def scan(st):
    cand=stage1();log(f'Stage 1: {len(cand)} Binance Spot candidates >= {usd(MIN_24H_VOL)}')
    # Mix absolute liquidity with strongest 24h movers so low-cap activity is not buried.
    cand.sort(key=lambda x:(x[2],float(x[1].get('priceChangePercent',0))),reverse=True)
    # Reserve slots for both liquid coins and the strongest daily movers.
    by_volume=sorted(cand,key=lambda x:x[2],reverse=True)[:max(1,DEEP_N//2)]
    by_change=sorted(cand,key=lambda x:float(x[1].get('priceChangePercent',0)),reverse=True)[:max(1,DEEP_N//2)]
    picked={x[0]:x for x in by_volume}
    for x in by_change:picked.setdefault(x[0],x)
    shortlist=list(picked.values())[:DEEP_N]
    caps=refresh_market_caps([x[0] for x in shortlist],st)
    # Re-rank candidates by short-term change plus 24h volume/market-cap turnover where available.
    def priority(item):
        sym,t,q=item; mc=caps.get(sym[:-4]); turnover=(q/mc) if mc else 0
        ch=float(t.get('priceChangePercent',0))
        return (min(turnover,0.75)*100 + max(min(ch,20),-10)*0.5, q)
    shortlist=sorted(shortlist,key=priority,reverse=True)
    items=[]
    for sym,t,_ in shortlist:
        try:
            it=evaluate(sym,t,st,caps)
            if it and it['score']>=SIGNAL_MIN and not it['blocked']:items.append(it)
        except Exception as e:log(f'eval {sym}: {e}')
        time.sleep(.12)
    items.sort(key=lambda x:x['score'],reverse=True)
    return items

def send_signals(st,items):
    now=time.time()
    sent=0
    for it in items:
        key=it['symbol'];old=st['alerts'].get(key)
        if old:
            age=now-old.get('ts',0)
            # Default: one alert per coin per 6h. Only allow an early repeat after 3h
            # when the score improves by 15+ points, reducing repeated near-identical alerts.
            major_upgrade=(age>=3*3600 and it['score']>=old.get('score',0)+15)
            if age<COOLDOWN_MIN*60 and not major_upgrade:continue
        post_tg(signal_text(it));st['alerts'][key]={'ts':now,'score':it['score'],'stage':it['stage']};sent+=1
    return sent

def tick(force=False):
    st=load_state();now=time.time()
    if not force and now-st['last_scan']<SCAN_EVERY_MIN*60-5:return
    regime,f=btc_regime();log(regime)
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
