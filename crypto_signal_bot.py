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
MIN_24H_VOL=float(os.getenv('BINANCE_MIN_24H_VOLUME_USD','5000000'))
STAGE1_TOP=int(os.getenv('STAGE1_TOP','60'))
DEEP_N=int(os.getenv('DEEP_SCAN_COUNT','18'))
SIGNAL_MIN=float(os.getenv('SIGNAL_MIN_SCORE','45'))
COOLDOWN_MIN=int(os.getenv('ALERT_COOLDOWN_MIN','60'))
ENABLE_DEFILLAMA=os.getenv('ENABLE_DEFILLAMA','true').lower()=='true'
ENABLE_FUTURES=os.getenv('ENABLE_FUTURES_CONFIRMATION','true').lower()=='true'

BINANCE_SPOT=['https://api.binance.com','https://data-api.binance.vision']
FAPI=['https://fapi.binance.com','https://fapi.binance.com']
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
    st.setdefault('last_scan',0); st.setdefault('alerts',{}); st.setdefault('oi',{}); st.setdefault('cg',{}); st.setdefault('llama',{})
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
    movers=sorted(arr,key=lambda z:float(z[1].get('priceChangePercent',0)),reverse=True)[:20]
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

def volume_score(k):
    if len(k)<35:return 0,0,'unavailable'
    v=[x['v'] for x in k]; recent=v[-3:];base=sum(v[-27:-3])/24 if sum(v[-27:-3]) else 0
    ratio=(sum(recent)/3)/base if base else 0
    accel=(v[-1]/max(v[-2],1))-1
    s=min(max(ratio-1,0)/2,1)*9 + min(max(accel,0)/1,1)*4
    return min(W['volume'],s),ratio,f'volume {ratio:.1f}x baseline'

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
    return get_json(f'https://fapi.binance.com{path}',params,retries=1)

def futures_data(sym,k1,st):
    out={'oi':0,'funding':0,'liq':0,'pressure':0,'notes':[],'scores':{'oi':0,'funding':0,'liq':0}}
    base=sym[:-4]
    oi=futures_json('/fapi/v1/openInterest',{'symbol':sym})
    try:cur=float(oi['openInterest'])
    except:cur=0
    if cur:
        prev=st['oi'].get(base);out['oi']=cur
        if prev and prev.get('v'):
            ch=(cur/prev['v']-1)*100
            if ch>8:out['scores']['oi']=W['oi'];out['notes'].append(f'OI rising {ch:+.1f}%')
            elif ch>3:out['scores']['oi']=6;out['notes'].append(f'OI rising {ch:+.1f}%')
            elif ch>0:out['scores']['oi']=3
        st['oi'][base]={'v':cur,'ts':time.time()}
    fr=futures_json('/fapi/v1/fundingRate',{'symbol':sym,'limit':1})
    try:f=float(fr[-1]['fundingRate']) if isinstance(fr,list) else 0
    except:f=0
    out['funding']=f
    if -0.0005<=f<=0.0005:out['scores']['funding']=W['funding'];out['notes'].append('funding neutral/healthy')
    elif f<0:out['scores']['funding']=4;out['notes'].append('funding negative')
    else:out['scores']['funding']=1;out['notes'].append('funding crowded positive')
    # Force orders are sampled for the last 15 minutes. Missing data = neutral, never invented.
    fo=futures_json('/fapi/v1/allForceOrders',{'symbol':sym,'limit':100})
    if isinstance(fo,list) and fo:
        cutoff=int((time.time()-900)*1000);short=long=0
        for x in fo:
            if int(x.get('time',0))<cutoff:continue
            q=float(x.get('origQty',0))*float(x.get('price',0)); side=x.get('side','')
            if side=='SELL':short+=q
            elif side=='BUY':long+=q
        total=short+long
        if total:
            # SELL liquidation means longs were liquidated; BUY liquidation means shorts.
            if short>long*1.25:out['scores']['liq']=W['liq'];out['notes'].append(f'short liquidation heavy {usd(short)}')
            elif long>short*1.25:out['scores']['liq']=2;out['notes'].append(f'long liquidation heavy {usd(long)}')
            else:out['scores']['liq']=3
    return out

def cg_id(symbol,st):
    s=symbol[:-4].lower(); cache=st['cg']; now=time.time()
    if s in cache and now-cache[s].get('ts',0)<30*86400:return cache[s].get('id')
    d=get_json('https://api.coingecko.com/api/v3/search',{'query':s},retries=0)
    cid=None
    try:
        coins=d.get('coins',[])
        exact=[x for x in coins if str(x.get('symbol','')).lower()==s]
        cid=(exact[0] if exact else coins[0]).get('id') if (exact or coins) else None
    except:pass
    cache[s]={'id':cid,'ts':now};return cid

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

def evaluate(sym,t,st):
    ks={tf:spot_klines(sym,tf,120 if tf in ('5m','15m','30m') else 100) for tf in INTERVALS}
    if len(ks['1h'])<60 or len(ks['4h'])<50:return None
    score=0;notes=[];blocked=[];detail={}
    for tf in ('5m','15m','30m','1h','4h'):
        s,n=tf_score(ks[tf],tf);score+=s;notes.append(n)
    sv,vr,vn=volume_score(ks['15m']);score+=sv;notes.append(vn);detail['volume_ratio']=vr
    sa,an=accel_score(ks['5m']);score+=sa;notes.append(an)
    sc,cn=compression_score(ks['30m']);score+=sc;notes.append(cn)
    se,en=ema_structure(ks['1h']);score+=se;notes.append(en)
    sp,pn=pressure_score(ks['5m']);score+=sp;notes.append(pn)
    sr,room,rn=resistance(ks['1h']);score+=sr;notes.append(rn);detail['room']=room
    fd=futures_data(sym,ks['1h'],st);score+=sum(fd['scores'].values());notes+=fd['notes']
    sl,ln=llama_score(sym,st);score+=sl;notes.append(ln)
    score=min(100,score)
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
    return {'symbol':sym,'base':sym[:-4],'score':score,'stage':stage,'blocked':blocked,'notes':notes,'detail':detail,'price':float(t.get('lastPrice',0)),'change24':ch24,'volume24':float(t.get('quoteVolume',0)),'funding':fd['funding'],'oi':fd['oi'],'url':f'https://www.binance.com/en/trade/{sym}','rsi1':r1}

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
    return (f"🚨 {it['stage']} — ${it['base']}\n\n"
            f"🔥 Score: {score:.0f}/100\n"
            f"📈 Spot Momentum: building\n\n"
            f"💰 Price: ${entry:.8g}\n📊 24H: {it['change24']:+.1f}%\n💵 24H Volume: {usd(it['volume24'])}\n\n"
            f"🎯 Entry Watch: ${entry:.8g}\n🛑 Invalid Below: ${sl:.8g}\n🎯 TP1 Watch: ${tp1:.8g}\n🎯 TP2 Watch: ${tp2:.8g}\n\n"
            f"🧠 Why I'm Watching\n{human_reason(it)}\n\n"
            f"🔗 Binance Spot: {it['url']}")

def scan(st):
    cand=stage1();log(f'Stage 1: {len(cand)} Binance Spot candidates >= {usd(MIN_24H_VOL)}')
    # Deep priority: volume, short-term change, acceleration proxy.
    cand.sort(key=lambda x:(x[2],float(x[1].get('priceChangePercent',0))),reverse=True)
    items=[]
    for sym,t,_ in cand[:DEEP_N]:
        try:
            it=evaluate(sym,t,st)
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
        if old and now-old.get('ts',0)<COOLDOWN_MIN*60 and it['score']<old.get('score',0)+8:continue
        post_tg(signal_text(it));st['alerts'][key]={'ts':now,'score':it['score']};sent+=1
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
