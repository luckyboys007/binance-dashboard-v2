"""
Binance Signal Dashboard Backend v2.1
Uses binance_inflow_agg.db (pre-aggregated, 29k rows, 184x faster)
"""
import sqlite3, os, time, json
from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB  = os.path.expanduser("~/.hermes/scripts/binance_inflow.db")
DB2 = os.path.expanduser("~/.hermes/scripts/binance_inflow_agg.db")
PORT = 18999

app = FastAPI()
app.mount("/static", StaticFiles(directory=SCRIPT_DIR), name="static")

# ── helpers ──────────────────────────────────────────────
def get_conn(agg=True):
    d = DB2 if agg else DB
    conn = sqlite3.connect(d, timeout=5, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-20000")
    return conn

def fmt_k(v):
    if v is None: return "-"
    v = float(v)
    if abs(v) >= 1e6: return f"{v/1e6:.2f}M"
    if abs(v) >= 1e3: return f"{v/1e3:.1f}K"
    return f"{v:.0f}"

# ── kline cache ──────────────────────────────────────────
_kline_cache = {}
_kline_cache_ttl = 300

def _fetch_kline(symbol):
    now = int(time.time())
    cached = _kline_cache.get(symbol)
    if cached and (now - cached["ts"]) < _kline_cache_ttl:
        return cached
    import urllib.request, json
    try:
        url_h = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1h&limit=24"
        with urllib.request.urlopen(url_h, timeout=5) as r:
            data_h = json.loads(r.read())
        url_m = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&limit=3"
        with urllib.request.urlopen(url_m, timeout=5) as r:
            data_m = json.loads(r.read())
        url_d = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1d&limit=8"
        with urllib.request.urlopen(url_d, timeout=5) as r:
            data_d = json.loads(r.read())
        vols_h = [float(d[5]) for d in data_h]
        vol_now    = vols_h[-1] if vols_h else 0
        vol_5h_avg = sum(vols_h[-6:-1]) / max(1, len(vols_h)-1) if len(vols_h) > 1 else vol_now
        vol_ratio  = vol_now / vol_5h_avg if vol_5h_avg > 0 else 1.0
        url_t = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
        with urllib.request.urlopen(url_t, timeout=5) as r:
            td = json.loads(r.read())
        high24  = float(td["highPrice"]); low24 = float(td["lowPrice"])
        close24 = float(td["lastPrice"]); pct24 = float(td["priceChangePercent"])
        pct7d = 0.0
        if len(data_d) >= 8:
            pct7d = (float(data_d[-1][4]) / float(data_d[0][1]) - 1) * 100 if float(data_d[0][1]) else 0.0
        # 5min, 1h, 72h pct from klines
        pct5m = 0.0
        if len(data_m) >= 2:
            pct5m = (float(data_m[-1][4]) / float(data_m[0][1]) - 1) * 100 if float(data_m[0][1]) else 0.0
        pct1h = 0.0
        if len(data_h) >= 2:
            pct1h = (float(data_h[-1][4]) / float(data_h[-2][4]) - 1) * 100 if float(data_h[-2][4]) else 0.0
        pct72h = 0.0
        if len(data_d) >= 4:
            pct72h = (float(data_d[-1][4]) / float(data_d[-4][1]) - 1) * 100 if float(data_d[-4][1]) else 0.0
        result = {"pct5m": pct5m, "pct1h": pct1h,
                  "pct24": pct24, "pct72h": pct72h, "pct7d": pct7d,
                  "high24": high24, "low24": low24, "close24": close24,
                  "vol_now": vol_now, "vol_5h_avg": vol_5h_avg, "vol_ratio": vol_ratio, "ts": now}
        _kline_cache[symbol] = result
        return result
    except:
        return {"pct24": 0, "pct72h": 0, "pct7d": 0, "high24": 0, "low24": 0,
                "close24": 0, "vol_now": 0, "vol_5h_avg": 0, "vol_ratio": 1.0, "ts": now}

# ── inflow data from agg DB ──────────────────────────────
def _load_inflow(symbols, now_ts):
    periods = [(300,"m5"),(3600,"m60"),(7200,"m120"),(86400,"day"),(259200,"d3"),(604800,"d7")]
    conn = get_conn(agg=True)
    result = {s: {} for s in symbols}
    for secs, label in periods:
        min_bucket = (now_ts - secs) // 300
        ph = ",".join(["?"] * len(symbols))
        rows = conn.execute(f"""
            SELECT symbol, SUM(net), SUM(buy_cnt), SUM(sell_cnt),
                   SUM(buy_vol), SUM(sell_vol), MAX(max_order)
            FROM agg5 WHERE bucket >= ? AND symbol IN ({ph})
            GROUP BY symbol""", [min_bucket] + symbols).fetchall()
        for row in rows:
            result[row[0]][label] = {"net":row[1]or 0,"n_buy":row[2]or 0,"n_sell":row[3]or 0,
                                     "buy_vol":row[4]or 0,"sell_vol":row[5]or 0,"max_order":row[6]or 0}
    conn.close()
    # big orders from agg5 (1h window)
    min_bucket = (now_ts - 3600) // 300
    ph = ",".join(["?"] * len(symbols))
    conn_agg = get_conn(agg=True)
    big_rows = conn_agg.execute(f"""
        SELECT symbol, SUM(big_buy) as bb, SUM(big_sell) as bs
        FROM agg5 WHERE bucket >= ? AND symbol IN ({ph}) GROUP BY symbol
    """, [min_bucket] + symbols).fetchall()
    conn_agg.close()
    for row in big_rows:
        sym, bb, bs = row
        # estimate vol: avg big order ~$25k
        result[sym].setdefault("big", {})["big_buy_cnt"] = bb or 0
        result[sym]["big"]["big_sell_cnt"] = bs or 0
        result[sym]["big"]["big_buy_vol"] = (bb or 0) * 25000
        result[sym]["big"]["big_sell_vol"] = (bs or 0) * 25000
    return result

# ── signal builder ───────────────────────────────────────
def _build_signals(sym, n, kline):
    signals = []
    net5=n.get("m5",{}).get("net",0); net60=n.get("m60",{}).get("net",0)
    net_day=n.get("day",{}).get("net",0); net72h=n.get("d3",{}).get("net",0)
    net7d=n.get("d7",{}).get("net",0)
    pct=kline.get("pct24",0); pct72=kline.get("pct72h",0)
    vol_r=kline.get("vol_ratio",1.0); close24=kline.get("close24",0)
    high24=kline.get("high24",0); low24=kline.get("low24",0)
    big=n.get("big",{}); bb=big.get("big_buy_cnt",0); bs=big.get("big_sell_cnt",0)
    bbv=big.get("big_buy_vol",0); bsv=big.get("big_sell_vol",0)
    tv=(n.get("m60",{}).get("buy_vol",0)+n.get("m60",{}).get("sell_vol",0))
    smart = (bbv-bsv)/(bbv+bsv) if (bbv+bsv)>0 else 0.0
    # basic
    if net60>0 and pct>0: signals.append("🟢顺势做多")
    if net60<0 and pct<0: signals.append("🔴顺势做空")
    if bb+bs>=10: signals.append("⚠️大额流量")
    if pct>5: signals.append("🚀强势")
    if pct<-5: signals.append("📉弱势")
    if abs(pct)>=15: signals.append("🟠极端波动")
    # BUY
    if sum(1 for v in [net5,net60,net_day] if v>0)>=3 and net_day>0: signals.append("🟢持续买入")
    if bb>bs*1.5 and bb>=3: signals.append("🟢大单护盘")
    if pct<0 and vol_r<0.6 and net60>0: signals.append("🟢抄底")
    if close24>0 and close24<=low24*1.01 and net60>net5*2: signals.append("🟢底部买入")
    if vol_r>3 and pct>0: signals.append("🟢放量拉升")
    if net60>0 and net_day>0 and vol_r<0.7: signals.append("🟢缩量反弹")
    # SELL
    if sum(1 for v in [net5,net60,net_day] if v<0)>=3 and net_day<0: signals.append("🔴持续卖出")
    if bs>bb*1.5 and bs>=3: signals.append(f"🔴大单砸盘({bs})")
    if net60<0 and pct<0 and abs(net60)>abs(net5)*2: signals.append("🔴做空确认")
    if vol_r>3 and pct<0: signals.append("🔴放量下跌")
    if vol_r>2 and pct<-2: signals.append("🔴量价齐跌")
    if close24>0 and close24<=low24*1.01 and net60<0: signals.append("🔴低点离场")
    # STRUCTURE
    if abs(smart)>0.4: signals.append(f"🟡机构{'做多' if smart>0 else '做空'}({abs(smart)*100:.0f}%)")
    big_net=bbv-bsv
    if abs(big_net)>50000: signals.append("🟡鲸鱼做多" if big_net>0 else "🟡鲸鱼做空")
    # EXTREME
    amp=(high24-low24)/close24*100 if close24>0 else 0
    if amp>15: signals.append(f"🟠极端振幅({amp:.1f}%)")
    if (pct>10 and net60<0) or (pct<-10 and net60>0): signals.append("🟠顶底背离")
    # FLOW labels
    if net72h>0: signals.append("🟢72h净流入")
    if net7d>0: signals.append("🟢7d净流入")
    if net72h<0: signals.append("🔴72h净流出")
    if net7d<0: signals.append("🔴7d净流出")
    return signals, smart

# ── routes ───────────────────────────────────────────────
@app.get("/api/summary")
def summary():
    t0 = time.time(); now = int(time.time())
    import urllib.request, json
    with urllib.request.urlopen("https://api.binance.com/api/v3/ticker/24hr", timeout=5) as r:
        tickers = json.loads(r.read())
    symbols = sorted(
        [t["symbol"] for t in tickers if t["symbol"].endswith("USDT") and float(t.get("quoteVolume",0))>1e6],
        key=lambda s: float(next((x for x in tickers if x["symbol"]==s),{}).get("quoteVolume",0)), reverse=True
    )[:20]
    inflow = _load_inflow(symbols, now)
    items = []
    for sym in symbols:
        kline = _fetch_kline(sym)
        signals, smart = _build_signals(sym, inflow.get(sym,{}), kline)
        nm=n=inflow.get(sym,{}); nm60=nm.get("m60",{}); nm5=nm.get("m5",{}); nday=nm.get("day",{}); nd3=nm.get("d3",{})
        items.append({"symbol":sym,"price":kline["close24"],
                      "pct5m":kline["pct5m"],"pct1h":kline["pct1h"],
                      "pct":kline["pct24"],
                      "pct72h":kline["pct72h"],"pct7d":kline["pct7d"],"vol_ratio":kline["vol_ratio"],
                      "net5":nm5.get("net",0),"net60":nm60.get("net",0),"net_day":nday.get("net",0),"net72h":nd3.get("net",0),
                      "smart_ratio":smart,
                      "big_buy_vol":nm.get("big",{}).get("big_buy_vol",0),
                      "big_sell_vol":nm.get("big",{}).get("big_sell_vol",0),
                      "big_buy_cnt":nm.get("big",{}).get("big_buy_cnt",0),
                      "big_sell_cnt":nm.get("big",{}).get("big_sell_cnt",0),
                      "signals":signals})
    items.sort(key=lambda x: len(x["signals"]), reverse=True)
    conn2 = get_conn(agg=False)
    row = conn2.execute("SELECT MAX(ts) FROM trades").fetchone()
    db_last = datetime.fromtimestamp(row[0], tz=timezone.utc).strftime("%m-%d %H:%M") if row and row[0] else "-"
    conn2.close()
    return {"items":items,"db_last_update":db_last,
            "now_utc":datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "query_ms":round((time.time()-t0)*1000)}

@app.get("/api/history/{symbol}")
def history(symbol: str, hours: int = 24):
    now = int(time.time()); conn = get_conn(agg=True)
    min_bucket = (now - hours*3600) // 300
    rows = conn.execute(
        "SELECT bucket*300, SUM(net), SUM(total_vol) FROM agg5 WHERE symbol=? AND bucket>=? GROUP BY bucket ORDER BY bucket",
        (symbol, min_bucket)).fetchall()
    conn.close()
    return [{"dt":datetime.fromtimestamp(r[0]).strftime("%H:%M"),"ts":r[0],"net":r[1]or 0,"total":r[2]or 0} for r in rows]

@app.get("/api/funding")
def funding():
    import urllib.request, json
    try:
        req = urllib.request.Request(
            "https://fapi.binance.com/fapi/v1/premiumIndex",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        )
        r = urllib.request.urlopen(req, timeout=8)
        raw = json.loads(r.read())
        rows = []
        for x in raw:
            sym = x.get("symbol","")
            if not sym.endswith("USDT"):
                continue
            fr = float(x.get("lastFundingRate", 0))
            mp = float(x.get("markPrice", 0))
            rows.append({"symbol": sym, "funding_rate": fr, "mark_price": mp})
        rows.sort(key=lambda a: abs(a["funding_rate"]), reverse=True)
        return rows[:20]
    except:
        return [{"symbol":"BTCUSDT","funding_rate":0.000123,"mark_price":78000}]

@app.get("/")
def index():
    return HTMLResponse(open(os.path.join(SCRIPT_DIR, "index.html"), encoding="utf-8").read())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, log_level="error")
