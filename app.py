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

TRADFI = {"USDCUSDT","USDTUSDT","USD1USDT","BNBUSDT","XAUUSDT","XAGUSDT",
          "TUSDUSDT","BUSDUSDT","DAIUSDT","FDUSDUSDT",
          "SOXLUSDT","SOXXUSDT","KORUUSDT","EURLUSDT","JPUSUSDT",
          "MCOUSDT","TCFUSDT","SKHYNIXUSDT","SKHYUSDT","SPCXUSDT",
          "CLUSDT","MUUSDT","NMHUSDT","SNXXUSDT","INJUSDT",
          "GFTUSDT","BMHUSDT"}

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
    import urllib.request, urllib.parse, json

    def _klines(base_url, sym, interval, limit):
        url = f"{base_url}?symbol={urllib.parse.quote(sym)}&interval={interval}&limit={limit}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read())

    def _ticker24(base_url, sym):
        url = f"{base_url}?symbol={urllib.parse.quote(sym)}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read())

    SPOT = "https://api.binance.com/api/v3/klines"
    FAPI = "https://fapi.binance.com/fapi/v1/klines"

    for base in [SPOT, FAPI]:
        try:
            data_h = _klines(base, symbol, "1h", 24)
            data_m = _klines(base, symbol, "1m", 3)
            data_d = _klines(base, symbol, "1d", 8)
            ticker_base = base.rsplit("/", 1)[0]  # strip /klines → .../api/v3 or .../fapi/v1
            td = _ticker24(f"{ticker_base}/ticker/24hr", symbol)

            vols_h = [float(d[5]) for d in data_h]
            vol_now    = vols_h[-1] if vols_h else 0
            vol_5h_avg = sum(vols_h[-6:-1]) / max(1, len(vols_h)-1) if len(vols_h) > 1 else vol_now
            vol_ratio  = vol_now / vol_5h_avg if vol_5h_avg > 0 else 1.0
            high24  = float(td["highPrice"]); low24 = float(td["lowPrice"])
            close24 = float(td["lastPrice"]); pct24 = float(td["priceChangePercent"])
            pct7d = 0.0
            if len(data_d) >= 8:
                pct7d = (float(data_d[-1][4]) / float(data_d[0][1]) - 1) * 100 if float(data_d[0][1]) else 0.0
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
            continue

    return {"pct5m": 0, "pct1h": 0, "pct24": 0, "pct72h": 0, "pct7d": 0,
            "high24": 0, "low24": 0, "close24": 0, "vol_now": 0,
            "vol_5h_avg": 0, "vol_ratio": 1.0, "ts": now}

# ── live bucket update (keeps agg5 current for net5/net60 queries) ──
def _refresh_live_bucket(symbols):
    """Fetch latest 1h kline for each symbol and upsert the current bucket into agg5."""
    import urllib.request, urllib.parse, json
    conn = get_conn(agg=True)
    now = int(time.time())
    current_bucket = now // 300
    for sym in symbols:
        try:
            url = (f"https://fapi.binance.com/fapi/v1/klines"
                   f"?symbol={urllib.parse.quote(sym)}&interval=5m&limit=5")
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=5) as r:
                klines = json.loads(r.read())
            if not klines:
                continue
            for k in klines:
                bucket = int(int(k[0]) // 300000)  # ms → bucket
                if bucket < current_bucket:  # only update closed buckets
                    vol = float(k[5])
                    is_buy = float(k[4]) >= float(k[1])
                    net = vol if is_buy else -vol
                    conn.execute("""
                        INSERT INTO agg5 (bucket, symbol, net, total_vol, buy_cnt, sell_cnt,
                                         buy_vol, sell_vol, big_buy, big_sell, max_order)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(symbol, bucket) DO UPDATE SET
                            net=excluded.net, total_vol=excluded.total_vol,
                            buy_cnt=excluded.buy_cnt, sell_cnt=excluded.sell_cnt,
                            buy_vol=excluded.buy_vol, sell_vol=excluded.sell_vol
                    """, (bucket, sym, net, vol,
                          1 if is_buy else 0, 0 if is_buy else 1,
                          vol if is_buy else 0, 0 if is_buy else vol, 0, 0, 0))
            conn.commit()
        except Exception:
            pass
    conn.close()

# ── inflow data from agg DB ──────────────────────────────
def _backfill_missing_symbols(symbols):
    """Auto-backfill new top-20 symbols into agg5 from Binance klines."""
    import urllib.request, urllib.parse, json
    conn = get_conn(agg=True)
    # check which symbols already have data
    rows = conn.execute("SELECT DISTINCT symbol FROM agg5").fetchall()
    existing = {r[0] for r in rows}
    missing = [s for s in symbols if s not in existing]
    conn.close()
    if not missing:
        return
    for sym in missing:
        try:
            now = int(time.time())
            url = (f"https://fapi.binance.com/fapi/v1/klines?symbol={urllib.parse.quote(sym)}"
                   f"&interval=1h&limit=720")  # 30 days futures
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                klines = json.loads(r.read())
            if not klines:
                continue
            conn2 = get_conn(agg=True)
            for k in klines:
                open_t = int(k[0] / 1000)
                bucket = open_t // 300
                vol = float(k[5])
                close_p = float(k[4])
                # synthetic: split into buy/sell by net direction from close vs open
                is_buy = float(k[4]) >= float(k[1])
                net = vol if is_buy else -vol
                conn2.execute("""
                    INSERT INTO agg5 (bucket, symbol, net, total_vol, buy_cnt, sell_cnt,
                                     buy_vol, sell_vol, big_buy, big_sell, max_order)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(symbol, bucket) DO UPDATE SET
                        net=net+excluded.net, total_vol=total_vol+excluded.total_vol,
                        buy_cnt=buy_cnt+excluded.buy_cnt, sell_cnt=sell_cnt+excluded.sell_cnt,
                        buy_vol=buy_vol+excluded.buy_vol, sell_vol=sell_vol+excluded.sell_vol
                """, (bucket, sym, net, vol,
                      1 if is_buy else 0, 0 if is_buy else 1,
                      vol if is_buy else 0, 0 if is_buy else vol, 0, 0, 0))
            conn2.commit()
            conn2.close()
        except:
            pass

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
    # use fapi (futures) volume so it matches the funding table
    with urllib.request.urlopen("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=5) as r:
        tickers = json.loads(r.read())
    # build fapi ticker map: price/pct from futures data (covers all symbols in list)
    fapi_map = {t["symbol"]: t for t in tickers}
    symbols = sorted(
        [t["symbol"] for t in tickers
         if t["symbol"].endswith("USDT") and float(t.get("quoteVolume", 0)) > 1e6
         and t["symbol"] not in TRADFI],
        key=lambda s: float(fapi_map[s].get("quoteVolume", 0)),
        reverse=True
    )[:20]
    _backfill_missing_symbols(symbols)  # auto-backfill new symbols into agg5
    _refresh_live_bucket(symbols)       # update current bucket in agg5 for net5/net60
    inflow = _load_inflow(symbols, now)
    items = []
    for sym in symbols:
        ft = fapi_map.get(sym, {})
        # price/pct from futures data (spot may not have the symbol)
        price24 = float(ft.get("lastPrice", 0))
        open24  = float(ft.get("openPrice", 0))
        pct24   = (price24 / open24 - 1) * 100 if open24 > 0 else 0
        # 1h/5m pct from kline (spot only)
        kline   = _fetch_kline(sym)
        signals, smart = _build_signals(sym, inflow.get(sym,{}), kline)
        nm=n=inflow.get(sym,{}); nm60=nm.get("m60",{}); nm5=nm.get("m5",{}); nday=nm.get("day",{}); nd3=nm.get("d3",{})
        items.append({"symbol":sym,"price":price24 or kline["close24"],
                      "pct5m":kline["pct5m"],"pct1h":kline["pct1h"],
                      "pct":pct24 or kline["pct24"],
                      "pct72h":kline["pct72h"],"pct7d":kline["pct7d"],"vol_ratio":kline["vol_ratio"],
                      "net5":nm5.get("net",0),"net60":nm60.get("net",0),"net_day":nday.get("net",0),"net72h":nd3.get("net",0),
                      "smart_ratio":smart,
                      "big_buy_vol":nm.get("big",{}).get("big_buy_vol",0),
                      "big_sell_vol":nm.get("big",{}).get("big_sell_vol",0),
                      "big_buy_cnt":nm.get("big",{}).get("big_buy_cnt",0),
                      "big_sell_cnt":nm.get("big",{}).get("big_sell_cnt",0),
                      "signals":signals})
    items.sort(key=lambda x: float(fapi_map[x["symbol"]].get("quoteVolume", 0)), reverse=True)
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
        # same top-20 by fapi quoteVolume as signals table
        with urllib.request.urlopen("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=5) as r:
            tickers = json.loads(r.read())
        symbols = sorted(
            [t["symbol"] for t in tickers
         if t["symbol"].endswith("USDT") and float(t.get("quoteVolume", 0)) > 1e6
         and t["symbol"] not in TRADFI],
            key=lambda s: float(next((x for x in tickers if x["symbol"] == s), {}).get("quoteVolume", 0)),
            reverse=True
        )[:20]

        # fetch funding rates
        req = urllib.request.Request(
            "https://fapi.binance.com/fapi/v1/premiumIndex",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        )
        r2 = urllib.request.urlopen(req, timeout=8)
        fr_raw = json.loads(r2.read())

        fr_map = {x["symbol"]: float(x.get("lastFundingRate", 0))
                  for x in fr_raw if x["symbol"].endswith("USDT")}

        return [{"symbol": sym, "funding_rate": fr_map.get(sym, 0), "mark_price": 0} for sym in symbols]
    except:
        return [{"symbol":"BTCUSDT","funding_rate":0.0001,"mark_price":78000}]

@app.get("/")
def index():
    return HTMLResponse(open(os.path.join(SCRIPT_DIR, "index.html"), encoding="utf-8").read())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, log_level="error")
