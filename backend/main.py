"""
VN Stock Real-time Market Monitor – Backend
FastAPI + WebSocket, polls VPS every POLL_INTERVAL seconds.
"""
import asyncio
import datetime
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

# Load .env before importing providers (so FIREANT_TOKEN is available)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass  # dotenv optional – token can be set via env var directly

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from providers.market import MarketProvider, VN30_SYMBOLS, VN100_SYMBOLS
from signal_engine import SignalEngine
from strength_engine import StrengthEngine
import ichimoku as ichi

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(name)s – %(message)s",
)
logger = logging.getLogger("main")

POLL_INTERVAL = 5  # seconds
VN_TZ = datetime.timezone(datetime.timedelta(hours=7))

provider = MarketProvider()
engine   = SignalEngine()
strength = StrengthEngine()

# ---------------------------------------------------------------------------
# Watchlist – persisted to watchlist.json
# ---------------------------------------------------------------------------

_WATCHLIST_FILE = Path(__file__).parent / "watchlist.json"

def _load_watchlist() -> list[str]:
    try:
        return json.loads(_WATCHLIST_FILE.read_text(encoding="utf-8"))
    except Exception:
        return list(VN30_SYMBOLS)

def _save_watchlist(symbols: list[str]) -> None:
    _WATCHLIST_FILE.write_text(json.dumps(symbols, ensure_ascii=False, indent=2), encoding="utf-8")

watchlist: list[str] = _load_watchlist()

# Ichimoku: separate concerns
#   ohlcv_history_cache – historical daily OHLCV (N-1 days), refreshed once/day from Yahoo
#   ichimoku_cache      – latest computed levels, recalculated each poll from VPS today's candle
ohlcv_history_cache: dict[str, dict] = {}
ichimoku_cache: dict[str, dict] = {}
_ichi_tf_cache: dict[str, dict] = {}  # {"{sym}:{tf}": {"data": ..., "ts": datetime}}

# Shared in-memory state (single process, fine for MVP)
state: dict = {
    "indices": {},
    "stocks": {},
    "signals": [],
    "breadth": {},
    "market_status": "UNKNOWN",
    "last_update": None,
    "watchlist": watchlist,
    "ichimoku_updated": None,
    "strength": [],
}

connected: list[WebSocket] = []


def _now_vn() -> datetime.datetime:
    return datetime.datetime.now(VN_TZ)


# ---------------------------------------------------------------------------
# Market session helper
# ---------------------------------------------------------------------------

def _market_status() -> str:
    now = _now_vn()
    if now.weekday() >= 5:
        return "HOLIDAY"
    h, m = now.hour, now.minute
    total = h * 60 + m
    if total < 9 * 60:
        return "PRE_OPEN"
    if total < 9 * 60 + 15:
        return "ATO"
    if total < 11 * 60 + 30:
        return "OPEN"
    if total < 13 * 60:
        return "BREAK"
    if total < 14 * 60 + 30:
        return "OPEN"
    if total < 14 * 60 + 45:
        return "ATC"
    if total < 15 * 60:
        return "CLOSE"
    return "AFTER_MARKET"


# ---------------------------------------------------------------------------
# Broadcast helpers
# ---------------------------------------------------------------------------

async def _broadcast(message: dict) -> None:
    dead: list[WebSocket] = []
    payload = json.dumps(message, ensure_ascii=False)
    for ws in list(connected):
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        if ws in connected:
            connected.remove(ws)


# ---------------------------------------------------------------------------
# Background polling loop
# ---------------------------------------------------------------------------

async def _poll_loop() -> None:
    while True:
        try:
            async with asyncio.timeout(25):   # skip cycle if stuck > 25s
                indices, raw_stocks = await asyncio.gather(
                    provider.get_market_indices(),
                    provider.get_stocks(watchlist),
                )

                processed: dict = {}
                for sym, data in raw_stocks.items():
                    prev = state["stocks"].get(sym, {})
                    signals = engine.process(sym, data, prev)
                    data["signals"] = signals
                    data["score"] = engine.calculate_score(signals)
                    st = engine._states[sym]
                    data["matchedBuyVol"]  = st.matched_buy
                    data["matchedSellVol"] = st.matched_sell
                    if sym in ohlcv_history_cache:
                        hist = ohlcv_history_cache[sym]
                        highs  = hist["high"]  + [data.get("high")  or data["price"]]
                        lows   = hist["low"]   + [data.get("low")   or data["price"]]
                        closes = hist["close"] + [data["price"]]
                        result = ichi.calculate(highs, lows, closes)
                        if result:
                            ichimoku_cache[sym] = result
                            data["ichimoku"] = result
                    elif sym in ichimoku_cache:
                        data["ichimoku"] = ichimoku_cache[sym]
                    processed[sym] = data

                strength.record_market(processed)
                for sym, data in processed.items():
                    sr = strength.update(sym, data)
                    data["strength"] = {
                        "total": sr.total, "label": sr.label,
                        "rs": sr.rs_score, "mf": sr.mf_score, "mom": sr.mom_score,
                        "rs_5m": sr.rs_5m, "rs_15m": sr.rs_15m,
                        "rs_1h": sr.rs_1h, "rs_1d": sr.rs_1d,
                        "vol_ratio": sr.vol_ratio,
                        "buy_sell_ratio": sr.buy_sell_ratio,
                        "net_buy_pct": sr.net_buy_pct,
                        "mom_5m": sr.mom_5m, "mom_15m": sr.mom_15m,
                        "mom_1h": sr.mom_1h, "mom_1d": sr.mom_1d,
                    }

                _DAY_WINDOWS = [(5, "ret_1w"), (20, "ret_1m")]
                for sym, data in processed.items():
                    price = data.get("price", 0.0)
                    if sym in ohlcv_history_cache and price > 0:
                        day_closes = ohlcv_history_cache[sym].get("close", [])
                        for days, key in _DAY_WINDOWS:
                            if len(day_closes) >= days:
                                c = day_closes[-days]
                                if c and c > 0:
                                    data["strength"][key] = round((price - c) / c * 100, 2)

                import statistics as _stats
                _mkt_hist: dict[str, list] = {"ret_1w": [], "ret_1m": []}
                for data in processed.values():
                    st_d = data.get("strength", {})
                    for key in _mkt_hist:
                        if key in st_d:
                            _mkt_hist[key].append(st_d[key])
                _mkt_avg = {k: round(_stats.mean(v), 2) for k, v in _mkt_hist.items() if v}
                for data in processed.values():
                    st_d = data.get("strength", {})
                    for stock_key, mkt_key, rs_key in [
                        ("ret_1w", "ret_1w", "rs_1w"),
                        ("ret_1m", "ret_1m", "rs_1m"),
                    ]:
                        if stock_key in st_d and mkt_key in _mkt_avg:
                            st_d[rs_key] = round(st_d[stock_key] - _mkt_avg[mkt_key], 2)
                            st_d["mkt_" + mkt_key] = _mkt_avg[mkt_key]

                strength_alerts = strength.flush_alerts()
                top_signals = engine.get_top_signals(processed)
                breadth = engine.get_market_breadth(processed)
                top_strength = strength.get_ranking()[:10]

                state.update(
                    indices=indices, stocks=processed, signals=top_signals,
                    breadth=breadth, market_status=_market_status(),
                    last_update=_now_vn().isoformat(timespec="milliseconds"),
                    watchlist=list(watchlist), strength=top_strength,
                )
                await _broadcast({
                    "type": "market_update",
                    "indices": indices, "stocks": processed,
                    "signals": top_signals, "breadth": breadth,
                    "market_status": state["market_status"],
                    "last_update": state["last_update"],
                    "watchlist": list(watchlist),
                    "strength": top_strength,
                    "strength_alerts": strength_alerts,
                })

        except asyncio.TimeoutError:
            logger.warning("Poll cycle timed out (>25s) – skipping this cycle")
        except Exception as exc:
            logger.error("Poll error: %s", exc, exc_info=True)

        await asyncio.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# App lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    poll_task  = asyncio.create_task(_poll_loop())
    ichi_task  = asyncio.create_task(_ichimoku_loop())
    logger.info("Market data polling started (interval=%ds)", POLL_INTERVAL)
    yield
    poll_task.cancel()
    ichi_task.cancel()
    await provider.close()


# ---------------------------------------------------------------------------
# Ichimoku background loop – refreshes OHLCV history once per day
# ---------------------------------------------------------------------------

ICHIMOKU_INTERVAL = 86400  # refresh history once per day; intraday calc is inline

async def _ichimoku_loop() -> None:
    await asyncio.sleep(5)   # let server fully start first
    while True:
        await _refresh_ohlcv_history()
        await asyncio.sleep(ICHIMOKU_INTERVAL)


async def _refresh_ohlcv_history(symbols: list[str] | None = None) -> None:
    """Fetch N-1 days of daily OHLCV from Yahoo and cache without today's row.
    Today's candle is provided in real-time by VPS in the poll loop.
    """
    syms = symbols or list(watchlist)
    logger.info("Fetching OHLCV history for %d symbols …", len(syms))
    ohlcv_map = await provider.get_all_daily_ohlcv(syms)
    updated = 0
    for sym, ohlcv in ohlcv_map.items():
        closes = ohlcv.get("close", [])
        if len(closes) < 2:
            continue
        # Strip the last row (today, potentially stale 15-min-delayed from Yahoo)
        # VPS will provide the real-time today's candle during polling
        ohlcv_history_cache[sym] = {
            k: v[:-1] for k, v in ohlcv.items()
        }
        updated += 1
    now = _now_vn().isoformat(timespec="seconds")
    state["ichimoku_updated"] = now
    logger.info("OHLCV history cached for %d/%d symbols at %s – Ichimoku now recalculates each poll from VPS real-time",
                updated, len(syms), now)


# Keep alias so add/remove watchlist endpoints can trigger a targeted refresh
_refresh_ichimoku = _refresh_ohlcv_history


def _resample_monthly(ohlcv: dict) -> dict:
    """Resample daily OHLCV → monthly candles (OHLC + sum volume)."""
    from collections import defaultdict
    import datetime as dt
    buckets: dict = defaultdict(lambda: {"high": [], "low": [], "open": None, "close": None, "volume": 0})
    for i, date_str in enumerate(ohlcv["dates"]):
        try:
            d = dt.datetime.fromisoformat(str(date_str))
            key = (d.year, d.month)
        except Exception:
            continue
        b = buckets[key]
        h = ohlcv["high"][i]
        l = ohlcv["low"][i]
        c = ohlcv["close"][i]
        v = ohlcv["volume"][i] if ohlcv["volume"] else 0
        if h: b["high"].append(h)
        if l: b["low"].append(l)
        if b["open"] is None and c: b["open"] = c
        if c: b["close"] = c
        b["volume"] += v or 0
    keys = sorted(buckets)
    return {
        "dates":  [f"{y}-{m:02d}-01" for y, m in keys],
        "open":   [buckets[k]["open"]            for k in keys],
        "high":   [max(buckets[k]["high"]) if buckets[k]["high"] else None for k in keys],
        "low":    [min(buckets[k]["low"])  if buckets[k]["low"]  else None for k in keys],
        "close":  [buckets[k]["close"]           for k in keys],
        "volume": [buckets[k]["volume"]           for k in keys],
    }


app = FastAPI(title="VN Stock Monitor API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten this to your Vercel domain in production
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
    allow_credentials=False,
)


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------

@app.get("/api/state")
async def get_state():
    return state


@app.get("/api/indices")
async def get_indices():
    return state["indices"]


@app.get("/api/stocks")
async def get_stocks():
    return state["stocks"]


@app.get("/api/signals")
async def get_signals():
    return state["signals"]


@app.get("/api/breadth")
async def get_breadth():
    return state["breadth"]


@app.get("/api/strength")
async def get_strength():
    return strength.get_ranking()


@app.get("/api/strength/{symbol}")
async def get_strength_symbol(symbol: str):
    sym = symbol.upper()
    r = strength.results.get(sym)
    if not r:
        raise HTTPException(404, f"Strength data not available for {sym}")
    from strength_engine import _to_dict
    return _to_dict(r)


@app.get("/api/ichimoku")
async def get_ichimoku():
    return ichimoku_cache


@app.get("/api/ichimoku/{symbol}/{timeframe}")
async def get_ichimoku_tf(symbol: str, timeframe: str):
    """Return Ichimoku levels for a given symbol and timeframe.
    timeframe: 1h | 1d | 1mo
    1d uses the real-time cache (VPS today + Yahoo history).
    1h / 1mo are fetched from Yahoo on demand and cached for 10 min.
    """
    sym = symbol.upper()
    tf  = timeframe.lower()

    _TF_CONFIG = {
        "1h":  ("1h",  "60d"),
        "1d":  ("1d",  None),   # served from real-time cache
        "1mo": ("1mo", "7y"),
    }
    if tf not in _TF_CONFIG:
        raise HTTPException(400, "timeframe must be 1h, 1d or 1mo")

    # Daily: serve from real-time cache (most up-to-date)
    if tf == "1d":
        if sym in ichimoku_cache:
            return {"symbol": sym, "timeframe": "1d", **ichimoku_cache[sym]}
        # Check if symbol exists in history cache (Yahoo might not have it)
        if sym not in ohlcv_history_cache:
            raise HTTPException(404, f"Không có dữ liệu lịch s\u1eed cho {sym} (Yahoo Finance kh\u00f4ng h\u1ed7 tr\u1ee3 m\u00e3 n\u00e0y)")
        raise HTTPException(503, f"Ichimoku {sym} \u0111ang t\u00ednh to\u00e1n, vui l\u00f2ng th\u1eed l\u1ea1i sau v\u00e0i gi\u00e2y")

    # Hourly / Monthly: fetch from Yahoo (or FireAnt for daily timeframes)
    cache_key = f"{sym}:{tf}"
    cached = _ichi_tf_cache.get(cache_key)
    if cached and (_now_vn() - cached["ts"]).seconds < 600:
        return {"symbol": sym, "timeframe": tf, **cached["data"]}

    interval, range_ = _TF_CONFIG[tf]

    if tf == "1mo":
        # Monthly: FireAnt daily → resample, fallback to Yahoo monthly
        from providers.market import _FIREANT_TOKEN
        if _FIREANT_TOKEN:
            ohlcv = await provider._get_ohlcv_fireant(sym, days=2500)
            if ohlcv and ohlcv.get("close"):
                ohlcv = _resample_monthly(ohlcv)
            else:
                ohlcv = await provider.get_ohlcv(sym, interval=interval, range_=range_)
        else:
            ohlcv = await provider.get_ohlcv(sym, interval=interval, range_=range_)
    elif tf == "1h":
        # Hourly: try Yahoo 1H first, fallback to FireAnt daily as proxy
        from providers.market import _FIREANT_TOKEN as _fa_token
        ohlcv = await provider.get_ohlcv(sym, interval="1h", range_="60d")
        daily_fallback = False
        if not ohlcv or not ohlcv.get("close"):
            if sym in ohlcv_history_cache:
                # Take last 120 daily candles as proxy (Ichimoku needs ≥78)
                h = ohlcv_history_cache[sym]
                ohlcv = {k: v[-120:] for k, v in h.items()}
                daily_fallback = True
            elif _fa_token:
                ohlcv = await provider._get_ohlcv_fireant(sym, days=60)
                daily_fallback = bool(ohlcv and ohlcv.get("close"))
        if not ohlcv or not ohlcv.get("close"):
            raise HTTPException(404, f"Không có dữ liệu lịch sử cho {sym} (không hỗ trợ khung 1H)")
        result = ichi.calculate(ohlcv["high"], ohlcv["low"], ohlcv["close"])
        if not result:
            raise HTTPException(503, f"Không đủ dữ liệu Ichimoku 1H cho {sym}")
        if daily_fallback:
            result["note"]      = "daily_proxy"
            result["note_text"] = "Không có dữ liệu theo giờ – hiển thị dữ liệu theo ngày (60 phiên gần nhất)"
        _ichi_tf_cache[f"{sym}:{tf}"] = {"data": result, "ts": _now_vn()}
        return {"symbol": sym, "timeframe": tf, **result}
    else:
        ohlcv = await provider.get_ohlcv(sym, interval=interval, range_=range_)
    if not ohlcv or not ohlcv.get("close"):
        raise HTTPException(503, f"Could not fetch OHLCV for {sym} [{tf}]")

    result = ichi.calculate(ohlcv["high"], ohlcv["low"], ohlcv["close"])
    if not result:
        raise HTTPException(503, f"Not enough data for Ichimoku [{tf}] ({len(ohlcv.get('close',[]))} bars)")

    _ichi_tf_cache[cache_key] = {"data": result, "ts": _now_vn()}
    return {"symbol": sym, "timeframe": tf, **result}


@app.get("/api/ichimoku/{symbol}")
async def get_ichimoku_symbol(symbol: str):
    sym = symbol.upper()
    if sym not in ichimoku_cache:
        raise HTTPException(404, f"Ichimoku not yet calculated for {sym}")
    return ichimoku_cache[sym]


@app.post("/api/ichimoku/refresh")
async def refresh_ichimoku():
    asyncio.create_task(_refresh_ohlcv_history())
    return {"status": "refresh started"}


# ---------------------------------------------------------------------------
# Watchlist endpoints
# ---------------------------------------------------------------------------

@app.get("/api/watchlist")
async def get_watchlist():
    return {"symbols": watchlist}


@app.post("/api/watchlist/preset/{name}")
async def load_preset(name: str):
    """Bulk-add a preset symbol list. Currently supports: vn30, vn100."""
    presets = {"vn30": VN30_SYMBOLS, "vn100": VN100_SYMBOLS}
    name = name.lower()
    if name not in presets:
        raise HTTPException(400, f"Unknown preset. Available: {list(presets)}")
    preset = presets[name]
    added, skipped = [], []
    for sym in preset:
        if sym not in watchlist:
            watchlist.append(sym)
            added.append(sym)
        else:
            skipped.append(sym)
    if added:
        _save_watchlist(watchlist)
        state["watchlist"] = list(watchlist)
        asyncio.create_task(_refresh_ichimoku([s for s in added]))
        await _broadcast({"type": "watchlist_update", "watchlist": list(watchlist)})
    logger.info("Preset %s: added %d, skipped %d", name, len(added), len(skipped))
    return {"added": len(added), "skipped": len(skipped), "total": len(watchlist), "symbols": watchlist}


@app.post("/api/watchlist/{symbol}")
async def add_symbol(symbol: str):
    sym = symbol.upper().strip()
    if not sym.isalpha() or len(sym) > 10:
        raise HTTPException(400, "Invalid symbol format")
    if sym in watchlist:
        return {"symbols": watchlist, "message": f"{sym} already in watchlist"}
    # Validate against VPS before adding
    valid = await provider.validate_symbol(sym)
    if not valid:
        raise HTTPException(404, f"Symbol {sym} not found on VPS")
    watchlist.append(sym)
    _save_watchlist(watchlist)
    state["watchlist"] = list(watchlist)
    logger.info("Added %s to watchlist (total=%d)", sym, len(watchlist))
    # Fetch Ichimoku for the new symbol in background
    asyncio.create_task(_refresh_ichimoku([sym]))
    # Broadcast watchlist change immediately
    await _broadcast({"type": "watchlist_update", "watchlist": list(watchlist)})
    return {"symbols": watchlist}


@app.delete("/api/watchlist/{symbol}")
async def remove_symbol(symbol: str):
    sym = symbol.upper().strip()
    if sym not in watchlist:
        raise HTTPException(404, f"{sym} not in watchlist")
    if len(watchlist) <= 1:
        raise HTTPException(400, "Cannot remove the last symbol")
    watchlist.remove(sym)
    # Clean up stale stock state
    state["stocks"].pop(sym, None)
    _save_watchlist(watchlist)
    state["watchlist"] = list(watchlist)
    logger.info("Removed %s from watchlist (total=%d)", sym, len(watchlist))
    await _broadcast({"type": "watchlist_update", "watchlist": list(watchlist)})
    return {"symbols": watchlist}


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "last_update": state["last_update"],
        "market_status": state["market_status"],
        "connected_clients": len(connected),
        "stocks_loaded": len(state["stocks"]),
    }


# ---------------------------------------------------------------------------
# WebSocket endpoint
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected.append(websocket)
    client = websocket.client
    logger.info("Client connected: %s  (total=%d)", client, len(connected))

    # Send current state immediately on connect
    try:
        await websocket.send_text(json.dumps({
            "type": "initial",
            **{k: state[k] for k in
               ("indices", "stocks", "signals", "breadth", "market_status",
                "last_update", "watchlist", "strength")},
        }, ensure_ascii=False))
    except Exception:
        pass

    try:
        while True:
            await websocket.receive_text()   # keep-alive; client can send pings
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in connected:
            connected.remove(websocket)
        logger.info("Client disconnected: %s  (total=%d)", client, len(connected))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
