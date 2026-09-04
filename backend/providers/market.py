"""
Market Data Provider – VPS Securities (bgapidatafeed.vps.com.vn)
Truly real-time, no authentication required.
Stock prices are in units of 1,000 VND (nghìn đồng) – multiply × 1000 for full VND.

Historical OHLCV: FireAnt (primary, if token set) → Yahoo Finance (fallback)
"""
import asyncio
import logging
import os
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

VN30_SYMBOLS = [
    "ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR",
    "HDB", "HPG", "MBB", "MSN", "MWG", "NVL", "PDR", "PLX",
    "POW", "SAB", "SSI", "STB", "TCB", "TPB", "VCB", "VHM",
    "VIB", "VIC", "VJC", "VNM", "VPB", "VRE",
]

# Additional 70 symbols to complete VN100 (confirmed on VPS)
VN100_EXTRA_SYMBOLS = [
    "AGG", "ANV", "BAB", "BCG", "BFC", "BMI", "BVS", "BWE",
    "CII", "CMG", "CTD", "DBC", "DCM", "DGC", "DGW", "DIG",
    "DPM", "DRC", "DXG", "DXS", "EVF", "FCN", "GEG", "GEX",
    "GMD", "HAH", "HAR", "HBC", "HCM", "HDC", "HDG", "HHV",
    "HPX", "HSG", "HVN", "IMP", "KBC", "KDC", "KDH", "KLB",
    "LCG", "LPB", "MBS", "MCH", "MSB", "NAB", "NKG", "NLG",
    "NT2", "OCB", "PC1", "PHR", "PNJ", "PVD", "PVT", "QNS",
    "REE", "SBT", "SCR", "SHB", "SHS", "SKG", "SSB", "TDG",
    "TLH", "VCI", "VGC", "VHC", "VIX", "VNS", "VPI",
]

VN100_SYMBOLS = VN30_SYMBOLS + VN100_EXTRA_SYMBOLS

_VN30_QUERY = ",".join(VN30_SYMBOLS)
_BASE = "https://bgapidatafeed.vps.com.vn"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Cache-Control": "no-cache, no-store, max-age=0",
    "Pragma": "no-cache",
    "Origin": "https://dchart.vps.com.vn",
    "Referer": "https://dchart.vps.com.vn/",
}


_YAHOO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}
_YAHOO_CHART = "https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
_YAHOO_SEM = asyncio.Semaphore(5)

_FIREANT_BASE = "https://restv2.fireant.vn"
_FIREANT_TOKEN: str = os.environ.get("FIREANT_TOKEN", "")
_FIREANT_SEM = asyncio.Semaphore(5)
_VPS_STOCK_SEM = asyncio.Semaphore(10)


class MarketProvider:
    def __init__(self):
        self._client = httpx.AsyncClient(timeout=10.0, headers=_HEADERS)
        self._yahoo  = httpx.AsyncClient(timeout=12.0, headers=_YAHOO_HEADERS)
        self._fireant = httpx.AsyncClient(timeout=12.0, headers={
            "Authorization": _FIREANT_TOKEN,
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        self._stock_cache: dict = {}
        if _FIREANT_TOKEN:
            logger.info("FireAnt token loaded – using FireAnt as primary OHLCV source")
        else:
            logger.info("No FireAnt token – using Yahoo Finance for OHLCV")

    async def get_market_indices(self) -> dict:
        """Compute indices from current VN30 stock data (VPS has no index endpoint)."""
        if not self._stock_cache:
            return {}
        return _compute_indices(self._stock_cache)

    async def get_stocks(self, symbols: list[str]) -> dict:
        if not symbols:
            return {}
        query = ",".join(symbols)
        try:
            # The VPS endpoint can otherwise return a CDN snapshot for the
            # same URL, which makes the realtime table appear frozen.
            r = await self._client.get(
                f"{_BASE}/getliststockdata/{query}",
                params={"_": time.time_ns()},
            )
            if r.status_code == 200:
                items = r.json()
                if items:
                    parsed = {
                        item["sym"]: _parse_vps(item)
                        for item in items
                        if item.get("sym")
                    }
                    if parsed:
                            missing = [sym for sym in symbols if sym not in parsed]
                            # VPS occasionally returns a partial batch. Refill
                            # missing symbols instead of replacing a full snapshot
                            # with one incomplete response.
                            if missing and len(parsed) < len(symbols) * 0.8:
                                recovered = await asyncio.gather(
                                    *(self._get_stock_symbol(sym) for sym in missing),
                                    return_exceptions=True,
                                )
                                for sym, item in zip(missing, recovered):
                                    if isinstance(item, dict):
                                        parsed[sym] = item
                            if len(parsed) >= len(symbols) * 0.8:
                                self._stock_cache = parsed
                                return parsed
                            if self._stock_cache:
                                return {sym: self._stock_cache[sym] for sym in symbols if sym in self._stock_cache}
                            return parsed
        except Exception as e:
            logger.warning("VPS fetch error: %s", e)
        # Return stale cache on error so UI stays populated
        return self._stock_cache

        async def _get_stock_symbol(self, symbol: str) -> dict | None:
            async with _VPS_STOCK_SEM:
                try:
                    r = await self._client.get(
                        f"{_BASE}/getliststockdata/{symbol}",
                        params={"_": time.time_ns()},
                    )
                    if r.status_code == 200:
                        items = r.json()
                        if isinstance(items, list):
                            for item in items:
                                if item.get("sym") == symbol:
                                    return _parse_vps(item)
                except Exception as e:
                    logger.debug("VPS single-symbol fetch error %s: %s", symbol, e)
            return None

    async def validate_symbol(self, symbol: str) -> bool:
        """Check if a symbol exists on VPS."""
        try:
            r = await self._client.get(f"{_BASE}/getliststockdata/{symbol.upper()}")
            if r.status_code == 200:
                data = r.json()
                return bool(data) and any(item.get("sym") for item in data)
        except Exception:
            pass
        return False

    async def get_daily_ohlcv(self, symbol: str, months: int = 6) -> dict | None:
        """Convenience alias – kept for backwards compatibility."""
        return await self.get_ohlcv(symbol, interval="1d", range_=f"{months}mo")

    async def get_ohlcv(self, symbol: str, interval: str = "1d", range_: str = "6mo") -> dict | None:
        """Fetch OHLCV from Yahoo Finance for any interval/range combination."""
        yahoo_sym = symbol.upper() + ".VN"
        url = _YAHOO_CHART.format(symbol=yahoo_sym)
        async with _YAHOO_SEM:
            try:
                r = await self._yahoo.get(url, params={"interval": interval, "range": range_})
                if r.status_code != 200:
                    return None
                data = r.json()
                result = data.get("chart", {}).get("result")
                if not result:
                    return None
                res    = result[0]
                quotes = res.get("indicators", {}).get("quote", [{}])[0]
                return {
                    "dates":  res.get("timestamp", []),
                    "open":   quotes.get("open",   []),
                    "high":   quotes.get("high",   []),
                    "low":    quotes.get("low",    []),
                    "close":  quotes.get("close",  []),
                    "volume": quotes.get("volume", []),
                }
            except Exception as e:
                logger.debug("OHLCV fetch error %s [%s/%s]: %s", symbol, interval, range_, e)
                return None

    async def get_all_daily_ohlcv(self, symbols: list[str]) -> dict[str, dict]:
        """Batch fetch daily OHLCV. Uses FireAnt if token available, else Yahoo."""
        if _FIREANT_TOKEN:
            tasks = [self._get_ohlcv_fireant(sym) for sym in symbols]
        else:
            tasks = [self.get_ohlcv(sym) for sym in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out: dict[str, dict] = {}
        for sym, res in zip(symbols, results):
            if isinstance(res, dict) and res.get("close"):
                out[sym] = res
            elif _FIREANT_TOKEN:
                # Fallback to Yahoo for this specific symbol
                fallback = await self.get_ohlcv(sym)
                if isinstance(fallback, dict) and fallback.get("close"):
                    out[sym] = fallback
        return out

    async def _get_ohlcv_fireant(self, symbol: str, days: int = 300) -> dict | None:
        """Fetch daily OHLCV from FireAnt. Prices returned as full VND (×unit)."""
        import datetime
        end   = datetime.date.today().strftime("%Y-%m-%d")
        start = (datetime.date.today() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
        url   = f"{_FIREANT_BASE}/symbols/{symbol}/historical-quotes"
        params = {"startDate": start, "endDate": end, "offset": 0, "limit": days}
        async with _FIREANT_SEM:
            try:
                r = await self._fireant.get(url, params=params)
                if r.status_code != 200:
                    logger.debug("FireAnt %s -> %d", symbol, r.status_code)
                    return None
                rows = r.json()
                if not rows:
                    return None
                # FireAnt returns newest-first; reverse to oldest-first for Ichimoku
                rows = list(reversed(rows))
                unit = float(rows[0].get("unit", 1) or 1)
                return {
                    "dates":  [r["date"] for r in rows],
                    "open":   [float(r["priceOpen"]  or 0) * unit for r in rows],
                    "high":   [float(r["priceHigh"]  or 0) * unit for r in rows],
                    "low":    [float(r["priceLow"]   or 0) * unit for r in rows],
                    "close":  [float(r["priceClose"] or 0) * unit for r in rows],
                    "volume": [int(r["totalVolume"]  or 0)        for r in rows],
                    "source": "fireant",
                }
            except Exception as e:
                logger.debug("FireAnt fetch error %s: %s", symbol, e)
                return None

    async def close(self):
        await self._client.aclose()
        await self._yahoo.aclose()


# ── VPS field mapping ──────────────────────────────────────────────────────

def _parse_vps(item: dict) -> dict:
    sym = item.get("sym", "")
    # VPS prices are in thousands of VND → ×1000 for full VND
    price = float(item.get("lastPrice", 0) or 0) * 1000
    ref   = float(item.get("r", 0) or 0) * 1000
    ceil_ = float(item.get("c", 0) or 0) * 1000
    fl_   = float(item.get("f", 0) or 0) * 1000
    high  = float(item.get("highPrice", 0) or 0) * 1000
    low   = float(item.get("lowPrice", 0) or 0) * 1000

    # changePc can be negative string like "-1.21" or positive "0.23"
    change_pct = float(item.get("changePc", 0) or 0)
    # VPS changePc is already a percentage; sign depends on direction
    # Determine sign from price vs reference
    if ref > 0 and price > 0:
        change_pct = round((price - ref) / ref * 100, 2)
    change = round(price - ref, 1)

    # Volume: VPS `lot` field (unit is 10 shares for HOSE)
    vol = int(item.get("lot", 0) or 0) * 10

    # Order book (g1-g3 = bids, g4-g6 = asks)
    bids = [_parse_ob(item.get(f"g{i}", "")) for i in range(1, 4)]
    asks = [_parse_ob(item.get(f"g{i}", "")) for i in range(4, 7)]
    bid_vol = sum(v for _, v in bids if v > 0)
    ask_vol = sum(v for _, v in asks if v > 0)

    # Foreign trading
    f_buy  = int(float(item.get("fBVol", 0) or 0)) * 10
    f_sell = int(float(item.get("fSVolume", 0) or 0)) * 10

    return {
        "symbol": sym,
        "price": price,
        "referencePrice": ref,
        "change": change,
        "changePercent": change_pct,
        "volume": vol,
        "buyVolume": bid_vol * 10,   # bid order vol (order book)
        "sellVolume": ask_vol * 10,  # ask order vol (order book)
        "foreignBuyVol": f_buy,
        "foreignSellVol": f_sell,
        "high": high or price,
        "low": low or price,
        "ceiling": ceil_,
        "floor": fl_,
        "openPrice": float(item.get("openPrice", 0) or 0) * 1000,
        "avgPrice": float(item.get("avePrice", 0) or 0) * 1000,
        "lastSide": item.get("side", ""),
        # Raw order book levels for display
        "bids": [{"price": p * 1000, "volume": v * 10} for p, v in bids if v > 0],
        "asks": [{"price": p * 1000, "volume": v * 10} for p, v in asks if v > 0],
    }


def _parse_ob(raw: str) -> tuple:
    """Parse 'price|volume|flag' string → (price_float, volume_int)."""
    try:
        parts = raw.split("|")
        if len(parts) >= 2:
            return float(parts[0] or 0), int(parts[1] or 0)
    except Exception:
        pass
    return 0.0, 0


def _compute_indices(stocks: dict) -> dict:
    """Compute simple index stats from VN30 component stocks."""
    valid = [v for v in stocks.values() if v.get("price", 0) > 0]
    if not valid:
        return {}
    adv = sum(1 for v in valid if v.get("changePercent", 0) > 0.05)
    dec = sum(1 for v in valid if v.get("changePercent", 0) < -0.05)
    unc = len(valid) - adv - dec
    total_vol = sum(v.get("volume", 0) for v in valid)

    # Weighted-avg change (equal weight as approximation)
    avg_pct = sum(v.get("changePercent", 0) for v in valid) / len(valid)

    return {
        "VN30": {
            "value": 0,          # not computable without index weights
            "change": 0,
            "changePercent": round(avg_pct, 2),
            "volume": total_vol,
            "valueTraded": 0,
            "advances": adv,
            "declines": dec,
            "unchanged": unc,
            "approx": True,
        }
    }
