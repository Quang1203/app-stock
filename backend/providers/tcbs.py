"""
TCBS Market Data Provider (free, no auth required)
Endpoints documented by the vnstock community.
"""
import asyncio
import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

VN30_SYMBOLS = [
    "ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR",
    "HDB", "HPG", "MBB", "MSN", "MWG", "NVL", "PDR", "PLX",
    "POW", "SAB", "SSI", "STB", "TCB", "TPB", "VCB", "VHM",
    "VIB", "VIC", "VJC", "VNM", "VPB", "VRE",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://tcinvest.tcbs.com.vn/",
}


class TCBSProvider:
    BASE = "https://apipubaws.tcbs.com.vn"
    BACKUP_BASE = "https://iboard-query.ssi.com.vn"

    def __init__(self):
        self._client = httpx.AsyncClient(timeout=12.0, headers=HEADERS)

    # ------------------------------------------------------------------
    # Market indices
    # ------------------------------------------------------------------
    async def get_market_indices(self) -> dict:
        try:
            url = f"{self.BASE}/market-monitor/v1/market/market-index"
            r = await self._client.get(url)
            if r.status_code == 200:
                return _parse_indices(r.json())
        except Exception as e:
            logger.warning("TCBS index fetch failed: %s", e)
        # Fallback – SSI board-index
        try:
            url = f"{self.BACKUP_BASE}/v2/stock/board-index"
            r = await self._client.get(url)
            if r.status_code == 200:
                return _parse_ssi_indices(r.json())
        except Exception as e:
            logger.warning("SSI index fetch failed: %s", e)
        return {}

    # ------------------------------------------------------------------
    # Stock prices
    # ------------------------------------------------------------------
    async def get_vn30_stocks(self) -> dict:
        # Try batch endpoint first
        try:
            url = f"{self.BASE}/market-monitor/v1/stock/ticker-list"
            params = {"watchingGroup": "VN30", "types": "stock"}
            r = await self._client.get(url, params=params)
            if r.status_code == 200:
                parsed = _parse_ticker_list(r.json())
                if parsed:
                    return parsed
        except Exception as e:
            logger.warning("Batch fetch failed: %s – falling back to individual", e)

        # Fallback: individual requests (parallel)
        tasks = [self._fetch_single(sym) for sym in VN30_SYMBOLS]
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        return {
            sym: res
            for sym, res in zip(VN30_SYMBOLS, results_list)
            if isinstance(res, dict) and res.get("price", 0) > 0
        }

    async def _fetch_single(self, ticker: str) -> dict:
        try:
            url = f"{self.BASE}/stock-insight/v1/stock/ticker-price"
            r = await self._client.get(url, params={"ticker": ticker, "type": "stock"})
            if r.status_code == 200:
                return _parse_single(ticker, r.json())
        except Exception:
            pass
        # Fallback to tcanalysis endpoint
        try:
            url = f"{self.BASE}/tcanalysis/v1/ticker/{ticker}/overview"
            r = await self._client.get(url)
            if r.status_code == 200:
                return _parse_overview(ticker, r.json())
        except Exception:
            pass
        return _empty_stock(ticker)

    async def close(self):
        await self._client.aclose()


# ------------------------------------------------------------------
# Parsing helpers
# ------------------------------------------------------------------

def _parse_indices(data) -> dict:
    result: dict = {}
    items = data if isinstance(data, list) else data.get("data", [])
    for item in items:
        code = (
            item.get("indexId")
            or item.get("code")
            or item.get("index", "")
        ).upper()
        if not code:
            continue
        result[code] = {
            "value": float(item.get("indexValue", item.get("currentValue", 0)) or 0),
            "change": float(item.get("change", 0) or 0),
            "changePercent": float(item.get("percentChange", item.get("changePercent", 0)) or 0),
            "volume": int(item.get("totalVolume", item.get("volume", 0)) or 0),
            "valueTraded": float(item.get("totalValue", item.get("valueTraded", 0)) or 0),
            "advances": int(item.get("advances", item.get("noIncrease", 0)) or 0),
            "declines": int(item.get("declines", item.get("noDecrease", 0)) or 0),
            "unchanged": int(item.get("unchanged", item.get("noUnchange", 0)) or 0),
        }
    return result


def _parse_ssi_indices(data) -> dict:
    result: dict = {}
    items = data.get("data", data) if isinstance(data, dict) else data
    if not isinstance(items, list):
        return result
    for item in items:
        code = (item.get("indexCode") or item.get("code") or "").upper()
        if not code:
            continue
        result[code] = {
            "value": float(item.get("indexValue", 0) or 0),
            "change": float(item.get("change", 0) or 0),
            "changePercent": float(item.get("percentChange", 0) or 0),
            "volume": int(item.get("totalVolume", 0) or 0),
            "valueTraded": float(item.get("totalValue", 0) or 0),
            "advances": int(item.get("advances", 0) or 0),
            "declines": int(item.get("declines", 0) or 0),
            "unchanged": int(item.get("unchanged", 0) or 0),
        }
    return result


def _parse_ticker_list(data) -> dict:
    items = data if isinstance(data, list) else data.get("data", data.get("items", []))
    if not isinstance(items, list):
        return {}
    result: dict = {}
    for item in items:
        sym = (item.get("ticker") or item.get("symbol") or "").upper()
        if not sym:
            continue
        price = _f(item, "lastPrice", "price", "closePrice")
        ref = _f(item, "referencePrice", "refPrice") or price
        result[sym] = {
            "symbol": sym,
            "price": price,
            "referencePrice": ref,
            "change": round(price - ref, 2),
            "changePercent": round((price - ref) / ref * 100, 2) if ref else 0.0,
            "volume": _i(item, "totalVolume", "volume", "matchedVolume"),
            "buyVolume": _i(item, "buyVolume", "totalBuyVolume", "accumulatedBuyVolume"),
            "sellVolume": _i(item, "sellVolume", "totalSellVolume", "accumulatedSellVolume"),
            "high": _f(item, "highPrice", "high") or price,
            "low": _f(item, "lowPrice", "low") or price,
            "ceiling": _f(item, "ceilingPrice", "ceiling"),
            "floor": _f(item, "floorPrice", "floor"),
            "openPrice": _f(item, "openPrice", "open"),
        }
    return result


def _parse_single(ticker: str, data) -> dict:
    d = data if isinstance(data, dict) else {}
    price = _f(d, "lastPrice", "price", "closePrice")
    ref = _f(d, "referencePrice", "refPrice") or price
    return {
        "symbol": ticker,
        "price": price,
        "referencePrice": ref,
        "change": round(price - ref, 2),
        "changePercent": round((price - ref) / ref * 100, 2) if ref else 0.0,
        "volume": _i(d, "totalVolume", "volume"),
        "buyVolume": _i(d, "buyVolume", "totalBuyVolume"),
        "sellVolume": _i(d, "sellVolume", "totalSellVolume"),
        "high": _f(d, "highPrice", "high") or price,
        "low": _f(d, "lowPrice", "low") or price,
        "ceiling": _f(d, "ceilingPrice", "ceiling"),
        "floor": _f(d, "floorPrice", "floor"),
        "openPrice": _f(d, "openPrice", "open"),
    }


def _parse_overview(ticker: str, data) -> dict:
    d = data if isinstance(data, dict) else {}
    price = _f(d, "price", "lastPrice")
    ref = _f(d, "referencePrice") or price
    return {
        "symbol": ticker,
        "price": price,
        "referencePrice": ref,
        "change": round(price - ref, 2),
        "changePercent": round((price - ref) / ref * 100, 2) if ref else 0.0,
        "volume": _i(d, "volume", "totalVolume"),
        "buyVolume": 0,
        "sellVolume": 0,
        "high": _f(d, "high", "highPrice") or price,
        "low": _f(d, "low", "lowPrice") or price,
        "ceiling": _f(d, "ceiling", "ceilingPrice"),
        "floor": _f(d, "floor", "floorPrice"),
        "openPrice": _f(d, "open", "openPrice"),
    }


def _empty_stock(ticker: str) -> dict:
    return {
        "symbol": ticker, "price": 0, "referencePrice": 0,
        "change": 0, "changePercent": 0, "volume": 0,
        "buyVolume": 0, "sellVolume": 0,
        "high": 0, "low": 0, "ceiling": 0, "floor": 0, "openPrice": 0,
    }


def _f(d: dict, *keys) -> float:
    for k in keys:
        v = d.get(k)
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                pass
    return 0.0


def _i(d: dict, *keys) -> int:
    for k in keys:
        v = d.get(k)
        if v is not None:
            try:
                return int(float(v))
            except (ValueError, TypeError):
                pass
    return 0
