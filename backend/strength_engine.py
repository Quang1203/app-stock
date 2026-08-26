"""
Stock Strength Score Engine
Components: Relative Strength (40%) + Money Flow (35%) + Momentum (25%)
All scores 0–100. Updates every poll cycle (~5s).
"""
import statistics
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Optional

# 720 polls × 5s = 1 hour of intraday history per symbol
_HIST = 720

_LABELS = [
    (90, "Very Strong"),
    (80, "Strong"),
    (70, "Positive"),
    (40, "Neutral"),
    (30, "Weak"),
    (0,  "Very Weak"),
]


@dataclass
class StrengthResult:
    symbol: str
    total:     float = 50.0
    rs_score:  float = 50.0
    mf_score:  float = 50.0
    mom_score: float = 50.0
    # Relative Strength vs market proxy
    rs_5m:  float = 0.0
    rs_15m: float = 0.0
    rs_1h:  float = 0.0
    rs_1d:  float = 0.0
    # Money Flow detail
    vol_ratio:      float = 1.0
    buy_sell_ratio: float = 1.0
    net_buy_pct:    float = 0.0
    # Momentum detail
    mom_5m:  float = 0.0
    mom_15m: float = 0.0
    mom_1h:  float = 0.0
    mom_1d:  float = 0.0
    label: str = "Neutral"
    alert: Optional[str] = None


class StrengthEngine:
    def __init__(self):
        # changePercent history per symbol (relative to session reference price)
        self._chg:  dict[str, deque] = defaultdict(lambda: deque(maxlen=_HIST))
        # Market proxy: equal-weight avg changePercent of all watched stocks
        self._mkt:  deque = deque(maxlen=_HIST)
        # Previous total for alert detection
        self._prev: dict[str, float] = {}
        # Output cache
        self.results: dict[str, StrengthResult] = {}
        # Pending alerts list (consumed by main.py)
        self.pending_alerts: list[dict] = []

    # ------------------------------------------------------------------
    def record_market(self, stocks: dict) -> None:
        """Record equal-weight market proxy from current poll data."""
        vals = [d["changePercent"] for d in stocks.values() if d.get("price", 0) > 0]
        if vals:
            self._mkt.append(statistics.mean(vals))

    def update(self, symbol: str, data: dict) -> StrengthResult:
        price   = data.get("price", 0.0)
        chg_pct = data.get("changePercent", 0.0)
        buy_vol = data.get("matchedBuyVol", 0)
        sell_vol= data.get("matchedSellVol", 0)
        high    = data.get("high", price) or price
        signals = data.get("signals", [])

        if not price:
            return self.results.get(symbol, StrengthResult(symbol=symbol))

        # Update changePercent history
        ch = self._chg[symbol]
        ch.append(chg_pct)
        n = len(ch)

        # ── Relative Strength ──────────────────────────────────────────

        def stock_ret(lookback: int) -> float:
            """Stock return over last `lookback` polls (change in chg_pct)."""
            if n > lookback:
                return ch[-1] - ch[-lookback - 1]
            return ch[-1] - ch[0] if n > 1 else 0.0

        def mkt_ret(lookback: int) -> float:
            mh = self._mkt
            ln = len(mh)
            if ln > lookback:
                return mh[-1] - mh[-lookback - 1]
            return mh[-1] - mh[0] if ln > 1 else 0.0

        P5M = 60; P15M = 180; P1H = 720

        s5m  = stock_ret(P5M);  m5m  = mkt_ret(P5M)
        s15m = stock_ret(P15M); m15m = mkt_ret(P15M)
        s1h  = stock_ret(P1H);  m1h  = mkt_ret(P1H)
        m1d  = self._mkt[-1] if self._mkt else 0.0

        rs_5m  = s5m  - m5m
        rs_15m = s15m - m15m
        rs_1h  = s1h  - m1h
        rs_1d  = chg_pct - m1d

        rs_raw   = rs_5m * 0.15 + rs_15m * 0.20 + rs_1h * 0.25 + rs_1d * 0.40
        rs_score = _clamp(50 + rs_raw * 5)

        # ── Money Flow ─────────────────────────────────────────────────

        # Reuse vol spike ratio already computed by signal engine
        vol_ratio = 1.0
        for sig in signals:
            if sig.get("type") == "VOLUME_SPIKE":
                vol_ratio = max(1.0, sig.get("value", 1.0))
                break

        total_mv = buy_vol + sell_vol
        bs_ratio = buy_vol / sell_vol if sell_vol > 0 else (2.0 if buy_vol > 0 else 1.0)
        net_pct  = (buy_vol - sell_vol) / total_mv * 100 if total_mv > 0 else 0.0

        vol_s = _clamp(50 + (vol_ratio - 1) * 25)    # 1x→50, 2x→75, 3x→100
        bs_s  = _clamp(50 + (bs_ratio - 1) * 20)     # 1→50, 2→70, 3→90
        nb_s  = _clamp(50 + net_pct * 1.5)           # 0%→50, +33%→100
        mf_score = _clamp(vol_s * 0.40 + bs_s * 0.35 + nb_s * 0.25)

        # ── Momentum ──────────────────────────────────────────────────

        accel      = s5m - (s15m / 3 if s15m else 0)   # positive = accelerating
        near_high  = 5.0 if price >= high * 0.998 and chg_pct > 0 else 0.0
        mom_raw    = s5m * 0.20 + s15m * 0.25 + s1h * 0.25 + chg_pct * 0.30
        mom_score  = _clamp(50 + mom_raw * 5 + max(0, accel) * 3 + near_high)

        # ── Final ─────────────────────────────────────────────────────

        total = round(rs_score * 0.40 + mf_score * 0.35 + mom_score * 0.25, 1)
        label = next(lbl for thr, lbl in _LABELS if total >= thr)

        # Alert detection
        prev  = self._prev.get(symbol, total)
        alert: Optional[str] = None
        if total >= 80 and prev < 80:
            alert = f"{symbol} Stock Strength tăng lên {total:.0f}"
            self.pending_alerts.append({"symbol": symbol, "type": "STRENGTH_CROSS_80",
                                        "score": total, "message": alert})
        elif total - prev >= 18 and n >= P15M:
            alert = f"{symbol} Stock Strength tăng +{int(total - prev)} điểm"
            self.pending_alerts.append({"symbol": symbol, "type": "STRENGTH_SURGE",
                                        "score": total, "delta": total - prev,
                                        "message": alert})
        self._prev[symbol] = total

        r = StrengthResult(
            symbol=symbol, total=total, label=label,
            rs_score=round(rs_score, 1), mf_score=round(mf_score, 1), mom_score=round(mom_score, 1),
            rs_5m=round(rs_5m, 2), rs_15m=round(rs_15m, 2), rs_1h=round(rs_1h, 2), rs_1d=round(rs_1d, 2),
            vol_ratio=round(vol_ratio, 2), buy_sell_ratio=round(bs_ratio, 2), net_buy_pct=round(net_pct, 1),
            mom_5m=round(s5m, 2), mom_15m=round(s15m, 2), mom_1h=round(s1h, 2), mom_1d=round(chg_pct, 2),
            alert=alert,
        )
        self.results[symbol] = r
        return r

    def get_ranking(self) -> list[dict]:
        """Returns ranking sorted by total; stock data comes from results cache only.
        Historical (1W/1M) fields are injected by main.py from ohlcv_history_cache.
        """
        return sorted(
            [_to_dict(r) for r in self.results.values()],
            key=lambda x: x["total"], reverse=True,
        )

    def flush_alerts(self) -> list[dict]:
        alerts, self.pending_alerts = self.pending_alerts, []
        return alerts


# ── helpers ───────────────────────────────────────────────────────────────

def _clamp(v: float) -> float:
    return max(0.0, min(100.0, float(v)))


def _to_dict(r: StrengthResult) -> dict:
    return {
        "symbol": r.symbol, "total": r.total, "label": r.label,
        "rs": r.rs_score, "mf": r.mf_score, "mom": r.mom_score,
        "rs_5m": r.rs_5m, "rs_15m": r.rs_15m, "rs_1h": r.rs_1h, "rs_1d": r.rs_1d,
        "vol_ratio": r.vol_ratio, "buy_sell_ratio": r.buy_sell_ratio, "net_buy_pct": r.net_buy_pct,
        "mom_5m": r.mom_5m, "mom_15m": r.mom_15m, "mom_1h": r.mom_1h, "mom_1d": r.mom_1d,
        "alert": r.alert,
    }
