"""
Signal Engine – detects anomalies and computes Signal Score.
Tracks incremental volume (ΔVol) between poll cycles to approximate
intraday 5-min volume bursts without needing a dedicated tick feed.
"""
import statistics
from collections import defaultdict, deque
from dataclasses import dataclass, field


@dataclass
class SymbolState:
    delta_vol: deque = field(default_factory=lambda: deque(maxlen=60))
    baseline_delta_vols: deque = field(default_factory=lambda: deque(maxlen=200))
    last_volume: int = 0
    last_price: float = 0.0
    # Matched buy/sell volume accumulated each poll (price-direction method)
    matched_buy: int = 0   # total session estimated buy-initiated volume
    matched_sell: int = 0  # total session estimated sell-initiated volume
    matched_buy_history: deque = field(default_factory=lambda: deque(maxlen=120))
    matched_sell_history: deque = field(default_factory=lambda: deque(maxlen=120))
    ratio_history: deque = field(default_factory=lambda: deque(maxlen=120))


# Threshold config (would be loaded from DB in full version)
THRESHOLDS = {
    "volume_spike": {"warning": 2.0, "high": 3.0, "critical": 5.0},
    "price_change_pct": {"warning": 1.5, "high": 3.0, "critical": 5.0},
    "buy_sell_ratio": {"warning": 2.0, "high": 3.0, "critical": 5.0},
    "large_trade_value_m": 500,   # VND million
}

SCORE_WEIGHTS = {
    "VOLUME_SPIKE": {"base": 35, "per_x": 4},
    "PRICE_SURGE": {"base": 30, "per_pct": 5},
    "PRICE_DROP": {"base": 30, "per_pct": 5},
    "STRONG_BUY_PRESSURE": {"base": 20, "per_ratio": 4},
    "STRONG_SELL_PRESSURE": {"base": 20, "per_ratio": 4},
    "CEILING_HIT": {"base": 15},
    "FLOOR_HIT": {"base": 15},
}


class SignalEngine:
    def __init__(self):
        self._states: dict[str, SymbolState] = defaultdict(SymbolState)

    def process(self, symbol: str, current: dict, prev: dict) -> list[dict]:
        state = self._states[symbol]
        signals: list[dict] = []

        price = current.get("price", 0.0)
        volume = current.get("volume", 0)
        ref_price = current.get("referencePrice", 0.0)
        ceiling = current.get("ceiling", 0.0)
        floor_price = current.get("floor", 0.0)
        change_pct = current.get("changePercent", 0.0)
        last_side = current.get("lastSide", "")  # "B" buy, "S" sell from VPS

        # Compute incremental volume since last poll
        delta_vol = max(0, volume - state.last_volume)
        if state.last_volume > 0:
            state.delta_vol.append(delta_vol)
            state.baseline_delta_vols.append(delta_vol)

        # Classify delta volume as matched buy or sell
        if delta_vol > 0 and state.last_volume > 0:
            if price > state.last_price or last_side == "B":
                state.matched_buy  += delta_vol
                state.matched_sell += 0
            elif price < state.last_price or last_side == "S":
                state.matched_sell += delta_vol
                state.matched_buy  += 0
            else:
                # price unchanged – split 50/50
                state.matched_buy  += delta_vol // 2
                state.matched_sell += delta_vol - delta_vol // 2

        # Snapshot of session-total matched buy/sell for this cycle
        state.matched_buy_history.append(state.matched_buy)
        state.matched_sell_history.append(state.matched_sell)

        state.last_volume = volume
        state.last_price  = price

        buy_vol  = state.matched_buy
        ask_vol  = state.matched_sell

        # ----------------------------------------------------------
        # 1. Volume spike (based on rolling ΔVol vs. baseline)
        # ----------------------------------------------------------
        if len(state.baseline_delta_vols) >= 20:
            baseline = list(state.baseline_delta_vols)[:-5]  # exclude very recent
            if baseline:
                avg = statistics.mean(baseline) or 1
                window_vol = sum(list(state.delta_vol)[-12:])  # last ~1 min
                avg_window = avg * 12
                ratio = window_vol / avg_window if avg_window > 0 else 0
                t = THRESHOLDS["volume_spike"]
                if ratio >= t["warning"]:
                    level = _level(ratio, t)
                    signals.append({
                        "type": "VOLUME_SPIKE",
                        "value": round(ratio, 2),
                        "level": level,
                        "description": f"Volume {ratio:.1f}x mức TB",
                    })

        # ----------------------------------------------------------
        # 2. Price surge / drop
        # ----------------------------------------------------------
        t = THRESHOLDS["price_change_pct"]
        if abs(change_pct) >= t["warning"]:
            sig_type = "PRICE_SURGE" if change_pct > 0 else "PRICE_DROP"
            signals.append({
                "type": sig_type,
                "value": round(change_pct, 2),
                "level": _level(abs(change_pct), t),
                "description": f"Giá {'+' if change_pct >= 0 else ''}{change_pct:.1f}%",
            })

        # ----------------------------------------------------------
        # 3. Buy / Sell pressure vs session baseline
        # ----------------------------------------------------------
        if buy_vol > 0 or ask_vol > 0:
            state.matched_buy_history.append(buy_vol)
            state.matched_sell_history.append(ask_vol)

        if buy_vol > 0 and ask_vol > 0:
            ratio = buy_vol / ask_vol
            state.ratio_history.append(ratio)

            t = THRESHOLDS["buy_sell_ratio"]
            enough = len(state.ratio_history) >= 10

            # Compute session-average context for display
            if enough:
                hist_bid = list(state.matched_buy_history)
                hist_ask = list(state.matched_sell_history)
                avg_bid = statistics.mean(hist_bid) or 1
                avg_ask = statistics.mean(hist_ask) or 1
                bid_x = buy_vol / avg_bid   # current bid vs session avg bid
                ask_x = ask_vol / avg_ask   # current ask vs session avg ask
                ctx_buy  = f" · bid {bid_x:.1f}x TB"
                ctx_sell = f" · ask {ask_x:.1f}x TB"
            else:
                ctx_buy = ctx_sell = ""

            if ratio >= t["warning"]:
                signals.append({
                    "type": "STRONG_BUY_PRESSURE",
                    "value": round(ratio, 2),
                    "level": _level(ratio, t),
                    "description": f"Lực MUA {ratio:.1f}x Ask{ctx_buy}",
                })
            elif ratio <= (1 / t["warning"]):
                inv = ask_vol / buy_vol
                signals.append({
                    "type": "STRONG_SELL_PRESSURE",
                    "value": round(inv, 2),
                    "level": _level(inv, t),
                    "description": f"Lực BÁN {inv:.1f}x Bid{ctx_sell}",
                })

        # ----------------------------------------------------------
        # 4. Ceiling / Floor hit
        # ----------------------------------------------------------
        if ceiling and price >= ceiling * 0.999:
            signals.append({"type": "CEILING_HIT", "value": price, "level": "CRITICAL",
                            "description": "Giá kịch trần"})
        elif floor_price and price <= floor_price * 1.001:
            signals.append({"type": "FLOOR_HIT", "value": price, "level": "CRITICAL",
                            "description": "Giá kịch sàn"})

        return signals

    def calculate_score(self, signals: list[dict]) -> int:
        if not signals:
            return 0
        score = 0
        for s in signals:
            sig_type = s.get("type", "")
            val = abs(s.get("value", 0))
            w = SCORE_WEIGHTS.get(sig_type, {})

            if sig_type == "VOLUME_SPIKE":
                score += w.get("base", 0) + int(val * w.get("per_x", 0))
            elif sig_type in ("PRICE_SURGE", "PRICE_DROP"):
                score += w.get("base", 0) + int(val * w.get("per_pct", 0))
            elif sig_type in ("STRONG_BUY_PRESSURE", "STRONG_SELL_PRESSURE"):
                score += w.get("base", 0) + int(val * w.get("per_ratio", 0))
            elif sig_type in ("CEILING_HIT", "FLOOR_HIT"):
                score += w.get("base", 0)

            # Level bonus
            level = s.get("level", "")
            score += {"WARNING": 0, "HIGH": 5, "CRITICAL": 10}.get(level, 0)

        return min(100, score)

    def get_top_signals(self, stocks: dict) -> list[dict]:
        result = []
        for sym, data in stocks.items():
            score = data.get("score", 0)
            if score >= 25:
                result.append({
                    "symbol": sym,
                    "score": score,
                    "price": data.get("price", 0),
                    "changePercent": data.get("changePercent", 0),
                    "volume": data.get("volume", 0),
                    "matchedBuyVol":  data.get("matchedBuyVol", 0),
                    "matchedSellVol": data.get("matchedSellVol", 0),
                    "signals": data.get("signals", []),
                })
        result.sort(key=lambda x: x["score"], reverse=True)
        return result[:10]

    def get_market_breadth(self, stocks: dict) -> dict:
        advances = declines = unchanged = ceiling = floor_count = 0
        for data in stocks.values():
            cp = data.get("changePercent", 0)
            if cp > 0.05:
                advances += 1
            elif cp < -0.05:
                declines += 1
            else:
                unchanged += 1
            if data.get("ceiling") and data.get("price", 0) >= data.get("ceiling", 1) * 0.999:
                ceiling += 1
            if data.get("floor") and data.get("price", 0) <= data.get("floor", 0) * 1.001:
                floor_count += 1
        return {
            "advances": advances,
            "declines": declines,
            "unchanged": unchanged,
            "ceiling": ceiling,
            "floor": floor_count,
        }


def _level(val: float, thresholds: dict) -> str:
    if val >= thresholds.get("critical", 999):
        return "CRITICAL"
    if val >= thresholds.get("high", 999):
        return "HIGH"
    return "WARNING"
