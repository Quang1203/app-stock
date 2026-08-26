"""
Ichimoku Kinko Hyo – biến thể Trịnh Phát (9, 26, 26, 26, 26)
Tenkan=9  Kijun=26  Senkou-B=26  Displacement=26  Chikou=26

Giải thích:
  Tenkan-sen  : trung điểm high/low 9 kỳ → đường tín hiệu ngắn hạn
  Kijun-sen   : trung điểm high/low 26 kỳ → đường cơ sở
  Senkou A    : (Tenkan + Kijun) / 2, dịch 26 kỳ vào tương lai
  Senkou B    : trung điểm high/low 26 kỳ, dịch 26 kỳ vào tương lai
  Cloud hiện tại = Senkou A/B được tính cách đây 26 kỳ
"""

TENKAN_P   = 9
KIJUN_P    = 26
SENKOU_B_P = 26
DISPLACE   = 26


def _mid(highs: list, lows: list, period: int, end: int) -> float | None:
    """Midpoint of high/low over `period` bars ending at index `end`."""
    start = end - period + 1
    if start < 0 or end >= len(highs):
        return None
    # Filter out None values that Yahoo sometimes returns for missing bars
    h_slice = [v for v in highs[start : end + 1] if v is not None]
    l_slice = [v for v in lows [start : end + 1] if v is not None]
    if len(h_slice) < period // 2:   # tolerate up to half missing
        return None
    return (max(h_slice) + min(l_slice)) / 2


def calculate(highs: list, lows: list, closes: list) -> dict | None:
    """
    Compute current Ichimoku levels from daily OHLC arrays (oldest → newest).
    Prices expected in full VND (same unit as VPS real-time feed after ×1000).
    Returns None if not enough data.
    """
    n = len(closes)
    required = KIJUN_P + DISPLACE + SENKOU_B_P
    if n < required:
        return None

    i = n - 1  # latest bar

    # ── Current Tenkan & Kijun ────────────────────────────────────────────
    tenkan = _mid(highs, lows, TENKAN_P, i)
    kijun  = _mid(highs, lows, KIJUN_P,  i)

    # ── Current Cloud (lines calculated DISPLACE bars ago) ───────────────
    past = i - DISPLACE
    if past < 0:
        return None
    t_past  = _mid(highs, lows, TENKAN_P,   past)
    k_past  = _mid(highs, lows, KIJUN_P,    past)
    sb_past = _mid(highs, lows, SENKOU_B_P, past)
    senkou_a = (t_past + k_past) / 2 if (t_past and k_past) else None
    senkou_b = sb_past

    # ── Future Cloud (projected DISPLACE bars ahead from today) ──────────
    sa_future = (tenkan + kijun) / 2 if (tenkan and kijun) else None
    sb_future = _mid(highs, lows, SENKOU_B_P, i)

    close = next((c for c in reversed(closes) if c), None)
    if not close:
        return None

    cloud_top    = max(senkou_a, senkou_b) if (senkou_a and senkou_b) else None
    cloud_bottom = min(senkou_a, senkou_b) if (senkou_a and senkou_b) else None

    # ── Price position ────────────────────────────────────────────────────
    if cloud_top and cloud_bottom:
        if close > cloud_top:
            position = "above"
        elif close < cloud_bottom:
            position = "below"
        else:
            position = "inside"
    else:
        position = "unknown"

    # ── Tenkan/Kijun cross signal ─────────────────────────────────────────
    tk_signal = None
    if tenkan and kijun:
        if tenkan > kijun:
            tk_signal = "bullish"   # Tenkan trên Kijun
        elif tenkan < kijun:
            tk_signal = "bearish"   # Tenkan dưới Kijun
        else:
            tk_signal = "flat"

    # ── Support & Resistance: nearest named levels around close ──────────
    named = [
        ("Tenkan",       tenkan),
        ("Kijun",        kijun),
        ("Cloud Top",    cloud_top),
        ("Cloud Bottom", cloud_bottom),
    ]
    valid = [(lbl, round(v)) for lbl, v in named if v]
    supports    = sorted([(l, v) for l, v in valid if v < close],
                         key=lambda x: x[1], reverse=True)[:3]
    resistances = sorted([(l, v) for l, v in valid if v > close],
                         key=lambda x: x[1])[:3]

    # ── Chikou: close 26 bars ago vs price 52 bars ago (price lagged) ────
    chikou_pos = "unknown"
    if n > DISPLACE:
        chikou_price = closes[i - DISPLACE]          # Chikou = close shifted back 26
        compare_price = closes[i - DISPLACE * 2] if n > DISPLACE * 2 else None
        if chikou_price and compare_price:
            chikou_pos = "above" if chikou_price > compare_price else "below"

    # ── Future Kumo direction ─────────────────────────────────────────────
    future_kumo = "bullish" if (sa_future and sb_future and sa_future > sb_future) \
                  else ("bearish" if (sa_future and sb_future) else "unknown")

    # ── Ichimoku Score 0–100 ──────────────────────────────────────────────
    score = 0

    # 1. Price vs Kumo (40 pts)
    if position == "above":
        score += 40
    elif position == "inside":
        score += 20

    # 2. Tenkan / Kijun (20 pts)
    if tenkan and kijun:
        if tenkan > kijun:
            score += 20
        elif tenkan == kijun:
            score += 10

    # 3. Future Kumo (20 pts)
    if future_kumo == "bullish":
        score += 20
    elif future_kumo == "unknown":
        score += 10

    # 4. Chikou (10 pts)
    if chikou_pos == "above":
        score += 10

    # 5. Price above Kijun (bonus 10 pts)
    if kijun and close > kijun:
        score += 10

    score = min(100, score)

    # Score label
    if   score >= 80: score_label = "Rất mạnh"
    elif score >= 65: score_label = "Tăng"
    elif score >= 50: score_label = "Trung tính"
    elif score >= 35: score_label = "Yếu"
    else:             score_label = "Giảm"

    # ── Detect Ichimoku signals (compare vs previous bar) ────────────────
    signals: list[dict] = []

    # Kumo Breakout Up/Down (price just crossed cloud)
    prev_close = closes[i - 1] if i > 0 else close
    prev_cloud_top    = cloud_top    # approximation – uses same cloud
    prev_cloud_bottom = cloud_bottom

    if prev_close and prev_cloud_top and prev_cloud_bottom:
        if close > cloud_top and prev_close <= prev_cloud_top:
            signals.append({"type": "KUMO_BREAKOUT_UP",    "label": "Giá vượt mây lên ▲"})
        elif close < cloud_bottom and prev_close >= prev_cloud_bottom:
            signals.append({"type": "KUMO_BREAKOUT_DOWN",  "label": "Giá phá mây xuống ▼"})

    # TK Cross (needs 2 consecutive bars)
    if i > 0:
        prev_t = _mid(highs, lows, TENKAN_P, i - 1)
        prev_k = _mid(highs, lows, KIJUN_P,  i - 1)
        if tenkan and kijun and prev_t and prev_k:
            if prev_t <= prev_k and tenkan > kijun:
                signals.append({"type": "TK_BULLISH_CROSS", "label": "Tenkan cắt Kijun lên ↑"})
            elif prev_t >= prev_k and tenkan < kijun:
                signals.append({"type": "TK_BEARISH_CROSS", "label": "Tenkan cắt Kijun xuống ↓"})

    # Kumo Twist (future SA/SB swap)
    if i > 0:
        prev_sa_f = (_mid(highs, lows, TENKAN_P, i - 1) + _mid(highs, lows, KIJUN_P, i - 1)) / 2 \
                    if _mid(highs, lows, TENKAN_P, i - 1) and _mid(highs, lows, KIJUN_P, i - 1) else None
        prev_sb_f = _mid(highs, lows, SENKOU_B_P, i - 1)
        if sa_future and sb_future and prev_sa_f and prev_sb_f:
            if prev_sa_f <= prev_sb_f and sa_future > sb_future:
                signals.append({"type": "KUMO_TWIST_BULLISH", "label": "Mây tương lai đảo tăng ☁↑"})
            elif prev_sa_f >= prev_sb_f and sa_future < sb_future:
                signals.append({"type": "KUMO_TWIST_BEARISH", "label": "Mây tương lai đảo giảm ☁↓"})

    # Chikou confirmation
    if chikou_pos == "above" and position == "above":
        signals.append({"type": "CHIKOU_CONFIRM_BULL", "label": "Chikou xác nhận tăng ✓"})

    # Lost Kijun (bearish warning)
    if kijun and close < kijun and position != "above":
        signals.append({"type": "LOST_KIJUN", "label": "Mất Kijun ⚠"})

    def _r(v):
        return round(v) if v else None

    return {
        "tenkan":       _r(tenkan),
        "kijun":        _r(kijun),
        "senkou_a":     _r(senkou_a),
        "senkou_b":     _r(senkou_b),
        "cloud_top":    _r(cloud_top),
        "cloud_bottom": _r(cloud_bottom),
        "sa_future":    _r(sa_future),
        "sb_future":    _r(sb_future),
        "position":     position,
        "tk_signal":    tk_signal,
        "future_kumo":  future_kumo,
        "chikou_pos":   chikou_pos,
        "score":        score,
        "score_label":  score_label,
        "signals":      signals,
        "supports":    [{"label": l, "value": v} for l, v in supports],
        "resistances": [{"label": l, "value": v} for l, v in resistances],
    }
