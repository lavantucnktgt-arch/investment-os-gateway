from typing import List, Dict, Optional


def sma(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def ema_series(values: List[float], period: int) -> List[Optional[float]]:
    if len(values) < period:
        return [None] * len(values)

    result = [None] * len(values)

    initial = sum(values[:period]) / period
    result[period - 1] = initial

    multiplier = 2 / (period + 1)
    previous = initial

    for i in range(period, len(values)):
        current = (values[i] - previous) * multiplier + previous
        result[i] = current
        previous = current

    return result


def macd(
    closes: List[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9
) -> Dict[str, Optional[float]]:

    if len(closes) < slow_period:
        return {
            "macd": None,
            "signal": None,
            "histogram": None
        }

    fast = ema_series(closes, fast_period)
    slow = ema_series(closes, slow_period)

    macd_values = []

    for i in range(len(closes)):
        if fast[i] is not None and slow[i] is not None:
            macd_values.append(fast[i] - slow[i])

    if not macd_values:
        return {
            "macd": None,
            "signal": None,
            "histogram": None
        }

    signal_values = ema_series(macd_values, signal_period)

    current_macd = macd_values[-1]
    current_signal = signal_values[-1]

    if current_signal is None:
        return {
            "macd": current_macd,
            "signal": None,
            "histogram": None
        }

    return {
        "macd": current_macd,
        "signal": current_signal,
        "histogram": current_macd - current_signal
    }


def calculate_indicators(
    candles: List[Dict]
) -> Dict:

    closes = [
        float(c["close"])
        for c in candles
        if c.get("close") is not None
    ]

    volumes = [
        float(c["volume"])
        for c in candles
        if c.get("volume") is not None
    ]

    result = {
        "data_points": len(closes),
        "close": closes[-1] if closes else None,
        "ma20": sma(closes, 20),
        "ma50": sma(closes, 50),
        "volume": volumes[-1] if volumes else None,
        "volume_ma20": sma(volumes, 20),
    }

    result.update(macd(closes))

    return result
