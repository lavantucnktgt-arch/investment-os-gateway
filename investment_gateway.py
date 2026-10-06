from fastapi import FastAPI, Query
import httpx

from adapters.dnse import fetch_dnse_ohlcv


app = FastAPI(
    title="Investment OS Data Gateway",
    version="1.0.0",
)


VCI_SYMBOLS_URL = "https://trading.vietcap.com.vn/api/price/symbols/getAll"
VCI_PRICE_URL = "https://trading.vietcap.com.vn/api/price/symbols/getList"

VCI_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://trading.vietcap.com.vn/",
    "Origin": "https://trading.vietcap.com.vn",
}


def _float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _sma(values, period):
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def _ema_series(values, period):
    if len(values) < period:
        return []

    seed = sum(values[:period]) / period
    result = [seed]
    multiplier = 2 / (period + 1)
    ema = seed

    for value in values[period:]:
        ema = (value - ema) * multiplier + ema
        result.append(ema)

    return result


def _macd(values, fast=12, slow=26, signal=9):
    if len(values) < slow:
        return None, None, None

    slow_ema = _ema_series(values, slow)
    fast_ema = _ema_series(values, fast)

    if not slow_ema or not fast_ema:
        return None, None, None

    offset = len(fast_ema) - len(slow_ema)
    fast_aligned = fast_ema[offset:]

    macd_series = [
        fast_aligned[i] - slow_ema[i]
        for i in range(len(slow_ema))
    ]

    if len(macd_series) < signal:
        return None, None, None

    signal_series = _ema_series(macd_series, signal)

    if not signal_series:
        return None, None, None

    macd_value = macd_series[-1]
    signal_value = signal_series[-1]
    histogram = macd_value - signal_value

    return macd_value, signal_value, histogram


def _market_snapshot(candles):
    if not candles:
        return {
            "close": None,
            "ma20": None,
            "ma50": None,
            "volume": None,
            "volume_ma20": None,
            "macd": None,
            "signal": None,
            "histogram": None,
            "data_points": 0,
        }

    candles = sorted(
        candles,
        key=lambda x: x.get("time", 0),
    )

    closes = [_float(x.get("close")) for x in candles]
    volumes = [_float(x.get("volume")) for x in candles]

    macd_value, signal_value, histogram = _macd(closes)

    return {
        "close": closes[-1],
        "ma20": _sma(closes, 20),
        "ma50": _sma(closes, 50),
        "volume": volumes[-1],
        "volume_ma20": _sma(volumes, 20),
        "macd": macd_value,
        "signal": signal_value,
        "histogram": histogram,
        "data_points": len(candles),
    }


def _collect_volume_candidates(obj, found=None, path=""):
    if found is None:
        found = []

    if not isinstance(obj, dict):
        return found

    excluded_words = (
        "foreign",
        "bid",
        "ask",
        "room",
        "listed",
        "ceiling",
        "floor",
    )

    preferred_words = (
        "totalvolume",
        "total_volume",
        "totalvol",
        "total_vol",
        "totalqty",
        "total_qty",
        "totalquantity",
        "total_quantity",
        "tradedvolume",
        "traded_volume",
        "tradingvolume",
        "trading_volume",
        "matchedvolume",
        "matched_volume",
        "totalmatchedvolume",
        "total_matched_volume",
        "totalmatchvolume",
        "total_match_volume",
        "volume",
        "vol",
    )

    for key, value in obj.items():
        key_text = str(key).lower().replace("-", "_").replace(" ", "_")
        key_compact = key_text.replace("_", "")

        current_path = f"{path}.{key}" if path else str(key)

        if isinstance(value, dict):
            _collect_volume_candidates(value, found, current_path)
            continue

        if isinstance(value, list):
            continue

        if any(word in key_text for word in excluded_words):
            continue

        is_candidate = (
            key_text in preferred_words
            or key_compact in {
                word.replace("_", "")
                for word in preferred_words
            }
        )

        if not is_candidate:
            continue

        number = _float(value, -1)

        if number > 0:
            found.append({
                "key": str(key),
                "path": current_path,
                "value": number,
            })

    return found


def _extract_volume(item):
    candidates = _collect_volume_candidates(item)

    if not candidates:
        return 0.0, "not_found"

    priority = {
        "totalvolume": 100,
        "total_volume": 100,
        "totalvol": 95,
        "total_vol": 95,
        "totalqty": 95,
        "total_qty": 95,
        "totalquantity": 95,
        "total_quantity": 95,
        "totalmatchedvolume": 90,
        "total_matched_volume": 90,
        "totalmatchvolume": 90,
        "total_match_volume": 90,
        "tradedvolume": 85,
        "traded_volume": 85,
        "tradingvolume": 85,
        "trading_volume": 85,
        "matchedvolume": 80,
        "matched_volume": 80,
        "volume": 60,
        "vol": 50,
    }

    ranked = sorted(
        candidates,
        key=lambda x: (
            priority.get(x["key"].lower(), 0),
            x["value"],
        ),
        reverse=True,
    )

    best_priority = priority.get(ranked[0]["key"].lower(), 0)

    same_priority = [
        item
        for item in candidates
        if priority.get(item["key"].lower(), 0) == best_priority
    ]

    best = max(same_priority, key=lambda x: x["value"])

    return best["value"], best["path"]


def _extract_stock_row(item):
    if not isinstance(item, dict):
        return None

    listing = item.get("listingInfo") or {}
    match = item.get("matchPrice") or {}

    symbol = (
        listing.get("symbol")
        or listing.get("ticker")
        or item.get("symbol")
    )

    ref = _float(
        listing.get("refPrice")
        or listing.get("referencePrice")
        or listing.get("reference_price")
    )

    price = _float(
        match.get("matchPrice")
        or match.get("match_price")
        or item.get("matchPriceValue")
    )

    volume, volume_source = _extract_volume(item)

    if not symbol or ref <= 0 or price <= 0:
        return None

    return {
        "symbol": symbol,
        "price": price,
        "change_pct": round((price / ref - 1) * 100, 2),
        "volume": int(volume),
        "volume_source": volume_source,
    }


async def _load_vci_stock_prices():
    async with httpx.AsyncClient(timeout=60) as client:
        universe_response = await client.get(
            VCI_SYMBOLS_URL,
            headers=VCI_HEADERS,
        )

        if not universe_response.is_success:
            return {
                "error": (
                    "getAll failed: "
                    f"HTTP {universe_response.status_code}"
                )
            }

        universe = universe_response.json()

        if isinstance(universe, dict):
            universe = universe.get("data", [])

        if not isinstance(universe, list):
            universe = []

        symbols = []

        for item in universe:
            if not isinstance(item, dict):
                continue

            if item.get("type") != "STOCK":
                continue

            if item.get("board") not in ["HSX", "HNX", "UPCOM"]:
                continue

            symbol = item.get("symbol")

            if symbol:
                symbols.append(symbol)

        symbols = list(dict.fromkeys(symbols))

        prices = []
        batch_size = 50

        total_batches = (
            len(symbols) + batch_size - 1
        ) // batch_size

        successful_batches = 0
        failed_batches = 0
        failed_batch_details = []

        for i in range(
            0,
            len(symbols),
            batch_size,
        ):
            batch = symbols[i:i + batch_size]

            try:
                response = await client.post(
                    VCI_PRICE_URL,
                    headers=VCI_HEADERS,
                    json={"symbols": batch},
                )

                if not response.is_success:
                    failed_batches += 1

                    failed_batch_details.append({
                        "batch_start": i,
                        "status": response.status_code,
                    })

                    continue

                data = response.json()

                if isinstance(data, dict):
                    data = data.get("data", [])

                if not isinstance(data, list):
                    data = []

                prices.extend(data)
                successful_batches += 1

            except Exception as exc:
                failed_batches += 1

                failed_batch_details.append({
                    "batch_start": i,
                    "error": str(exc),
                })

    return {
        "symbols": symbols,
        "prices": prices,
        "total_batches": total_batches,
        "successful_batches": successful_batches,
        "failed_batches": failed_batches,
        "failed_batch_details": failed_batch_details,
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "Investment OS Data Gateway",
        "version": "1.0.0",
    }


@app.get("/ohlcv")
async def ohlcv(
    symbol: str = Query(
        ...,
        description="Mã cổ phiếu hoặc chỉ số",
    ),
    market: str = Query(
        "stock",
        description="stock | index | derivative",
    ),
    resolution: str = Query(
        "1D",
        description="1D | 60 | 30 | 15 | 5 | 1",
    ),
    days: int = Query(
        30,
        description="Số ngày dữ liệu",
    ),
):
    candles = await fetch_dnse_ohlcv(
        symbol=symbol.upper(),
        market=market,
        resolution=resolution,
        days=days,
    )

    return {
        "symbol": symbol.upper(),
        "market": market,
        "resolution": resolution,
        "source": "DNSE",
        "count": len(candles),
        "candles": candles,
    }


@app.get("/market")
async def market():
    vnindex_daily = await fetch_dnse_ohlcv(
        symbol="VNINDEX",
        market="index",
        resolution="1D",
        days=180,
    )

    vn30_daily = await fetch_dnse_ohlcv(
        symbol="VN30",
        market="index",
        resolution="1D",
        days=180,
    )

    return {
        "source": "DNSE",
        "resolution": "1D",
        "vnindex": _market_snapshot(vnindex_daily),
        "vn30": _market_snapshot(vn30_daily),
    }


@app.get("/breadth")
async def breadth():
    data = await _load_vci_stock_prices()

    if "error" in data:
        return {
            "source": "VCI",
            "status": "ERROR",
            "error": data["error"],
        }

    prices = data["prices"]

    advances = 0
    declines = 0
    unchanged = 0
    strong_advances = 0
    strong_declines = 0
    priced_stocks = 0

    for item in prices:
        row = _extract_stock_row(item)

        if not row:
            continue

        priced_stocks += 1
        change_pct = row["change_pct"]

        if change_pct > 0:
            advances += 1
        elif change_pct < 0:
            declines += 1
        else:
            unchanged += 1

        if change_pct >= 5:
            strong_advances += 1
        elif change_pct <= -5:
            strong_declines += 1

    return {
        "source": "VCI",
        "status": "OK",
        "universe": len(data["symbols"]),
        "total_batches": data["total_batches"],
        "successful_batches": data["successful_batches"],
        "failed_batches": data["failed_batches"],
        "symbols_received": len(prices),
        "priced_stocks": priced_stocks,
        "advances": advances,
        "declines": declines,
        "unchanged": unchanged,
        "strong_advances_5pct": strong_advances,
        "strong_declines_5pct": strong_declines,
        "failed_batch_details": data["failed_batch_details"],
    }


@app.get("/debug-vci")
async def debug_vci():
    payload = {
        "symbols": ["VCI", "VCB", "ACB"]
    }

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            VCI_PRICE_URL,
            headers=VCI_HEADERS,
            json=payload,
        )

    return {
        "status": response.status_code,
        "source": "VCI",
        "success": response.is_success,
        "request_symbols": payload["symbols"],
        "data": response.json(),
    }


@app.get("/leaders")
async def leaders():
    data = await _load_vci_stock_prices()

    if "error" in data:
        return {
            "source": "VCI",
            "status": "ERROR",
            "error": data["error"],
        }

    stocks = []

    for item in data["prices"]:
        row = _extract_stock_row(item)

        if row:
            stocks.append(row)

    top_gainers = sorted(
        stocks,
        key=lambda x: x["change_pct"],
        reverse=True,
    )[:20]

    stocks_with_volume = [
        x
        for x in stocks
        if x["volume"] > 0
    ]

    top_volume = sorted(
        stocks_with_volume,
        key=lambda x: x["volume"],
        reverse=True,
    )[:20]

    volume_source_counts = {}

    for stock in stocks_with_volume:
        source = stock["volume_source"]
        volume_source_counts[source] = (
            volume_source_counts.get(source, 0) + 1
        )

    return {
        "source": "VCI",
        "status": "OK",
        "universe": len(data["symbols"]),
        "total_batches": data["total_batches"],
        "successful_batches": data["successful_batches"],
        "failed_batches": data["failed_batches"],
        "symbols_received": len(data["prices"]),
        "priced_stocks": len(stocks),
        "stocks_with_volume": len(stocks_with_volume),
        "volume_coverage_pct": round(
            len(stocks_with_volume) / len(stocks) * 100,
            2,
        ) if stocks else 0,
        "volume_source_counts": volume_source_counts,
        "top_gainers": top_gainers,
        "top_volume": top_volume,
        "failed_batch_details": data["failed_batch_details"],
    }
