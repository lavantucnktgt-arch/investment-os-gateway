from fastapi import FastAPI, Query
from adapters.dnse import fetch_dnse_ohlcv
import httpx

app = FastAPI(
    title="Investment OS Data Gateway",
    version="2.0.0"
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
    "Origin": "https://trading.vietcap.com.vn"
}


# =========================================================
# HEALTH
# =========================================================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "Investment OS Data Gateway",
        "version": "2.0.0"
    }


# =========================================================
# OHLCV
# =========================================================

@app.get("/ohlcv")
async def ohlcv(
    symbol: str = Query(..., description="Mã cổ phiếu hoặc chỉ số"),
    market: str = Query("stock", description="stock | index | derivative"),
    resolution: str = Query("1D", description="1D | 60 | 30 | 15 | 5 | 1"),
    days: int = Query(30, description="Số ngày dữ liệu")
):
    candles = await fetch_dnse_ohlcv(
        symbol=symbol.upper(),
        market=market,
        resolution=resolution,
        days=days
    )
    return {
        "symbol": symbol.upper(),
        "market": market,
        "resolution": resolution,
        "source": "DNSE",
        "count": len(candles),
        "candles": candles
    }


# =========================================================
# MARKET INDICATORS
# =========================================================

def sma(values, period):
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def ema_series(values, period):
    if len(values) < period:
        return []
    multiplier = 2 / (period + 1)
    ema = sum(values[:period]) / period
    result = [ema]
    for value in values[period:]:
        ema = (value - ema) * multiplier + ema
        result.append(ema)
    return result


def calculate_macd(values):
    if len(values) < 35:
        return {
            "macd": None,
            "signal": None,
            "histogram": None
        }

    ema12 = ema_series(values, 12)
    ema26 = ema_series(values, 26)

    if not ema12 or not ema26:
        return {
            "macd": None,
            "signal": None,
            "histogram": None
        }

    offset = 26 - 12
    macd_values = [
        ema12[i + offset] - ema26[i]
        for i in range(len(ema26))
    ]

    signal_values = ema_series(macd_values, 9)

    if not signal_values:
        return {
            "macd": round(macd_values[-1], 4),
            "signal": None,
            "histogram": None
        }

    macd_value = macd_values[-1]
    signal_value = signal_values[-1]

    return {
        "macd": round(macd_value, 4),
        "signal": round(signal_value, 4),
        "histogram": round(macd_value - signal_value, 4)
    }


def calculate_market_indicators(candles):
    closes = []
    volumes = []

    for candle in candles or []:
        try:
            close = float(candle.get("close", 0))
            volume = float(candle.get("volume", 0))
        except (TypeError, ValueError):
            continue

        if close > 0:
            closes.append(close)
            volumes.append(volume)

    if not closes:
        return {
            "close": None,
            "ma20": None,
            "ma50": None,
            "volume": None,
            "volume_ma20": None,
            "macd": None,
            "signal": None,
            "histogram": None,
            "data_points": 0
        }

    macd = calculate_macd(closes)

    return {
        "close": round(closes[-1], 4),
        "ma20": round(sma(closes, 20), 4) if len(closes) >= 20 else None,
        "ma50": round(sma(closes, 50), 4) if len(closes) >= 50 else None,
        "volume": int(volumes[-1]),
        "volume_ma20": round(sma(volumes, 20), 2) if len(volumes) >= 20 else None,
        "macd": macd["macd"],
        "signal": macd["signal"],
        "histogram": macd["histogram"],
        "data_points": len(closes)
    }


# =========================================================
# MARKET
# =========================================================

@app.get("/market")
async def market():
    vnindex = await fetch_dnse_ohlcv(
        symbol="VNINDEX",
        market="index",
        resolution="1D",
        days=180
    )

    vn30 = await fetch_dnse_ohlcv(
        symbol="VN30",
        market="index",
        resolution="1D",
        days=180
    )

    return {
        "source": "DNSE",
        "resolution": "1D",
        "vnindex": calculate_market_indicators(vnindex),
        "vn30": calculate_market_indicators(vn30)
    }


# =========================================================
# VCI DATA HELPERS
# =========================================================

async def fetch_vci_universe(client):
    response = await client.get(
        VCI_SYMBOLS_URL,
        headers=VCI_HEADERS
    )

    if not response.is_success:
        return None, {
            "source": "VCI",
            "status": "ERROR",
            "stage": "getAll",
            "http_status": response.status_code
        }

    data = response.json()

    if isinstance(data, dict):
        data = data.get("data", [])

    if not isinstance(data, list):
        data = []

    symbols = []

    for item in data:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "STOCK":
            continue
        if item.get("board") not in ["HSX", "HNX", "UPCOM"]:
            continue

        symbol = item.get("symbol")
        if symbol:
            symbols.append(symbol)

    return list(dict.fromkeys(symbols)), None


async def fetch_vci_prices(client, symbols):
    batch_size = 50
    prices = []
    failed_batch_details = []
    successful_batches = 0
    failed_batches = 0

    total_batches = (
        (len(symbols) + batch_size - 1) // batch_size
    )

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]

        try:
            response = await client.post(
                VCI_PRICE_URL,
                headers=VCI_HEADERS,
                json={"symbols": batch}
            )

            if not response.is_success:
                failed_batches += 1
                failed_batch_details.append({
                    "batch_start": i,
                    "status": response.status_code
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
                "error": str(exc)
            })

    return {
        "prices": prices,
        "total_batches": total_batches,
        "successful_batches": successful_batches,
        "failed_batches": failed_batches,
        "failed_batch_details": failed_batch_details
    }


# =========================================================
# BREADTH
# =========================================================

@app.get("/breadth")
async def breadth():
    async with httpx.AsyncClient(timeout=60) as client:
        symbols, error = await fetch_vci_universe(client)

        if error:
            error["universe"] = 0
            error["priced_stocks"] = 0
            return error

        result = await fetch_vci_prices(
            client,
            symbols
        )

    prices = result["prices"]

    advances = 0
    declines = 0
    unchanged = 0
    strong_advances = 0
    strong_declines = 0
    priced_stocks = 0

    for item in prices:
        if not isinstance(item, dict):
            continue

        listing = item.get("listingInfo") or {}
        match = item.get("matchPrice") or {}

        try:
            ref_price = float(listing.get("refPrice"))
            match_price = float(match.get("matchPrice"))
        except (TypeError, ValueError):
            continue

        if ref_price <= 0 or match_price <= 0:
            continue

        priced_stocks += 1
        change_pct = (
            (match_price - ref_price)
            / ref_price
            * 100
        )

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
        "universe": len(symbols),
        "total_batches": result["total_batches"],
        "successful_batches": result["successful_batches"],
        "failed_batches": result["failed_batches"],
        "symbols_received": len(prices),
        "priced_stocks": priced_stocks,
        "advances": advances,
        "declines": declines,
        "unchanged": unchanged,
        "strong_advances_5pct": strong_advances,
        "strong_declines_5pct": strong_declines,
        "failed_batch_details": result["failed_batch_details"]
    }


# =========================================================
# LEADER V1
# =========================================================

@app.get("/leaders")
async def leaders():
    async with httpx.AsyncClient(timeout=60) as client:
        symbols, error = await fetch_vci_universe(client)

        if error:
            error["universe"] = 0
            error["priced_stocks"] = 0
            return error

        result = await fetch_vci_prices(
            client,
            symbols
        )

    stocks = []

    for item in result["prices"]:
        if not isinstance(item, dict):
            continue

        listing = item.get("listingInfo") or {}
        match = item.get("matchPrice") or {}

        try:
            ref_price = float(listing.get("refPrice"))
            current_price = float(match.get("matchPrice"))
            volume = float(match.get("totalVolume", 0))
        except (TypeError, ValueError):
            continue

        symbol = listing.get("symbol")

        if (
            not symbol
            or ref_price <= 0
            or current_price <= 0
        ):
            continue

        change_pct = (
            (current_price - ref_price)
            / ref_price
            * 100
        )

        stocks.append({
            "symbol": symbol,
            "price": current_price,
            "change_pct": round(change_pct, 2),
            "volume": int(volume)
        })

    top_gainers = sorted(
        stocks,
        key=lambda x: x["change_pct"],
        reverse=True
    )[:20]

    top_volume = sorted(
        stocks,
        key=lambda x: x["volume"],
        reverse=True
    )[:20]

    return {
        "source": "VCI",
        "status": "OK",
        "universe": len(symbols),
        "total_batches": result["total_batches"],
        "successful_batches": result["successful_batches"],
        "failed_batches": result["failed_batches"],
        "priced_stocks": len(stocks),
        "top_gainers": top_gainers,
        "top_volume": top_volume,
        "failed_batch_details": result["failed_batch_details"]
    }
