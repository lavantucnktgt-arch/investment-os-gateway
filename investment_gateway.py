from fastapi import FastAPI, Query
from adapters.dnse import fetch_dnse_ohlcv
from indicators import calculate_indicators


app = FastAPI(
    title="Investment OS Data Gateway",
    version="1.0.0"
)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "Investment OS Data Gateway"
    }


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


@app.get("/market")
async def market():
    vnindex_candles = await fetch_dnse_ohlcv(
        symbol="VNINDEX",
        market="index",
        resolution="1D",
        days=120
    )

    vn30_candles = await fetch_dnse_ohlcv(
        symbol="VN30",
        market="index",
        resolution="1D",
        days=120
    )

    vnindex = calculate_indicators(vnindex_candles)
    vn30 = calculate_indicators(vn30_candles)

    return {
        "source": "DNSE",
        "resolution": "1D",
        "vnindex": vnindex,
        "vn30": vn30
    }
@app.get("/breadth")
async def breadth():
    import httpx

    symbols_url = "https://trading.vietcap.com.vn/api/price/symbols/getAll"
    price_url = "https://trading.vietcap.com.vn/api/price/symbols/getList"

    async with httpx.AsyncClient(timeout=30) as client:

        # 1. Get full market universe
        universe_response = await client.get(symbols_url)
        universe_response.raise_for_status()

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

        # Remove duplicate symbols
        symbols = list(dict.fromkeys(symbols))

        # 2. Get realtime prices in batches
        prices = []

        batch_size = 50

        total_batches = 0
        successful_batches = 0
        failed_batches = 0
        failed_batch_details = []

        for i in range(0, len(symbols), batch_size):

            batch = symbols[i:i + batch_size]
            total_batches += 1

            try:
                response = await client.post(
                    price_url,
                    json={"symbols": batch}
                )

                if not response.is_success:
                    failed_batches += 1

                    failed_batch_details.append({
                        "batch": total_batches,
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
                    "batch": total_batches,
                    "error": str(exc)
                })

        # 3. Calculate market breadth
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

            ref_price = listing.get("refPrice")
            match_price = match.get("matchPrice")

            if ref_price is None or match_price is None:
                continue

            try:
                ref_price = float(ref_price)
                match_price = float(match_price)
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

        # 4. Return clean data for Investment OS
        return {
            "source": "VCI",
            "universe": len(symbols),
            "total_batches": total_batches,
            "successful_batches": successful_batches,
            "failed_batches": failed_batches,
            "symbols_received": len(prices),
            "priced_stocks": priced_stocks,
            "advances": advances,
            "declines": declines,
            "unchanged": unchanged,
            "strong_advances_5pct": strong_advances,
            "strong_declines_5pct": strong_declines,
            "failed_batch_details": failed_batch_details
        }
