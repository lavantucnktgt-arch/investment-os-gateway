from fastapi import FastAPI, Query
import httpx

from adapters.dnse import fetch_dnse_ohlcv


app = FastAPI(
    title="Investment OS Data Gateway",
    version="1.0.0",
)


VCI_SYMBOLS_URL = "https://trading.vietcap.com.vn/api/price/symbols/getAll"
VCI_PRICE_URL = "https://trading.vietcap.com.vn/api/price/symbols/getList"
VCI_ICB_URL = "https://iq.vietcap.com.vn/api/iq-insight-service/v1/sectors/icb-codes"

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


def _extract_volume(item):
    """
    VCI raw price-board data stores the session's accumulated
    traded volume inside matchPrice.accumulatedVolume.
    Use that field first, then fall back to other known fields.
    """
    if not isinstance(item, dict):
        return 0.0, "not_found"

    match = item.get("matchPrice") or {}

    if isinstance(match, dict):
        for key in (
            "accumulatedVolume",
            "accumulated_volume",
            "totalVolume",
            "total_volume",
            "tradedVolume",
            "traded_volume",
            "matchVolume",
            "matchVol",
            "volume",
        ):
            if key not in match:
                continue

            value = _float(match.get(key), -1)

            if value > 0:
                return value, f"matchPrice.{key}"

    for key in (
        "accumulatedVolume",
        "accumulated_volume",
        "totalVolume",
        "total_volume",
        "tradedVolume",
        "traded_volume",
        "volume",
    ):
        if key not in item:
            continue

        value = _float(item.get(key), -1)

        if value > 0:
            return value, key

    return 0.0, "not_found"


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

        universe_items = universe if isinstance(universe, list) else (universe.get("data", []) if isinstance(universe, dict) else [])

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
        "universe_items": universe_items,
        "total_batches": total_batches,
        "successful_batches": successful_batches,
        "failed_batches": failed_batches,
        "failed_batch_details": failed_batch_details,
    }




async def _load_vci_icb_mapping(client, level=2):
    """Load ICB code -> Vietnamese industry name mapping from VCI."""
    try:
        response = await client.get(
            VCI_ICB_URL,
            headers=VCI_HEADERS,
        )

        if not response.is_success:
            return {
                "status": "ERROR",
                "error": f"icb-codes failed: HTTP {response.status_code}",
                "mapping": {},
                "rows": 0,
            }

        payload = response.json()
        rows = payload.get("data", []) if isinstance(payload, dict) else []

        if not isinstance(rows, list):
            rows = []

        mapping = {}

        for item in rows:
            if not isinstance(item, dict):
                continue

            item_level = item.get("icbLevel")
            try:
                item_level = int(item_level)
            except (TypeError, ValueError):
                continue

            if item_level != level:
                continue

            code = item.get("name")
            name = item.get("viSector") or item.get("enSector")

            if code is None or not name:
                continue

            mapping[str(code).strip()] = str(name).strip()

        if not mapping:
            return {
                "status": "ERROR",
                "error": f"No ICB mapping found at level {level}",
                "mapping": {},
                "rows": len(rows),
            }

        return {
            "status": "OK",
            "error": None,
            "mapping": mapping,
            "rows": len(rows),
        }

    except Exception as exc:
        return {
            "status": "ERROR",
            "error": str(exc),
            "mapping": {},
            "rows": 0,
        }


def _extract_icb_code(item):
    if not isinstance(item, dict):
        return None

    for key in ("icbCode2", "icb_code2", "icbCode"):
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()

    return None


@app.get("/groups")
async def groups(
    level: int = Query(
        2,
        ge=1,
        le=4,
        description="Cấp ICB dùng để gom nhóm ngành; mặc định cấp 2",
    ),
    top_n: int = Query(
        5,
        ge=3,
        le=10,
        description="Số nhóm mạnh/yếu trả về ở phần tóm tắt",
    ),
    leaders_per_group: int = Query(
        3,
        ge=1,
        le=5,
        description="Số cổ phiếu dẫn đầu mỗi nhóm",
    ),
):
    """
    Group V2:
    - Aggregate breadth by ICB group.
    - Rank strong and weak groups.
    - Link each group to its leading stocks.
    - Keep the output factual; interpretation remains with ORCHESTRATOR.
    """
    data = await _load_vci_stock_prices()

    if "error" in data:
        return {
            "source": "VCI",
            "status": "ERROR",
            "error": data["error"],
        }

    async with httpx.AsyncClient(timeout=30) as client:
        icb = await _load_vci_icb_mapping(client, level=level)

    mapping = icb["mapping"]
    groups_data = {}
    mapped_symbols = 0
    unmapped_symbols = 0

    # Build symbol -> ICB code from the original getAll universe.
    universe_items = data.get("universe_items", [])
    symbol_to_code = {}

    for item in universe_items:
        if not isinstance(item, dict):
            continue

        symbol = item.get("symbol")
        code = _extract_icb_code(item)

        if symbol and code:
            symbol_to_code[str(symbol).upper()] = code

    # Parse price-board data once and attach each stock to its group.
    all_stocks = []

    for item in data["prices"]:
        row = _extract_stock_row(item)

        if not row:
            continue

        symbol = str(row["symbol"]).upper()
        code = symbol_to_code.get(symbol)

        if not code:
            unmapped_symbols += 1
            continue

        group_name = mapping.get(code)
        mapping_source = "VCI_ICB"

        if not group_name:
            group_name = f"ICB {code}"
            mapping_source = "ICB_CODE_FALLBACK"

        row["icb_code"] = code
        row["group"] = group_name
        row["mapping_source"] = mapping_source
        all_stocks.append(row)
        mapped_symbols += 1

        if group_name not in groups_data:
            groups_data[group_name] = {
                "group": group_name,
                "icb_code": code,
                "stocks": 0,
                "advances": 0,
                "declines": 0,
                "unchanged": 0,
                "strong_advances_5pct": 0,
                "strong_declines_5pct": 0,
                "leaders": [],
                "total_volume": 0,
                "avg_change_pct_sum": 0.0,
            }

        group = groups_data[group_name]
        group["stocks"] += 1
        group["total_volume"] += row["volume"]
        group["avg_change_pct_sum"] += row["change_pct"]

        change_pct = row["change_pct"]

        if change_pct > 0:
            group["advances"] += 1
        elif change_pct < 0:
            group["declines"] += 1
        else:
            group["unchanged"] += 1

        if change_pct >= 5:
            group["strong_advances_5pct"] += 1
        elif change_pct <= -5:
            group["strong_declines_5pct"] += 1

        group["leaders"].append(row)

    result = []

    for group in groups_data.values():
        stocks = group["stocks"]

        group["advance_ratio"] = round(
            group["advances"] / stocks * 100,
            2,
        ) if stocks else 0

        group["decline_ratio"] = round(
            group["declines"] / stocks * 100,
            2,
        ) if stocks else 0

        group["breadth_score"] = round(
            (group["advances"] - group["declines"]) / stocks * 100,
            2,
        ) if stocks else 0

        group["avg_change_pct"] = round(
            group["avg_change_pct_sum"] / stocks,
            2,
        ) if stocks else 0

        group["volume_total"] = int(group.pop("total_volume", 0))
        group.pop("avg_change_pct_sum", None)

        # Leader ranking is deliberately simple and transparent:
        # price strength first, then traded volume.
        leaders = sorted(
            group.pop("leaders", []),
            key=lambda x: (
                x["change_pct"],
                x["volume"],
            ),
            reverse=True,
        )[:leaders_per_group]

        group["leaders"] = [
            {
                "symbol": stock["symbol"],
                "change_pct": stock["change_pct"],
                "volume": stock["volume"],
                "volume_source": stock["volume_source"],
            }
            for stock in leaders
        ]

        if group["breadth_score"] >= 25:
            group["strength"] = "STRONG"
        elif group["breadth_score"] >= 10:
            group["strength"] = "POSITIVE"
        elif group["breadth_score"] <= -25:
            group["strength"] = "WEAK"
        elif group["breadth_score"] <= -10:
            group["strength"] = "NEGATIVE"
        else:
            group["strength"] = "NEUTRAL"

        result.append(group)

    # Rank groups by breadth first, then average price change.
    result.sort(
        key=lambda x: (
            x["breadth_score"],
            x["avg_change_pct"],
            x["advance_ratio"],
            x["stocks"],
        ),
        reverse=True,
    )

    for rank, group in enumerate(result, start=1):
        group["rank"] = rank

    strong_groups = [
        group for group in result
        if group["breadth_score"] > 0
    ][:top_n]

    weak_groups = sorted(
        [
            group for group in result
            if group["breadth_score"] < 0
        ],
        key=lambda x: (
            x["breadth_score"],
            x["avg_change_pct"],
        ),
    )[:top_n]

    # Compact summaries are intended for the ORCHESTRATOR.
    top_strong_groups = [
        {
            "rank": group["rank"],
            "group": group["group"],
            "icb_code": group["icb_code"],
            "stocks": group["stocks"],
            "advance_ratio": group["advance_ratio"],
            "decline_ratio": group["decline_ratio"],
            "breadth_score": group["breadth_score"],
            "avg_change_pct": group["avg_change_pct"],
            "strength": group["strength"],
            "leaders": group["leaders"],
        }
        for group in strong_groups
    ]

    top_weak_groups = [
        {
            "rank": group["rank"],
            "group": group["group"],
            "icb_code": group["icb_code"],
            "stocks": group["stocks"],
            "advance_ratio": group["advance_ratio"],
            "decline_ratio": group["decline_ratio"],
            "breadth_score": group["breadth_score"],
            "avg_change_pct": group["avg_change_pct"],
            "strength": group["strength"],
            "leaders": group["leaders"],
        }
        for group in weak_groups
    ]

    return {
        "source": "VCI",
        "status": "OK",
        "version": "2.0",
        "icb_level": level,
        "mapping_status": icb["status"],
        "mapping_rows": icb["rows"],
        "mapping_error": icb["error"],
        "universe": len(data["symbols"]),
        "total_batches": data["total_batches"],
        "successful_batches": data["successful_batches"],
        "failed_batches": data["failed_batches"],
        "priced_stocks": sum(
            1 for item in data["prices"] if _extract_stock_row(item)
        ),
        "mapped_symbols": mapped_symbols,
        "unmapped_symbols": unmapped_symbols,
        "groups_count": len(result),
        "top_strong_groups": top_strong_groups,
        "top_weak_groups": top_weak_groups,
        "groups": result,
        "failed_batch_details": data["failed_batch_details"],
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
