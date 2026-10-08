from fastapi import FastAPI, Query, BackgroundTasks
import asyncio
import time
import json
from pathlib import Path
import httpx

from adapters.dnse import fetch_dnse_ohlcv


app = FastAPI(
    title="Investment OS Data Gateway",
    version="5.0.0",
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

RS_CHECKPOINT_PATH = Path("rs_batch_v2_checkpoint.json")


def _save_rs_checkpoint(all_results):
    """Save the current RS batch state to a local JSON checkpoint."""
    payload = {
        "saved_at": time.time(),
        "cache": {
            key: value
            for key, value in RS_CACHE.items()
            if key not in ("rows", "by_symbol")
        },
        "results": all_results,
    }
    try:
        RS_CHECKPOINT_PATH.write_text(
            json.dumps(payload, ensure_ascii=False),
            encoding="utf-8",
        )
        return True
    except Exception:
        return False


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




# ---------------------------------------------------------------------------
# RS O'NEIL ENGINE CACHE
# ---------------------------------------------------------------------------

RS_CACHE = {
    "status": "EMPTY",
    "ranking_status": "NOT_READY",
    "started_at": None,
    "finished_at": None,
    "elapsed_ms": None,
    "requested_symbols": 0,
    "processed_symbols": 0,
    "successful_symbols": 0,
    "eligible_symbols": 0,
    "empty_symbols": 0,
    "insufficient_history_symbols": 0,
    "failed_symbols": 0,
    "days": 600,
    "concurrency": 8,
    "batch_size": 50,
    "current_batch": 0,
    "completed_batches": 0,
    "total_batches": 0,
    "last_batch_status": None,
    "checkpoint_saved_at": None,
    "formula": {
        "3m": 0.40,
        "6m": 0.30,
        "9m": 0.20,
        "12m": 0.10,
    },
    "minimum_candles": 251,
    "rows": [],
    "by_symbol": {},
    "errors": [],
    "history_counts": [],
}

def _period_return(closes, sessions):
    """Percentage change in price over the requested trailing sessions."""
    if len(closes) <= sessions:
        return None

    start_price = _float(closes[-1 - sessions])
    end_price = _float(closes[-1])

    if start_price <= 0 or end_price <= 0:
        return None

    return (end_price / start_price - 1) * 100


def _oneil_price_score(candles):
    """
    Investment OS RS price score, using trailing cumulative price change.

    The requested horizons are independent trailing windows:
      3 months  = price % change over the latest ~63 trading sessions
      6 months  = price % change over the latest ~126 trading sessions
      9 months  = price % change over the latest ~189 trading sessions
      12 months = price % change over the latest ~250 trading sessions

    Weighted P_Score:
      3m  40%
      6m  30%
      9m  20%
      12m 10%

    Minimum history is >250 valid daily candles (>=251), so the full
    12-month trailing return can be calculated. VN-Index is NOT used
    inside the stock RS formula.
    """
    candles = [
        item for item in candles
        if isinstance(item, dict) and _float(item.get("close")) > 0
    ]
    candles = sorted(candles, key=lambda x: x.get("time", 0))

    if len(candles) < 251:
        return None

    closes = [_float(item.get("close")) for item in candles]

    r3 = _period_return(closes, 63)
    r6 = _period_return(closes, 126)
    r9 = _period_return(closes, 189)
    r12 = _period_return(closes, 250)

    if any(value is None for value in (r3, r6, r9, r12)):
        return None

    p_score = (
        0.40 * r3
        + 0.30 * r6
        + 0.20 * r9
        + 0.10 * r12
    )

    return {
        "return_3m_pct": round(r3, 2),
        "return_6m_pct": round(r6, 2),
        "return_9m_pct": round(r9, 2),
        "return_12m_pct": round(r12, 2),
        "p_score": round(p_score, 4),
        "candles": len(candles),
        "first_time": candles[0].get("time"),
        "last_time": candles[-1].get("time"),
    }

def _assign_rs_ratings(rows):
    """
    Convert P_Score cross-section into RS Rating 1-99.
    Highest P_Score receives 99; lowest receives 1.
    """
    ranked = sorted(rows, key=lambda x: x["p_score"])

    n = len(ranked)
    if n == 0:
        return []

    if n == 1:
        ranked[0]["rs_rating"] = 99
        ranked[0]["rs_percentile"] = 100.0
        return ranked

    # Average percentile for exact P_Score ties.
    i = 0
    while i < n:
        j = i
        while (
            j + 1 < n
            and ranked[j + 1]["p_score"] == ranked[i]["p_score"]
        ):
            j += 1

        avg_rank_zero_based = (i + j) / 2
        percentile = avg_rank_zero_based / (n - 1) * 100
        rating = int(round(1 + percentile / 100 * 98))
        rating = max(1, min(99, rating))

        for k in range(i, j + 1):
            ranked[k]["rs_percentile"] = round(percentile, 2)
            ranked[k]["rs_rating"] = rating

        i = j + 1

    return sorted(
        ranked,
        key=lambda x: (
            x["rs_rating"],
            x["p_score"],
        ),
        reverse=True,
    )


async def _fetch_rs_history_one(symbol, semaphore, days):
    async with semaphore:
        started = time.perf_counter()
        try:
            candles = await fetch_dnse_ohlcv(
                symbol=symbol,
                market="stock",
                resolution="1D",
                days=days,
            )
            candles = candles if isinstance(candles, list) else []
            score = _oneil_price_score(candles)

            return {
                "symbol": symbol,
                "status": (
                    "ELIGIBLE"
                    if score is not None
                    else ("INSUFFICIENT_HISTORY" if candles else "EMPTY")
                ),
                "count": len(candles),
                "score": score,
                "elapsed_ms": round(
                    (time.perf_counter() - started) * 1000,
                    1,
                ),
            }
        except Exception as exc:
            return {
                "symbol": symbol,
                "status": "ERROR",
                "count": 0,
                "score": None,
                "elapsed_ms": round(
                    (time.perf_counter() - started) * 1000,
                    1,
                ),
                "error": str(exc),
            }


async def _load_vci_symbols_only():
    """Load the stock universe without fetching the current price board."""
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(
            VCI_SYMBOLS_URL,
            headers=VCI_HEADERS,
        )

    if not response.is_success:
        return {
            "error": f"getAll failed: HTTP {response.status_code}"
        }

    payload = response.json()

    if isinstance(payload, dict):
        universe = payload.get("data", [])
    elif isinstance(payload, list):
        universe = payload
    else:
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
            symbols.append(str(symbol).upper())

    return {
        "symbols": list(dict.fromkeys(symbols))
    }


@app.get("/leaders")
async def leaders(
    top_n: int = Query(
        20,
        ge=5,
        le=50,
        description="Số cổ phiếu dẫn dắt trả về",
    ),
    min_volume: int = Query(
        100000,
        ge=0,
        description="Thanh khoản phiên tối thiểu",
    ),
    min_price: float = Query(
        10000,
        ge=0,
        description="Thị giá tối thiểu",
    ),
):
    """
    Leader V3.

    Locked formula:
      RS Rating O'Neil 40%
      Liquidity percentile 30%
      Group strength 30%

    RS must already exist in RS_CACHE. No VN-Index comparison is used
    in the RS component.
    """
    if RS_CACHE.get("status") not in ("OK", "PARTIAL"):
        return {
            "source": "Investment OS",
            "status": "DATA_INSUFFICIENT",
            "version": "LEADER_V5",
            "reason": "O'Neil RS cache is not ready",
            "next": "Run /rs-refresh and wait for /rs-status",
        }

    data = await _load_vci_stock_prices()

    if "error" in data:
        return {
            "source": "VCI",
            "status": "ERROR",
            "error": data["error"],
        }

    async with httpx.AsyncClient(timeout=30) as client:
        icb = await _load_vci_icb_mapping(client, level=2)

    mapping = icb["mapping"]

    symbol_to_code = {}
    for item in data.get("universe_items", []):
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        code = _extract_icb_code(item)
        if symbol and code:
            symbol_to_code[str(symbol).upper()] = code

    stocks = []
    for item in data["prices"]:
        row = _extract_stock_row(item)
        if not row:
            continue

        symbol = str(row["symbol"]).upper()
        code = symbol_to_code.get(symbol)

        if code:
            group_name = mapping.get(code) or f"ICB {code}"
        else:
            group_name = "UNKNOWN"

        row["symbol"] = symbol
        row["icb_code"] = code
        row["group"] = group_name
        stocks.append(row)

    # Group breadth score from current cross-section.
    group_stats = {}
    for stock in stocks:
        group = stock["group"]
        stats = group_stats.setdefault(
            group,
            {
                "stocks": 0,
                "advances": 0,
                "declines": 0,
            },
        )
        stats["stocks"] += 1
        if stock["change_pct"] > 0:
            stats["advances"] += 1
        elif stock["change_pct"] < 0:
            stats["declines"] += 1

    for stats in group_stats.values():
        count = stats["stocks"]
        stats["breadth_score"] = (
            (stats["advances"] - stats["declines"]) / count * 100
            if count else 0.0
        )
        # Normalize -100..+100 to 0..100.
        stats["group_score"] = max(
            0.0,
            min(100.0, (stats["breadth_score"] + 100) / 2),
        )

    # Liquidity percentile from current session volume.
    volume_stocks = sorted(
        [stock for stock in stocks if stock["volume"] > 0],
        key=lambda x: x["volume"],
    )
    volume_count = len(volume_stocks)
    volume_score = {}

    for rank, stock in enumerate(volume_stocks, start=1):
        if volume_count <= 1:
            percentile = 100.0
        else:
            percentile = (
                (rank - 1) / (volume_count - 1) * 100
            )
        volume_score[stock["symbol"]] = percentile

    rs_map = RS_CACHE.get("by_symbol", {})
    eligible = []

    for stock in stocks:
        rs_row = rs_map.get(stock["symbol"])
        if not rs_row:
            continue

        liquidity_score = volume_score.get(stock["symbol"], 0.0)
        group = group_stats.get(stock["group"], {})
        group_score = group.get("group_score", 0.0)
        rs_rating = rs_row["rs_rating"]

        # Normalize 1..99 RS rating to 0..100 before applying 40%.
        rs_score_100 = (rs_rating - 1) / 98 * 100

        leader_score = (
            0.40 * rs_score_100
            + 0.30 * liquidity_score
            + 0.30 * group_score
        )

        stock["rs_rating"] = rs_rating
        stock["rs_percentile"] = rs_row["rs_percentile"]
        stock["p_score"] = rs_row["p_score"]
        stock["return_3m_pct"] = rs_row["return_3m_pct"]
        stock["return_6m_pct"] = rs_row["return_6m_pct"]
        stock["return_9m_pct"] = rs_row["return_9m_pct"]
        stock["return_12m_pct"] = rs_row["return_12m_pct"]
        stock["liquidity_score"] = round(liquidity_score, 2)
        stock["group_breadth_score"] = round(
            group.get("breadth_score", 0.0),
            2,
        )
        stock["group_score"] = round(group_score, 2)
        stock["leader_score"] = round(leader_score, 2)
        stock["eligible_default"] = (
            stock["price"] >= min_price
            and stock["volume"] >= min_volume
        )

        if stock["eligible_default"]:
            eligible.append(stock)

    eligible.sort(
        key=lambda x: (
            x["leader_score"],
            x["rs_rating"],
            x["liquidity_score"],
        ),
        reverse=True,
    )

    def compact(stock):
        return {
            "symbol": stock["symbol"],
            "price": stock["price"],
            "change_pct": stock["change_pct"],
            "volume": stock["volume"],
            "group": stock["group"],
            "icb_code": stock["icb_code"],
            "rs_rating": stock["rs_rating"],
            "rs_percentile": stock["rs_percentile"],
            "p_score": stock["p_score"],
            "return_3m_pct": stock["return_3m_pct"],
            "return_6m_pct": stock["return_6m_pct"],
            "return_9m_pct": stock["return_9m_pct"],
            "return_12m_pct": stock["return_12m_pct"],
            "liquidity_score": stock["liquidity_score"],
            "group_breadth_score": stock["group_breadth_score"],
            "group_score": stock["group_score"],
            "leader_score": stock["leader_score"],
            "volume_source": stock["volume_source"],
        }

    return {
        "source": "VCI + DNSE",
        "status": "OK",
        "version": "LEADER_V5",
        "weights": {
            "rs_rating": 0.40,
            "liquidity": 0.30,
            "group_strength": 0.30,
        },
        "rs_engine": {
            "version": "RS_BATCH_V2",
            "eligible_symbols": RS_CACHE.get("eligible_symbols", 0),
            "vnindex_in_formula": False,
        },
        "screen": {
            "min_price": min_price,
            "min_volume": min_volume,
        },
        "priced_stocks": len(stocks),
        "eligible_leader_stocks": len(eligible),
        "top_leaders": [
            compact(stock)
            for stock in eligible[:top_n]
        ],
        "mapping_status": icb["status"],
        "mapping_error": icb["error"],
        "failed_batch_details": data["failed_batch_details"],
    }


async def _load_rs_history_batch(
    symbols,
    days=600,
    concurrency=8,
):
    """Fetch daily history and calculate the RS score for one batch."""
    semaphore = asyncio.Semaphore(max(1, min(concurrency, 20)))

    async def fetch_one(symbol):
        async with semaphore:
            started = time.perf_counter()

            try:
                candles = await fetch_dnse_ohlcv(
                    symbol=symbol,
                    market="stock",
                    resolution="1D",
                    days=days,
                )

                elapsed_ms = round(
                    (time.perf_counter() - started) * 1000,
                    1,
                )

                candles = candles if isinstance(candles, list) else []
                candles = [
                    item for item in candles
                    if isinstance(item, dict)
                    and _float(item.get("close")) > 0
                ]
                candles.sort(key=lambda x: x.get("time", 0))

                if not candles:
                    return {
                        "symbol": symbol,
                        "status": "EMPTY",
                        "count": 0,
                        "score": None,
                        "first_time": None,
                        "last_time": None,
                        "elapsed_ms": elapsed_ms,
                    }

                score = _oneil_price_score(candles)

                return {
                    "symbol": symbol,
                    "status": (
                        "ELIGIBLE"
                        if score is not None
                        else "INSUFFICIENT_HISTORY"
                    ),
                    "count": len(candles),
                    "score": score,
                    "first_time": candles[0].get("time"),
                    "last_time": candles[-1].get("time"),
                    "elapsed_ms": elapsed_ms,
                }

            except Exception as exc:
                return {
                    "symbol": symbol,
                    "status": "ERROR",
                    "count": 0,
                    "score": None,
                    "first_time": None,
                    "last_time": None,
                    "elapsed_ms": round(
                        (time.perf_counter() - started) * 1000,
                        1,
                    ),
                    "error": str(exc),
                }

    return await asyncio.gather(
        *(fetch_one(symbol) for symbol in symbols)
    )


async def _load_vci_symbols_only():
    """Load the eligible stock universe without current prices."""
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(
            VCI_SYMBOLS_URL,
            headers=VCI_HEADERS,
        )

    if not response.is_success:
        return {
            "error": f"getAll failed: HTTP {response.status_code}"
        }

    payload = response.json()

    if isinstance(payload, dict):
        universe = payload.get("data", [])
    elif isinstance(payload, list):
        universe = payload
    else:
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
            symbols.append(str(symbol).upper())

    return {"symbols": list(dict.fromkeys(symbols))}


def _build_rs_rows(all_results):
    eligible_rows = []

    for item in all_results:
        if item.get("status") != "ELIGIBLE":
            continue

        score = item.get("score")
        if not score:
            continue

        eligible_rows.append({
            "symbol": item["symbol"],
            "candles": score["candles"],
            "first_time": score["first_time"],
            "last_time": score["last_time"],
            "return_3m_pct": score["return_3m_pct"],
            "return_6m_pct": score["return_6m_pct"],
            "return_9m_pct": score["return_9m_pct"],
            "return_12m_pct": score["return_12m_pct"],
            "p_score": score["p_score"],
        })

    return _assign_rs_ratings(eligible_rows)


async def _refresh_rs_cache(
    days=600,
    concurrency=8,
    limit=100,
    batch_size=50,
):
    """
    Run RS history in explicit batches with in-memory checkpoints.

    The run is intentionally sequential by batch: each batch has limited
    concurrent DNSE requests, then progress/checkpoint state is updated before
    the next batch begins. Final RS percentile/rating is calculated only after
    all requested symbols finish.
    """
    global RS_CACHE

    if RS_CACHE.get("status") == "RUNNING":
        return

    started = time.perf_counter()

    RS_CACHE = {
        "status": "RUNNING",
        "ranking_status": "NOT_READY",
        "started_at": time.time(),
        "finished_at": None,
        "elapsed_ms": None,
        "requested_symbols": 0,
        "processed_symbols": 0,
        "successful_symbols": 0,
        "eligible_symbols": 0,
        "empty_symbols": 0,
        "insufficient_history_symbols": 0,
        "failed_symbols": 0,
        "days": days,
        "concurrency": concurrency,
        "batch_size": batch_size,
        "current_batch": 0,
        "completed_batches": 0,
        "total_batches": 0,
        "last_batch_status": None,
        "checkpoint_saved_at": None,
        "formula": {
            "3m": 0.40,
            "6m": 0.30,
            "9m": 0.20,
            "12m": 0.10,
        },
        "minimum_candles": 251,
        "rows": [],
        "by_symbol": {},
        "errors": [],
        "history_counts": [],
    }

    all_results = []

    try:
        universe = await _load_vci_symbols_only()

        if "error" in universe:
            RS_CACHE["status"] = "ERROR"
            RS_CACHE["ranking_status"] = "ERROR"
            RS_CACHE["errors"] = [universe["error"]]
            return

        symbols = universe.get("symbols", [])
        if limit and limit > 0:
            symbols = symbols[:limit]

        RS_CACHE["requested_symbols"] = len(symbols)

        if not symbols:
            RS_CACHE["status"] = "ERROR"
            RS_CACHE["ranking_status"] = "ERROR"
            RS_CACHE["errors"] = ["No eligible stock symbols found"]
            return

        batches = [
            symbols[i:i + batch_size]
            for i in range(0, len(symbols), batch_size)
        ]
        RS_CACHE["total_batches"] = len(batches)

        for batch_number, batch in enumerate(batches, start=1):
            RS_CACHE["current_batch"] = batch_number
            RS_CACHE["last_batch_status"] = "RUNNING"

            results = await _load_rs_history_batch(
                symbols=batch,
                days=days,
                concurrency=concurrency,
            )
            all_results.extend(results)

            RS_CACHE["processed_symbols"] = len(all_results)
            RS_CACHE["completed_batches"] = batch_number
            RS_CACHE["last_batch_status"] = "OK"
            checkpoint_ok = _save_rs_checkpoint(all_results)
            RS_CACHE["checkpoint_saved_at"] = (
                time.time() if checkpoint_ok else None
            )

            RS_CACHE["successful_symbols"] = sum(
                1 for item in all_results
                if item.get("status") in (
                    "ELIGIBLE",
                    "INSUFFICIENT_HISTORY",
                )
            )
            RS_CACHE["eligible_symbols"] = sum(
                1 for item in all_results
                if item.get("status") == "ELIGIBLE"
            )
            RS_CACHE["empty_symbols"] = sum(
                1 for item in all_results
                if item.get("status") == "EMPTY"
            )
            RS_CACHE["insufficient_history_symbols"] = sum(
                1 for item in all_results
                if item.get("status") == "INSUFFICIENT_HISTORY"
            )
            RS_CACHE["failed_symbols"] = sum(
                1 for item in all_results
                if item.get("status") == "ERROR"
            )
            RS_CACHE["history_counts"] = [
                item["count"]
                for item in all_results
                if item.get("count", 0) > 0
            ]

            # Monitoring ranking only; final percentile is recomputed after
            # the final batch, using the complete eligible cross-section.
            partial_rows = _build_rs_rows(all_results)
            RS_CACHE["rows"] = partial_rows
            RS_CACHE["by_symbol"] = {
                row["symbol"]: row for row in partial_rows
            }
            RS_CACHE["ranking_status"] = (
                "FINAL" if batch_number == len(batches) else "PARTIAL"
            )

            await asyncio.sleep(0)

        RS_CACHE["errors"] = [
            {
                "symbol": item["symbol"],
                "error": item.get("error"),
            }
            for item in all_results
            if item.get("status") == "ERROR"
        ][:50]

        # Authoritative final ranking over the complete eligible universe.
        final_rows = _build_rs_rows(all_results)
        RS_CACHE["rows"] = final_rows
        RS_CACHE["by_symbol"] = {
            row["symbol"]: row for row in final_rows
        }
        RS_CACHE["eligible_symbols"] = len(final_rows)
        RS_CACHE["ranking_status"] = "FINAL"
        RS_CACHE["status"] = (
            "OK" if RS_CACHE["failed_symbols"] == 0 else "PARTIAL"
        )

    except Exception as exc:
        RS_CACHE["status"] = "ERROR"
        RS_CACHE["ranking_status"] = "ERROR"
        RS_CACHE["last_batch_status"] = "ERROR"
        RS_CACHE["errors"] = [str(exc)]

    finally:
        RS_CACHE["finished_at"] = time.time()
        RS_CACHE["elapsed_ms"] = round(
            (time.perf_counter() - started) * 1000,
            1,
        )


@app.get("/rs-refresh")
async def rs_refresh(
    background_tasks: BackgroundTasks,
    days: int = Query(
        600, ge=400, le=900,
        description="Ngày lịch sử DNSE; mặc định 600",
    ),
    concurrency: int = Query(
        8, ge=1, le=12,
        description="Số request DNSE đồng thời",
    ),
    limit: int = Query(
        100, ge=0, le=2000,
        description="Mặc định 100 mã; 0 = toàn bộ universe",
    ),
    batch_size: int = Query(
        50, ge=25, le=100,
        description="Số mã mỗi batch",
    ),
):
    """Start the checkpointed RS Batch V2 engine."""
    if RS_CACHE.get("status") == "RUNNING":
        return {
            "status": "RUNNING",
            "message": "RS Batch V2 is already running",
            "requested_symbols": RS_CACHE.get("requested_symbols", 0),
            "processed_symbols": RS_CACHE.get("processed_symbols", 0),
            "current_batch": RS_CACHE.get("current_batch", 0),
            "completed_batches": RS_CACHE.get("completed_batches", 0),
            "total_batches": RS_CACHE.get("total_batches", 0),
        }

    background_tasks.add_task(
        _refresh_rs_cache,
        days, concurrency, limit, batch_size,
    )

    return {
        "status": "STARTED",
        "engine": "RS Batch V2",
        "formula": {
            "3m": "40% × % price change in latest ~63 sessions",
            "6m": "30% × % price change in latest ~126 sessions",
            "9m": "20% × % price change in latest ~189 sessions",
            "12m": "10% × % price change in latest ~250 sessions",
        },
        "minimum_candles": 251,
        "vnindex_in_formula": False,
        "days": days,
        "concurrency": concurrency,
        "limit": limit,
        "batch_size": batch_size,
        "next": "Check /rs-status",
    }


@app.get("/rs-status")
async def rs_status():
    processed = RS_CACHE.get("processed_symbols", 0)
    requested = RS_CACHE.get("requested_symbols", 0)

    return {
        "status": RS_CACHE.get("status"),
        "ranking_status": RS_CACHE.get("ranking_status"),
        "requested_symbols": requested,
        "processed_symbols": processed,
        "progress_pct": round(processed / requested * 100, 2) if requested else 0,
        "successful_symbols": RS_CACHE.get("successful_symbols", 0),
        "eligible_symbols": RS_CACHE.get("eligible_symbols", 0),
        "empty_symbols": RS_CACHE.get("empty_symbols", 0),
        "insufficient_history_symbols": RS_CACHE.get("insufficient_history_symbols", 0),
        "failed_symbols": RS_CACHE.get("failed_symbols", 0),
        "days": RS_CACHE.get("days"),
        "concurrency": RS_CACHE.get("concurrency"),
        "batch_size": RS_CACHE.get("batch_size"),
        "current_batch": RS_CACHE.get("current_batch", 0),
        "completed_batches": RS_CACHE.get("completed_batches", 0),
        "total_batches": RS_CACHE.get("total_batches", 0),
        "last_batch_status": RS_CACHE.get("last_batch_status"),
        "checkpoint_saved_at": RS_CACHE.get("checkpoint_saved_at"),
        "checkpoint_file": str(RS_CHECKPOINT_PATH),
        "minimum_candles": RS_CACHE.get("minimum_candles", 251),
        "formula": RS_CACHE.get("formula"),
        "vnindex_in_formula": False,
        "elapsed_ms": RS_CACHE.get("elapsed_ms"),
        "errors": RS_CACHE.get("errors", [])[:10],
    }


@app.get("/rs-reset")
async def rs_reset():
    global RS_CACHE
    if RS_CACHE.get("status") == "RUNNING":
        return {
            "status": "RUNNING",
            "message": "Cannot reset while RS Batch V2 is running",
        }
    try:
        RS_CHECKPOINT_PATH.unlink(missing_ok=True)
    except Exception:
        pass
    RS_CACHE = {
        "status": "EMPTY",
        "ranking_status": "NOT_READY",
        "started_at": None,
        "finished_at": None,
        "elapsed_ms": None,
        "requested_symbols": 0,
        "processed_symbols": 0,
        "successful_symbols": 0,
        "eligible_symbols": 0,
        "empty_symbols": 0,
        "insufficient_history_symbols": 0,
        "failed_symbols": 0,
        "days": 600,
        "concurrency": 8,
        "batch_size": 50,
        "current_batch": 0,
        "completed_batches": 0,
        "total_batches": 0,
        "last_batch_status": None,
        "checkpoint_saved_at": None,
        "formula": {"3m": 0.40, "6m": 0.30, "9m": 0.20, "12m": 0.10},
        "minimum_candles": 251,
        "rows": [],
        "by_symbol": {},
        "errors": [],
        "history_counts": [],
    }
    return {"status": "RESET", "engine": "RS Batch V2"}


@app.get("/rs")
async def rs_ranking(
    top_n: int = Query(50, ge=5, le=200, description="Số mã RS cao nhất trả về"),
):
    """Return cached RS Batch V2 ranking."""
    status = RS_CACHE.get("status")
    if status not in ("OK", "PARTIAL"):
        return {
            "status": status,
            "ranking_status": RS_CACHE.get("ranking_status"),
            "message": "RS ranking is not ready. Run /rs-refresh and wait for /rs-status.",
            "eligible_symbols": RS_CACHE.get("eligible_symbols", 0),
        }

    return {
        "source": "VCI universe + DNSE history",
        "status": status,
        "ranking_status": RS_CACHE.get("ranking_status"),
        "version": "RS_BATCH_V2",
        "formula": {
            "3m": "40%",
            "6m": "30%",
            "9m": "20%",
            "12m": "10%",
        },
        "definition": {
            "3m": "% change in price over latest ~63 trading sessions",
            "6m": "% change in price over latest ~126 trading sessions",
            "9m": "% change in price over latest ~189 trading sessions",
            "12m": "% change in price over latest ~250 trading sessions",
        },
        "minimum_candles": 251,
        "vnindex_in_formula": False,
        "requested_symbols": RS_CACHE.get("requested_symbols", 0),
        "successful_symbols": RS_CACHE.get("successful_symbols", 0),
        "eligible_symbols": RS_CACHE.get("eligible_symbols", 0),
        "empty_symbols": RS_CACHE.get("empty_symbols", 0),
        "insufficient_history_symbols": RS_CACHE.get("insufficient_history_symbols", 0),
        "failed_symbols": RS_CACHE.get("failed_symbols", 0),
        "elapsed_ms": RS_CACHE.get("elapsed_ms"),
        "top_rs": RS_CACHE.get("rows", [])[:top_n],
    }


@app.get("/rs-benchmark")
async def rs_benchmark():
    """Validate the two mandatory long-history benchmarks used by the OS."""
    vnindex = await fetch_dnse_ohlcv(
        symbol="VNINDEX", market="index", resolution="1D", days=600
    )
    vcb = await fetch_dnse_ohlcv(
        symbol="VCB", market="stock", resolution="1D", days=600
    )

    vnindex_count = len(vnindex) if isinstance(vnindex, list) else 0
    vcb_count = len(vcb) if isinstance(vcb, list) else 0

    return {
        "source": "DNSE",
        "status": "OK",
        "minimum_required_candles": 251,
        "vnindex": {
            "count": vnindex_count,
            "eligible": vnindex_count >= 251,
            "first_time": vnindex[0].get("time") if vnindex else None,
            "last_time": vnindex[-1].get("time") if vnindex else None,
        },
        "vcb": {
            "count": vcb_count,
            "eligible": vcb_count >= 251,
            "first_time": vcb[0].get("time") if vcb else None,
            "last_time": vcb[-1].get("time") if vcb else None,
        },
        "vnindex_in_stock_rs_formula": False,
    }


@app.get("/rs-history-test")
async def rs_history_test(
    limit: int = Query(
        20,
        ge=5,
        le=100,
        description="Số mã dùng để benchmark lấy lịch sử RS",
    ),
    days: int = Query(
        600,
        ge=300,
        le=900,
        description="Số ngày lịch sử cần lấy; 600 là mặc định để có đủ ~12 tháng giao dịch",
    ),
    concurrency: int = Query(
        8,
        ge=1,
        le=20,
        description="Số request lịch sử chạy đồng thời",
    ),
):
    """Benchmark the bulk historical-data mechanism before full-market RS.

    It uses the VCI universe to select stock symbols, then fetches DNSE daily
    history concurrently. The endpoint intentionally stops at data validation;
    no RS score/ranking is produced here.
    """
    started = time.perf_counter()

    data = await _load_vci_stock_prices()

    if "error" in data:
        return {
            "source": "VCI + DNSE",
            "status": "ERROR",
            "stage": "universe",
            "error": data["error"],
        }

    symbols = data.get("symbols", [])[:limit]
    results = await _load_rs_history_batch(
        symbols=symbols,
        days=days,
        concurrency=concurrency,
    )

    successful = [
        item for item in results
        if item["status"] in ("ELIGIBLE", "INSUFFICIENT_HISTORY")
    ]
    empty = [item for item in results if item["status"] == "EMPTY"]
    errors = [item for item in results if item["status"] == "ERROR"]

    counts = [item["count"] for item in successful]
    elapsed_ms = round((time.perf_counter() - started) * 1000, 1)

    return {
        "source": "VCI + DNSE",
        "status": "OK" if not errors else "PARTIAL",
        "purpose": "RS Batch V2 history benchmark; >250-session eligibility gate",
        "requested_symbols": len(symbols),
        "successful_symbols": len(successful),
        "empty_symbols": len(empty),
        "failed_symbols": len(errors),
        "history_days_requested": days,
        "concurrency": concurrency,
        "min_candles": min(counts) if counts else 0,
        "max_candles": max(counts) if counts else 0,
        "avg_candles": round(sum(counts) / len(counts), 2) if counts else 0,
        "minimum_eligible_candles": 251,
        "symbols_below_251": sum(1 for count in counts if count < 251),
        "symbols_at_least_251": sum(1 for count in counts if count >= 251),
        "elapsed_ms": elapsed_ms,
        "results": results,
    }
