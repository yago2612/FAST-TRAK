import base64
import hashlib
import hmac
import html
import json
import os
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http import cookies
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


APP_NAME = "Simple Tracker Whale"
INFO_URL = os.getenv("HYPERLIQUID_INFO_URL", "https://api.hyperliquid.xyz/info")
DB_PATH = os.getenv("DATABASE_PATH", os.path.join("data", "hyper_whales.sqlite3"))
MIN_ACCOUNT_VALUE = float(os.getenv("MIN_ACCOUNT_VALUE", "250000"))
DISCOVERY_COINS = os.getenv("DISCOVERY_COINS", "BTC,ETH,SOL,HYPE,ETHFI")
DISCOVERY_SECONDS = int(os.getenv("DISCOVERY_SECONDS", "25"))
DISCOVERY_MAX_CANDIDATES = int(os.getenv("DISCOVERY_MAX_CANDIDATES", "80"))
DISCOVERY_MIN_TRADE_NOTIONAL = float(os.getenv("DISCOVERY_MIN_TRADE_NOTIONAL", "25000"))
WS_URL = os.getenv("HYPERLIQUID_WS_URL", "wss://api.hyperliquid.xyz/ws")
AUTO_DISCOVERY_ENABLED = os.getenv("AUTO_DISCOVERY_ENABLED", "1") == "1"
AUTO_DISCOVERY_INTERVAL = int(os.getenv("AUTO_DISCOVERY_INTERVAL", "180"))
AUTO_REFRESH_INTERVAL = float(os.getenv("AUTO_REFRESH_INTERVAL", "5"))
AUTO_REFRESH_BATCH = int(os.getenv("AUTO_REFRESH_BATCH", "5"))
POSITION_EVENT_EPSILON = float(os.getenv("POSITION_EVENT_EPSILON", "0.00000001"))
POSITION_CLOSE_CONFIRMATIONS = int(os.getenv("POSITION_CLOSE_CONFIRMATIONS", "2"))
MARK_WS_ENABLED = os.getenv("MARK_WS_ENABLED", "1") == "1"
PRICE_UI_REFRESH_MS = int(os.getenv("PRICE_UI_REFRESH_MS", "200"))
FILL_SYNC_ENABLED = os.getenv("FILL_SYNC_ENABLED", "1") == "1"
FILL_SYNC_INTERVAL = int(os.getenv("FILL_SYNC_INTERVAL", "60"))
FILL_SYNC_BATCH = int(os.getenv("FILL_SYNC_BATCH", "3"))
FILL_LOOKBACK_DAYS = int(os.getenv("FILL_LOOKBACK_DAYS", "14"))
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin")
SECRET_KEY = os.getenv("SECRET_KEY", "change-me-for-production")
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
AUTO_STATUS = {
    "started": False,
    "mark_started": False,
    "last_refresh": "",
    "last_discovery": "",
    "last_mark": "",
    "last_fill_sync": "",
    "last_error": "",
}
MARK_PRICE_CACHE = {"prices": {}, "updated_at": 0.0, "source": ""}
MARK_PRICE_LOCK = threading.Lock()


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def usd(value):
    try:
        value = float(value or 0)
    except (TypeError, ValueError):
        value = 0
    if abs(value) >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if abs(value) >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:,.2f}"


def full_usd(value):
    try:
        value = float(value or 0)
    except (TypeError, ValueError):
        value = 0
    return f"${value:,.2f}"


def pct(value, decimals=1):
    try:
        return f"{float(value) * 100:.{decimals}f}%"
    except (TypeError, ValueError):
        return f"{0:.{decimals}f}%"


def price(value):
    try:
        value = float(value or 0)
    except (TypeError, ValueError):
        value = 0
    absolute = abs(value)
    if absolute >= 100:
        decimals = 2
    elif absolute >= 1:
        decimals = 4
    elif absolute >= 0.01:
        decimals = 6
    else:
        decimals = 8
    return f"${value:,.{decimals}f}"


def price_or_dash(value):
    try:
        value = float(value or 0)
    except (TypeError, ValueError):
        value = 0
    return "-" if value <= 0 else price(value)


def signed_usd(value):
    try:
        value = float(value or 0)
    except (TypeError, ValueError):
        value = 0
    sign = "+" if value > 0 else ""
    cls = "num-positive" if value > 0 else "num-negative" if value < 0 else "num-neutral"
    return f'<span class="{cls}">{sign}{usd(abs(value))}</span>'


def signed_full_usd(value):
    try:
        value = float(value or 0)
    except (TypeError, ValueError):
        value = 0
    sign = "+" if value > 0 else ""
    cls = "num-positive" if value > 0 else "num-negative" if value < 0 else "num-neutral"
    return f'<span class="{cls}">{sign}{full_usd(abs(value))}</span>'


def signed_pct(value, decimals=2):
    try:
        value = float(value or 0)
    except (TypeError, ValueError):
        value = 0
    sign = "+" if value > 0 else ""
    cls = "num-positive" if value > 0 else "num-negative" if value < 0 else "num-neutral"
    return f'<span class="{cls}">{sign}{pct(abs(value), decimals)}</span>'


def short_addr(address):
    if not address:
        return ""
    return f"{address[:6]}...{address[-4:]}"


def to_float(value, default=0.0):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def connect_db():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with connect_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS wallets (
                address TEXT PRIMARY KEY,
                alias TEXT NOT NULL DEFAULT '',
                account_value REAL NOT NULL DEFAULT 0,
                total_ntl_pos REAL NOT NULL DEFAULT 0,
                total_raw_usd REAL NOT NULL DEFAULT 0,
                margin_used REAL NOT NULL DEFAULT 0,
                withdrawable REAL NOT NULL DEFAULT 0,
                active_positions INTEGER NOT NULL DEFAULT 0,
                long_value REAL NOT NULL DEFAULT 0,
                short_value REAL NOT NULL DEFAULT 0,
                gross_exposure REAL NOT NULL DEFAULT 0,
                net_exposure REAL NOT NULL DEFAULT 0,
                direction_bias TEXT NOT NULL DEFAULT 'Neutral',
                diversification_score REAL NOT NULL DEFAULT 0,
                top_coin TEXT,
                source TEXT,
                last_seen TEXT NOT NULL,
                raw_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address TEXT NOT NULL,
                coin TEXT NOT NULL,
                side TEXT NOT NULL,
                size REAL NOT NULL DEFAULT 0,
                entry_px REAL NOT NULL DEFAULT 0,
                current_px REAL NOT NULL DEFAULT 0,
                position_value REAL NOT NULL DEFAULT 0,
                capital_used REAL NOT NULL DEFAULT 0,
                unrealized_pnl REAL NOT NULL DEFAULT 0,
                roi_price REAL NOT NULL DEFAULT 0,
                roi_capital REAL NOT NULL DEFAULT 0,
                return_on_equity REAL NOT NULL DEFAULT 0,
                leverage TEXT,
                leverage_value REAL NOT NULL DEFAULT 0,
                liquidation_px REAL NOT NULL DEFAULT 0,
                margin_used REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(wallet_address) REFERENCES wallets(address) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS wallet_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address TEXT NOT NULL,
                account_value REAL NOT NULL DEFAULT 0,
                total_ntl_pos REAL NOT NULL DEFAULT 0,
                gross_exposure REAL NOT NULL DEFAULT 0,
                long_value REAL NOT NULL DEFAULT 0,
                short_value REAL NOT NULL DEFAULT 0,
                active_positions INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS scan_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                scanned_count INTEGER NOT NULL DEFAULT 0,
                saved_count INTEGER NOT NULL DEFAULT 0,
                discovered_count INTEGER NOT NULL DEFAULT 0,
                errors TEXT
            );

            CREATE TABLE IF NOT EXISTS position_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address TEXT NOT NULL,
                event_type TEXT NOT NULL,
                coin TEXT NOT NULL,
                side TEXT NOT NULL,
                size REAL NOT NULL DEFAULT 0,
                entry_px REAL NOT NULL DEFAULT 0,
                current_px REAL NOT NULL DEFAULT 0,
                notional REAL NOT NULL DEFAULT 0,
                margin_used REAL NOT NULL DEFAULT 0,
                unrealized_pnl REAL NOT NULL DEFAULT 0,
                source TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS position_state (
                wallet_address TEXT NOT NULL,
                coin TEXT NOT NULL,
                side TEXT NOT NULL,
                size REAL NOT NULL DEFAULT 0,
                entry_px REAL NOT NULL DEFAULT 0,
                current_px REAL NOT NULL DEFAULT 0,
                position_value REAL NOT NULL DEFAULT 0,
                capital_used REAL NOT NULL DEFAULT 0,
                unrealized_pnl REAL NOT NULL DEFAULT 0,
                missing_count INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(wallet_address, coin, side)
            );

            CREATE TABLE IF NOT EXISTS fills (
                fill_id TEXT PRIMARY KEY,
                wallet_address TEXT NOT NULL,
                tid TEXT,
                oid TEXT,
                coin TEXT NOT NULL,
                dir TEXT NOT NULL,
                side TEXT,
                px REAL NOT NULL DEFAULT 0,
                size REAL NOT NULL DEFAULT 0,
                start_position REAL NOT NULL DEFAULT 0,
                closed_pnl REAL NOT NULL DEFAULT 0,
                fee REAL NOT NULL DEFAULT 0,
                builder_fee REAL NOT NULL DEFAULT 0,
                fee_token TEXT,
                crossed INTEGER NOT NULL DEFAULT 0,
                hash TEXT,
                time_ms INTEGER NOT NULL,
                raw_json TEXT NOT NULL,
                synced_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS fill_sync_state (
                wallet_address TEXT PRIMARY KEY,
                last_time_ms INTEGER NOT NULL DEFAULT 0,
                synced_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trade_episodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address TEXT NOT NULL,
                coin TEXT NOT NULL,
                side TEXT NOT NULL,
                status TEXT NOT NULL,
                opened_at_ms INTEGER,
                closed_at_ms INTEGER,
                open_size REAL NOT NULL DEFAULT 0,
                close_size REAL NOT NULL DEFAULT 0,
                avg_entry_px REAL NOT NULL DEFAULT 0,
                avg_close_px REAL NOT NULL DEFAULT 0,
                gross_pnl REAL NOT NULL DEFAULT 0,
                fees REAL NOT NULL DEFAULT 0,
                net_pnl REAL NOT NULL DEFAULT 0,
                fill_count INTEGER NOT NULL DEFAULT 0,
                maker_fills INTEGER NOT NULL DEFAULT 0,
                taker_fills INTEGER NOT NULL DEFAULT 0,
                rebuilt_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_fills_wallet_time
                ON fills(wallet_address, time_ms);

            CREATE INDEX IF NOT EXISTS idx_trade_episodes_wallet_closed
                ON trade_episodes(wallet_address, status, closed_at_ms);
            """
        )
        ensure_column(conn, "wallets", "alias", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "positions", "current_px", "REAL NOT NULL DEFAULT 0")
        ensure_column(conn, "positions", "capital_used", "REAL NOT NULL DEFAULT 0")
        ensure_column(conn, "positions", "roi_price", "REAL NOT NULL DEFAULT 0")
        ensure_column(conn, "positions", "roi_capital", "REAL NOT NULL DEFAULT 0")
        ensure_column(conn, "positions", "leverage_value", "REAL NOT NULL DEFAULT 0")


def ensure_column(conn, table, column, definition):
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def sign_payload(payload):
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    sig = hmac.new(SECRET_KEY.encode(), encoded.encode(), hashlib.sha256).hexdigest()
    return f"{encoded}.{sig}"


def read_session(cookie_header):
    if not cookie_header:
        return {}
    jar = cookies.SimpleCookie()
    jar.load(cookie_header)
    morsel = jar.get("session")
    if not morsel:
        return {}
    try:
        encoded, sig = morsel.value.rsplit(".", 1)
        expected = hmac.new(SECRET_KEY.encode(), encoded.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return {}
        return json.loads(base64.urlsafe_b64decode(encoded.encode()).decode())
    except Exception:
        return {}


def hyperliquid_info(payload):
    body = json.dumps(payload).encode()
    request = urllib.request.Request(
        INFO_URL,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "simple-tracker-whale/1.0"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode())


def update_mark_price_cache(prices, source):
    if not prices:
        return
    cleaned = {str(coin): to_float(price) for coin, price in prices.items() if to_float(price) > 0}
    if not cleaned:
        return
    with MARK_PRICE_LOCK:
        MARK_PRICE_CACHE["prices"].update(cleaned)
        MARK_PRICE_CACHE["updated_at"] = time.time()
        MARK_PRICE_CACHE["source"] = source
    AUTO_STATUS["last_mark"] = f"{now_iso()} | {len(cleaned)} markets | {source}"


def get_cached_mark_prices():
    with MARK_PRICE_LOCK:
        return dict(MARK_PRICE_CACHE["prices"]), MARK_PRICE_CACHE["updated_at"], MARK_PRICE_CACHE["source"]


def fetch_mark_prices():
    try:
        data = hyperliquid_info({"type": "metaAndAssetCtxs"})
    except Exception:
        prices, _, _ = get_cached_mark_prices()
        return prices
    if not isinstance(data, list) or len(data) < 2:
        return {}
    meta, asset_contexts = data[0], data[1]
    universe = (meta or {}).get("universe") or []
    prices = {}
    for asset, context in zip(universe, asset_contexts or []):
        coin = asset.get("name") if isinstance(asset, dict) else None
        if not coin or not isinstance(context, dict):
            continue
        mark_px = to_float(context.get("markPx"))
        if mark_px > 0:
            prices[str(coin)] = mark_px
    update_mark_price_cache(prices, "rest")
    return prices


def mark_price_worker():
    if not MARK_WS_ENABLED:
        return
    AUTO_STATUS["mark_started"] = True
    while True:
        ws = None
        try:
            from websocket import WebSocketTimeoutException, create_connection

            ws = create_connection(WS_URL, timeout=10)
            ws.settimeout(10)
            ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}}))
            while True:
                try:
                    message = json.loads(ws.recv())
                except WebSocketTimeoutException:
                    continue
                channel = message.get("channel")
                data = message.get("data") or {}
                if channel == "allMids" and isinstance(data, dict):
                    mids = data.get("mids") if isinstance(data.get("mids"), dict) else data
                    update_mark_price_cache(mids, "ws")
        except Exception as exc:
            AUTO_STATUS["last_error"] = f"mark ws: {exc}"
            try:
                fetch_mark_prices()
            except Exception:
                pass
            time.sleep(3)
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass


def start_mark_price_worker():
    if AUTO_STATUS["mark_started"]:
        return
    thread = threading.Thread(target=mark_price_worker, name="mark-price-worker", daemon=True)
    thread.start()


def discover_wallets_from_live_trades(coins_text="", seconds=None, max_candidates=None, min_notional=None):
    coins = [coin.strip().upper() for coin in (coins_text or DISCOVERY_COINS).split(",") if coin.strip()]
    seconds = max(3, min(int(seconds or DISCOVERY_SECONDS), 180))
    max_candidates = max(1, min(int(max_candidates or DISCOVERY_MAX_CANDIDATES), 500))
    min_notional = float(min_notional if min_notional not in (None, "") else DISCOVERY_MIN_TRADE_NOTIONAL)
    stats = {}

    if not coins:
        return [], {"trades": 0, "coins": [], "errors": ["No hay coins configuradas para escuchar trades."]}

    try:
        from websocket import WebSocketTimeoutException, create_connection
    except ImportError:
        return [], {
            "trades": 0,
            "coins": coins,
            "errors": ["Falta instalar websocket-client para descubrir wallets desde trades."],
        }

    errors = []
    trade_count = 0
    ws = None
    try:
        ws = create_connection(WS_URL, timeout=5)
        ws.settimeout(2)
        for coin in coins:
            ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": coin}}))

        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                message = json.loads(ws.recv())
            except WebSocketTimeoutException:
                continue
            except Exception as exc:
                errors.append(f"websocket: {exc}")
                break

            if message.get("channel") != "trades":
                continue
            trades = message.get("data") or []
            if isinstance(trades, dict):
                trades = [trades]
            for trade in trades:
                users = trade.get("users") or []
                if len(users) != 2:
                    continue
                notional = abs(to_float(trade.get("px")) * to_float(trade.get("sz")))
                if notional < min_notional:
                    continue
                trade_count += 1
                for address in users:
                    address = str(address or "").lower()
                    if not ADDRESS_RE.match(address):
                        continue
                    item = stats.setdefault(
                        address,
                        {
                            "address": address,
                            "notional": 0.0,
                            "trades": 0,
                            "coins": set(),
                            "last_seen": 0,
                        },
                    )
                    item["notional"] += notional
                    item["trades"] += 1
                    item["coins"].add(str(trade.get("coin") or ""))
                    item["last_seen"] = max(item["last_seen"], int(trade.get("time") or 0))
    except Exception as exc:
        errors.append(f"websocket connect: {exc}")
    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    ranked = sorted(stats.values(), key=lambda item: (item["notional"], item["trades"]), reverse=True)
    candidates = [item["address"] for item in ranked[:max_candidates]]
    summary = {
        "trades": trade_count,
        "coins": coins,
        "errors": errors,
        "candidates": len(candidates),
        "top_notional": ranked[0]["notional"] if ranked else 0,
    }
    return candidates, summary


def extract_addresses(text):
    found = set()
    for match in re.findall(r"0x[a-fA-F0-9]{40}", text or ""):
        found.add(match.lower())
    return sorted(found)


def load_seed_wallets(extra_text=""):
    seeds = set(extract_addresses(extra_text))
    env_seeds = os.getenv("HYPERLIQUID_SEED_WALLETS", "")
    seeds.update(extract_addresses(env_seeds))
    seed_path = os.path.join("data", "seed_wallets.txt")
    if os.path.exists(seed_path):
        with open(seed_path, "r", encoding="utf-8") as handle:
            seeds.update(extract_addresses(handle.read()))
    return sorted(seeds)


def parse_wallet(address, state, source, mark_prices=None):
    mark_prices = mark_prices or {}
    margin = state.get("marginSummary") or {}
    cross = state.get("crossMarginSummary") or {}
    account_value = to_float(margin.get("accountValue", cross.get("accountValue")))
    total_ntl_pos = abs(to_float(margin.get("totalNtlPos", cross.get("totalNtlPos"))))
    total_raw_usd = to_float(margin.get("totalRawUsd", cross.get("totalRawUsd")))
    margin_used = to_float(margin.get("totalMarginUsed", cross.get("totalMarginUsed")))
    withdrawable = to_float(state.get("withdrawable"))

    parsed_positions = []
    long_value = 0.0
    short_value = 0.0
    coin_values = {}

    for item in state.get("assetPositions") or []:
        position = item.get("position") or {}
        coin = str(position.get("coin") or "UNKNOWN")
        size = to_float(position.get("szi"))
        entry_px = to_float(position.get("entryPx"))
        api_position_value = abs(to_float(position.get("positionValue")))
        current_px = to_float(mark_prices.get(coin))
        if current_px <= 0 and size:
            current_px = api_position_value / abs(size) if api_position_value > 0 else 0.0
        position_value = abs(size * current_px) if current_px > 0 and size else api_position_value
        if position_value <= 0 and size:
            position_value = abs(size * entry_px)
        side = "Long" if size > 0 else "Short" if size < 0 else "Flat"
        if side == "Long":
            long_value += position_value
        elif side == "Short":
            short_value += position_value
        coin_values[coin] = coin_values.get(coin, 0.0) + position_value
        leverage = position.get("leverage") or {}
        leverage_value = 0.0
        if isinstance(leverage, dict):
            leverage_value = to_float(leverage.get("value"))
            leverage_label = f"{leverage.get('type', 'cross')} {leverage.get('value', '')}x".strip()
        else:
            leverage_label = str(leverage or "")
            match = re.search(r"([0-9]+(?:\.[0-9]+)?)", leverage_label)
            leverage_value = to_float(match.group(1)) if match else 0.0
        margin_used_pos = to_float(position.get("marginUsed"))
        capital_used = margin_used_pos if margin_used_pos > 0 else position_value / leverage_value if leverage_value > 0 else 0.0
        unrealized_pnl = to_float(position.get("unrealizedPnl"))
        if entry_px > 0 and current_px > 0 and side == "Long":
            roi_price = (current_px - entry_px) / entry_px
        elif entry_px > 0 and current_px > 0 and side == "Short":
            roi_price = (entry_px - current_px) / entry_px
        else:
            roi_price = 0.0
        roi_capital = unrealized_pnl / capital_used if capital_used else 0.0
        parsed_positions.append(
            {
                "coin": coin,
                "side": side,
                "size": size,
                "entry_px": entry_px,
                "current_px": current_px,
                "position_value": position_value,
                "capital_used": capital_used,
                "unrealized_pnl": unrealized_pnl,
                "roi_price": roi_price,
                "roi_capital": roi_capital,
                "return_on_equity": to_float(position.get("returnOnEquity")),
                "leverage": leverage_label,
                "leverage_value": leverage_value,
                "liquidation_px": to_float(position.get("liquidationPx")),
                "margin_used": margin_used_pos,
            }
        )

    gross_exposure = long_value + short_value
    net_exposure = long_value - short_value
    bias_ratio = net_exposure / gross_exposure if gross_exposure else 0
    if bias_ratio > 0.2:
        direction_bias = "Alcista"
    elif bias_ratio < -0.2:
        direction_bias = "Bajista"
    else:
        direction_bias = "Neutral"

    top_coin = None
    top_coin_value = 0.0
    if coin_values:
        top_coin, top_coin_value = max(coin_values.items(), key=lambda item: item[1])
    concentration = top_coin_value / gross_exposure if gross_exposure else 0
    diversification_score = max(0.0, min(1.0, 1.0 - concentration))

    return {
        "address": address.lower(),
        "account_value": account_value,
        "total_ntl_pos": total_ntl_pos,
        "total_raw_usd": total_raw_usd,
        "margin_used": margin_used,
        "withdrawable": withdrawable,
        "active_positions": len([p for p in parsed_positions if p["side"] != "Flat"]),
        "long_value": long_value,
        "short_value": short_value,
        "gross_exposure": gross_exposure or total_ntl_pos,
        "net_exposure": net_exposure,
        "direction_bias": direction_bias,
        "diversification_score": diversification_score,
        "top_coin": top_coin,
        "source": source,
        "positions": parsed_positions,
        "raw_json": json.dumps(state, separators=(",", ":")),
    }


def save_wallet(wallet):
    seen = now_iso()
    with connect_db() as conn:
        existed = conn.execute("SELECT 1 FROM wallets WHERE address = ?", (wallet["address"],)).fetchone() is not None
        previous_positions = conn.execute(
            """
            SELECT coin, side, size, entry_px, current_px, position_value,
                   capital_used, unrealized_pnl
            FROM positions
            WHERE wallet_address = ?
            """,
            (wallet["address"],),
        ).fetchall()
        conn.execute(
            """
            INSERT INTO wallets (
                address, account_value, total_ntl_pos, total_raw_usd, margin_used,
                withdrawable, active_positions, long_value, short_value, gross_exposure,
                net_exposure, direction_bias, diversification_score, top_coin, source,
                last_seen, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(address) DO UPDATE SET
                account_value=excluded.account_value,
                total_ntl_pos=excluded.total_ntl_pos,
                total_raw_usd=excluded.total_raw_usd,
                margin_used=excluded.margin_used,
                withdrawable=excluded.withdrawable,
                active_positions=excluded.active_positions,
                long_value=excluded.long_value,
                short_value=excluded.short_value,
                gross_exposure=excluded.gross_exposure,
                net_exposure=excluded.net_exposure,
                direction_bias=excluded.direction_bias,
                diversification_score=excluded.diversification_score,
                top_coin=excluded.top_coin,
                source=excluded.source,
                last_seen=excluded.last_seen,
                raw_json=excluded.raw_json
            """,
            (
                wallet["address"],
                wallet["account_value"],
                wallet["total_ntl_pos"],
                wallet["total_raw_usd"],
                wallet["margin_used"],
                wallet["withdrawable"],
                wallet["active_positions"],
                wallet["long_value"],
                wallet["short_value"],
                wallet["gross_exposure"],
                wallet["net_exposure"],
                wallet["direction_bias"],
                wallet["diversification_score"],
                wallet["top_coin"],
                wallet["source"],
                seen,
                wallet["raw_json"],
            ),
        )
        sync_position_state_and_events(conn, wallet, previous_positions, seen)
        conn.execute("DELETE FROM positions WHERE wallet_address = ?", (wallet["address"],))
        for position in wallet["positions"]:
            conn.execute(
                """
                INSERT INTO positions (
                    wallet_address, coin, side, size, entry_px, current_px,
                    position_value, capital_used, unrealized_pnl, roi_price,
                    roi_capital, return_on_equity, leverage, leverage_value,
                    liquidation_px, margin_used, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    wallet["address"],
                    position["coin"],
                    position["side"],
                    position["size"],
                    position["entry_px"],
                    position["current_px"],
                    position["position_value"],
                    position["capital_used"],
                    position["unrealized_pnl"],
                    position["roi_price"],
                    position["roi_capital"],
                    position["return_on_equity"],
                    position["leverage"],
                    position["leverage_value"],
                    position["liquidation_px"],
                    position["margin_used"],
                    seen,
                ),
            )
        conn.execute(
            """
            INSERT INTO wallet_snapshots (
                wallet_address, account_value, total_ntl_pos, gross_exposure,
                long_value, short_value, active_positions, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                wallet["address"],
                wallet["account_value"],
                wallet["total_ntl_pos"],
                wallet["gross_exposure"],
                wallet["long_value"],
                wallet["short_value"],
                wallet["active_positions"],
                seen,
            ),
        )
    return not existed


def position_key(position):
    return str(position["coin"]), str(position["side"])


def is_active_position(position):
    return str(position["side"]) in {"Long", "Short"} and abs(to_float(position["size"])) > POSITION_EVENT_EPSILON


def row_to_position(row):
    return {
        "coin": row["coin"],
        "side": row["side"],
        "size": row["size"],
        "entry_px": row["entry_px"],
        "current_px": row["current_px"],
        "position_value": row["position_value"],
        "capital_used": row["capital_used"],
        "unrealized_pnl": row["unrealized_pnl"],
    }


def insert_position_event(conn, wallet_address, event_type, position, source, created_at):
    conn.execute(
        """
        INSERT INTO position_events (
            wallet_address, event_type, coin, side, size, entry_px,
            current_px, notional, margin_used, unrealized_pnl, source, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            wallet_address,
            event_type,
            position["coin"],
            position["side"],
            abs(to_float(position["size"])),
            to_float(position["entry_px"]),
            to_float(position["current_px"]),
            to_float(position["position_value"]),
            to_float(position["capital_used"]),
            to_float(position["unrealized_pnl"]),
            source,
            created_at,
        ),
    )


def upsert_position_state(conn, wallet_address, position, missing_count, updated_at):
    conn.execute(
        """
        INSERT INTO position_state (
            wallet_address, coin, side, size, entry_px, current_px,
            position_value, capital_used, unrealized_pnl, missing_count, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(wallet_address, coin, side) DO UPDATE SET
            size=excluded.size,
            entry_px=excluded.entry_px,
            current_px=excluded.current_px,
            position_value=excluded.position_value,
            capital_used=excluded.capital_used,
            unrealized_pnl=excluded.unrealized_pnl,
            missing_count=excluded.missing_count,
            updated_at=excluded.updated_at
        """,
        (
            wallet_address,
            position["coin"],
            position["side"],
            to_float(position["size"]),
            to_float(position["entry_px"]),
            to_float(position["current_px"]),
            to_float(position["position_value"]),
            to_float(position["capital_used"]),
            to_float(position["unrealized_pnl"]),
            missing_count,
            updated_at,
        ),
    )


def state_row_to_position(row):
    return {
        "coin": row["coin"],
        "side": row["side"],
        "size": row["size"],
        "entry_px": row["entry_px"],
        "current_px": row["current_px"],
        "position_value": row["position_value"],
        "capital_used": row["capital_used"],
        "unrealized_pnl": row["unrealized_pnl"],
        "missing_count": row["missing_count"],
    }


def sync_position_state_and_events(conn, wallet, previous_rows, created_at):
    state_rows = conn.execute(
        """
        SELECT coin, side, size, entry_px, current_px, position_value,
               capital_used, unrealized_pnl, missing_count
        FROM position_state
        WHERE wallet_address = ?
        """,
        (wallet["address"],),
    ).fetchall()

    current = {
        position_key(position): position
        for position in wallet["positions"]
        if is_active_position(position)
    }

    if not state_rows:
        baseline_rows = previous_rows or []
        baseline = {
            position_key(row_to_position(row)): row_to_position(row)
            for row in baseline_rows
            if is_active_position(row_to_position(row))
        }
        baseline.update(current)
        for position in baseline.values():
            upsert_position_state(conn, wallet["address"], position, 0, created_at)
        return

    previous = {
        position_key(state_row_to_position(row)): state_row_to_position(row)
        for row in state_rows
        if is_active_position(state_row_to_position(row))
    }

    for key, position in current.items():
        if key not in previous:
            insert_position_event(conn, wallet["address"], "open", position, wallet["source"], created_at)
        upsert_position_state(conn, wallet["address"], position, 0, created_at)

    for key, position in previous.items():
        if key not in current:
            flipped_same_coin = any(current_position["coin"] == position["coin"] for current_position in current.values())
            if flipped_same_coin:
                insert_position_event(conn, wallet["address"], "close", position, wallet["source"], created_at)
                conn.execute(
                    "DELETE FROM position_state WHERE wallet_address = ? AND coin = ? AND side = ?",
                    (wallet["address"], position["coin"], position["side"]),
                )
                continue
            missing_count = int(position.get("missing_count") or 0) + 1
            if missing_count >= POSITION_CLOSE_CONFIRMATIONS:
                insert_position_event(conn, wallet["address"], "close", position, wallet["source"], created_at)
                conn.execute(
                    "DELETE FROM position_state WHERE wallet_address = ? AND coin = ? AND side = ?",
                    (wallet["address"], position["coin"], position["side"]),
                )
            else:
                upsert_position_state(conn, wallet["address"], position, missing_count, created_at)


def scan_wallets(seed_text="", use_live_discovery=True, coins_text="", discovery_seconds=None, max_candidates=None):
    started = now_iso()
    seeds = load_seed_wallets(seed_text)
    live_candidates = []
    live_summary = {"trades": 0, "coins": [], "errors": [], "candidates": 0, "top_notional": 0}
    if use_live_discovery:
        live_candidates, live_summary = discover_wallets_from_live_trades(
            coins_text=coins_text,
            seconds=discovery_seconds,
            max_candidates=max_candidates,
        )

    queue = list(dict.fromkeys(live_candidates + seeds))
    seen = set()
    scanned_count = 0
    saved_count = 0
    new_count = 0
    updated_count = 0
    duplicate_count = 0
    below_min_count = 0
    discovered = set(live_candidates)
    errors = list(live_summary.get("errors") or [])
    mark_prices = fetch_mark_prices()

    with connect_db() as conn:
        cursor = conn.execute("INSERT INTO scan_runs (started_at) VALUES (?)", (started,))
        run_id = cursor.lastrowid

    while queue and len(seen) < 250:
        address = queue.pop(0).lower()
        if address in seen or not ADDRESS_RE.match(address):
            continue
        seen.add(address)
        scanned_count += 1
        try:
            state = hyperliquid_info({"type": "clearinghouseState", "user": address})
            wallet = parse_wallet(address, state, "seed" if address in seeds else "subaccount", mark_prices)
            if wallet["account_value"] >= MIN_ACCOUNT_VALUE:
                is_new = save_wallet(wallet)
                saved_count += 1
                if is_new:
                    new_count += 1
                else:
                    updated_count += 1
                    duplicate_count += 1
            else:
                below_min_count += 1

            try:
                subaccounts = hyperliquid_info({"type": "subAccounts", "user": address}) or []
                for sub in subaccounts:
                    sub_address = str(sub.get("subAccountUser") or "").lower()
                    if ADDRESS_RE.match(sub_address) and sub_address not in seen:
                        discovered.add(sub_address)
                        queue.append(sub_address)
            except Exception as exc:
                errors.append(f"{short_addr(address)} subaccounts: {exc}")
        except urllib.error.HTTPError as exc:
            errors.append(f"{short_addr(address)} HTTP {exc.code}")
        except Exception as exc:
            errors.append(f"{short_addr(address)} {exc}")
        time.sleep(0.08)

    finished = now_iso()
    with connect_db() as conn:
        conn.execute(
            """
            UPDATE scan_runs
            SET finished_at = ?, scanned_count = ?, saved_count = ?,
                discovered_count = ?, errors = ?
            WHERE id = ?
            """,
            (finished, scanned_count, new_count, len(discovered), "\n".join(errors[-25:]), run_id),
        )
    return {
        "scanned": scanned_count,
        "saved": saved_count,
        "new": new_count,
        "updated": updated_count,
        "duplicates": duplicate_count,
        "below_min": below_min_count,
        "discovered": len(discovered),
        "live": live_summary,
        "errors": errors,
    }


def q_one(query, params=()):
    with connect_db() as conn:
        return conn.execute(query, params).fetchone()


def q_all(query, params=()):
    with connect_db() as conn:
        return conn.execute(query, params).fetchall()


def now_ms():
    return int(time.time() * 1000)


def fill_id(fill, address=""):
    tid = fill.get("tid")
    prefix = address.lower() + ":" if address else ""
    if tid not in (None, ""):
        return prefix + str(tid)
    return prefix + "|".join(
        str(fill.get(key, ""))
        for key in ("hash", "oid", "time", "coin", "dir", "px", "sz")
    )


def fetch_user_fills_by_time(address, start_ms, end_ms=None):
    payload = {
        "type": "userFillsByTime",
        "user": address,
        "startTime": int(start_ms),
        "aggregateByTime": False,
    }
    if end_ms:
        payload["endTime"] = int(end_ms)
    data = hyperliquid_info(payload)
    return data if isinstance(data, list) else []


def save_fills(conn, address, fills):
    synced_at = now_iso()
    inserted = 0
    max_time = 0
    for fill in fills:
        time_ms = int(to_float(fill.get("time"), 0))
        max_time = max(max_time, time_ms)
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO fills (
                fill_id, wallet_address, tid, oid, coin, dir, side, px, size,
                start_position, closed_pnl, fee, builder_fee, fee_token,
                crossed, hash, time_ms, raw_json, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fill_id(fill, address),
                address,
                str(fill.get("tid", "")),
                str(fill.get("oid", "")),
                str(fill.get("coin", "")),
                str(fill.get("dir", "")),
                str(fill.get("side", "")),
                to_float(fill.get("px")),
                abs(to_float(fill.get("sz"))),
                to_float(fill.get("startPosition")),
                to_float(fill.get("closedPnl")),
                to_float(fill.get("fee")),
                to_float(fill.get("builderFee")),
                str(fill.get("feeToken", "")),
                1 if fill.get("crossed") else 0,
                str(fill.get("hash", "")),
                time_ms,
                json.dumps(fill, separators=(",", ":")),
                synced_at,
            ),
        )
        inserted += cursor.rowcount
    return inserted, max_time


def fill_position_side(direction):
    direction = str(direction or "")
    if "Long" in direction:
        return "Long"
    if "Short" in direction:
        return "Short"
    return ""


def fill_action(direction):
    direction = str(direction or "")
    if direction.startswith("Open"):
        return "open"
    if direction.startswith("Close"):
        return "close"
    return ""


def new_episode(address, coin, side, opened_at_ms=None):
    return {
        "wallet_address": address,
        "coin": coin,
        "side": side,
        "status": "open",
        "opened_at_ms": opened_at_ms,
        "closed_at_ms": None,
        "open_size": 0.0,
        "open_notional": 0.0,
        "close_size": 0.0,
        "close_notional": 0.0,
        "gross_pnl": 0.0,
        "fees": 0.0,
        "fill_count": 0,
        "maker_fills": 0,
        "taker_fills": 0,
    }


def add_fill_to_episode(episode, fill, action):
    size = to_float(fill["size"])
    px = to_float(fill["px"])
    notional = abs(size * px)
    episode["fees"] += to_float(fill["fee"]) + to_float(fill["builder_fee"])
    episode["fill_count"] += 1
    if int(fill["crossed"]):
        episode["taker_fills"] += 1
    else:
        episode["maker_fills"] += 1
    if action == "open":
        if episode["opened_at_ms"] is None:
            episode["opened_at_ms"] = int(fill["time_ms"])
        episode["open_size"] += size
        episode["open_notional"] += notional
    elif action == "close":
        episode["close_size"] += size
        episode["close_notional"] += notional
        episode["gross_pnl"] += to_float(fill["closed_pnl"])
        episode["closed_at_ms"] = int(fill["time_ms"])


def persist_episode(conn, episode, rebuilt_at):
    avg_entry = episode["open_notional"] / episode["open_size"] if episode["open_size"] else 0.0
    avg_close = episode["close_notional"] / episode["close_size"] if episode["close_size"] else 0.0
    net_pnl = episode["gross_pnl"] - episode["fees"]
    conn.execute(
        """
        INSERT INTO trade_episodes (
            wallet_address, coin, side, status, opened_at_ms, closed_at_ms,
            open_size, close_size, avg_entry_px, avg_close_px, gross_pnl,
            fees, net_pnl, fill_count, maker_fills, taker_fills, rebuilt_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            episode["wallet_address"],
            episode["coin"],
            episode["side"],
            episode["status"],
            episode["opened_at_ms"],
            episode["closed_at_ms"],
            episode["open_size"],
            episode["close_size"],
            avg_entry,
            avg_close,
            episode["gross_pnl"],
            episode["fees"],
            net_pnl,
            episode["fill_count"],
            episode["maker_fills"],
            episode["taker_fills"],
            rebuilt_at,
        ),
    )


def rebuild_trade_episodes(address):
    rebuilt_at = now_iso()
    with connect_db() as conn:
        conn.execute("DELETE FROM trade_episodes WHERE wallet_address = ?", (address,))
        fills = conn.execute(
            """
            SELECT * FROM fills
            WHERE wallet_address = ?
            ORDER BY time_ms ASC, fill_id ASC
            """,
            (address,),
        ).fetchall()
        active = {}
        for fill in fills:
            side = fill_position_side(fill["dir"])
            action = fill_action(fill["dir"])
            if not side or not action:
                continue
            key = (fill["coin"], side)
            if action == "open":
                episode = active.get(key)
                if not episode:
                    episode = new_episode(address, fill["coin"], side, int(fill["time_ms"]))
                    active[key] = episode
                add_fill_to_episode(episode, fill, action)
                continue

            episode = active.get(key)
            if not episode:
                episode = new_episode(address, fill["coin"], side, None)
                active[key] = episode
            add_fill_to_episode(episode, fill, action)
            remaining = abs(to_float(fill["start_position"])) - abs(to_float(fill["size"]))
            if remaining <= POSITION_EVENT_EPSILON:
                episode["status"] = "closed"
                persist_episode(conn, episode, rebuilt_at)
                active.pop(key, None)

        for episode in active.values():
            persist_episode(conn, episode, rebuilt_at)


def sync_wallet_fills(address, lookback_days=None):
    address = address.lower()
    end_ms = now_ms()
    lookback_ms = int((lookback_days or FILL_LOOKBACK_DAYS) * 24 * 60 * 60 * 1000)
    with connect_db() as conn:
        state = conn.execute(
            "SELECT last_time_ms FROM fill_sync_state WHERE wallet_address = ?",
            (address,),
        ).fetchone()
        start_ms = int(state["last_time_ms"]) + 1 if state else end_ms - lookback_ms

    total_inserted = 0
    max_seen = start_ms
    cursor_ms = start_ms
    while cursor_ms <= end_ms:
        fills = fetch_user_fills_by_time(address, cursor_ms, end_ms)
        if not fills:
            break
        with connect_db() as conn:
            inserted, batch_max = save_fills(conn, address, fills)
            total_inserted += inserted
            max_seen = max(max_seen, batch_max)
        if len(fills) < 2000 or batch_max <= cursor_ms:
            break
        cursor_ms = batch_max + 1

    with connect_db() as conn:
        conn.execute(
            """
            INSERT INTO fill_sync_state (wallet_address, last_time_ms, synced_at)
            VALUES (?, ?, ?)
            ON CONFLICT(wallet_address) DO UPDATE SET
                last_time_ms=excluded.last_time_ms,
                synced_at=excluded.synced_at
            """,
            (address, max(max_seen, end_ms - 1), now_iso()),
        )
    rebuild_trade_episodes(address)
    return total_inserted


def sync_saved_wallet_fills(batch_size=None):
    batch_size = max(1, int(batch_size or FILL_SYNC_BATCH))
    wallets = q_all(
        """
        SELECT w.address
        FROM wallets w
        LEFT JOIN fill_sync_state s ON s.wallet_address = w.address
        ORDER BY COALESCE(s.synced_at, '') ASC, w.last_seen DESC
        LIMIT ?
        """,
        (batch_size,),
    )
    synced = 0
    errors = []
    for wallet in wallets:
        try:
            sync_wallet_fills(wallet["address"])
            synced += 1
        except Exception as exc:
            errors.append(f"{short_addr(wallet['address'])}: {exc}")
        time.sleep(0.1)
    return synced, errors


def wallet_name(wallet):
    alias = wallet["alias"] if "alias" in wallet.keys() else ""
    return alias.strip() or short_addr(wallet["address"])


def save_wallet_alias(address, alias):
    alias = (alias or "").strip()[:80]
    with connect_db() as conn:
        conn.execute("UPDATE wallets SET alias = ? WHERE address = ?", (alias, address.lower()))


def refresh_wallet_state(address, source="refresh", mark_prices=None):
    address = (address or "").lower()
    if not ADDRESS_RE.match(address):
        return False
    mark_prices = mark_prices if mark_prices is not None else fetch_mark_prices()
    state = hyperliquid_info({"type": "clearinghouseState", "user": address})
    wallet = parse_wallet(address, state, source, mark_prices)
    save_wallet(wallet)
    return True


def refresh_saved_wallets(batch_size=None):
    batch_size = max(1, int(batch_size or AUTO_REFRESH_BATCH))
    wallets = q_all(
        "SELECT address FROM wallets ORDER BY last_seen ASC LIMIT ?",
        (batch_size,),
    )
    refreshed = 0
    errors = []
    mark_prices = fetch_mark_prices()
    for wallet in wallets:
        try:
            if refresh_wallet_state(wallet["address"], "auto-refresh", mark_prices):
                refreshed += 1
        except Exception as exc:
            errors.append(f"{short_addr(wallet['address'])}: {exc}")
        time.sleep(0.15)
    return refreshed, errors


def auto_worker():
    AUTO_STATUS["started"] = True
    next_discovery = time.monotonic() + 5
    next_fill_sync = time.monotonic() + 10
    while True:
        try:
            refreshed, errors = refresh_saved_wallets()
            AUTO_STATUS["last_refresh"] = f"{now_iso()} | {refreshed} wallets"
            if errors:
                AUTO_STATUS["last_error"] = "; ".join(errors[-3:])

            if FILL_SYNC_ENABLED and time.monotonic() >= next_fill_sync:
                synced, fill_errors = sync_saved_wallet_fills()
                AUTO_STATUS["last_fill_sync"] = f"{now_iso()} | {synced} wallets"
                if fill_errors:
                    AUTO_STATUS["last_error"] = "; ".join(fill_errors[-3:])
                next_fill_sync = time.monotonic() + max(10, FILL_SYNC_INTERVAL)

            if AUTO_DISCOVERY_ENABLED and time.monotonic() >= next_discovery:
                result = scan_wallets(use_live_discovery=True)
                AUTO_STATUS["last_discovery"] = (
                    f"{now_iso()} | {result['discovered']} candidatas | {result['new']} nuevas | {result['updated']} repetidas"
                )
                if result["errors"]:
                    AUTO_STATUS["last_error"] = "; ".join(result["errors"][-3:])
                next_discovery = time.monotonic() + max(60, AUTO_DISCOVERY_INTERVAL)
        except Exception as exc:
            AUTO_STATUS["last_error"] = str(exc)
        time.sleep(max(1.0, AUTO_REFRESH_INTERVAL))


def start_auto_worker():
    if AUTO_STATUS["started"]:
        return
    thread = threading.Thread(target=auto_worker, name="wallet-auto-worker", daemon=True)
    thread.start()


def render_layout(title, body, active="dashboard", message=""):
    nav = [
        ("dashboard", "/", "Dashboard"),
        ("wallets", "/wallets", "Wallets"),
        ("events", "/events", "Actividad"),
        ("trends", "/trends", "Tendencias"),
    ]
    links = "".join(
        f'<a class="nav-link {"active" if key == active else ""}" href="{href}">{label}</a>'
        for key, href, label in nav
    )
    message_html = f'<div class="notice">{html.escape(message)}</div>' if message else ""
    return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)} - {APP_NAME}</title>
  <style>
    :root {{
      --bg: #f5f7fb;
      --panel: #ffffff;
      --ink: #182033;
      --muted: #647086;
      --line: #dfe5ef;
      --green: #14865f;
      --red: #b43b4a;
      --blue: #2f68d8;
      --amber: #9a6400;
      --shadow: 0 12px 28px rgba(24,32,51,.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: 0;
    }}
    a {{ color: inherit; text-decoration: none; }}
    .shell {{ min-height: 100vh; display: grid; grid-template-columns: 248px 1fr; }}
    aside {{
      background: #111827;
      color: #f8fafc;
      padding: 24px 18px;
      position: sticky;
      top: 0;
      height: 100vh;
    }}
    .brand {{ font-size: 18px; font-weight: 800; margin-bottom: 26px; }}
    .nav-link {{
      display: flex;
      align-items: center;
      min-height: 42px;
      padding: 10px 12px;
      border-radius: 8px;
      color: #cbd5e1;
      margin-bottom: 6px;
      font-size: 14px;
    }}
    .nav-link.active, .nav-link:hover {{ background: #243044; color: #fff; }}
    .logout {{ position: absolute; bottom: 20px; left: 18px; right: 18px; color: #cbd5e1; font-size: 14px; }}
    main {{ padding: 28px; max-width: 1440px; width: 100%; }}
    .topbar {{ display:flex; justify-content:space-between; gap:16px; align-items:flex-start; margin-bottom:22px; }}
    h1 {{ margin: 0; font-size: 26px; line-height: 1.2; }}
    h2 {{ margin: 0 0 14px; font-size: 17px; }}
    .subtle {{ color: var(--muted); font-size: 13px; margin-top: 5px; }}
    .grid {{ display: grid; gap: 16px; }}
    .metrics {{ grid-template-columns: repeat(4, minmax(0, 1fr)); margin-bottom: 18px; }}
    .two {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    .three {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      padding: 18px;
    }}
    .metric-label {{ color: var(--muted); font-size: 12px; text-transform: uppercase; font-weight: 750; }}
    .metric-value {{ font-size: 27px; font-weight: 850; margin-top: 8px; overflow-wrap: anywhere; }}
    .table-wrap {{ overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 11px 10px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-size: 12px; text-transform: uppercase; font-weight: 800; }}
    tr:last-child td {{ border-bottom: 0; }}
    .badge {{ display:inline-flex; min-height:24px; align-items:center; border-radius: 999px; padding: 3px 9px; font-size: 12px; font-weight: 800; }}
    .bull {{ color: var(--green); background: #e7f6ef; }}
    .bear {{ color: var(--red); background: #fae8eb; }}
    .flat {{ color: var(--amber); background: #fff3d8; }}
    .neutral {{ color: var(--blue); background: #e9f0ff; }}
    .num-positive {{ color: var(--green); font-weight: 850; }}
    .num-negative {{ color: var(--red); font-weight: 850; }}
    .num-neutral {{ color: var(--muted); font-weight: 750; }}
    .btn {{
      border: 0;
      background: #182033;
      color: #fff;
      min-height: 40px;
      border-radius: 8px;
      padding: 9px 13px;
      font-weight: 800;
      cursor: pointer;
    }}
    .btn.secondary {{ background: #e7ecf5; color: #182033; }}
    input, textarea {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 11px 12px;
      font: inherit;
      background: #fff;
      color: var(--ink);
    }}
    textarea {{ min-height: 92px; resize: vertical; }}
    .form-row {{ display: grid; gap: 10px; }}
    .notice {{
      border: 1px solid #bdd7ff;
      background: #edf5ff;
      color: #173f7a;
      border-radius: 8px;
      padding: 12px 14px;
      margin-bottom: 16px;
      font-size: 14px;
    }}
    .bars {{ display: grid; gap: 10px; }}
    .bar {{ display:grid; grid-template-columns: 110px 1fr 80px; gap:10px; align-items:center; font-size: 13px; }}
    .track {{ height: 10px; background:#e8edf6; border-radius:999px; overflow:hidden; }}
    .fill {{ height:100%; background:#2f68d8; }}
    .fill.short {{ background:#b43b4a; }}
    @media (max-width: 980px) {{
      .shell {{ grid-template-columns: 1fr; }}
      aside {{ position: static; height: auto; display: flex; align-items:center; gap: 12px; flex-wrap: wrap; }}
      .brand {{ margin: 0 10px 0 0; }}
      .logout {{ position: static; margin-left: auto; }}
      main {{ padding: 18px; }}
      .metrics, .two, .three {{ grid-template-columns: 1fr; }}
      .topbar {{ flex-direction: column; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <aside>
      <div class="brand">{APP_NAME}</div>
      {links}
      <a class="logout" href="/logout">Cerrar sesion</a>
    </aside>
    <main>
      {message_html}
      {body}
    </main>
  </div>
</body>
</html>"""


def render_login(message=""):
    error = f"<div class='login-error'>{html.escape(message)}</div>" if message else ""
    return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Login - {APP_NAME}</title>
  <style>
    body {{ margin:0; min-height:100vh; display:grid; place-items:center; background:#f5f7fb; color:#182033; font-family:Inter, ui-sans-serif, system-ui, "Segoe UI", sans-serif; }}
    .login {{ width:min(420px, calc(100vw - 32px)); background:#fff; border:1px solid #dfe5ef; border-radius:8px; box-shadow:0 18px 40px rgba(24,32,51,.1); padding:26px; }}
    h1 {{ margin:0 0 6px; font-size:26px; }}
    p {{ margin:0 0 20px; color:#647086; font-size:14px; }}
    label {{ display:block; font-size:12px; color:#647086; text-transform:uppercase; font-weight:800; margin:14px 0 7px; }}
    input {{ width:100%; min-height:42px; border:1px solid #dfe5ef; border-radius:8px; padding:10px 12px; font:inherit; box-sizing:border-box; }}
    button {{ width:100%; min-height:42px; border:0; border-radius:8px; background:#182033; color:#fff; font-weight:850; margin-top:18px; cursor:pointer; }}
    .login-error {{ background:#fae8eb; color:#9f2637; border:1px solid #f1b8c2; border-radius:8px; padding:10px 12px; margin-bottom:12px; font-size:14px; }}
  </style>
</head>
<body>
  <form class="login" method="post" action="/login">
    <h1>{APP_NAME}</h1>
    <p>Acceso administrativo</p>
    {error}
    <label>Usuario</label>
    <input name="username" autocomplete="username" required autofocus>
    <label>Password</label>
    <input name="password" type="password" autocomplete="current-password" required>
    <button type="submit">Entrar</button>
  </form>
</body>
</html>"""


def badge(text):
    cls = "neutral"
    if text == "Alcista" or text == "Long":
        cls = "bull"
    elif text == "Bajista" or text == "Short":
        cls = "bear"
    elif text == "Neutral":
        cls = "flat"
    return f'<span class="badge {cls}">{html.escape(str(text))}</span>'


def wallet_link(address, label=None):
    safe = html.escape(address)
    return f'<a href="/wallet/{safe}">{html.escape(label or short_addr(address))}</a>'


def th(label, description):
    return f'<th title="{html.escape(description)}">{html.escape(label)}</th>'


def render_account_pnl_chart(snapshots):
    if len(snapshots) < 2:
        return '<div class="subtle">Aun no hay suficientes snapshots para graficar PnL historico.</div>'
    values = [to_float(row["account_value"]) for row in snapshots]
    base = values[0]
    pnls = [value - base for value in values]
    min_pnl = min(pnls)
    max_pnl = max(pnls)
    span = max(max_pnl - min_pnl, 1)
    width = 760
    height = 220
    pad_x = 28
    pad_y = 24
    points = []
    for idx, pnl_value in enumerate(pnls):
        x = pad_x + (width - pad_x * 2) * (idx / max(len(pnls) - 1, 1))
        y = height - pad_y - ((pnl_value - min_pnl) / span) * (height - pad_y * 2)
        points.append(f"{x:.1f},{y:.1f}")
    zero_y = height - pad_y - ((0 - min_pnl) / span) * (height - pad_y * 2)
    last_pnl = pnls[-1]
    line_color = "#14865f" if last_pnl >= 0 else "#b43b4a"
    return f"""
      <svg viewBox="0 0 {width} {height}" role="img" aria-label="Grafico de PnL de cuenta" style="width:100%; height:240px;">
        <line x1="{pad_x}" y1="{zero_y:.1f}" x2="{width - pad_x}" y2="{zero_y:.1f}" stroke="#dfe5ef" stroke-width="1" />
        <polyline points="{' '.join(points)}" fill="none" stroke="{line_color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" />
        <circle cx="{points[-1].split(',')[0]}" cy="{points[-1].split(',')[1]}" r="5" fill="{line_color}" />
        <text x="{pad_x}" y="18" fill="#647086" font-size="12">Inicio {full_usd(base)}</text>
        <text x="{width - pad_x}" y="18" text-anchor="end" fill="{line_color}" font-size="12">PnL {('+' if last_pnl > 0 else '') + full_usd(abs(last_pnl))}</text>
      </svg>
    """


def render_wallet_table(rows, value_key="account_value"):
    if not rows:
        return '<div class="subtle">Sin datos guardados todavia.</div>'
    trs = []
    for row in rows:
        total = row["account_value"] + row["margin_used"]
        trs.append(
            "<tr>"
            f"<td>{wallet_link(row['address'], wallet_name(row))}<div class='subtle'>{short_addr(row['address'])}</div></td>"
            f"<td>{usd(row['account_value'])}</td>"
            f"<td>{usd(row['margin_used'])}</td>"
            f"<td>{usd(row['total_ntl_pos'])}</td>"
            f"<td>{usd(total)}</td>"
            f"<td>{int(row['active_positions'])}</td>"
            f"<td>{badge(row['direction_bias'])}</td>"
            f"<td>{html.escape(row['top_coin'] or '-')}</td>"
            "</tr>"
        )
    return (
        '<div class="table-wrap"><table><thead><tr>'
        "<th>Wallet</th><th>Balance</th><th>Margen pos.</th><th>Notional</th><th>Balance + margen</th>"
        "<th>Activas</th><th>Sesgo</th><th>Top coin</th>"
        "</tr></thead><tbody>"
        + "".join(trs)
        + "</tbody></table></div>"
    )


def event_badge(event_type):
    if event_type == "open":
        return '<span class="badge bull">Apertura</span>'
    if event_type == "close":
        return '<span class="badge bear">Cierre</span>'
    return f'<span class="badge neutral">{html.escape(event_type)}</span>'


def fetch_position_events(limit=30):
    return q_all(
        """
        SELECT e.*, w.alias
        FROM position_events e
        LEFT JOIN wallets w ON w.address = e.wallet_address
        ORDER BY e.id DESC
        LIMIT ?
        """,
        (limit,),
    )


def render_events_table(events, compact=False):
    if not events:
        return '<div class="subtle">Sin eventos de apertura/cierre todavia.</div>'
    rows = []
    for event in events:
        label = event["alias"] or short_addr(event["wallet_address"])
        rows.append(
            "<tr>"
            f"<td>{html.escape(event['created_at'])}</td>"
            f"<td>{event_badge(event['event_type'])}</td>"
            f"<td>{wallet_link(event['wallet_address'], label)}<div class='subtle'>{short_addr(event['wallet_address'])}</div></td>"
            f"<td>{html.escape(event['coin'])}</td>"
            f"<td>{badge(event['side'])}</td>"
            f"<td>{event['size']:,.6f}</td>"
            f"<td>{full_usd(event['notional'])}</td>"
            f"<td>{full_usd(event['margin_used'])}</td>"
            f"<td>{price_or_dash(event['entry_px'])}</td>"
            f"<td>{price_or_dash(event['current_px'])}</td>"
            "</tr>"
        )
    limit_note = '<div class="subtle" style="margin-top:10px;"><a href="/events">Ver toda la actividad</a></div>' if compact else ""
    return (
        '<div class="table-wrap"><table><thead><tr>'
        '<th>Fecha</th><th>Evento</th><th>Wallet</th><th>Coin</th><th>Lado</th>'
        '<th>Tamano coin</th><th>Notional</th><th>Margen</th><th>Entry px</th><th>Mark px</th>'
        '</tr></thead><tbody>'
        + "".join(rows)
        + "</tbody></table></div>"
        + limit_note
    )


def ms_to_local_text(ms):
    if not ms:
        return "-"
    try:
        return datetime.fromtimestamp(int(ms) / 1000, timezone.utc).replace(microsecond=0).isoformat()
    except Exception:
        return "-"


def wallet_trade_stats(address):
    now_value = now_ms()
    day_start = now_value - 24 * 60 * 60 * 1000
    month_start = now_value - 30 * 24 * 60 * 60 * 1000
    rows = q_all(
        """
        SELECT * FROM trade_episodes
        WHERE wallet_address = ? AND status = 'closed'
        ORDER BY closed_at_ms DESC
        """,
        (address,),
    )
    def bucket(start_ms=None):
        selected = [row for row in rows if start_ms is None or int(row["closed_at_ms"] or 0) >= start_ms]
        wins = sum(1 for row in selected if to_float(row["net_pnl"]) > 0)
        gross = sum(to_float(row["gross_pnl"]) for row in selected)
        fees = sum(to_float(row["fees"]) for row in selected)
        net = sum(to_float(row["net_pnl"]) for row in selected)
        losses_abs = sum(abs(to_float(row["net_pnl"])) for row in selected if to_float(row["net_pnl"]) < 0)
        wins_sum = sum(to_float(row["net_pnl"]) for row in selected if to_float(row["net_pnl"]) > 0)
        return {
            "trades": len(selected),
            "wins": wins,
            "winrate": wins / len(selected) if selected else 0,
            "gross": gross,
            "fees": fees,
            "net": net,
            "profit_factor": wins_sum / losses_abs if losses_abs else 0,
        }
    return {
        "day": bucket(day_start),
        "month": bucket(month_start),
        "all": bucket(None),
        "recent": rows[:25],
    }


def render_trade_episodes_table(rows):
    if not rows:
        return '<div class="subtle">Aun no hay trades cerrados reconstruidos desde fills.</div>'
    trs = []
    for row in rows:
        trs.append(
            "<tr>"
            f"<td>{ms_to_local_text(row['closed_at_ms'])}</td>"
            f"<td>{html.escape(row['coin'])}</td>"
            f"<td>{badge(row['side'])}</td>"
            f"<td>{row['close_size']:,.6f}</td>"
            f"<td>{price_or_dash(row['avg_entry_px'])}</td>"
            f"<td>{price_or_dash(row['avg_close_px'])}</td>"
            f"<td>{signed_full_usd(row['gross_pnl'])}</td>"
            f"<td>{full_usd(row['fees'])}</td>"
            f"<td>{signed_full_usd(row['net_pnl'])}</td>"
            f"<td>{int(row['fill_count'])}</td>"
            f"<td>{int(row['maker_fills'])}/{int(row['taker_fills'])}</td>"
            "</tr>"
        )
    return (
        '<div class="table-wrap"><table><thead><tr>'
        '<th>Cierre</th><th>Coin</th><th>Lado</th><th>Tamano cerrado</th>'
        '<th>Avg entry</th><th>Avg close</th><th>PnL bruto</th><th>Fees netas</th>'
        '<th>PnL neto</th><th>Fills</th><th>Maker/Taker</th>'
        '</tr></thead><tbody>'
        + "".join(trs)
        + "</tbody></table></div>"
    )


def dashboard(message=""):
    stats = q_one(
        """
        SELECT COUNT(*) wallet_count,
               COALESCE(SUM(account_value), 0) account_value,
               COALESCE(SUM(total_ntl_pos), 0) total_ntl_pos,
               COALESCE(SUM(margin_used), 0) margin_used,
               COALESCE(SUM(long_value), 0) long_value,
               COALESCE(SUM(short_value), 0) short_value,
               COALESCE(AVG(active_positions), 0) avg_positions
        FROM wallets
        """
    )
    last_scan = q_one("SELECT * FROM scan_runs ORDER BY id DESC LIMIT 1")
    top_balance = q_all("SELECT * FROM wallets ORDER BY account_value DESC LIMIT 5")
    top_active = q_all("SELECT * FROM wallets ORDER BY active_positions DESC, account_value DESC LIMIT 5")
    top_positions = q_all("SELECT * FROM wallets ORDER BY margin_used DESC LIMIT 5")
    recent_events = fetch_position_events(8)
    coins = q_all(
        """
        SELECT coin, SUM(position_value) value, COUNT(*) n
        FROM positions
        GROUP BY coin
        ORDER BY value DESC
        LIMIT 8
        """
    )
    max_coin = max([row["value"] for row in coins], default=1) or 1
    coin_bars = "".join(
        f'<div class="bar"><strong>{html.escape(row["coin"])}</strong><div class="track"><div class="fill" style="width:{min(100, row["value"] / max_coin * 100):.1f}%"></div></div><span>{usd(row["value"])}</span></div>'
        for row in coins
    )
    scan_copy = "Nunca"
    if last_scan:
        scan_copy = f"{last_scan['finished_at'] or last_scan['started_at']} | escaneadas {last_scan['scanned_count']} | nuevas {last_scan['saved_count']}"
    body = f"""
    <div class="topbar">
      <div>
        <h1>Dashboard general</h1>
        <div class="subtle">Ultimo escaneo: {html.escape(scan_copy)}</div>
        <div class="subtle">Worker: refresh {html.escape(AUTO_STATUS['last_refresh'] or 'pendiente')} | fills {html.escape(AUTO_STATUS['last_fill_sync'] or 'pendiente')} | discovery {html.escape(AUTO_STATUS['last_discovery'] or 'pendiente')}</div>
      </div>
      <form method="post" action="/scan" class="card" style="width:min(520px,100%); box-shadow:none;">
        <div class="form-row">
          <textarea name="wallets" placeholder="0x... 0x..."></textarea>
          <label style="display:flex; align-items:center; gap:8px; color:var(--muted); font-size:13px; font-weight:750;">
            <input type="checkbox" name="use_live_discovery" value="1" checked style="width:16px; min-height:16px;">
            Descubrir wallets desde trades en vivo
          </label>
          <div class="grid two">
            <input name="coins" value="{html.escape(DISCOVERY_COINS)}" placeholder="BTC,ETH,SOL,HYPE">
            <input name="seconds" type="number" min="3" max="180" value="{DISCOVERY_SECONDS}" placeholder="Segundos">
          </div>
          <input name="max_candidates" type="number" min="1" max="500" value="{DISCOVERY_MAX_CANDIDATES}" placeholder="Candidatas maximas">
          <button class="btn" type="submit">Escanear wallets</button>
        </div>
      </form>
    </div>
    <section class="grid metrics">
      <div class="card"><div class="metric-label">Wallets encontradas</div><div class="metric-value">{int(stats['wallet_count'])}</div></div>
      <div class="card"><div class="metric-label">Balance agregado</div><div class="metric-value">{usd(stats['account_value'])}</div></div>
      <div class="card"><div class="metric-label">Margen en posiciones</div><div class="metric-value">{usd(stats['margin_used'])}</div></div>
      <div class="card"><div class="metric-label">Posiciones promedio</div><div class="metric-value">{float(stats['avg_positions']):.1f}</div></div>
    </section>
    <section class="grid two">
      <div class="card"><h2>Top 5 por balance</h2>{render_wallet_table(top_balance)}</div>
      <div class="card"><h2>Top 5 por posiciones activas</h2>{render_wallet_table(top_active)}</div>
      <div class="card"><h2>Top 5 por margen en posiciones</h2>{render_wallet_table(top_positions)}</div>
      <div class="card">
        <h2>Macro de exposicion notional</h2>
        <div class="grid two" style="margin-bottom:14px;">
          <div><div class="metric-label">Long</div><div class="metric-value">{usd(stats['long_value'])}</div></div>
          <div><div class="metric-label">Short</div><div class="metric-value">{usd(stats['short_value'])}</div></div>
        </div>
        <div class="bars">{coin_bars or '<div class="subtle">Sin posiciones activas.</div>'}</div>
      </div>
    </section>
    <section class="card" style="margin-top:16px;">
      <h2>Actividad reciente</h2>
      {render_events_table(recent_events, compact=True)}
    </section>
    """
    return render_layout("Dashboard", body, "dashboard", message)


def wallets_page(query="", bias=""):
    query = (query or "").strip()
    bias = (bias or "").strip()
    params = []
    where = []
    if query:
        like = f"%{query.lower()}%"
        where.append(
            """
            (
                LOWER(w.address) LIKE ?
                OR EXISTS (
                    SELECT 1 FROM positions p
                    WHERE p.wallet_address = w.address AND LOWER(p.coin) LIKE ?
                )
            )
            """
        )
        params.extend([like, like])
    if bias in {"Alcista", "Bajista", "Neutral"}:
        where.append("w.direction_bias = ?")
        params.append(bias)
    where_sql = "WHERE " + " AND ".join(where) if where else ""
    rows = q_all(
        f"""
        SELECT w.*,
               GROUP_CONCAT(DISTINCT p.coin) coins
        FROM wallets w
        LEFT JOIN positions p ON p.wallet_address = w.address
        {where_sql}
        GROUP BY w.address
        ORDER BY (w.account_value + w.margin_used) DESC
        LIMIT 250
        """,
        tuple(params),
    )
    total_value = sum(row["account_value"] + row["margin_used"] for row in rows)
    total_margin = sum(row["margin_used"] for row in rows)
    total_exposure = sum(row["gross_exposure"] for row in rows)
    total_active = sum(row["active_positions"] for row in rows)
    options = "".join(
        f'<option value="{value}" {"selected" if bias == value else ""}>{label}</option>'
        for value, label in [("", "Todos"), ("Alcista", "Alcistas"), ("Bajista", "Bajistas"), ("Neutral", "Neutrales")]
    )
    trs = []
    for row in rows:
        coins = ", ".join((row["coins"] or "").split(",")[:5]) or "-"
        total = row["account_value"] + row["margin_used"]
        trs.append(
            "<tr>"
            f"<td>{wallet_link(row['address'], wallet_name(row))}<div class='subtle'>{html.escape(row['address'])}</div></td>"
            f"<td>{usd(row['account_value'])}</td>"
            f"<td>{usd(row['margin_used'])}</td>"
            f"<td>{usd(row['total_ntl_pos'])}</td>"
            f"<td>{usd(total)}</td>"
            f"<td>{usd(row['long_value'])}</td>"
            f"<td>{usd(row['short_value'])}</td>"
            f"<td>{int(row['active_positions'])}</td>"
            f"<td>{badge(row['direction_bias'])}</td>"
            f"<td>{html.escape(coins)}</td>"
            f"<td>{html.escape(row['last_seen'])}</td>"
            "</tr>"
        )
    body = f"""
    <div class="topbar">
      <div>
        <h1>Wallets</h1>
        <div class="subtle">Listado filtrable de whales guardadas</div>
      </div>
      <form method="get" action="/wallets" class="card" style="width:min(620px,100%); box-shadow:none;">
        <div class="grid two">
          <input name="q" value="{html.escape(query)}" placeholder="Buscar wallet o coin">
          <select name="bias" style="width:100%; border:1px solid var(--line); border-radius:8px; padding:11px 12px; font:inherit; background:#fff; color:var(--ink);">
            {options}
          </select>
        </div>
        <div style="display:flex; gap:10px; margin-top:10px;">
          <button class="btn" type="submit">Filtrar</button>
          <a class="btn secondary" href="/wallets">Limpiar</a>
        </div>
      </form>
    </div>
    <section class="grid metrics">
      <div class="card"><div class="metric-label">Wallets visibles</div><div class="metric-value">{len(rows)}</div></div>
      <div class="card"><div class="metric-label">Balance + margen</div><div class="metric-value">{usd(total_value)}</div></div>
      <div class="card"><div class="metric-label">Margen posiciones</div><div class="metric-value">{usd(total_margin)}</div></div>
      <div class="card"><div class="metric-label">Posiciones activas</div><div class="metric-value">{int(total_active)}</div></div>
    </section>
    <section class="card">
      <h2>Listado</h2>
      <div class="table-wrap"><table><thead><tr>
        <th>Wallet</th><th>Balance</th><th>Margen pos.</th><th>Notional</th><th>Balance + margen</th>
        <th>Long notional</th><th>Short notional</th><th>Activas</th><th>Sesgo</th><th>Coins</th><th>Actualizada</th>
      </tr></thead><tbody>{''.join(trs) or '<tr><td colspan="11">Sin wallets guardadas.</td></tr>'}</tbody></table></div>
    </section>
    """
    return render_layout("Wallets", body, "wallets")


def events_page():
    events = fetch_position_events(200)
    opens = sum(1 for event in events if event["event_type"] == "open")
    closes = sum(1 for event in events if event["event_type"] == "close")
    margin = sum(to_float(event["margin_used"]) for event in events)
    body = f"""
    <div class="topbar">
      <div>
        <h1>Actividad</h1>
        <div class="subtle">Aperturas y cierres detectados durante los refresh de wallets guardadas</div>
      </div>
    </div>
    <section class="grid metrics">
      <div class="card"><div class="metric-label">Eventos visibles</div><div class="metric-value">{len(events)}</div></div>
      <div class="card"><div class="metric-label">Aperturas</div><div class="metric-value">{opens}</div></div>
      <div class="card"><div class="metric-label">Cierres</div><div class="metric-value">{closes}</div></div>
      <div class="card"><div class="metric-label">Margen observado</div><div class="metric-value">{usd(margin)}</div></div>
    </section>
    <section class="card">
      <h2>Eventos recientes</h2>
      {render_events_table(events)}
    </section>
    """
    return render_layout("Actividad", body, "events")


def wallet_profile(address):
    address = address.lower()
    wallet = q_one("SELECT * FROM wallets WHERE address = ?", (address,))
    if not wallet:
        return render_layout("Wallet", "<h1>Wallet no encontrada</h1>", "dashboard")
    positions = q_all("SELECT * FROM positions WHERE wallet_address = ? ORDER BY position_value DESC", (address,))
    snapshots = q_all(
        "SELECT * FROM wallet_snapshots WHERE wallet_address = ? ORDER BY id DESC LIMIT 3",
        (address,),
    )
    chart_snapshots = q_all(
        """
        SELECT * FROM (
            SELECT * FROM wallet_snapshots
            WHERE wallet_address = ?
            ORDER BY id DESC
            LIMIT 48
        ) ORDER BY id ASC
        """,
        (address,),
    )
    rows = []
    profile_coins = sorted({str(pos["coin"]) for pos in positions})
    total_capital = sum(pos["capital_used"] for pos in positions)
    total_pnl = sum(pos["unrealized_pnl"] for pos in positions)
    trade_stats = wallet_trade_stats(address)
    fill_state = q_one("SELECT * FROM fill_sync_state WHERE wallet_address = ?", (address,))
    for pos in positions:
        rows.append(
            f"<tr class='position-row' data-coin='{html.escape(pos['coin'])}' data-side='{html.escape(pos['side'])}' data-size='{abs(pos['size'])}' data-entry='{pos['entry_px']}' data-margin='{pos['capital_used']}'>"
            f"<td>{html.escape(pos['coin'])}</td>"
            f"<td>{badge(pos['side'])}</td>"
            f"<td>{abs(pos['size']):,.6f}</td>"
            f"<td class='pos-notional'>{full_usd(pos['position_value'])}</td>"
            f"<td>{full_usd(pos['capital_used'])}</td>"
            f"<td>{price(pos['entry_px'])}</td>"
            f"<td class='pos-mark'>{price(pos['current_px'])}</td>"
            f"<td class='pos-upnl'>{signed_full_usd(pos['unrealized_pnl'])}</td>"
            f"<td class='pos-roi-price'>{signed_pct(pos['roi_price'])}</td>"
            f"<td class='pos-roi-margin'>{signed_pct(pos['roi_capital'])}</td>"
            f"<td>{html.escape(pos['leverage'] or '-')}</td>"
            f"<td>{price_or_dash(pos['liquidation_px'])}</td>"
            "</tr>"
        )
    snap_rows = "".join(
        "<tr>"
        f"<td>{html.escape(s['created_at'])}</td>"
        f"<td>{usd(s['account_value'])}</td>"
        f"<td>{usd(s['total_ntl_pos'])}</td>"
        f"<td>{int(s['active_positions'])}</td>"
        "</tr>"
        for s in snapshots
    )
    body = f"""
    <div class="topbar">
      <div>
        <h1>{html.escape(wallet_name(wallet))}</h1>
        <div class="subtle">{html.escape(address)}</div>
        <div class="subtle">Precios live cada {PRICE_UI_REFRESH_MS}ms | Mark stream: <span id="mark-age">esperando</span> | Worker: {html.escape(AUTO_STATUS['last_refresh'] or 'esperando datos')}</div>
      </div>
      <div style="display:flex; gap:10px; flex-wrap:wrap;">
        <form method="post" action="/wallet/{html.escape(address)}/refresh">
          <button class="btn secondary" type="submit">Refrescar</button>
        </form>
        <form method="post" action="/wallet/{html.escape(address)}/sync-fills">
          <button class="btn secondary" type="submit">Sync fills</button>
        </form>
        <a class="btn secondary" href="/wallets">Volver</a>
      </div>
    </div>
    <section class="card" style="margin-bottom:16px;">
      <form method="post" action="/wallet/{html.escape(address)}/name" class="grid two">
        <input name="alias" value="{html.escape(wallet['alias'] or '')}" placeholder="Nombre para esta wallet">
        <button class="btn" type="submit">Guardar nombre</button>
      </form>
    </section>
    <section class="grid metrics">
      <div class="card"><div class="metric-label">Balance</div><div class="metric-value">{usd(wallet['account_value'])}</div></div>
      <div class="card"><div class="metric-label">Notional abierto</div><div class="metric-value">{usd(wallet['total_ntl_pos'])}</div></div>
      <div class="card"><div class="metric-label">Margen usado posiciones</div><div class="metric-value">{usd(total_capital)}</div></div>
      <div class="card"><div class="metric-label">uPnL agregado</div><div class="metric-value">{signed_usd(total_pnl)}</div></div>
    </section>
    <section class="grid metrics">
      <div class="card"><div class="metric-label">Exposicion neta</div><div class="metric-value">{signed_usd(wallet['net_exposure'])}</div></div>
      <div class="card"><div class="metric-label">Sesgo</div><div class="metric-value">{badge(wallet['direction_bias'])}</div></div>
      <div class="card"><div class="metric-label">ROI sobre margen</div><div class="metric-value">{signed_pct(total_pnl / total_capital if total_capital else 0)}</div></div>
      <div class="card"><div class="metric-label">Posiciones activas</div><div class="metric-value">{int(wallet['active_positions'])}</div></div>
    </section>
    <section class="card">
      <h2>Detalle</h2>
      <table><tbody>
        <tr><th>Withdrawable</th><td>{usd(wallet['withdrawable'])}</td></tr>
        <tr><th>Margen usado</th><td>{usd(wallet['margin_used'])}</td></tr>
        <tr><th>Exposicion long</th><td>{usd(wallet['long_value'])}</td></tr>
        <tr><th>Exposicion short</th><td>{usd(wallet['short_value'])}</td></tr>
        <tr><th>Diversificacion</th><td>{pct(wallet['diversification_score'])}</td></tr>
        <tr><th>Top coin</th><td>{html.escape(wallet['top_coin'] or '-')}</td></tr>
        <tr><th>Ultima lectura</th><td>{html.escape(wallet['last_seen'])}</td></tr>
        <tr><th>Ultimo sync fills</th><td>{html.escape(fill_state['synced_at'] if fill_state else 'Nunca')}</td></tr>
      </tbody></table>
    </section>
    <section class="card" style="margin-top:16px;">
      <h2>Trading analytics</h2>
      <div class="grid three">
        <div><div class="metric-label">Trades 24h</div><div class="metric-value">{trade_stats['day']['trades']}</div><div class="subtle">Winrate {pct(trade_stats['day']['winrate'])}</div></div>
        <div><div class="metric-label">Trades 30d</div><div class="metric-value">{trade_stats['month']['trades']}</div><div class="subtle">Winrate {pct(trade_stats['month']['winrate'])}</div></div>
        <div><div class="metric-label">Trades total</div><div class="metric-value">{trade_stats['all']['trades']}</div><div class="subtle">Winrate {pct(trade_stats['all']['winrate'])}</div></div>
      </div>
      <div class="grid three" style="margin-top:16px;">
        <div><div class="metric-label">PnL neto total</div><div class="metric-value">{signed_usd(trade_stats['all']['net'])}</div></div>
        <div><div class="metric-label">Fees netas total</div><div class="metric-value">{usd(trade_stats['all']['fees'])}</div></div>
        <div><div class="metric-label">Profit factor</div><div class="metric-value">{trade_stats['all']['profit_factor']:.2f}</div></div>
      </div>
    </section>
    <section class="card" style="margin-top:16px;">
      <h2>Trades cerrados reales</h2>
      <div class="subtle">Reconstruido desde userFillsByTime. PnL neto = closedPnl - fee - builderFee reportadas por Hyperliquid.</div>
      {render_trade_episodes_table(trade_stats['recent'])}
    </section>
    <section class="card" style="margin-top:16px;">
      <h2>PnL de cuenta</h2>
      <div class="subtle">Variacion de account value contra el primer snapshot visible del grafico.</div>
      {render_account_pnl_chart(chart_snapshots)}
    </section>
    <section class="card" style="margin-top:16px;">
      <h2>Posiciones por moneda</h2>
      <div class="table-wrap"><table><thead><tr>
        {th('Coin', 'Mercado perpetuo de la posicion.')}
        {th('Lado', 'Long gana si el precio sube; Short gana si el precio baja.')}
        {th('Tamano coin', 'Cantidad absoluta del activo. La API entrega szi con signo; aqui el signo lo representa Lado.')}
        {th('Notional', 'Valor total de la posicion en USD: tamano aproximado por precio actual. Es el valor apalancado.')}
        {th('Margen usado', 'Margen/capital actualmente usado por la posicion segun Hyperliquid. En cross puede depender del margen compartido de la cuenta.')}
        {th('Entry px', 'Precio promedio de entrada de la posicion.')}
        {th('Mark px', 'Precio mark oficial de Hyperliquid desde metaAndAssetCtxs. Si no esta disponible, se usa positionValue dividido entre tamano como fallback.')}
        {th('uPnL', 'Ganancia o perdida no realizada recibida desde Hyperliquid.')}
        {th('ROI precio', 'Movimiento porcentual del precio desde entry, ajustado por Long o Short, sin apalancamiento.')}
        {th('ROI margen', 'uPnL dividido entre margen usado. Es el retorno sobre el capital/margen empleado.')}
        {th('Lev', 'Apalancamiento y tipo de margen reportado por Hyperliquid, por ejemplo cross 5x.')}
        {th('Liq.', 'Precio de liquidacion recibido desde Hyperliquid. No lo calculamos. En cross puede estar influido por todo el margen de la cuenta.')}
      </tr></thead><tbody>{''.join(rows) or '<tr><td colspan="12">Sin posiciones activas</td></tr>'}</tbody></table></div>
    </section>
    <section class="card" style="margin-top:16px;">
      <h2>Snapshots</h2>
      <div class="table-wrap"><table><thead><tr><th>Fecha</th><th>Balance</th><th>Posiciones</th><th>Activas</th></tr></thead><tbody>{snap_rows or '<tr><td colspan="4">Sin snapshots</td></tr>'}</tbody></table></div>
    </section>
    <script>
      const watchedCoins = {json.dumps(profile_coins)};
      const refreshMs = {PRICE_UI_REFRESH_MS};

      function fmtPrice(value) {{
        value = Number(value || 0);
        const abs = Math.abs(value);
        const decimals = abs >= 100 ? 2 : abs >= 1 ? 4 : abs >= 0.01 ? 6 : 8;
        return "$" + value.toLocaleString(undefined, {{ minimumFractionDigits: decimals, maximumFractionDigits: decimals }});
      }}

      function fmtUsd(value) {{
        value = Number(value || 0);
        return "$" + Math.abs(value).toLocaleString(undefined, {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});
      }}

      function signedHtml(value, suffix) {{
        value = Number(value || 0);
        const cls = value > 0 ? "num-positive" : value < 0 ? "num-negative" : "num-neutral";
        const sign = value > 0 ? "+" : "";
        return `<span class="${{cls}}">${{sign}}${{suffix}}</span>`;
      }}

      async function refreshLiveMarks() {{
        if (!watchedCoins.length) return;
        try {{
          const response = await fetch("/api/marks?coins=" + encodeURIComponent(watchedCoins.join(",")), {{ cache: "no-store" }});
          if (!response.ok) return;
          const payload = await response.json();
          const prices = payload.prices || {{}};
          const age = payload.updated_at ? Math.max(0, Date.now() / 1000 - payload.updated_at) : 0;
          const ageEl = document.getElementById("mark-age");
          if (ageEl) ageEl.textContent = `${{age.toFixed(2)}}s ${{payload.source || ""}}`;

          document.querySelectorAll(".position-row").forEach((row) => {{
            const coin = row.dataset.coin;
            const mark = Number(prices[coin] || 0);
            if (!mark) return;
            const size = Number(row.dataset.size || 0);
            const entry = Number(row.dataset.entry || 0);
            const margin = Number(row.dataset.margin || 0);
            const side = row.dataset.side;
            const notional = size * mark;
            const pnl = side === "Short" ? (entry - mark) * size : (mark - entry) * size;
            const roiPrice = entry ? pnl / (entry * size) : 0;
            const roiMargin = margin ? pnl / margin : 0;
            row.querySelector(".pos-mark").textContent = fmtPrice(mark);
            row.querySelector(".pos-notional").textContent = fmtUsd(notional);
            row.querySelector(".pos-upnl").innerHTML = signedHtml(pnl, fmtUsd(pnl));
            row.querySelector(".pos-roi-price").innerHTML = signedHtml(roiPrice, (Math.abs(roiPrice) * 100).toFixed(2) + "%");
            row.querySelector(".pos-roi-margin").innerHTML = signedHtml(roiMargin, (Math.abs(roiMargin) * 100).toFixed(2) + "%");
          }});
        }} catch (error) {{}}
      }}

      refreshLiveMarks();
      setInterval(refreshLiveMarks, Math.max(100, refreshMs));

      setInterval(function () {{
        var active = document.activeElement;
        if (!active || !['INPUT', 'TEXTAREA', 'SELECT'].includes(active.tagName)) {{
          window.location.reload();
        }}
      }}, 60000);
    </script>
    """
    return render_layout("Wallet", body, "dashboard")


def trend_sentence(wallet):
    total = wallet["account_value"] + wallet["margin_used"]
    concentration = 1 - float(wallet["diversification_score"])
    if concentration >= 0.65:
        diversification = f"concentrada en {wallet['top_coin'] or 'una moneda'}"
    elif wallet["active_positions"] >= 5:
        diversification = "diversificada"
    else:
        diversification = "moderadamente diversificada"
    exposure = "sin gran exposicion" if wallet["gross_exposure"] <= 0 else f"con {usd(wallet['gross_exposure'])} en mercado"
    return f"{short_addr(wallet['address'])} esta {wallet['direction_bias'].lower()}, {diversification}, {exposure}; balance + margen observado {usd(total)}."


def trends():
    top = q_all(
        """
        SELECT *
        FROM wallets
        ORDER BY (account_value + margin_used) DESC
        LIMIT 5
        """
    )
    total_long = sum(row["long_value"] for row in top)
    total_short = sum(row["short_value"] for row in top)
    gross = total_long + total_short
    net_ratio = (total_long - total_short) / gross if gross else 0
    macro = "Neutral"
    if net_ratio > 0.2:
        macro = "Alcista"
    elif net_ratio < -0.2:
        macro = "Bajista"

    coin_rows = q_all(
        """
        SELECT p.coin,
               SUM(CASE WHEN p.side = 'Long' THEN p.position_value ELSE 0 END) long_value,
               SUM(CASE WHEN p.side = 'Short' THEN p.position_value ELSE 0 END) short_value,
               SUM(p.position_value) total_value,
               COUNT(DISTINCT p.wallet_address) wallets
        FROM positions p
        JOIN (
            SELECT address FROM wallets ORDER BY (account_value + margin_used) DESC LIMIT 5
        ) w ON w.address = p.wallet_address
        GROUP BY p.coin
        ORDER BY total_value DESC
        LIMIT 10
        """
    )
    max_value = max([row["total_value"] for row in coin_rows], default=1) or 1
    coin_bars = "".join(
        f'<div class="bar"><strong>{html.escape(row["coin"])}</strong><div class="track"><div class="fill" style="width:{min(100, row["long_value"] / max_value * 100):.1f}%"></div></div><span>{usd(row["long_value"])}</span></div>'
        f'<div class="bar"><span></span><div class="track"><div class="fill short" style="width:{min(100, row["short_value"] / max_value * 100):.1f}%"></div></div><span>{usd(row["short_value"])}</span></div>'
        for row in coin_rows
    )
    cards = "".join(
        f'<div class="card"><h2>{wallet_link(row["address"], wallet_name(row))}</h2><div class="subtle">{html.escape(trend_sentence(row))}</div>'
        f'<table style="margin-top:12px;"><tbody><tr><th>Balance + margen</th><td>{usd(row["account_value"] + row["margin_used"])}</td></tr>'
        f'<tr><th>Notional</th><td>{usd(row["total_ntl_pos"])}</td></tr>'
        f'<tr><th>Long</th><td>{usd(row["long_value"])}</td></tr><tr><th>Short</th><td>{usd(row["short_value"])}</td></tr>'
        f'<tr><th>Diversificacion</th><td>{pct(row["diversification_score"])}</td></tr></tbody></table></div>'
        for row in top
    )
    body = f"""
    <div class="topbar">
      <div>
        <h1>Tendencias top 5</h1>
        <div class="subtle">Criterio: balance neto + margen usado en posiciones</div>
      </div>
    </div>
    <section class="grid metrics">
      <div class="card"><div class="metric-label">Lectura macro</div><div class="metric-value">{badge(macro)}</div></div>
      <div class="card"><div class="metric-label">Long notional top 5</div><div class="metric-value">{usd(total_long)}</div></div>
      <div class="card"><div class="metric-label">Short notional top 5</div><div class="metric-value">{usd(total_short)}</div></div>
      <div class="card"><div class="metric-label">Sesgo neto</div><div class="metric-value">{pct(net_ratio)}</div></div>
    </section>
    <section class="grid two">
      <div class="card"><h2>Wallets analizadas</h2><div class="grid">{cards or '<div class="subtle">Sin wallets para analizar.</div>'}</div></div>
      <div class="card"><h2>Coins dominantes</h2><div class="bars">{coin_bars or '<div class="subtle">Sin posiciones para comparar.</div>'}</div></div>
    </section>
    """
    return render_layout("Tendencias", body, "trends")


class AppHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"{self.address_string()} - {fmt % args}")

    def is_authed(self):
        return read_session(self.headers.get("Cookie")).get("user") == ADMIN_USER

    def send_html(self, content, status=200, headers=None):
        encoded = content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(encoded)

    def send_json(self, payload, status=200):
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)

    def redirect(self, location, headers=None):
        self.send_response(303)
        self.send_header("Location", location)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()

    def read_form(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode("utf-8") if length else ""
        return {key: values[0] for key, values in urllib.parse.parse_qs(raw).items()}

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/login":
            self.send_html(render_login())
            return
        if parsed.path == "/logout":
            self.redirect("/login", {"Set-Cookie": "session=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"})
            return
        if not self.is_authed():
            self.redirect("/login")
            return
        if parsed.path == "/api/marks":
            query = urllib.parse.parse_qs(parsed.query)
            requested = {coin.strip().upper() for coin in query.get("coins", [""])[0].split(",") if coin.strip()}
            prices, updated_at, source = get_cached_mark_prices()
            if not prices or time.time() - updated_at > 3:
                prices = fetch_mark_prices()
                updated_at = time.time()
                source = "rest"
            if requested:
                prices = {coin: prices.get(coin, 0) for coin in requested}
            self.send_json({"prices": prices, "updated_at": updated_at, "source": source})
            return
        if parsed.path == "/":
            message = urllib.parse.parse_qs(parsed.query).get("message", [""])[0]
            self.send_html(dashboard(message))
            return
        if parsed.path == "/wallets":
            query = urllib.parse.parse_qs(parsed.query)
            self.send_html(wallets_page(query.get("q", [""])[0], query.get("bias", [""])[0]))
            return
        if parsed.path == "/events":
            self.send_html(events_page())
            return
        if parsed.path == "/trends":
            self.send_html(trends())
            return
        if parsed.path.startswith("/wallet/"):
            address = urllib.parse.unquote(parsed.path.split("/wallet/", 1)[1])
            self.send_html(wallet_profile(address))
            return
        self.send_html(render_layout("404", "<h1>404</h1>", "dashboard"), 404)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        form = self.read_form()
        if parsed.path == "/login":
            if form.get("username") == ADMIN_USER and form.get("password") == ADMIN_PASSWORD:
                session = sign_payload({"user": ADMIN_USER, "iat": int(time.time())})
                self.redirect("/", {"Set-Cookie": f"session={session}; Path=/; HttpOnly; SameSite=Lax"})
            else:
                self.send_html(render_login("Credenciales invalidas"), 401)
            return
        if not self.is_authed():
            self.redirect("/login")
            return
        if parsed.path.startswith("/wallet/") and parsed.path.endswith("/name"):
            address = urllib.parse.unquote(parsed.path.split("/wallet/", 1)[1].rsplit("/name", 1)[0]).lower()
            if ADDRESS_RE.match(address):
                save_wallet_alias(address, form.get("alias", ""))
                self.redirect(f"/wallet/{address}")
            else:
                self.redirect("/wallets")
            return
        if parsed.path.startswith("/wallet/") and parsed.path.endswith("/refresh"):
            address = urllib.parse.unquote(parsed.path.split("/wallet/", 1)[1].rsplit("/refresh", 1)[0]).lower()
            if ADDRESS_RE.match(address):
                try:
                    refresh_wallet_state(address, "manual-refresh")
                except Exception:
                    pass
                self.redirect(f"/wallet/{address}")
            else:
                self.redirect("/wallets")
            return
        if parsed.path.startswith("/wallet/") and parsed.path.endswith("/sync-fills"):
            address = urllib.parse.unquote(parsed.path.split("/wallet/", 1)[1].rsplit("/sync-fills", 1)[0]).lower()
            if ADDRESS_RE.match(address):
                try:
                    sync_wallet_fills(address)
                except Exception:
                    pass
                self.redirect(f"/wallet/{address}")
            else:
                self.redirect("/wallets")
            return
        if parsed.path == "/scan":
            result = scan_wallets(
                form.get("wallets", ""),
                use_live_discovery=form.get("use_live_discovery") == "1",
                coins_text=form.get("coins", ""),
                discovery_seconds=form.get("seconds") or None,
                max_candidates=form.get("max_candidates") or None,
            )
            msg = (
                f"Escaneo listo: {result['discovered']} candidatas, {result['scanned']} revisadas, "
                f"{result['new']} nuevas, {result['updated']} repetidas actualizadas, "
                f"{result['below_min']} descartadas bajo ${MIN_ACCOUNT_VALUE:,.0f}."
            )
            if result.get("live", {}).get("candidates"):
                msg += f" Trades live: {result['live']['candidates']} candidatas desde {result['live']['trades']} trades."
            if result["errors"]:
                msg += f" Errores: {len(result['errors'])}."
            self.redirect("/?message=" + urllib.parse.quote(msg))
            return
        self.send_html(render_layout("404", "<h1>404</h1>", "dashboard"), 404)


def main():
    init_db()
    fetch_mark_prices()
    start_mark_price_worker()
    start_auto_worker()
    port = int(os.getenv("PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), AppHandler)
    print(f"{APP_NAME} running on http://localhost:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
