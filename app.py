import base64
import hashlib
import hmac
import html
import json
import math
import os
import re
import sqlite3
import threading
import time
import http.client
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
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
TRACKED_REFRESH_INTERVAL = float(os.getenv("TRACKED_REFRESH_INTERVAL", "1"))
TRACKED_FILL_SYNC_INTERVAL = float(os.getenv("TRACKED_FILL_SYNC_INTERVAL", "1"))
TRACKED_LEDGER_SYNC_INTERVAL = float(os.getenv("TRACKED_LEDGER_SYNC_INTERVAL", "1"))
TRACKED_MAX_WALLETS = int(os.getenv("TRACKED_MAX_WALLETS", "5"))
POSITION_EVENT_EPSILON = float(os.getenv("POSITION_EVENT_EPSILON", "0.00000001"))
POSITION_CLOSE_CONFIRMATIONS = int(os.getenv("POSITION_CLOSE_CONFIRMATIONS", "2"))
MARK_WS_ENABLED = os.getenv("MARK_WS_ENABLED", "1") == "1"
PRICE_UI_REFRESH_MS = int(os.getenv("PRICE_UI_REFRESH_MS", "200"))
FILL_SYNC_ENABLED = os.getenv("FILL_SYNC_ENABLED", "1") == "1"
FILL_SYNC_INTERVAL = int(os.getenv("FILL_SYNC_INTERVAL", "60"))
FILL_SYNC_BATCH = int(os.getenv("FILL_SYNC_BATCH", "10"))
FILL_SYNC_TOP_N = int(os.getenv("FILL_SYNC_TOP_N", "10"))
LEDGER_SYNC_ENABLED = os.getenv("LEDGER_SYNC_ENABLED", "1") == "1"
LEDGER_SYNC_INTERVAL = int(os.getenv("LEDGER_SYNC_INTERVAL", "120"))
LEDGER_INITIAL_LOOKBACK_DAYS = int(os.getenv("LEDGER_INITIAL_LOOKBACK_DAYS", "30"))
API_RETRIES = int(os.getenv("HYPERLIQUID_API_RETRIES", "3"))
WHALE_VIEW_MIN_TRADES = int(os.getenv("WHALE_VIEW_MIN_TRADES", "10"))
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin")
SECRET_KEY = os.getenv("SECRET_KEY", "change-me-for-production")
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
PERU_TZ = timezone(timedelta(hours=-5), "America/Lima")
AUTO_STATUS = {
    "started": False,
    "mark_started": False,
    "last_refresh": "",
    "last_discovery": "",
    "last_mark": "",
    "last_tracked": "",
    "last_fill_sync": "",
    "last_ledger_sync": "",
    "last_error": "",
}
MARK_PRICE_CACHE = {"prices": {}, "updated_at": 0.0, "source": ""}
MARK_PRICE_LOCK = threading.Lock()
CANDLE_CACHE = {}
CANDLE_LOCK = threading.Lock()


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def peru_time_text(value):
    if value in (None, ""):
        return "-"
    try:
        text = str(value)
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(PERU_TZ).strftime("%Y-%m-%d %H:%M:%S PET")
    except Exception:
        return str(value)


def peru_status_text(value):
    if not value:
        return ""
    text = str(value)
    if " | " not in text:
        return peru_time_text(text)
    head, tail = text.split(" | ", 1)
    return f"{peru_time_text(head)} | {tail}"


def iso_to_epoch_ms(value):
    if value in (None, ""):
        return 0
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


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
                tracked INTEGER NOT NULL DEFAULT 0,
                tracked_at TEXT NOT NULL DEFAULT '',
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
                unrealized_pnl REAL NOT NULL DEFAULT 0,
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
                synced_at TEXT NOT NULL,
                last_attempt_at TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                last_inserted INTEGER NOT NULL DEFAULT 0,
                last_seen_fills INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS ledger_updates (
                ledger_id TEXT PRIMARY KEY,
                wallet_address TEXT NOT NULL,
                event_type TEXT NOT NULL,
                amount_usdc REAL NOT NULL DEFAULT 0,
                flow_usdc REAL NOT NULL DEFAULT 0,
                fee_usdc REAL NOT NULL DEFAULT 0,
                token TEXT NOT NULL DEFAULT '',
                counterparty TEXT NOT NULL DEFAULT '',
                source_dex TEXT NOT NULL DEFAULT '',
                destination_dex TEXT NOT NULL DEFAULT '',
                hash TEXT NOT NULL DEFAULT '',
                time_ms INTEGER NOT NULL,
                raw_json TEXT NOT NULL,
                synced_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ledger_sync_state (
                wallet_address TEXT PRIMARY KEY,
                last_time_ms INTEGER NOT NULL DEFAULT 0,
                synced_at TEXT NOT NULL,
                last_attempt_at TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                last_inserted INTEGER NOT NULL DEFAULT 0,
                last_seen INTEGER NOT NULL DEFAULT 0
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

            DELETE FROM fills
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM fills
                GROUP BY
                    wallet_address,
                    COALESCE(
                        NULLIF(tid, ''),
                        COALESCE(hash, '') || '|' || COALESCE(oid, '') || '|' ||
                        time_ms || '|' || coin || '|' || dir || '|' || px || '|' || size
                    )
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_wallet_tid_unique
                ON fills(wallet_address, tid)
                WHERE tid IS NOT NULL AND tid != '';

            CREATE INDEX IF NOT EXISTS idx_trade_episodes_wallet_closed
                ON trade_episodes(wallet_address, status, closed_at_ms);

            CREATE INDEX IF NOT EXISTS idx_ledger_updates_wallet_time
                ON ledger_updates(wallet_address, time_ms);
            """
        )
        ensure_column(conn, "wallets", "alias", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "wallets", "tracked", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "wallets", "tracked_at", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "positions", "current_px", "REAL NOT NULL DEFAULT 0")
        ensure_column(conn, "positions", "capital_used", "REAL NOT NULL DEFAULT 0")
        ensure_column(conn, "positions", "roi_price", "REAL NOT NULL DEFAULT 0")
        ensure_column(conn, "positions", "roi_capital", "REAL NOT NULL DEFAULT 0")
        ensure_column(conn, "positions", "leverage_value", "REAL NOT NULL DEFAULT 0")
        ensure_column(conn, "fill_sync_state", "last_attempt_at", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "fill_sync_state", "last_error", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "fill_sync_state", "last_inserted", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "fill_sync_state", "last_seen_fills", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "wallet_snapshots", "unrealized_pnl", "REAL NOT NULL DEFAULT 0")
        ensure_column(conn, "ledger_updates", "amount_usdc", "REAL NOT NULL DEFAULT 0")
        ensure_column(conn, "ledger_updates", "flow_usdc", "REAL NOT NULL DEFAULT 0")
        ensure_column(conn, "ledger_updates", "fee_usdc", "REAL NOT NULL DEFAULT 0")
        ensure_column(conn, "ledger_updates", "token", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "ledger_updates", "counterparty", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "ledger_updates", "source_dex", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "ledger_updates", "destination_dex", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "ledger_sync_state", "last_attempt_at", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "ledger_sync_state", "last_error", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "ledger_sync_state", "last_inserted", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "ledger_sync_state", "last_seen", "INTEGER NOT NULL DEFAULT 0")


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
    last_error = None
    for attempt in range(max(1, API_RETRIES)):
        request = urllib.request.Request(
            INFO_URL,
            data=body,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "simple-tracker-whale/1.0",
                "Accept": "application/json",
                "Connection": "close",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=25) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in {429, 500, 502, 503, 504}:
                raise
        except (urllib.error.URLError, ConnectionResetError, TimeoutError, http.client.HTTPException) as exc:
            last_error = exc
        if attempt < max(1, API_RETRIES) - 1:
            time.sleep(0.6 * (attempt + 1))
    raise RuntimeError(f"Hyperliquid API no respondio despues de {max(1, API_RETRIES)} intentos: {last_error}")


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


def fetch_candles(coin, interval="1h", lookback_ms=None):
    coin = clean_coin(coin) if "clean_coin" in globals() else str(coin or "").strip()
    lookback_ms = int(lookback_ms or 7 * 24 * 60 * 60 * 1000)
    now_value = now_ms()
    cache_key = (coin, interval, lookback_ms)
    with CANDLE_LOCK:
        cached = CANDLE_CACHE.get(cache_key)
        if cached and time.time() - cached["updated_at"] < 60:
            return cached["candles"]
    try:
        data = hyperliquid_info({
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": now_value - lookback_ms,
                "endTime": now_value,
            },
        })
    except Exception:
        data = []
    candles = []
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            close = to_float(item.get("c"))
            if close <= 0:
                continue
            candles.append({
                "time": int(to_float(item.get("T") or item.get("t"))),
                "open": to_float(item.get("o")),
                "high": to_float(item.get("h")),
                "low": to_float(item.get("l")),
                "close": close,
            })
    candles.sort(key=lambda item: item["time"])
    with CANDLE_LOCK:
        CANDLE_CACHE[cache_key] = {"updated_at": time.time(), "candles": candles[-240:]}
    return candles[-240:]


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
                long_value, short_value, unrealized_pnl, active_positions, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                wallet["address"],
                wallet["account_value"],
                wallet["total_ntl_pos"],
                wallet["gross_exposure"],
                wallet["long_value"],
                wallet["short_value"],
                sum(to_float(position["unrealized_pnl"]) for position in wallet["positions"]),
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


def fetch_user_fills(address):
    data = hyperliquid_info({
        "type": "userFills",
        "user": address,
        "aggregateByTime": False,
    })
    return data if isinstance(data, list) else []


def fetch_user_ledger_updates(address, start_ms, end_ms=None):
    payload = {
        "type": "userNonFundingLedgerUpdates",
        "user": address,
        "startTime": int(start_ms),
    }
    if end_ms:
        payload["endTime"] = int(end_ms)
    data = hyperliquid_info(payload)
    if isinstance(data, dict) and isinstance(data.get("value"), list):
        return data["value"]
    return data if isinstance(data, list) else []


def ledger_id(update):
    delta = update.get("delta") or {}
    return "|".join(
        str(value)
        for value in (
            update.get("hash", ""),
            update.get("time", ""),
            delta.get("type", ""),
            delta.get("nonce", ""),
            delta.get("amount", delta.get("usdc", "")),
            delta.get("token", ""),
        )
    )


def classify_ledger_flow(address, update):
    address = address.lower()
    delta = update.get("delta") or {}
    event_type = str(delta.get("type", "unknown"))
    token = str(delta.get("token") or delta.get("feeToken") or ("USDC" if "usdc" in delta else ""))
    amount_usdc = 0.0
    flow_usdc = 0.0
    fee_usdc = 0.0
    source_dex = str(delta.get("sourceDex", ""))
    destination_dex = str(delta.get("destinationDex", ""))
    counterparty = str(delta.get("destination") or delta.get("user") or "")

    if event_type == "deposit":
        amount_usdc = to_float(delta.get("usdc"))
        flow_usdc = amount_usdc
        token = token or "USDC"
    elif event_type in {"withdraw", "withdrawal"}:
        amount_usdc = to_float(delta.get("usdc") or delta.get("amount") or delta.get("usdcValue"))
        flow_usdc = -abs(amount_usdc)
        token = token or "USDC"
    elif event_type == "send":
        amount_usdc = to_float(delta.get("usdcValue") or delta.get("amount"))
        fee_usdc = to_float(delta.get("fee")) if str(delta.get("feeToken", "USDC") or "USDC").upper() == "USDC" else 0.0
        sender = str(delta.get("user", "")).lower()
        destination = str(delta.get("destination", "")).lower()
        if sender == address and source_dex == "":
            flow_usdc = -abs(amount_usdc) - abs(fee_usdc)
        elif destination == address and destination_dex == "":
            flow_usdc = abs(amount_usdc)
        elif sender == address and destination == address and destination_dex == "":
            flow_usdc = abs(amount_usdc)
        elif sender == address:
            flow_usdc = -abs(fee_usdc)
    elif "usdc" in delta:
        amount_usdc = to_float(delta.get("usdc"))
        flow_usdc = amount_usdc
        token = token or "USDC"

    return {
        "event_type": event_type,
        "amount_usdc": amount_usdc,
        "flow_usdc": flow_usdc,
        "fee_usdc": fee_usdc,
        "token": token,
        "counterparty": counterparty,
        "source_dex": source_dex,
        "destination_dex": destination_dex,
    }


def save_ledger_updates(conn, address, updates):
    synced_at = now_iso()
    inserted = 0
    max_time = 0
    for update in updates:
        time_ms = int(to_float(update.get("time"), 0))
        max_time = max(max_time, time_ms)
        classified = classify_ledger_flow(address, update)
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO ledger_updates (
                ledger_id, wallet_address, event_type, amount_usdc, flow_usdc,
                fee_usdc, token, counterparty, source_dex, destination_dex,
                hash, time_ms, raw_json, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ledger_id(update),
                address,
                classified["event_type"],
                classified["amount_usdc"],
                classified["flow_usdc"],
                classified["fee_usdc"],
                classified["token"],
                classified["counterparty"],
                classified["source_dex"],
                classified["destination_dex"],
                str(update.get("hash", "")),
                time_ms,
                json.dumps(update, separators=(",", ":")),
                synced_at,
            ),
        )
        inserted += cursor.rowcount
    return inserted, max_time


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


def fill_reversal_sides(direction):
    direction = str(direction or "")
    if ">" not in direction:
        return "", ""
    before, after = [part.strip() for part in direction.split(">", 1)]
    if before in ("Long", "Short") and after in ("Long", "Short"):
        return before, after
    return "", ""


def derived_fill(fill, size, fee, builder_fee, closed_pnl):
    return {
        "size": size,
        "px": fill["px"],
        "fee": fee,
        "builder_fee": builder_fee,
        "crossed": fill["crossed"],
        "time_ms": fill["time_ms"],
        "closed_pnl": closed_pnl,
        "start_position": fill["start_position"],
    }


def fill_episode_steps(fill):
    direction = str(fill["dir"] or "")
    size = abs(to_float(fill["size"]))
    if size <= POSITION_EVENT_EPSILON:
        return []

    before, after = fill_reversal_sides(direction)
    if before and after:
        start_abs = abs(to_float(fill["start_position"]))
        close_size = min(start_abs, size)
        open_size = max(0.0, size - close_size)
        fee = to_float(fill["fee"])
        builder_fee = to_float(fill["builder_fee"])
        steps = []
        if close_size > POSITION_EVENT_EPSILON:
            close_share = close_size / size
            steps.append((
                "close",
                before,
                derived_fill(fill, close_size, fee * close_share, builder_fee * close_share, fill["closed_pnl"]),
                True,
            ))
        if open_size > POSITION_EVENT_EPSILON:
            open_share = open_size / size
            steps.append((
                "open",
                after,
                derived_fill(fill, open_size, fee * open_share, builder_fee * open_share, 0.0),
                False,
            ))
        return steps

    side = fill_position_side(direction)
    action = fill_action(direction)
    if not side or not action:
        return []
    closes_position = False
    if action == "close":
        remaining = abs(to_float(fill["start_position"])) - size
        closes_position = remaining <= POSITION_EVENT_EPSILON
    return [(action, side, fill, closes_position)]


def new_episode(address, coin, side, opened_at_ms=None):
    return {
        "wallet_address": address,
        "coin": coin,
        "side": side,
        "status": "open",
        "started_from_flat": None,
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
    # Hyperliquid's fill fee is the total fee; builderFee is already included when present.
    episode["fees"] += to_float(fill["fee"])
    episode["fill_count"] += 1
    if int(fill["crossed"]):
        episode["taker_fills"] += 1
    else:
        episode["maker_fills"] += 1
    if action == "open":
        if episode["open_size"] <= POSITION_EVENT_EPSILON:
            episode["started_from_flat"] = abs(to_float(row_value(fill, "start_position"))) <= POSITION_EVENT_EPSILON
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
    if episode["status"] == "closed":
        size_tolerance = max(POSITION_EVENT_EPSILON * 10, max(episode["open_size"], episode["close_size"]) * 0.01)
        if episode.get("started_from_flat") is not True or abs(episode["open_size"] - episode["close_size"]) > size_tolerance:
            episode["status"] = "partial"
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
            ORDER BY
                time_ms ASC,
                CASE
                    WHEN dir LIKE 'Close%' THEN -ABS(start_position)
                    ELSE ABS(start_position)
                END ASC,
                fill_id ASC
            """,
            (address,),
        ).fetchall()
        active = {}
        for fill in fills:
            for action, side, episode_fill, closes_position in fill_episode_steps(fill):
                key = (fill["coin"], side)
                if action == "open":
                    episode = active.get(key)
                    if episode and episode["open_size"] <= POSITION_EVENT_EPSILON and episode["close_size"] > POSITION_EVENT_EPSILON:
                        episode["status"] = "partial"
                        persist_episode(conn, episode, rebuilt_at)
                        active.pop(key, None)
                        episode = None
                    if not episode:
                        episode = new_episode(address, fill["coin"], side, int(fill["time_ms"]))
                        active[key] = episode
                    add_fill_to_episode(episode, episode_fill, action)
                    continue

                episode = active.get(key)
                if not episode:
                    episode = new_episode(address, fill["coin"], side, None)
                    episode["status"] = "partial"
                    active[key] = episode
                add_fill_to_episode(episode, episode_fill, action)
                if closes_position:
                    if episode["open_size"] > POSITION_EVENT_EPSILON:
                        episode["status"] = "closed"
                    persist_episode(conn, episode, rebuilt_at)
                    active.pop(key, None)

        for episode in active.values():
            persist_episode(conn, episode, rebuilt_at)


def sync_wallet_fills(address, force_rebuild=False):
    address = address.lower()
    end_ms = now_ms()
    attempted_at = now_iso()
    with connect_db() as conn:
        state = conn.execute(
            "SELECT * FROM fill_sync_state WHERE wallet_address = ?",
            (address,),
        ).fetchone()
        start_ms = int(state["last_time_ms"]) + 1 if state else 0

    total_inserted = 0
    total_seen = 0
    max_seen = start_ms
    cursor_ms = start_ms
    try:
        if not state:
            fills = fetch_user_fills(address)
            total_seen = len(fills)
            with connect_db() as conn:
                inserted, batch_max = save_fills(conn, address, fills)
                total_inserted += inserted
                max_seen = max(max_seen, batch_max)
        else:
            while cursor_ms <= end_ms:
                fills = fetch_user_fills_by_time(address, cursor_ms, end_ms)
                if not fills:
                    break
                total_seen += len(fills)
                with connect_db() as conn:
                    inserted, batch_max = save_fills(conn, address, fills)
                    total_inserted += inserted
                    max_seen = max(max_seen, batch_max)
                if len(fills) < 2000 or batch_max <= cursor_ms:
                    break
                cursor_ms = batch_max + 1
    except Exception as exc:
        with connect_db() as conn:
            conn.execute(
                """
                INSERT INTO fill_sync_state (
                    wallet_address, last_time_ms, synced_at, last_attempt_at,
                    last_error, last_inserted, last_seen_fills
                ) VALUES (?, ?, ?, ?, ?, 0, ?)
                ON CONFLICT(wallet_address) DO UPDATE SET
                    last_attempt_at=excluded.last_attempt_at,
                    last_error=excluded.last_error,
                    last_inserted=0,
                    last_seen_fills=excluded.last_seen_fills
                """,
                (
                    address,
                    int(state["last_time_ms"]) if state else 0,
                    state["synced_at"] if state else "",
                    attempted_at,
                    str(exc)[:400],
                    total_seen,
                ),
            )
        raise

    with connect_db() as conn:
        conn.execute(
            """
            INSERT INTO fill_sync_state (
                wallet_address, last_time_ms, synced_at, last_attempt_at,
                last_error, last_inserted, last_seen_fills
            ) VALUES (?, ?, ?, ?, '', ?, ?)
            ON CONFLICT(wallet_address) DO UPDATE SET
                last_time_ms=excluded.last_time_ms,
                synced_at=excluded.synced_at,
                last_attempt_at=excluded.last_attempt_at,
                last_error='',
                last_inserted=excluded.last_inserted,
                last_seen_fills=excluded.last_seen_fills
            """,
            (address, max(max_seen, end_ms - 1), now_iso(), attempted_at, total_inserted, total_seen),
        )
    if force_rebuild or total_inserted > 0:
        rebuild_trade_episodes(address)
    return total_inserted


def sync_wallet_ledger(address):
    address = address.lower()
    end_ms = now_ms()
    attempted_at = now_iso()
    with connect_db() as conn:
        state = conn.execute(
            "SELECT * FROM ledger_sync_state WHERE wallet_address = ?",
            (address,),
        ).fetchone()
        if state:
            start_ms = int(row_value(state, "last_time_ms", 0)) + 1
        else:
            start_ms = max(0, end_ms - int(LEDGER_INITIAL_LOOKBACK_DAYS * 24 * 60 * 60 * 1000))

    total_inserted = 0
    total_seen = 0
    max_seen = start_ms
    cursor_ms = start_ms
    try:
        while cursor_ms <= end_ms:
            updates = fetch_user_ledger_updates(address, cursor_ms, end_ms)
            if not updates:
                break
            total_seen += len(updates)
            with connect_db() as conn:
                inserted, batch_max = save_ledger_updates(conn, address, updates)
                total_inserted += inserted
                max_seen = max(max_seen, batch_max)
            if len(updates) < 2000 or batch_max <= cursor_ms:
                break
            cursor_ms = batch_max + 1
    except Exception as exc:
        with connect_db() as conn:
            conn.execute(
                """
                INSERT INTO ledger_sync_state (
                    wallet_address, last_time_ms, synced_at, last_attempt_at,
                    last_error, last_inserted, last_seen
                ) VALUES (?, ?, ?, ?, ?, 0, ?)
                ON CONFLICT(wallet_address) DO UPDATE SET
                    last_attempt_at=excluded.last_attempt_at,
                    last_error=excluded.last_error,
                    last_inserted=0,
                    last_seen=excluded.last_seen
                """,
                (
                    address,
                    int(row_value(state, "last_time_ms", 0)) if state else 0,
                    row_value(state, "synced_at", "") if state else "",
                    attempted_at,
                    str(exc)[:400],
                    total_seen,
                ),
            )
        raise

    with connect_db() as conn:
        conn.execute(
            """
            INSERT INTO ledger_sync_state (
                wallet_address, last_time_ms, synced_at, last_attempt_at,
                last_error, last_inserted, last_seen
            ) VALUES (?, ?, ?, ?, '', ?, ?)
            ON CONFLICT(wallet_address) DO UPDATE SET
                last_time_ms=excluded.last_time_ms,
                synced_at=excluded.synced_at,
                last_attempt_at=excluded.last_attempt_at,
                last_error='',
                last_inserted=excluded.last_inserted,
                last_seen=excluded.last_seen
            """,
            (address, max(max_seen, end_ms - 1), now_iso(), attempted_at, total_inserted, total_seen),
        )
    return total_inserted


def reset_all_pnl_graphs():
    reset_at = now_iso()
    with connect_db() as conn:
        conn.execute("DELETE FROM wallet_snapshots")
        conn.execute("DELETE FROM ledger_updates")
        conn.execute("DELETE FROM ledger_sync_state")
        wallets = conn.execute(
            """
            SELECT address, account_value, total_ntl_pos, gross_exposure,
                   long_value, short_value, active_positions
            FROM wallets
            """
        ).fetchall()
        for wallet in wallets:
            upnl = conn.execute(
                "SELECT COALESCE(SUM(unrealized_pnl), 0) FROM positions WHERE wallet_address = ?",
                (wallet["address"],),
            ).fetchone()[0]
            conn.execute(
                """
                INSERT INTO wallet_snapshots (
                    wallet_address, account_value, total_ntl_pos, gross_exposure,
                    long_value, short_value, unrealized_pnl, active_positions, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    wallet["address"],
                    wallet["account_value"],
                    wallet["total_ntl_pos"],
                    wallet["gross_exposure"],
                    wallet["long_value"],
                    wallet["short_value"],
                    to_float(upnl),
                    wallet["active_positions"],
                    reset_at,
                ),
            )
    return len(wallets)


def sync_saved_wallet_fills(batch_size=None):
    batch_size = max(1, int(batch_size or FILL_SYNC_BATCH))
    top_n = max(batch_size, int(FILL_SYNC_TOP_N))
    wallets = q_all(
        """
        WITH top_wallets AS (
            SELECT address, account_value, margin_used, last_seen
            FROM wallets
            ORDER BY account_value DESC
            LIMIT ?
        )
        SELECT t.address
        FROM top_wallets t
        LEFT JOIN fill_sync_state s ON s.wallet_address = t.address
        ORDER BY
            COALESCE(NULLIF(s.last_attempt_at, ''), NULLIF(s.synced_at, ''), '') ASC,
            t.account_value DESC
        LIMIT ?
        """,
        (top_n, batch_size),
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


def sync_saved_wallet_ledgers(batch_size=None):
    batch_size = max(1, int(batch_size or FILL_SYNC_BATCH))
    top_n = max(batch_size, int(FILL_SYNC_TOP_N))
    wallets = q_all(
        """
        WITH top_wallets AS (
            SELECT address, account_value, margin_used, last_seen
            FROM wallets
            ORDER BY account_value DESC
            LIMIT ?
        )
        SELECT t.address
        FROM top_wallets t
        LEFT JOIN ledger_sync_state s ON s.wallet_address = t.address
        ORDER BY
            COALESCE(NULLIF(s.last_attempt_at, ''), NULLIF(s.synced_at, ''), '') ASC,
            t.account_value DESC
        LIMIT ?
        """,
        (top_n, batch_size),
    )
    synced = 0
    inserted_total = 0
    errors = []
    for wallet in wallets:
        try:
            inserted_total += sync_wallet_ledger(wallet["address"])
            synced += 1
        except Exception as exc:
            errors.append(f"{short_addr(wallet['address'])}: {exc}")
        time.sleep(0.1)
    return synced, inserted_total, errors


def wallet_name(wallet):
    alias = wallet["alias"] if "alias" in wallet.keys() else ""
    return alias.strip() or short_addr(wallet["address"])


def save_wallet_alias(address, alias):
    alias = (alias or "").strip()[:80]
    with connect_db() as conn:
        conn.execute("UPDATE wallets SET alias = ? WHERE address = ?", (alias, address.lower()))


def set_wallet_tracked(address, tracked):
    address = address.lower()
    with connect_db() as conn:
        conn.execute(
            """
            UPDATE wallets
            SET tracked = ?, tracked_at = ?
            WHERE address = ?
            """,
            (1 if tracked else 0, now_iso() if tracked else "", address),
        )


def refresh_wallet_state(address, source="refresh", mark_prices=None):
    address = (address or "").lower()
    if not ADDRESS_RE.match(address):
        return False
    mark_prices = mark_prices if mark_prices is not None else fetch_mark_prices()
    state = hyperliquid_info({"type": "clearinghouseState", "user": address})
    wallet = parse_wallet(address, state, source, mark_prices)
    save_wallet(wallet)
    return True


def tracked_wallet_rows():
    return q_all(
        """
        SELECT address
        FROM wallets
        WHERE tracked = 1
        ORDER BY tracked_at ASC, account_value DESC
        LIMIT ?
        """,
        (max(1, TRACKED_MAX_WALLETS),),
    )


def refresh_tracked_wallets():
    wallets = tracked_wallet_rows()
    refreshed = 0
    errors = []
    if not wallets:
        return refreshed, errors
    mark_prices = fetch_mark_prices()
    for wallet in wallets:
        try:
            if refresh_wallet_state(wallet["address"], "tracked-refresh", mark_prices):
                refreshed += 1
        except Exception as exc:
            errors.append(f"{short_addr(wallet['address'])}: {exc}")
        time.sleep(0.05)
    return refreshed, errors


def sync_tracked_wallet_fills():
    wallets = tracked_wallet_rows()
    synced = 0
    inserted_total = 0
    errors = []
    for wallet in wallets:
        try:
            inserted = sync_wallet_fills(wallet["address"])
            inserted_total += inserted
            synced += 1
        except Exception as exc:
            errors.append(f"{short_addr(wallet['address'])}: {exc}")
        time.sleep(0.05)
    return synced, inserted_total, errors


def sync_tracked_wallet_ledgers():
    wallets = tracked_wallet_rows()
    synced = 0
    inserted_total = 0
    errors = []
    for wallet in wallets:
        try:
            inserted_total += sync_wallet_ledger(wallet["address"])
            synced += 1
        except Exception as exc:
            errors.append(f"{short_addr(wallet['address'])}: {exc}")
        time.sleep(0.05)
    return synced, inserted_total, errors


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
    next_tracked_refresh = time.monotonic()
    next_tracked_fill_sync = time.monotonic() + 1
    next_tracked_ledger_sync = time.monotonic() + 2
    next_general_refresh = time.monotonic()
    next_discovery = time.monotonic() + 5
    next_fill_sync = time.monotonic() + 10
    next_ledger_sync = time.monotonic() + 20
    while True:
        try:
            now_tick = time.monotonic()

            if now_tick >= next_tracked_refresh:
                tracked_refreshed, tracked_errors = refresh_tracked_wallets()
                if tracked_refreshed or tracked_errors:
                    AUTO_STATUS["last_tracked"] = f"{now_iso()} | refresh {tracked_refreshed}/{TRACKED_MAX_WALLETS}"
                if tracked_errors:
                    AUTO_STATUS["last_error"] = "; ".join(tracked_errors[-3:])
                next_tracked_refresh = now_tick + max(0.5, TRACKED_REFRESH_INTERVAL)

            if now_tick >= next_general_refresh:
                refreshed, errors = refresh_saved_wallets()
                AUTO_STATUS["last_refresh"] = f"{now_iso()} | {refreshed} wallets"
                if errors:
                    AUTO_STATUS["last_error"] = "; ".join(errors[-3:])
                next_general_refresh = now_tick + max(1.0, AUTO_REFRESH_INTERVAL)

            if now_tick >= next_tracked_fill_sync:
                tracked_synced, tracked_inserted, tracked_fill_errors = sync_tracked_wallet_fills()
                if tracked_synced or tracked_inserted or tracked_fill_errors:
                    AUTO_STATUS["last_tracked"] = f"{now_iso()} | refresh/fills {tracked_synced}/{TRACKED_MAX_WALLETS} | +{tracked_inserted}"
                if tracked_fill_errors:
                    AUTO_STATUS["last_error"] = "; ".join(tracked_fill_errors[-3:])
                next_tracked_fill_sync = now_tick + max(0.5, TRACKED_FILL_SYNC_INTERVAL)

            if LEDGER_SYNC_ENABLED and now_tick >= next_tracked_ledger_sync:
                ledger_synced, ledger_inserted, ledger_errors = sync_tracked_wallet_ledgers()
                if ledger_synced or ledger_inserted or ledger_errors:
                    AUTO_STATUS["last_ledger_sync"] = f"{now_iso()} | live {ledger_synced}/{TRACKED_MAX_WALLETS} | +{ledger_inserted}"
                if ledger_errors:
                    AUTO_STATUS["last_error"] = "; ".join(ledger_errors[-3:])
                next_tracked_ledger_sync = now_tick + max(0.5, TRACKED_LEDGER_SYNC_INTERVAL)

            if FILL_SYNC_ENABLED and now_tick >= next_fill_sync:
                synced, fill_errors = sync_saved_wallet_fills()
                AUTO_STATUS["last_fill_sync"] = f"{now_iso()} | {synced}/{FILL_SYNC_TOP_N} top wallets"
                if fill_errors:
                    AUTO_STATUS["last_error"] = "; ".join(fill_errors[-3:])
                next_fill_sync = now_tick + max(10, FILL_SYNC_INTERVAL)

            if LEDGER_SYNC_ENABLED and now_tick >= next_ledger_sync:
                ledger_synced, ledger_inserted, ledger_errors = sync_saved_wallet_ledgers()
                AUTO_STATUS["last_ledger_sync"] = f"{now_iso()} | top {ledger_synced}/{FILL_SYNC_TOP_N} | +{ledger_inserted}"
                if ledger_errors:
                    AUTO_STATUS["last_error"] = "; ".join(ledger_errors[-3:])
                next_ledger_sync = now_tick + max(30, LEDGER_SYNC_INTERVAL)

            if AUTO_DISCOVERY_ENABLED and now_tick >= next_discovery:
                result = scan_wallets(use_live_discovery=True)
                AUTO_STATUS["last_discovery"] = (
                    f"{now_iso()} | {result['discovered']} candidatas | {result['new']} nuevas | {result['updated']} repetidas"
                )
                if result["errors"]:
                    AUTO_STATUS["last_error"] = "; ".join(result["errors"][-3:])
                next_discovery = now_tick + max(60, AUTO_DISCOVERY_INTERVAL)
        except Exception as exc:
            AUTO_STATUS["last_error"] = str(exc)
        time.sleep(0.25)


def start_auto_worker():
    if AUTO_STATUS["started"]:
        return
    thread = threading.Thread(target=auto_worker, name="wallet-auto-worker", daemon=True)
    thread.start()


def recent_position_events(limit=5):
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


def event_label(event_type):
    return "Apertura" if event_type == "open" else "Cierre" if event_type == "close" else str(event_type)


def event_tone(event_type):
    return "open" if event_type == "open" else "close" if event_type == "close" else "neutral"


def event_payload(event):
    label = event["alias"] or short_addr(event["wallet_address"])
    return {
        "id": int(event["id"]),
        "type": event["event_type"],
        "label": event_label(event["event_type"]),
        "tone": event_tone(event["event_type"]),
        "wallet": label,
        "wallet_address": event["wallet_address"],
        "coin": event["coin"],
        "side": event["side"],
        "size": to_float(event["size"]),
        "notional": to_float(event["notional"]),
        "created_at": peru_time_text(event["created_at"]),
        "time_ms": iso_to_epoch_ms(event["created_at"]),
        "entry_px": to_float(event["entry_px"]),
        "current_px": to_float(event["current_px"]),
    }


def sidebar_activity_events(limit=5):
    return [event_payload(event) for event in recent_position_events(limit)]


def render_sidebar_activity(events):
    if not events:
        items = '<div class="side-empty">Sin aperturas/cierres detectados aun.</div>'
    else:
        items = "".join(
            "<a class='side-event' href='/wallet/{wallet_address}'>"
            "<span class='side-dot {tone}'></span>"
            "<span><b>{label}</b> {coin} {side}<small>{wallet} | {size:,.4f} | {notional}</small></span>"
            "</a>".format(
                wallet_address=html.escape(event["wallet_address"]),
                tone=html.escape(event["tone"]),
                label=html.escape(event["label"]),
                coin=html.escape(event["coin"]),
                side=html.escape(event["side"]),
                wallet=html.escape(event["wallet"]),
                size=event["size"],
                notional=html.escape(usd(event["notional"])),
            )
            for event in events
        )
    return (
        '<div class="side-activity">'
        '<div class="side-title">Alertas posiciones</div>'
        f'<div id="side-events">{items}</div>'
        '<a class="side-more" href="/events">Ver actividad</a>'
        '</div>'
    )


def render_layout(title, body, active="dashboard", message=""):
    nav = [
        ("dashboard", "/", "Dashboard"),
        ("wallets", "/wallets", "Wallets"),
        ("events", "/events", "Actividad"),
        ("whale", "/whale-view", "Whale view"),
        ("trends", "/trends", "Tendencias"),
    ]
    links = "".join(
        f'<a class="nav-link {"active" if key == active else ""}" href="{href}">{label}</a>'
        for key, href, label in nav
    )
    message_html = f'<div class="notice">{html.escape(message)}</div>' if message else ""
    sidebar_activity = render_sidebar_activity(sidebar_activity_events(5))
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
      overflow-y: auto;
      padding-bottom: 72px;
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
    .side-activity {{
      margin-top: 22px;
      border-top: 1px solid rgba(203,213,225,.18);
      padding-top: 16px;
    }}
    .side-title {{
      color: #94a3b8;
      font-size: 11px;
      font-weight: 850;
      letter-spacing: .04em;
      text-transform: uppercase;
      margin-bottom: 9px;
    }}
    .side-event {{
      display: grid;
      grid-template-columns: 9px 1fr;
      gap: 9px;
      color: #e5e7eb;
      padding: 9px 7px;
      border-radius: 8px;
      font-size: 12px;
      line-height: 1.25;
      margin-bottom: 5px;
    }}
    .side-event:hover {{ background: #243044; }}
    .side-event b {{ display: block; font-size: 12px; margin-bottom: 2px; }}
    .side-event small {{ display: block; color: #94a3b8; font-size: 11px; overflow-wrap: anywhere; }}
    .side-dot {{ width: 8px; height: 8px; border-radius: 999px; margin-top: 4px; background: #60a5fa; }}
    .side-dot.open {{ background: #34d399; box-shadow: 0 0 0 3px rgba(52,211,153,.12); }}
    .side-dot.close {{ background: #fb7185; box-shadow: 0 0 0 3px rgba(251,113,133,.12); }}
    .side-empty {{ color: #94a3b8; font-size: 12px; line-height: 1.35; padding: 7px; }}
    .side-more {{ display:block; color:#cbd5e1; font-size:12px; padding:7px; }}
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
    .whale-terminal {{
      background: #0b1119;
      color: #dbe4f0;
      border: 1px solid #202a38;
      border-radius: 8px;
      overflow: hidden;
      box-shadow: 0 18px 36px rgba(11,17,25,.22);
    }}
    .whale-terminal-head {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
      padding: 18px 20px 12px;
      border-bottom: 1px solid #202a38;
    }}
    .terminal-title {{ font-size: 22px; font-weight: 900; letter-spacing: 0; }}
    .terminal-subtitle {{ color: #7f8aa3; font-size: 13px; margin-top: 4px; }}
    .terminal-price {{ text-align: right; font-size: 24px; font-weight: 900; }}
    .terminal-price span {{ display:block; color:#79e0b3; font-size:14px; font-weight:800; margin-top:3px; }}
    .terminal-tabs {{ display:flex; justify-content:flex-end; gap:10px; padding: 12px 20px 0; color:#7f8aa3; font-weight:800; }}
    .terminal-tab {{ padding:7px 12px; border-radius:8px; }}
    .terminal-tab.active {{ color:#f8fafc; background:#263244; }}
    .chart-controls {{ display:flex; gap:6px; margin-left:8px; }}
    .chart-controls button {{
      min-height: 32px;
      border: 1px solid #263244;
      background: #111a26;
      color: #dbe4f0;
      border-radius: 7px;
      padding: 5px 10px;
      font-weight: 900;
      cursor: pointer;
    }}
    .chart-controls button:hover {{ background:#263244; }}
    .terminal-chart-wrap {{ height: 520px; padding: 8px 12px 0; }}
    #whale-chart {{ width:100%; height:100%; display:block; cursor: grab; }}
    #whale-chart.dragging {{ cursor: grabbing; }}
    .terminal-stats {{
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 1px;
      background: #202a38;
      border-top: 1px solid #202a38;
    }}
    .terminal-stats > div {{ background:#0b1119; padding:16px 18px; min-width:0; }}
    .terminal-stats span {{ display:block; color:#7f8aa3; font-size:13px; font-weight:800; margin-bottom:8px; }}
    .terminal-stats b {{ display:block; color:#f8fafc; font-size:21px; overflow-wrap:anywhere; }}
    .terminal-stats small {{ display:block; color:#7f8aa3; font-size:13px; margin-top:5px; }}
    @media (max-width: 980px) {{
      .shell {{ grid-template-columns: 1fr; }}
      aside {{ position: static; height: auto; display: flex; align-items:center; gap: 12px; flex-wrap: wrap; }}
      .brand {{ margin: 0 10px 0 0; }}
      .side-activity {{ width: 100%; order: 3; margin-top: 4px; padding-top: 12px; }}
      #side-events {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 6px; }}
      .logout {{ position: static; margin-left: auto; }}
      main {{ padding: 18px; }}
      .metrics, .two, .three {{ grid-template-columns: 1fr; }}
      .topbar {{ flex-direction: column; }}
      .terminal-chart-wrap {{ height: 420px; }}
      .terminal-stats {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <aside>
      <div class="brand">{APP_NAME}</div>
      {links}
      {sidebar_activity}
      <a class="logout" href="/logout">Cerrar sesion</a>
    </aside>
    <main>
      {message_html}
      {body}
    </main>
  </div>
  <script>
    async function refreshSideEvents() {{
      const target = document.getElementById("side-events");
      if (!target) return;
      const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({{
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
      }}[char]));
      try {{
        const response = await fetch("/api/position-events?limit=5", {{ cache: "no-store" }});
        if (!response.ok) return;
        const data = await response.json();
        if (!Array.isArray(data.events)) return;
        if (!data.events.length) {{
          target.innerHTML = '<div class="side-empty">Sin aperturas/cierres detectados aun.</div>';
          return;
        }}
        target.innerHTML = data.events.map((event) => {{
          const size = Number(event.size || 0).toLocaleString(undefined, {{ maximumFractionDigits: 4 }});
          const notional = new Intl.NumberFormat(undefined, {{ style: "currency", currency: "USD", maximumFractionDigits: 0 }}).format(Number(event.notional || 0));
          const href = "/wallet/" + encodeURIComponent(event.wallet_address || "");
          return `<a class="side-event" href="${{href}}">
            <span class="side-dot ${{event.tone || "neutral"}}"></span>
            <span><b>${{esc(event.label)}}</b> ${{esc(event.coin)}} ${{esc(event.side)}}<small>${{esc(event.wallet)}} | ${{esc(size)}} | ${{esc(notional)}}</small></span>
          </a>`;
        }}).join("");
      }} catch (error) {{}}
    }}
    setInterval(refreshSideEvents, 5000);
  </script>
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


def wallet_ledger_rows(address, start_ms=0, limit=20):
    return q_all(
        """
        SELECT *
        FROM ledger_updates
        WHERE wallet_address = ?
          AND time_ms >= ?
        ORDER BY time_ms DESC, ledger_id DESC
        LIMIT ?
        """,
        (address, int(start_ms or 0), int(limit)),
    )


def wallet_ledger_summary(address, start_ms=0, end_ms=None):
    params = [address, int(start_ms or 0)]
    end_clause = ""
    if end_ms:
        end_clause = "AND time_ms <= ?"
        params.append(int(end_ms))
    row = q_one(
        f"""
        SELECT
            COALESCE(SUM(CASE WHEN flow_usdc > 0 THEN flow_usdc ELSE 0 END), 0) inflow,
            COALESCE(SUM(CASE WHEN flow_usdc < 0 THEN flow_usdc ELSE 0 END), 0) outflow,
            COALESCE(SUM(flow_usdc), 0) net_flow,
            COUNT(*) events
        FROM ledger_updates
        WHERE wallet_address = ?
          AND time_ms >= ?
          {end_clause}
        """,
        tuple(params),
    )
    return {
        "inflow": to_float(row["inflow"]) if row else 0.0,
        "outflow": to_float(row["outflow"]) if row else 0.0,
        "net_flow": to_float(row["net_flow"]) if row else 0.0,
        "events": int(row["events"] or 0) if row else 0,
    }


def wallet_closed_pnl_points(address, start_ms, end_ms):
    return q_all(
        """
        SELECT closed_at_ms, net_pnl
        FROM trade_episodes
        WHERE wallet_address = ?
          AND status = 'closed'
          AND closed_at_ms > ?
          AND closed_at_ms <= ?
        ORDER BY closed_at_ms ASC, id ASC
        """,
        (address, int(start_ms or 0), int(end_ms or 0)),
    )


def wallet_pnl_reconciliation(snapshots, address):
    if len(snapshots) < 2:
        return None

    times = [iso_to_epoch_ms(row["created_at"]) for row in snapshots]
    values = [to_float(row["account_value"]) for row in snapshots]
    upnls = [to_float(row_value(row, "unrealized_pnl", 0)) for row in snapshots]
    base_time = times[0]
    base_value = values[0]
    base_upnl = upnls[0]
    end_time = times[-1]

    ledger_rows = q_all(
        """
        SELECT time_ms, flow_usdc
        FROM ledger_updates
        WHERE wallet_address = ?
          AND time_ms > ?
          AND time_ms <= ?
        ORDER BY time_ms ASC
        """,
        (address, base_time, end_time),
    )
    closed_rows = wallet_closed_pnl_points(address, base_time, end_time)

    flow_points = []
    closed_points = []
    flow_index = 0
    closed_index = 0
    cumulative_flow = 0.0
    cumulative_closed = 0.0
    for snapshot_time in times:
        while flow_index < len(ledger_rows) and int(ledger_rows[flow_index]["time_ms"]) <= snapshot_time:
            cumulative_flow += to_float(ledger_rows[flow_index]["flow_usdc"])
            flow_index += 1
        while closed_index < len(closed_rows) and int(closed_rows[closed_index]["closed_at_ms"]) <= snapshot_time:
            cumulative_closed += to_float(closed_rows[closed_index]["net_pnl"])
            closed_index += 1
        flow_points.append(cumulative_flow)
        closed_points.append(cumulative_closed)

    equity_delta = [value - base_value for value in values]
    upnl_delta = [upnl - base_upnl for upnl in upnls]
    operating_pnl = [delta - flow for delta, flow in zip(equity_delta, flow_points)]
    residual = [
        operating - closed - open_delta
        for operating, closed, open_delta in zip(operating_pnl, closed_points, upnl_delta)
    ]
    return {
        "times": times,
        "base_time": base_time,
        "base_value": base_value,
        "base_upnl": base_upnl,
        "equity_delta": equity_delta,
        "flow_points": flow_points,
        "closed_points": closed_points,
        "upnl_delta": upnl_delta,
        "operating_pnl": operating_pnl,
        "residual": residual,
        "ledger_events": len(ledger_rows),
        "closed_events": len(closed_rows),
    }


def ledger_sync_status(state):
    if not state:
        return "Nunca"
    last_error = state["last_error"] if "last_error" in state.keys() else ""
    last_attempt = state["last_attempt_at"] if "last_attempt_at" in state.keys() else ""
    synced_at = state["synced_at"] or ""
    if last_error:
        return f"Fallo {peru_time_text(last_attempt) if last_attempt else '-'}: {last_error}"
    if synced_at:
        inserted = int(state["last_inserted"] or 0) if "last_inserted" in state.keys() else 0
        seen = int(state["last_seen"] or 0) if "last_seen" in state.keys() else 0
        return f"{peru_time_text(synced_at)} | {inserted} nuevos | {seen} vistos"
    if last_attempt:
        return f"Intento {peru_time_text(last_attempt)}, sin sync OK"
    return "Nunca"


def render_ledger_table(rows):
    if not rows:
        return '<div class="subtle">Sin movimientos de capital sincronizados.</div>'
    trs = []
    for row in rows:
        flow = to_float(row["flow_usdc"])
        dex = " -> ".join(part or "perp" for part in (row["source_dex"], row["destination_dex"]))
        trs.append(
            "<tr>"
            f"<td>{ms_to_local_text(row['time_ms'])}</td>"
            f"<td>{html.escape(row['event_type'])}</td>"
            f"<td>{signed_full_usd(flow)}</td>"
            f"<td>{full_usd(row['amount_usdc'])}</td>"
            f"<td>{html.escape(row['token'] or '-')}</td>"
            f"<td>{html.escape(dex)}</td>"
            f"<td>{html.escape(short_addr(row['counterparty']) if str(row['counterparty']).startswith('0x') else (row['counterparty'] or '-'))}</td>"
            "</tr>"
        )
    return (
        '<div class="table-wrap"><table><thead><tr>'
        '<th>Fecha</th><th>Tipo</th><th>Flujo perp</th><th>Monto USDC</th><th>Token</th><th>Ruta</th><th>Contraparte</th>'
        '</tr></thead><tbody>'
        + "".join(trs)
        + "</tbody></table></div>"
    )


def render_account_pnl_chart(snapshots, address):
    if len(snapshots) < 2:
        return '<div class="subtle">Aun no hay suficientes snapshots para graficar PnL historico.</div>'
    reconciliation = wallet_pnl_reconciliation(snapshots, address)
    if not reconciliation:
        return '<div class="subtle">Aun no hay suficientes snapshots para graficar PnL historico.</div>'

    times = reconciliation["times"]
    base_value = reconciliation["base_value"]
    base_upnl = reconciliation["base_upnl"]
    equity_delta = reconciliation["equity_delta"]
    flow_points = reconciliation["flow_points"]
    closed_points = reconciliation["closed_points"]
    upnl_delta = reconciliation["upnl_delta"]
    operating_pnl = reconciliation["operating_pnl"]
    residual = reconciliation["residual"]

    all_values = operating_pnl + closed_points + upnl_delta + residual + [0.0]
    min_pnl = min(all_values)
    max_pnl = max(all_values)
    span = max(max_pnl - min_pnl, 1)
    width = 760
    height = 220
    pad_x = 28
    pad_y = 24
    min_time = min(times)
    max_time = max(times)

    def x_at(idx):
        if max_time <= min_time:
            return pad_x + (width - pad_x * 2) * (idx / max(len(times) - 1, 1))
        return pad_x + (width - pad_x * 2) * ((times[idx] - min_time) / (max_time - min_time))

    def y_at(value):
        return height - pad_y - ((value - min_pnl) / span) * (height - pad_y * 2)

    def make_points(series):
        return " ".join(f"{x_at(idx):.1f},{y_at(value):.1f}" for idx, value in enumerate(series))

    zero_y = y_at(0)
    last_equity = equity_delta[-1]
    last_flow = flow_points[-1]
    last_closed = closed_points[-1]
    last_upnl_delta = upnl_delta[-1]
    last_operating = operating_pnl[-1]
    last_residual = residual[-1]
    explain_rows = "".join(
        f"<tr><th>{label}</th><td>{value}</td></tr>"
        for label, value in [
            ("Cambio equity bruto", signed_full_usd(last_equity)),
            ("Flujo externo neto", signed_full_usd(last_flow)),
            ("PnL operativo estimado", signed_full_usd(last_operating)),
            ("PnL cerrado por fills", signed_full_usd(last_closed)),
            ("Cambio uPnL abierto", signed_full_usd(last_upnl_delta)),
            ("Residual no explicado", signed_full_usd(last_residual)),
        ]
    )
    return f"""
      <div class="grid three" style="margin-bottom:12px;">
        <div><div class="metric-label">Cambio equity</div><div class="metric-value">{signed_usd(last_equity)}</div></div>
        <div><div class="metric-label">Flujo externo neto</div><div class="metric-value">{signed_usd(last_flow)}</div></div>
        <div><div class="metric-label">PnL operativo</div><div class="metric-value">{signed_usd(last_operating)}</div></div>
      </div>
      <div class="grid three" style="margin-bottom:12px;">
        <div><div class="metric-label">PnL cerrado fills</div><div class="metric-value">{signed_usd(last_closed)}</div><div class="subtle">{reconciliation['closed_events']} cierres</div></div>
        <div><div class="metric-label">Cambio uPnL abierto</div><div class="metric-value">{signed_usd(last_upnl_delta)}</div><div class="subtle">Base uPnL {signed_full_usd(base_upnl)}</div></div>
        <div><div class="metric-label">Residual no explicado</div><div class="metric-value">{signed_usd(last_residual)}</div><div class="subtle">Funding, fees faltantes o data incompleta</div></div>
      </div>
      <svg viewBox="0 0 {width} {height}" role="img" aria-label="Grafico de PnL operativo de cuenta" style="width:100%; height:240px;">
        <line x1="{pad_x}" y1="{zero_y:.1f}" x2="{width - pad_x}" y2="{zero_y:.1f}" stroke="#dfe5ef" stroke-width="1" />
        <polyline points="{make_points(operating_pnl)}" fill="none" stroke="#14865f" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" />
        <polyline points="{make_points(closed_points)}" fill="none" stroke="#2f68d8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" opacity=".75" />
        <polyline points="{make_points(upnl_delta)}" fill="none" stroke="#c7781a" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" opacity=".75" />
        <text x="{pad_x}" y="18" fill="#647086" font-size="12">Base equity {full_usd(base_value)}</text>
        <text x="{width - pad_x}" y="18" text-anchor="end" fill="#14865f" font-size="12">PnL operativo {('+' if last_operating > 0 else '') + full_usd(abs(last_operating))}</text>
      </svg>
      <div class="subtle">Verde: equity descontando flujos externos. Azul: PnL cerrado neto desde fills. Naranja: cambio de uPnL abierto. Residual = verde - azul - naranja. Movimientos ledger en ventana: {reconciliation['ledger_events']}. Si el grafico mezcla snapshots anteriores a esta version, usa Reset PnL una vez para fijar una base limpia.</div>
      <div class="table-wrap" style="margin-top:12px;"><table><tbody>{explain_rows}</tbody></table></div>
    """


def render_wallet_table(rows, value_key="account_value"):
    if not rows:
        return '<div class="subtle">Sin datos guardados todavia.</div>'
    trs = []
    for row in rows:
        trs.append(
            "<tr>"
            f"<td>{wallet_link(row['address'], wallet_name(row))}<div class='subtle'>{short_addr(row['address'])}</div></td>"
            f"<td>{usd(row['account_value'])}</td>"
            f"<td>{usd(row['margin_used'])}</td>"
            f"<td>{usd(row['total_ntl_pos'])}</td>"
            f"<td>{int(row['active_positions'])}</td>"
            f"<td>{badge(row['direction_bias'])}</td>"
            f"<td>{html.escape(row['top_coin'] or '-')}</td>"
            "</tr>"
        )
    return (
        '<div class="table-wrap"><table><thead><tr>'
        "<th>Wallet</th><th>Equity real</th><th>Margen pos.</th><th>Notional</th>"
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
            f"<td>{html.escape(peru_time_text(event['created_at']))}</td>"
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
        return datetime.fromtimestamp(int(ms) / 1000, timezone.utc).astimezone(PERU_TZ).strftime("%Y-%m-%d %H:%M:%S PET")
    except Exception:
        return "-"


def row_value(row, key, default=None):
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key] if key in row.keys() else default
    except Exception:
        return default


def wallet_trade_stats(address):
    now_value = now_ms()
    day_start = now_value - 24 * 60 * 60 * 1000
    month_start = now_value - 30 * 24 * 60 * 60 * 1000
    rows = q_all(
        """
        SELECT * FROM trade_episodes
        WHERE wallet_address = ?
          AND status = 'closed'
          AND open_size > ?
          AND close_size > ?
          AND ABS(open_size - close_size) <= MAX(open_size, close_size) * 0.01
          AND (
              ABS(CASE
                  WHEN side = 'Short' THEN (avg_entry_px - avg_close_px) * close_size
                  WHEN side = 'Long' THEN (avg_close_px - avg_entry_px) * close_size
                  ELSE 0
              END) <= 1
              OR ABS(gross_pnl) <= 1
              OR (CASE
                  WHEN side = 'Short' THEN (avg_entry_px - avg_close_px) * close_size
                  WHEN side = 'Long' THEN (avg_close_px - avg_entry_px) * close_size
                  ELSE 0
              END) * gross_pnl >= 0
          )
        ORDER BY closed_at_ms DESC
        """,
        (address, POSITION_EVENT_EPSILON, POSITION_EVENT_EPSILON),
    )
    raw_open_rows = q_all(
        """
        SELECT * FROM trade_episodes
        WHERE wallet_address = ? AND status = 'open'
        ORDER BY COALESCE(opened_at_ms, 0) DESC, id DESC
        """,
        (address,),
    )
    current_rows = q_all(
        """
        SELECT coin, side, size, entry_px
        FROM positions
        WHERE wallet_address = ?
        """,
        (address,),
    )
    current_positions = {
        (str(row["coin"]), str(row["side"])): {
            "size": abs(to_float(row["size"])),
            "entry_px": to_float(row["entry_px"]),
        }
        for row in current_rows
    }
    open_rows = []
    matched_current = set()
    for row in raw_open_rows:
        key = (str(row["coin"]), str(row["side"]))
        current = current_positions.get(key)
        if not current:
            continue
        open_size = to_float(row["open_size"])
        close_size = to_float(row["close_size"])
        remaining = max(0.0, open_size - close_size)
        current_size = current["size"]
        tolerance = max(POSITION_EVENT_EPSILON * 10, current_size * 0.01)
        if not row["opened_at_ms"]:
            coverage = "Apertura fuera del historial disponible"
        elif abs(remaining - current_size) <= tolerance:
            coverage = "Reconciliado con posicion actual"
        else:
            coverage = "Historial parcial o fills pendientes"
        item = dict(row)
        item["remaining_size"] = remaining
        item["current_size"] = current_size
        item["current_entry_px"] = current["entry_px"]
        item["coverage"] = coverage
        open_rows.append(item)
        matched_current.add(key)

    for key, current in current_positions.items():
        if key in matched_current:
            continue
        coin, side = key
        open_rows.append({
            "coin": coin,
            "side": side,
            "status": "open",
            "opened_at_ms": None,
            "closed_at_ms": None,
            "open_size": 0.0,
            "close_size": 0.0,
            "remaining_size": 0.0,
            "current_size": current["size"],
            "current_entry_px": current["entry_px"],
            "avg_entry_px": 0.0,
            "avg_close_px": 0.0,
            "gross_pnl": 0.0,
            "fees": 0.0,
            "net_pnl": 0.0,
            "fill_count": 0,
            "maker_fills": 0,
            "taker_fills": 0,
            "coverage": "Sin apertura dentro de fills sincronizados",
        })
    open_rows.sort(key=lambda row: row_value(row, "opened_at_ms") or 0, reverse=True)

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
        "open": open_rows[:25],
    }


def fill_sync_status(fill_state):
    if not fill_state:
        return "Nunca"
    last_error = fill_state["last_error"] if "last_error" in fill_state.keys() else ""
    last_attempt = fill_state["last_attempt_at"] if "last_attempt_at" in fill_state.keys() else ""
    synced_at = fill_state["synced_at"] or ""
    if last_error:
        return f"Fallo {peru_time_text(last_attempt) if last_attempt else '-'}: {last_error}"
    if synced_at:
        inserted = int(fill_state["last_inserted"] or 0) if "last_inserted" in fill_state.keys() else 0
        seen = int(fill_state["last_seen_fills"] or 0) if "last_seen_fills" in fill_state.keys() else 0
        return f"{peru_time_text(synced_at)} | {inserted} nuevos | {seen} vistos"
    if last_attempt:
        return f"Intento {peru_time_text(last_attempt)}, sin sync OK"
    return "Nunca"


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


def render_open_trade_episodes_table(rows):
    if not rows:
        return '<div class="subtle">No hay posiciones actuales con reconstruccion desde fills. Usa la tabla de posiciones como fuente actual.</div>'
    trs = []
    for row in rows:
        opened_at = row_value(row, "opened_at_ms")
        opened = ms_to_local_text(opened_at) if opened_at else "Antes del historial disponible"
        remaining = to_float(row_value(row, "remaining_size"))
        current_size = to_float(row_value(row, "current_size", remaining))
        coverage = html.escape(str(row_value(row, "coverage", "")))
        trs.append(
            "<tr>"
            f"<td>{opened}</td>"
            f"<td>{html.escape(str(row_value(row, 'coin', '')))}</td>"
            f"<td>{badge(row_value(row, 'side', ''))}</td>"
            f"<td>{current_size:,.6f}</td>"
            f"<td>{remaining:,.6f}</td>"
            f"<td>{price_or_dash(row_value(row, 'avg_entry_px'))}</td>"
            f"<td>{price_or_dash(row_value(row, 'current_entry_px'))}</td>"
            f"<td>{full_usd(row_value(row, 'fees'))}</td>"
            f"<td>{coverage}</td>"
            f"<td>{int(row_value(row, 'fill_count', 0) or 0)}</td>"
            f"<td>{int(row_value(row, 'maker_fills', 0) or 0)}/{int(row_value(row, 'taker_fills', 0) or 0)}</td>"
            "</tr>"
        )
    return (
        '<div class="table-wrap"><table><thead><tr>'
        '<th>Apertura estimada</th><th>Coin</th><th>Lado</th><th>Tamano actual</th>'
        '<th>Tamano desde fills</th><th>Avg entry fills</th><th>Entry actual</th>'
        '<th>Fees netas</th><th>Cobertura</th><th>Fills</th><th>Maker/Taker</th>'
        '</tr></thead><tbody>'
        + "".join(trs)
        + "</tbody></table></div>"
    )


def whale_coin_options():
    rows = q_all(
        """
        SELECT coin, SUM(notional) score
        FROM (
            SELECT coin, position_value AS notional FROM positions
            UNION ALL
            SELECT coin, ABS(net_pnl) AS notional FROM trade_episodes WHERE status = 'closed'
        )
        GROUP BY coin
        ORDER BY score DESC, coin ASC
        LIMIT 80
        """
    )
    return [str(row["coin"]) for row in rows if row["coin"]]


def default_whale_coin():
    coins = whale_coin_options()
    if "BTC" in coins:
        return "BTC"
    return coins[0] if coins else "BTC"


def clean_coin(coin):
    coin = str(coin or "").strip()
    if not coin:
        return default_whale_coin()
    return coin[:40]


def confidence_label(trades, net, profit_factor):
    trades = int(trades or 0)
    net = to_float(net)
    profit_factor = to_float(profit_factor)
    if trades >= max(50, WHALE_VIEW_MIN_TRADES * 3) and net > 0 and profit_factor >= 1.2:
        return "Alta"
    if trades >= WHALE_VIEW_MIN_TRADES and net > 0 and profit_factor >= 1.0:
        return "Media"
    return "Baja"


def clamp(value, low=0.0, high=1.0):
    return max(low, min(high, to_float(value)))


def position_risk_score(margin_used, notional, leverage_value, account_value):
    margin_used = max(0.0, to_float(margin_used))
    notional = max(0.0, to_float(notional))
    leverage_value = max(0.0, to_float(leverage_value))
    account_value = max(0.0, to_float(account_value))
    if leverage_value <= 0 and margin_used > 0 and notional > 0:
        leverage_value = notional / margin_used
    leverage_norm = clamp(math.log1p(max(0.0, leverage_value - 1.0)) / math.log1p(75.0))
    absolute_cap_norm = clamp(math.log1p(margin_used) / math.log1p(200_000.0))
    account_weight_norm = clamp((margin_used / account_value) / 0.10) if account_value > 0 else 0.0
    capital_norm = clamp(0.65 * absolute_cap_norm + 0.35 * account_weight_norm)
    interaction = math.sqrt(leverage_norm * capital_norm) if leverage_norm > 0 and capital_norm > 0 else 0.0
    raw = clamp(0.45 * leverage_norm + 0.35 * capital_norm + 0.20 * interaction)
    score = max(1.0, min(10.0, 1.0 + 9.0 * raw))
    confidence_signal = clamp(capital_norm * (0.65 + 0.35 * leverage_norm))
    if score >= 8:
        label = "Muy alto"
    elif score >= 6.5:
        label = "Alto"
    elif score >= 4.5:
        label = "Medio"
    elif score >= 2.5:
        label = "Bajo"
    else:
        label = "Muy bajo"
    if confidence_signal >= 0.75:
        signal = "Apuesta pesada"
    elif confidence_signal >= 0.50:
        signal = "Conviccion media"
    elif leverage_norm >= 0.75 and margin_used < 5_000:
        signal = "Prueba apalancada"
    elif leverage_norm >= 0.70:
        signal = "Alta fragilidad"
    else:
        signal = "Exposicion ligera"
    return {
        "score": score,
        "label": label,
        "signal": signal,
        "leverage_norm": leverage_norm,
        "capital_norm": capital_norm,
        "account_weight_norm": account_weight_norm,
        "capital_used": margin_used,
        "notional": notional,
        "leverage": leverage_value,
        "confidence_signal": confidence_signal,
    }


def whale_rankings_for_coin(coin, limit=5):
    return q_all(
        """
        WITH stats AS (
            SELECT
                wallet_address,
                COUNT(*) AS trades,
                SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(net_pnl) AS net_pnl,
                SUM(gross_pnl) AS gross_pnl,
                SUM(fees) AS fees,
                SUM(CASE WHEN net_pnl > 0 THEN net_pnl ELSE 0 END) AS win_pnl,
                SUM(CASE WHEN net_pnl < 0 THEN -net_pnl ELSE 0 END) AS loss_abs
            FROM trade_episodes
            WHERE status = 'closed'
              AND coin = ?
              AND open_size > ?
              AND close_size > ?
              AND ABS(open_size - close_size) <= MAX(open_size, close_size) * 0.01
              AND (
                  ABS(CASE
                      WHEN side = 'Short' THEN (avg_entry_px - avg_close_px) * close_size
                      WHEN side = 'Long' THEN (avg_close_px - avg_entry_px) * close_size
                      ELSE 0
                  END) <= 1
                  OR ABS(gross_pnl) <= 1
                  OR (CASE
                      WHEN side = 'Short' THEN (avg_entry_px - avg_close_px) * close_size
                      WHEN side = 'Long' THEN (avg_close_px - avg_entry_px) * close_size
                      ELSE 0
                  END) * gross_pnl >= 0
              )
            GROUP BY wallet_address
        )
        SELECT
            w.address,
            w.alias,
            w.account_value,
            w.margin_used,
            w.last_seen,
            s.trades,
            s.wins,
            (1.0 * s.wins / s.trades) AS winrate,
            s.net_pnl,
            s.gross_pnl,
            s.fees,
            CASE
                WHEN s.loss_abs <= 0 AND s.win_pnl > 0 THEN 999.0
                WHEN s.loss_abs <= 0 THEN 0.0
                ELSE s.win_pnl / s.loss_abs
            END AS profit_factor
        FROM stats s
        JOIN wallets w ON w.address = s.wallet_address
        WHERE s.trades >= ?
          AND s.net_pnl > 0
        ORDER BY winrate DESC, profit_factor DESC, s.trades DESC
        LIMIT ?
        """,
        (coin, POSITION_EVENT_EPSILON, POSITION_EVENT_EPSILON, WHALE_VIEW_MIN_TRADES, limit),
    )


def whale_position_for(address, coin):
    return q_one(
        """
        SELECT *
        FROM positions
        WHERE wallet_address = ? AND coin = ?
        LIMIT 1
        """,
        (address, coin),
    )


def whale_open_episode_for(address, coin, side):
    return q_one(
        """
        SELECT *
        FROM trade_episodes
        WHERE wallet_address = ?
          AND coin = ?
          AND side = ?
          AND status = 'open'
        ORDER BY COALESCE(opened_at_ms, 0) DESC, id DESC
        LIMIT 1
        """,
        (address, coin, side),
    )


def whale_closed_positions(coin, addresses, limit=20):
    if not addresses:
        return []
    placeholders = ",".join("?" for _ in addresses)
    return q_all(
        f"""
        SELECT te.*, w.alias
        FROM trade_episodes te
        LEFT JOIN wallets w ON w.address = te.wallet_address
        WHERE te.coin = ?
          AND te.wallet_address IN ({placeholders})
          AND te.status = 'closed'
          AND te.open_size > ?
          AND te.close_size > ?
          AND ABS(te.open_size - te.close_size) <= MAX(te.open_size, te.close_size) * 0.01
          AND (
              ABS(CASE
                  WHEN te.side = 'Short' THEN (te.avg_entry_px - te.avg_close_px) * te.close_size
                  WHEN te.side = 'Long' THEN (te.avg_close_px - te.avg_entry_px) * te.close_size
                  ELSE 0
              END) <= 1
              OR ABS(te.gross_pnl) <= 1
              OR (CASE
                  WHEN te.side = 'Short' THEN (te.avg_entry_px - te.avg_close_px) * te.close_size
                  WHEN te.side = 'Long' THEN (te.avg_close_px - te.avg_entry_px) * te.close_size
                  ELSE 0
              END) * te.gross_pnl >= 0
          )
        ORDER BY te.closed_at_ms DESC
        LIMIT ?
        """,
        tuple([coin] + list(addresses) + [POSITION_EVENT_EPSILON, POSITION_EVENT_EPSILON, limit]),
    )


def mark_for_coin(coin):
    prices, updated_at, source = get_cached_mark_prices()
    if not prices or time.time() - updated_at > 3:
        try:
            prices = fetch_mark_prices()
            updated_at = time.time()
            source = "rest"
        except Exception:
            prices = prices or {}
    return to_float(prices.get(coin)), updated_at, source


def whale_view_payload(coin):
    coin = clean_coin(coin)
    rankings = whale_rankings_for_coin(coin, 5)
    mark_px, mark_updated_at, mark_source = mark_for_coin(coin)
    candles = fetch_candles(coin)
    whales = []
    long_notional = 0.0
    short_notional = 0.0
    active_count = 0
    total_upnl = 0.0
    total_margin = 0.0
    total_holding = 0.0
    entry_notional = 0.0
    entry_size = 0.0
    for row in rankings:
        address = row["address"]
        pos = whale_position_for(address, coin)
        position = None
        if pos:
            active_count += 1
            side = str(pos["side"])
            open_episode = whale_open_episode_for(address, coin, side)
            opened_at_ms = row_value(open_episode, "opened_at_ms") if open_episode else None
            open_from_fills = max(0.0, to_float(row_value(open_episode, "open_size")) - to_float(row_value(open_episode, "close_size"))) if open_episode else 0.0
            notional = to_float(pos["position_value"])
            if side == "Long":
                long_notional += notional
            elif side == "Short":
                short_notional += notional
            total_upnl += to_float(pos["unrealized_pnl"])
            total_margin += to_float(pos["capital_used"])
            total_holding += abs(to_float(pos["size"]))
            entry_notional += abs(to_float(pos["size"])) * to_float(pos["entry_px"])
            entry_size += abs(to_float(pos["size"]))
            risk = position_risk_score(
                pos["capital_used"],
                notional,
                pos["leverage_value"],
                row["account_value"],
            )
            position = {
                "side": side,
                "size": abs(to_float(pos["size"])),
                "entry_px": to_float(pos["entry_px"]),
                "mark_px": mark_px or to_float(pos["current_px"]),
                "current_px": to_float(pos["current_px"]),
                "notional": notional,
                "margin_used": to_float(pos["capital_used"]),
                "unrealized_pnl": to_float(pos["unrealized_pnl"]),
                "roi_margin": to_float(pos["roi_capital"]),
                "liquidation_px": to_float(pos["liquidation_px"]),
                "leverage": pos["leverage"] or "",
                "updated_at": peru_time_text(pos["updated_at"]),
                "opened_at": ms_to_local_text(opened_at_ms) if opened_at_ms else "",
                "open_coverage": "Apertura estimada desde fills" if opened_at_ms else "Apertura fuera del historial sincronizado",
                "open_size_from_fills": open_from_fills,
                "risk": risk,
            }
        profit_factor = to_float(row["profit_factor"])
        whales.append({
            "address": address,
            "name": wallet_name(row),
            "trades": int(row["trades"] or 0),
            "wins": int(row["wins"] or 0),
            "winrate": to_float(row["winrate"]),
            "net_pnl": to_float(row["net_pnl"]),
            "gross_pnl": to_float(row["gross_pnl"]),
            "fees": to_float(row["fees"]),
            "profit_factor": profit_factor,
            "confidence": confidence_label(row["trades"], row["net_pnl"], profit_factor),
            "account_value": to_float(row["account_value"]),
            "last_seen": peru_time_text(row["last_seen"]),
            "position": position,
        })
    closed_rows = whale_closed_positions(coin, [row["address"] for row in rankings])
    closed_positions = []
    for row in closed_rows:
        closed_positions.append({
            "wallet_address": row["wallet_address"],
            "wallet": row["alias"] or short_addr(row["wallet_address"]),
            "side": row["side"],
            "opened_at": ms_to_local_text(row["opened_at_ms"]),
            "closed_at": ms_to_local_text(row["closed_at_ms"]),
            "size": to_float(row["close_size"]),
            "avg_entry_px": to_float(row["avg_entry_px"]),
            "avg_close_px": to_float(row["avg_close_px"]),
            "gross_pnl": to_float(row["gross_pnl"]),
            "fees": to_float(row["fees"]),
            "net_pnl": to_float(row["net_pnl"]),
            "fill_count": int(row["fill_count"] or 0),
        })
    return {
        "coin": coin,
        "min_trades": WHALE_VIEW_MIN_TRADES,
        "mark_px": mark_px,
        "mark_updated_at": mark_updated_at,
        "mark_source": mark_source,
        "candles": candles,
        "active_count": active_count,
        "long_notional": long_notional,
        "short_notional": short_notional,
        "total_upnl": total_upnl,
        "total_margin": total_margin,
        "total_holding": total_holding,
        "avg_entry": entry_notional / entry_size if entry_size else 0.0,
        "whales": whales,
        "events": [],
        "closed_positions": closed_positions,
        "coins": whale_coin_options(),
    }


def render_whale_closed_positions(items):
    if not items:
        return '<div class="subtle">Sin posiciones cerradas confiables para estas top wallets en esta moneda.</div>'
    rows = []
    for item in items:
        rows.append(
            "<tr>"
            f"<td>{html.escape(item['closed_at'])}</td>"
            f"<td>{html.escape(item['opened_at'])}</td>"
            f"<td>{wallet_link(item['wallet_address'], item['wallet'])}</td>"
            f"<td>{badge(item['side'])}</td>"
            f"<td>{item['size']:,.6f}</td>"
            f"<td>{price_or_dash(item['avg_entry_px'])}</td>"
            f"<td>{price_or_dash(item['avg_close_px'])}</td>"
            f"<td>{signed_full_usd(item['net_pnl'])}</td>"
            f"<td>{int(item['fill_count'])}</td>"
            "</tr>"
        )
    return (
        '<div class="table-wrap"><table><thead><tr>'
        '<th>Cierre</th><th>Apertura</th><th>Whale</th><th>Lado</th><th>Tamano</th>'
        '<th>Avg entry</th><th>Avg close</th><th>PnL neto</th><th>Fills</th>'
        '</tr></thead><tbody>'
        + "".join(rows)
        + "</tbody></table></div>"
    )


def whale_view_page(coin=""):
    payload = whale_view_payload(coin)
    selected = payload["coin"]
    options = "".join(
        f'<option value="{html.escape(option)}" {"selected" if option == selected else ""}>{html.escape(option)}</option>'
        for option in payload["coins"]
    )
    whale_rows = []
    for whale in payload["whales"]:
        pos = whale["position"]
        if pos:
            position_text = (
                f"{badge(pos['side'])}<div class='subtle'>{pos['size']:,.6f} | {full_usd(pos['notional'])}</div>"
            )
            entry = price_or_dash(pos["entry_px"])
            mark = price_or_dash(pos["mark_px"])
            upnl = signed_full_usd(pos["unrealized_pnl"])
            opened = html.escape(pos["opened_at"] or "No disponible")
            opened += f"<div class='subtle'>{html.escape(pos['open_coverage'])}</div>"
        else:
            position_text = "<span class='subtle'>Sin posicion actual</span>"
            entry = "-"
            mark = price_or_dash(payload["mark_px"])
            upnl = "-"
            opened = "-"
        pf = whale["profit_factor"]
        pf_text = "inf" if pf >= 999 else f"{pf:.2f}"
        risk = pos.get("risk") if pos else None
        risk_text = "-"
        if risk:
            risk_text = (
                f"<strong>{risk['score']:.1f}/10</strong>"
                f"<div class='subtle'>{html.escape(risk['label'])} | {html.escape(risk['signal'])}</div>"
                f"<div class='subtle'>Cap {usd(risk['capital_used'])} | Lev {risk['leverage']:.1f}x</div>"
            )
        whale_rows.append(
            "<tr>"
            f"<td>{wallet_link(whale['address'], whale['name'])}<div class='subtle'>{short_addr(whale['address'])}</div></td>"
            f"<td>{pct(whale['winrate'], 1)}<div class='subtle'>{whale['wins']}/{whale['trades']} trades</div></td>"
            f"<td>{signed_full_usd(whale['net_pnl'])}</td>"
            f"<td>{risk_text}</td>"
            f"<td>{html.escape(whale['confidence'])}</td>"
            f"<td>{position_text}</td>"
            f"<td>{entry}</td>"
            f"<td>{mark}</td>"
            f"<td>{upnl}</td>"
            f"<td>{opened}</td>"
            f"<td>{html.escape(whale['last_seen'] or '-')}</td>"
            "</tr>"
        )
    body = f"""
    <div class="topbar">
      <div>
        <h1>Whale view</h1>
        <div class="subtle">Top 5 por winrate confiable en la moneda seleccionada. Ranking basado en trades cerrados reconstruidos desde fills.</div>
      </div>
      <form method="get" action="/whale-view" style="display:flex; gap:10px; min-width:min(420px, 100%);">
        <select name="coin" style="min-height:40px; border:1px solid var(--line); border-radius:8px; padding:9px 12px; font:inherit; width:100%;">
          {options}
        </select>
        <button class="btn" type="submit">Ver</button>
      </form>
    </div>
    <section class="grid metrics">
      <div class="card"><div class="metric-label">Moneda</div><div class="metric-value">{html.escape(selected)}</div></div>
      <div class="card"><div class="metric-label">Whales ranking</div><div class="metric-value" id="wv-count">{len(payload['whales'])}</div><div class="subtle">Min {payload['min_trades']} trades cerrados</div></div>
      <div class="card"><div class="metric-label">Long notional</div><div class="metric-value" id="wv-long">{usd(payload['long_notional'])}</div></div>
      <div class="card"><div class="metric-label">Short notional</div><div class="metric-value" id="wv-short">{usd(payload['short_notional'])}</div></div>
    </section>
    <section class="whale-terminal">
      <div class="whale-terminal-head">
        <div>
          <div class="terminal-title">{html.escape(selected)} Whale Position Map</div>
          <div class="terminal-subtitle">Candles reales 7d + posiciones actuales del top winrate confiable</div>
        </div>
        <div class="terminal-price">
          <div id="wv-mark">{price_or_dash(payload['mark_px'])}</div>
          <span id="wv-bias">Live mark</span>
        </div>
      </div>
      <div class="terminal-tabs">
        <span class="terminal-tab active">Price</span>
        <span class="terminal-tab">Auto 7d</span>
        <span class="terminal-tab">Top whales</span>
        <div class="chart-controls">
          <button type="button" id="wv-zoom-in">+</button>
          <button type="button" id="wv-zoom-out">-</button>
          <button type="button" id="wv-reset">Reset</button>
        </div>
      </div>
      <div class="terminal-chart-wrap">
        <canvas id="whale-chart"></canvas>
      </div>
      <div class="terminal-stats">
        <div><span>Unrealised PnL</span><b id="wv-stat-upnl">{signed_full_usd(payload['total_upnl'])}</b><small id="wv-stat-roi">{signed_pct(payload['total_upnl'] / payload['total_margin'] if payload['total_margin'] else 0)}</small></div>
        <div><span>Total margin</span><b id="wv-stat-margin">{usd(payload['total_margin'])}</b><small>Cross/isolated reported</small></div>
        <div><span>Holding</span><b id="wv-stat-holding">{payload['total_holding']:,.4f} {html.escape(selected)}</b><small id="wv-stat-active">{payload['active_count']} active positions</small></div>
        <div><span>Long / Short</span><b id="wv-stat-ls">{usd(payload['long_notional'])} / {usd(payload['short_notional'])}</b><small>Notional observado</small></div>
        <div><span>Avg Entry</span><b id="wv-stat-entry">{price_or_dash(payload['avg_entry'])}</b><small>Weighted by size</small></div>
        <div><span>Whale rank</span><b id="wv-stat-rank">{len(payload['whales'])}</b><small>Min {payload['min_trades']} closed trades</small></div>
      </div>
    </section>
    <section class="card" style="margin-top:16px;">
      <h2>Top whales en {html.escape(selected)}</h2>
      <div class="subtle">Winrate util = muestra minima, episodios cerrados completos y PnL neto positivo. Riesgo 1-10 combina apalancamiento, margen real usado y peso sobre la cuenta.</div>
      <div class="table-wrap"><table><thead><tr>
        <th>Whale</th><th>Winrate</th><th>PnL neto</th><th>Riesgo</th><th>Confianza</th>
        <th>Posicion actual</th><th>Entry</th><th>Mark</th><th>uPnL</th><th>Apertura est.</th><th>Ultimo refresh</th>
      </tr></thead><tbody id="wv-whales">{''.join(whale_rows) or '<tr><td colspan="11">Sin whales calificadas para esta moneda.</td></tr>'}</tbody></table></div>
    </section>
    <section class="card" style="margin-top:16px;">
      <h2>Posiciones cerradas de estas top whales en {html.escape(selected)}</h2>
      <div class="subtle">Historial reconstruido desde fills. Solo se muestran cierres confiables flat-to-flat de las wallets rankeadas arriba.</div>
      <div id="wv-history">{render_whale_closed_positions(payload['closed_positions'])}</div>
    </section>
    <script>
      const initialWhaleView = {json.dumps(payload, separators=(",", ":"))};
      let whaleViewData = initialWhaleView;
      let priceSamples = [];
      let chartView = null;
      let dragState = null;
      const chartCanvas = document.getElementById("whale-chart");
      const chartCtx = chartCanvas.getContext("2d");

      function wvUsd(value, decimals = 0) {{
        return new Intl.NumberFormat(undefined, {{ style: "currency", currency: "USD", maximumFractionDigits: decimals }}).format(Number(value || 0));
      }}
      function wvPrice(value) {{
        value = Number(value || 0);
        const abs = Math.abs(value);
        const decimals = abs >= 100 ? 2 : abs >= 1 ? 4 : abs >= 0.01 ? 6 : 8;
        return "$" + value.toLocaleString(undefined, {{ minimumFractionDigits: decimals, maximumFractionDigits: decimals }});
      }}
      function wvPct(value) {{
        return (Number(value || 0) * 100).toFixed(1) + "%";
      }}
      function wvEsc(value) {{
        return String(value ?? "").replace(/[&<>"']/g, (char) => ({{"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}}[char]));
      }}
      function wvSignedUsd(value) {{
        value = Number(value || 0);
        const cls = value > 0 ? "num-positive" : value < 0 ? "num-negative" : "num-neutral";
        const sign = value > 0 ? "+" : "";
        return `<span class="${{cls}}">${{sign}}${{wvUsd(Math.abs(value), 2)}}</span>`;
      }}

      function resizeWhaleChart() {{
        const rect = chartCanvas.getBoundingClientRect();
        const dpr = window.devicePixelRatio || 1;
        chartCanvas.width = Math.max(320, Math.floor(rect.width * dpr));
        chartCanvas.height = Math.floor(rect.height * dpr);
        chartCtx.setTransform(dpr, 0, 0, dpr, 0, 0);
      }}

      function roundRect(ctx, x, y, width, height, radius) {{
        const r = Math.min(radius, width / 2, height / 2);
        ctx.beginPath();
        ctx.moveTo(x + r, y);
        ctx.arcTo(x + width, y, x + width, y + height, r);
        ctx.arcTo(x + width, y + height, x, y + height, r);
        ctx.arcTo(x, y + height, x, y, r);
        ctx.arcTo(x, y, x + width, y, r);
        ctx.closePath();
      }}

      function drawPriceLabel(text, x, y, color, alignRight = true) {{
        chartCtx.font = "700 13px system-ui";
        const padding = 8;
        const width = chartCtx.measureText(text).width + padding * 2;
        const height = 26;
        const left = alignRight ? x - width : x;
        roundRect(chartCtx, left, y - height / 2, width, height, 4);
        chartCtx.fillStyle = color;
        chartCtx.fill();
        chartCtx.fillStyle = "#071018";
        chartCtx.fillText(text, left + padding, y + 4);
      }}

      function drawEventMarker(event, x, y) {{
        const isOpen = event.type === "open";
        const color = isOpen ? "#67e8b9" : "#f05a8a";
        chartCtx.fillStyle = color;
        chartCtx.strokeStyle = "#0b1119";
        chartCtx.lineWidth = 2;
        chartCtx.beginPath();
        chartCtx.arc(x, y, 9, 0, Math.PI * 2);
        chartCtx.fill();
        chartCtx.stroke();
        chartCtx.fillStyle = "#071018";
        chartCtx.font = "800 11px system-ui";
        chartCtx.textAlign = "center";
        chartCtx.fillText(isOpen ? "B" : "S", x, y + 4);
        chartCtx.textAlign = "left";
      }}

      function stackLabels(labels, top, bottom, gap = 30) {{
        const sorted = labels
          .map((label) => ({{ ...label, y: Math.max(top, Math.min(bottom, label.desiredY)) }}))
          .sort((a, b) => a.y - b.y);
        for (let i = 1; i < sorted.length; i += 1) {{
          if (sorted[i].y - sorted[i - 1].y < gap) sorted[i].y = sorted[i - 1].y + gap;
        }}
        const overflow = sorted.length ? sorted[sorted.length - 1].y - bottom : 0;
        if (overflow > 0) {{
          for (let i = sorted.length - 1; i >= 0; i -= 1) {{
            sorted[i].y -= overflow;
            if (i < sorted.length - 1 && sorted[i + 1].y - sorted[i].y < gap) {{
              sorted[i].y = sorted[i + 1].y - gap;
            }}
          }}
        }}
        for (let i = 0; i < sorted.length; i += 1) {{
          sorted[i].y = Math.max(top, Math.min(bottom, sorted[i].y));
        }}
        return sorted;
      }}

      function drawGutterLabel(label, x, plotEndX) {{
        const text = label.text;
        chartCtx.font = "800 12px system-ui";
        const padding = 8;
        const boxW = Math.min(152, chartCtx.measureText(text).width + padding * 2);
        const boxH = 24;
        const left = x - boxW;
        const y = label.y;
        chartCtx.setLineDash(label.dashed ? [2, 5] : []);
        chartCtx.strokeStyle = label.color;
        chartCtx.globalAlpha = 0.8;
        chartCtx.beginPath();
        chartCtx.moveTo(plotEndX, label.desiredY);
        chartCtx.lineTo(left - 7, y);
        chartCtx.stroke();
        chartCtx.globalAlpha = 1;
        chartCtx.setLineDash([]);
        roundRect(chartCtx, left, y - boxH / 2, boxW, boxH, 5);
        chartCtx.fillStyle = label.color;
        chartCtx.fill();
        chartCtx.fillStyle = "#071018";
        chartCtx.fillText(text.length > 24 ? text.slice(0, 23) + "..." : text, left + padding, y + 4);
      }}

      function fullSeries() {{
        const candles = (whaleViewData.candles || []).filter((c) => Number(c.close || 0) > 0);
        const live = priceSamples.map((s) => ({{ time: s.t, close: s.price }}));
        return candles.length ? candles.concat(live.slice(-8)) : live;
      }}

      function ensureChartView(series) {{
        const times = series.map((s) => Number(s.time || 0)).filter(Boolean);
        if (!times.length) return null;
        const fullStart = Math.min(...times);
        const fullEnd = Math.max(...times);
        if (!chartView) {{
          chartView = {{ start: fullStart, end: fullEnd, fullStart, fullEnd }};
        }} else {{
          const span = chartView.end - chartView.start;
          const wasAtRightEdge = Math.abs(chartView.end - chartView.fullEnd) < 2000;
          chartView.fullStart = fullStart;
          chartView.fullEnd = fullEnd;
          if (wasAtRightEdge) {{
            chartView.end = fullEnd;
            chartView.start = Math.max(fullStart, fullEnd - span);
          }}
          chartView.start = Math.max(chartView.fullStart, Math.min(chartView.start, chartView.fullEnd - 1));
          chartView.end = Math.min(chartView.fullEnd, Math.max(chartView.end, chartView.start + 1));
        }}
        return chartView;
      }}

      function zoomChart(factor, anchorRatio = 0.5) {{
        const series = fullSeries();
        const view = ensureChartView(series);
        if (!view) return;
        const span = view.end - view.start;
        const minSpan = Math.max(60 * 60 * 1000, (view.fullEnd - view.fullStart) * 0.03);
        const maxSpan = view.fullEnd - view.fullStart;
        const nextSpan = Math.max(minSpan, Math.min(maxSpan, span * factor));
        const anchor = view.start + span * anchorRatio;
        view.start = anchor - nextSpan * anchorRatio;
        view.end = view.start + nextSpan;
        if (view.start < view.fullStart) {{
          view.start = view.fullStart;
          view.end = view.start + nextSpan;
        }}
        if (view.end > view.fullEnd) {{
          view.end = view.fullEnd;
          view.start = view.end - nextSpan;
        }}
        drawWhaleChart();
      }}

      function panChart(deltaRatio) {{
        const series = fullSeries();
        const view = ensureChartView(series);
        if (!view) return;
        const span = view.end - view.start;
        const delta = span * deltaRatio;
        view.start += delta;
        view.end += delta;
        if (view.start < view.fullStart) {{
          view.start = view.fullStart;
          view.end = view.start + span;
        }}
        if (view.end > view.fullEnd) {{
          view.end = view.fullEnd;
          view.start = view.end - span;
        }}
        drawWhaleChart();
      }}

      function drawWhaleChart() {{
        resizeWhaleChart();
        const width = chartCanvas.clientWidth;
        const height = chartCanvas.clientHeight;
        const plot = {{ left: 34, top: 18, right: 190, bottom: 46 }};
        const plotW = Math.max(100, width - plot.left - plot.right);
        const plotH = Math.max(100, height - plot.top - plot.bottom);
        chartCtx.clearRect(0, 0, width, height);
        chartCtx.fillStyle = "#0b1119";
        chartCtx.fillRect(0, 0, width, height);
        const positions = whaleViewData.whales.map((w) => w.position).filter(Boolean);
        const allSeries = fullSeries();
        const view = ensureChartView(allSeries);
        let series = view ? allSeries.filter((s) => Number(s.time || 0) >= view.start && Number(s.time || 0) <= view.end) : allSeries;
        if (series.length < 2 && allSeries.length >= 2) series = allSeries.slice(-2);
        const values = series.map((s) => Number(s.close || 0)).filter(Boolean);
        if (Number(whaleViewData.mark_px || 0) > 0) values.push(Number(whaleViewData.mark_px));
        positions.forEach((pos) => {{
          values.push(Number(pos.entry_px || 0));
          values.push(Number(pos.mark_px || 0));
        }});
        if (!values.length) {{
          chartCtx.fillStyle = "#94a3b8";
          chartCtx.font = "14px system-ui";
          chartCtx.fillText("Sin precio o posiciones para graficar.", 24, 40);
          return;
        }}
        let min = Math.min(...values);
        let max = Math.max(...values);
        const pad = Math.max((max - min) * 0.2, Math.abs(max) * 0.003, 1);
        min -= pad;
        max += pad;
        const minTime = view ? view.start : Date.now() - 1;
        const maxTime = view ? view.end : Date.now();
        const xForTime = (timeValue) => plot.left + plotW * ((Number(timeValue || minTime) - minTime) / Math.max(maxTime - minTime, 1));
        const xForIndex = (i) => plot.left + plotW * (series.length <= 1 ? 1 : i / (series.length - 1));
        const yFor = (priceValue) => plot.top + plotH * (1 - (priceValue - min) / Math.max(max - min, 1e-9));
        chartCtx.strokeStyle = "rgba(130,146,170,.18)";
        chartCtx.lineWidth = 1;
        for (let i = 0; i < 5; i += 1) {{
          const y = plot.top + i * (plotH / 4);
          chartCtx.beginPath();
          chartCtx.moveTo(plot.left, y);
          chartCtx.lineTo(width - plot.right, y);
          chartCtx.stroke();
          const priceAtLine = max - i * ((max - min) / 4);
          chartCtx.fillStyle = "#7f8aa3";
          chartCtx.font = "12px system-ui";
          chartCtx.fillText(wvPrice(priceAtLine), width - plot.right + 16, y + 4);
        }}
        if (series.length) {{
          const gradient = chartCtx.createLinearGradient(0, plot.top, 0, plot.top + plotH);
          gradient.addColorStop(0, "rgba(103,232,185,.18)");
          gradient.addColorStop(1, "rgba(103,232,185,0)");
          chartCtx.beginPath();
          series.forEach((sample, i) => {{
            const x = Number(sample.time || 0) ? xForTime(sample.time) : xForIndex(i);
            const y = yFor(Number(sample.close || 0));
            if (i === 0) chartCtx.moveTo(x, y);
            else chartCtx.lineTo(x, y);
          }});
          chartCtx.lineTo(Number(series[series.length - 1].time || 0) ? xForTime(series[series.length - 1].time) : xForIndex(series.length - 1), plot.top + plotH);
          chartCtx.lineTo(plot.left, plot.top + plotH);
          chartCtx.closePath();
          chartCtx.fillStyle = gradient;
          chartCtx.fill();
          chartCtx.strokeStyle = "#52b788";
          chartCtx.lineWidth = 3;
          chartCtx.beginPath();
          series.forEach((sample, i) => {{
            const x = Number(sample.time || 0) ? xForTime(sample.time) : xForIndex(i);
            const y = yFor(Number(sample.close || 0));
            if (i === 0) chartCtx.moveTo(x, y);
            else chartCtx.lineTo(x, y);
          }});
          chartCtx.stroke();
          const last = series[series.length - 1];
          const lastX = Number(last.time || 0) ? xForTime(last.time) : xForIndex(series.length - 1);
          const lastY = yFor(Number(last.close || 0));
          chartCtx.fillStyle = "#52b788";
          chartCtx.beginPath();
          chartCtx.arc(lastX, lastY, 5, 0, Math.PI * 2);
          chartCtx.fill();
          chartCtx.fillStyle = "#7f8aa3";
          chartCtx.font = "12px system-ui";
          const firstDate = new Date(Number(series[0].time || Date.now())).toLocaleString(undefined, {{ month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" }});
          const lastDate = new Date(Number(last.time || Date.now())).toLocaleString(undefined, {{ month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" }});
          chartCtx.fillText(firstDate, plot.left + 6, height - 16);
          chartCtx.fillText(lastDate, width - plot.right - 132, height - 16);
        }}
        const mark = Number(whaleViewData.mark_px || 0);
        const labels = [];
        if (mark > 0) {{
          const y = yFor(mark);
          chartCtx.setLineDash([2, 6]);
          chartCtx.strokeStyle = "rgba(248,250,252,.52)";
          chartCtx.beginPath();
          chartCtx.moveTo(plot.left, y);
          chartCtx.lineTo(width - plot.right, y);
          chartCtx.stroke();
          chartCtx.setLineDash([]);
          labels.push({{ text: "Mark " + wvPrice(mark), desiredY: y, color: "#dbe4f0", dashed: true }});
        }}
        positions.forEach((pos, index) => {{
          const y = yFor(Number(pos.entry_px || 0));
          const isLong = pos.side === "Long";
          const color = isLong ? "#67e8b9" : "#f05a8a";
          chartCtx.strokeStyle = color;
          chartCtx.lineWidth = 2;
          chartCtx.setLineDash([2, 5]);
          chartCtx.beginPath();
          chartCtx.moveTo(plot.left, y);
          chartCtx.lineTo(width - plot.right, y);
          chartCtx.stroke();
          chartCtx.setLineDash([]);
          const whale = whaleViewData.whales.filter((w) => w.position)[index];
          const sideLabel = pos.side === "Long" ? "Avg Entry" : "Short Entry";
          const suffix = whale ? ` | ${{(whale.name || "Whale").slice(0, 10)}}` : "";
          labels.push({{ text: `${{sideLabel}} ${{wvPrice(pos.entry_px)}}${{suffix}}`, desiredY: y, color }});
          const liq = Number(pos.liquidation_px || 0);
          if (liq > min && liq < max) {{
            const ly = yFor(liq);
            chartCtx.setLineDash([8, 6]);
            chartCtx.strokeStyle = "rgba(245,158,11,.8)";
            chartCtx.beginPath();
            chartCtx.moveTo(plot.left, ly);
            chartCtx.lineTo(width - plot.right, ly);
            chartCtx.stroke();
            chartCtx.setLineDash([]);
            labels.push({{ text: "Liq " + wvPrice(liq), desiredY: ly, color: "#fbbf24" }});
          }}
        }});
        stackLabels(labels, plot.top + 14, plot.top + plotH - 14).forEach((label) => {{
          drawGutterLabel(label, width - 12, width - plot.right);
        }});
        const markerSlots = [];
        (whaleViewData.events || []).slice(0, 24).forEach((event) => {{
          const eventPrice = Number(event.current_px || event.entry_px || 0);
          const eventTime = Number(event.time_ms || 0);
          if (!eventPrice || !eventTime || eventTime < minTime || eventTime > maxTime || eventPrice < min || eventPrice > max) return;
          const x = xForTime(eventTime);
          let y = yFor(eventPrice);
          let attempts = 0;
          while (markerSlots.some((slot) => Math.abs(slot.x - x) < 18 && Math.abs(slot.y - y) < 18) && attempts < 6) {{
            y += (attempts % 2 === 0 ? 1 : -1) * (18 + attempts * 2);
            y = Math.max(plot.top + 12, Math.min(plot.top + plotH - 12, y));
            attempts += 1;
          }}
          markerSlots.push({{ x, y }});
          drawEventMarker(event, x, y);
        }});
      }}

      function renderWhaleRows() {{
        const target = document.getElementById("wv-whales");
        if (!target) return;
        if (!whaleViewData.whales.length) {{
          target.innerHTML = '<tr><td colspan="11">Sin whales calificadas para esta moneda.</td></tr>';
          return;
        }}
        target.innerHTML = whaleViewData.whales.map((whale) => {{
          const pos = whale.position;
          const pf = Number(whale.profit_factor || 0) >= 999 ? "inf" : Number(whale.profit_factor || 0).toFixed(2);
          const position = pos
            ? `${{pos.side}}<div class="subtle">${{Number(pos.size || 0).toLocaleString(undefined, {{ maximumFractionDigits: 6 }})}} | ${{wvUsd(pos.notional, 0)}}</div>`
            : '<span class="subtle">Sin posicion actual</span>';
          const opened = pos
            ? `${{wvEsc(pos.opened_at || "No disponible")}}<div class="subtle">${{wvEsc(pos.open_coverage || "")}}</div>`
            : "-";
          const risk = pos && pos.risk
            ? `<strong>${{Number(pos.risk.score || 0).toFixed(1)}}/10</strong><div class="subtle">${{wvEsc(pos.risk.label)}} | ${{wvEsc(pos.risk.signal)}}</div><div class="subtle">Cap ${{wvUsd(pos.risk.capital_used, 0)}} | Lev ${{Number(pos.risk.leverage || 0).toFixed(1)}}x</div>`
            : "-";
          return `<tr>
            <td><a href="/wallet/${{encodeURIComponent(whale.address)}}">${{wvEsc(whale.name)}}</a><div class="subtle">${{wvEsc(whale.address.slice(0, 6) + "..." + whale.address.slice(-4))}}</div></td>
            <td>${{wvPct(whale.winrate)}}<div class="subtle">${{whale.wins}}/${{whale.trades}} trades</div></td>
            <td>${{wvSignedUsd(whale.net_pnl)}}</td>
            <td>${{risk}}</td>
            <td>${{wvEsc(whale.confidence)}}</td>
            <td>${{position}}</td>
            <td>${{pos ? wvPrice(pos.entry_px) : "-"}}</td>
            <td>${{wvPrice(pos ? pos.mark_px : whaleViewData.mark_px)}}</td>
            <td>${{pos ? wvSignedUsd(pos.unrealized_pnl) : "-"}}</td>
            <td>${{opened}}</td>
            <td>${{wvEsc(whale.last_seen || "-")}}</td>
          </tr>`;
        }}).join("");
      }}

      function renderHistory() {{
        const target = document.getElementById("wv-history");
        if (!target) return;
        const closed = whaleViewData.closed_positions || [];
        if (!closed.length) {{
          target.innerHTML = '<div class="subtle">Sin posiciones cerradas confiables para estas top wallets en esta moneda.</div>';
          return;
        }}
        const rows = closed.map((item) => `<tr>
          <td>${{wvEsc(item.closed_at)}}</td>
          <td>${{wvEsc(item.opened_at)}}</td>
          <td><a href="/wallet/${{encodeURIComponent(item.wallet_address)}}">${{wvEsc(item.wallet)}}</a></td>
          <td>${{wvEsc(item.side)}}</td>
          <td>${{Number(item.size || 0).toLocaleString(undefined, {{ maximumFractionDigits: 6 }})}}</td>
          <td>${{wvPrice(item.avg_entry_px)}}</td>
          <td>${{wvPrice(item.avg_close_px)}}</td>
          <td>${{wvSignedUsd(item.net_pnl)}}</td>
          <td>${{Number(item.fill_count || 0)}}</td>
        </tr>`).join("");
        target.innerHTML = `<div class="table-wrap"><table><thead><tr><th>Cierre</th><th>Apertura</th><th>Whale</th><th>Lado</th><th>Tamano</th><th>Avg entry</th><th>Avg close</th><th>PnL neto</th><th>Fills</th></tr></thead><tbody>${{rows}}</tbody></table></div>`;
      }}

      function renderWhaleView() {{
        document.getElementById("wv-count").textContent = whaleViewData.whales.length;
        document.getElementById("wv-long").textContent = wvUsd(whaleViewData.long_notional, 0);
        document.getElementById("wv-short").textContent = wvUsd(whaleViewData.short_notional, 0);
        document.getElementById("wv-mark").textContent = wvPrice(whaleViewData.mark_px);
        document.getElementById("wv-bias").textContent = "Live mark | " + (whaleViewData.mark_source || "cache");
        document.getElementById("wv-stat-upnl").innerHTML = wvSignedUsd(whaleViewData.total_upnl);
        document.getElementById("wv-stat-roi").innerHTML = wvPct(Number(whaleViewData.total_upnl || 0) / Math.max(Number(whaleViewData.total_margin || 0), 1));
        document.getElementById("wv-stat-margin").textContent = wvUsd(whaleViewData.total_margin, 0);
        document.getElementById("wv-stat-holding").textContent = Number(whaleViewData.total_holding || 0).toLocaleString(undefined, {{ maximumFractionDigits: 4 }}) + " " + whaleViewData.coin;
        document.getElementById("wv-stat-active").textContent = Number(whaleViewData.active_count || 0) + " active positions";
        document.getElementById("wv-stat-ls").textContent = wvUsd(whaleViewData.long_notional, 0) + " / " + wvUsd(whaleViewData.short_notional, 0);
        document.getElementById("wv-stat-entry").textContent = wvPrice(whaleViewData.avg_entry);
        document.getElementById("wv-stat-rank").textContent = whaleViewData.whales.length;
        const mark = Number(whaleViewData.mark_px || 0);
        if (mark > 0) {{
          priceSamples.push({{ t: Date.now(), price: mark }});
          priceSamples = priceSamples.slice(-120);
        }}
        renderWhaleRows();
        renderHistory();
        drawWhaleChart();
      }}

      async function refreshWhaleView() {{
        try {{
          const response = await fetch("/api/whale-view?coin=" + encodeURIComponent(whaleViewData.coin), {{ cache: "no-store" }});
          if (!response.ok) return;
          whaleViewData = await response.json();
          renderWhaleView();
        }} catch (error) {{}}
      }}

      chartCanvas.addEventListener("wheel", (event) => {{
        event.preventDefault();
        const rect = chartCanvas.getBoundingClientRect();
        const ratio = Math.max(0.05, Math.min(0.95, (event.clientX - rect.left) / Math.max(rect.width, 1)));
        zoomChart(event.deltaY > 0 ? 1.18 : 0.84, ratio);
      }}, {{ passive: false }});
      chartCanvas.addEventListener("pointerdown", (event) => {{
        chartCanvas.setPointerCapture(event.pointerId);
        chartCanvas.classList.add("dragging");
        dragState = {{ x: event.clientX }};
      }});
      chartCanvas.addEventListener("pointermove", (event) => {{
        if (!dragState) return;
        const rect = chartCanvas.getBoundingClientRect();
        const deltaPx = event.clientX - dragState.x;
        dragState.x = event.clientX;
        panChart(-deltaPx / Math.max(rect.width, 1));
      }});
      function endChartDrag(event) {{
        dragState = null;
        chartCanvas.classList.remove("dragging");
        try {{ chartCanvas.releasePointerCapture(event.pointerId); }} catch (error) {{}}
      }}
      chartCanvas.addEventListener("pointerup", endChartDrag);
      chartCanvas.addEventListener("pointercancel", endChartDrag);
      document.getElementById("wv-zoom-in").addEventListener("click", () => zoomChart(0.78, 0.85));
      document.getElementById("wv-zoom-out").addEventListener("click", () => zoomChart(1.28, 0.85));
      document.getElementById("wv-reset").addEventListener("click", () => {{
        const series = fullSeries();
        const times = series.map((s) => Number(s.time || 0)).filter(Boolean);
        if (times.length) {{
          chartView = {{ start: Math.min(...times), end: Math.max(...times), fullStart: Math.min(...times), fullEnd: Math.max(...times) }};
        }}
        drawWhaleChart();
      }});
      window.addEventListener("resize", drawWhaleChart);
      renderWhaleView();
      setInterval(refreshWhaleView, 5000);
    </script>
    """
    return render_layout("Whale view", body, "whale")


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
        <div class="subtle">Worker: live {html.escape(peru_status_text(AUTO_STATUS['last_tracked']) or 'sin wallets seguidas')} | refresh {html.escape(peru_status_text(AUTO_STATUS['last_refresh']) or 'pendiente')} | fills {html.escape(peru_status_text(AUTO_STATUS['last_fill_sync']) or 'pendiente')} | ledger {html.escape(peru_status_text(AUTO_STATUS['last_ledger_sync']) or 'pendiente')} | discovery {html.escape(peru_status_text(AUTO_STATUS['last_discovery']) or 'pendiente')}</div>
      </div>
      <div style="display:flex; gap:10px; flex-wrap:wrap; align-items:flex-start;">
        <form method="post" action="/reset-pnl" class="card" style="box-shadow:none; padding:12px;">
          <button class="btn secondary" type="submit">Reset PnL</button>
        </form>
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
      <div class="card"><div class="metric-label">Equity real agregado</div><div class="metric-value">{usd(stats['account_value'])}</div></div>
      <div class="card"><div class="metric-label">Margen en posiciones</div><div class="metric-value">{usd(stats['margin_used'])}</div></div>
      <div class="card"><div class="metric-label">Posiciones promedio</div><div class="metric-value">{float(stats['avg_positions']):.1f}</div></div>
    </section>
    <section class="grid two">
      <div class="card"><h2>Top 5 por equity real</h2>{render_wallet_table(top_balance)}</div>
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
        ORDER BY w.account_value DESC
        LIMIT 250
        """,
        tuple(params),
    )
    total_value = sum(row["account_value"] for row in rows)
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
        tracked = int(row["tracked"] or 0)
        trs.append(
            "<tr>"
            f"<td>{wallet_link(row['address'], wallet_name(row))}<div class='subtle'>{html.escape(row['address'])}</div></td>"
            f"<td>{badge('Seguida' if tracked else 'Normal')}</td>"
            f"<td>{usd(row['account_value'])}</td>"
            f"<td>{usd(row['margin_used'])}</td>"
            f"<td>{usd(row['total_ntl_pos'])}</td>"
            f"<td>{usd(row['long_value'])}</td>"
            f"<td>{usd(row['short_value'])}</td>"
            f"<td>{int(row['active_positions'])}</td>"
            f"<td>{badge(row['direction_bias'])}</td>"
            f"<td>{html.escape(coins)}</td>"
            f"<td>{html.escape(peru_time_text(row['last_seen']))}</td>"
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
      <div class="card"><div class="metric-label">Equity real visible</div><div class="metric-value">{usd(total_value)}</div></div>
      <div class="card"><div class="metric-label">Margen posiciones</div><div class="metric-value">{usd(total_margin)}</div></div>
      <div class="card"><div class="metric-label">Posiciones activas</div><div class="metric-value">{int(total_active)}</div></div>
    </section>
    <section class="card">
      <h2>Listado</h2>
      <div class="table-wrap"><table><thead><tr>
        <th>Wallet</th><th>Modo</th><th>Equity real</th><th>Margen pos.</th><th>Notional</th>
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
        <div class="subtle">Eventos inferidos por cambios de posiciones durante refresh; winrate y PnL cerrado usan fills reales.</div>
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


def wallet_profile(address, message=""):
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
    reserve_available = to_float(wallet["withdrawable"])
    locked_or_buffer = max(0.0, to_float(wallet["account_value"]) - total_capital - reserve_available)
    trade_stats = wallet_trade_stats(address)
    open_by_position = {
        (str(row_value(row, "coin", "")), str(row_value(row, "side", ""))): row
        for row in trade_stats["open"]
    }
    fill_state = q_one("SELECT * FROM fill_sync_state WHERE wallet_address = ?", (address,))
    ledger_state = q_one("SELECT * FROM ledger_sync_state WHERE wallet_address = ?", (address,))
    ledger_start_ms = iso_to_epoch_ms(chart_snapshots[0]["created_at"]) if chart_snapshots else 0
    ledger_summary = wallet_ledger_summary(address, ledger_start_ms)
    ledger_rows = wallet_ledger_rows(address, ledger_start_ms, 12)
    is_tracked = int(wallet["tracked"] or 0) == 1
    track_label = "Dejar de seguir" if is_tracked else "Seguir live"
    track_value = "0" if is_tracked else "1"
    for pos in positions:
        open_episode = open_by_position.get((str(pos["coin"]), str(pos["side"])))
        opened_at = row_value(open_episode, "opened_at_ms") if open_episode else None
        if opened_at:
            opened_text = ms_to_local_text(opened_at)
        else:
            opened_text = "Antes del historial"
        coverage_text = row_value(open_episode, "coverage", "Sin fills sincronizados") if open_episode else "Sin fills sincronizados"
        rows.append(
            f"<tr class='position-row' data-coin='{html.escape(pos['coin'])}' data-side='{html.escape(pos['side'])}' data-size='{abs(pos['size'])}' data-entry='{pos['entry_px']}' data-margin='{pos['capital_used']}'>"
            f"<td>{html.escape(pos['coin'])}</td>"
            f"<td>{badge(pos['side'])}</td>"
            f"<td>{html.escape(opened_text)}<div class='subtle'>{html.escape(str(coverage_text))}</div></td>"
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
        f"<td>{html.escape(peru_time_text(s['created_at']))}</td>"
        f"<td>{usd(s['account_value'])}</td>"
        f"<td>{signed_full_usd(row_value(s, 'unrealized_pnl', 0))}</td>"
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
        <div class="subtle">Precios live cada {PRICE_UI_REFRESH_MS}ms | Mark stream: <span id="mark-age">esperando</span> | Worker live: {html.escape(peru_status_text(AUTO_STATUS['last_tracked']) or 'sin wallets seguidas')}</div>
      </div>
      <div style="display:flex; gap:10px; flex-wrap:wrap;">
        <form method="post" action="/wallet/{html.escape(address)}/track">
          <input type="hidden" name="tracked" value="{track_value}">
          <button class="btn {'secondary' if is_tracked else ''}" type="submit">{track_label}</button>
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
      <div class="card"><div class="metric-label">Equity real cuenta</div><div class="metric-value">{usd(wallet['account_value'])}</div><div class="subtle">accountValue de Hyperliquid</div></div>
      <div class="card"><div class="metric-label">Capital en posiciones</div><div class="metric-value">{usd(total_capital)}</div><div class="subtle">margen real usado, no notional</div></div>
      <div class="card"><div class="metric-label">Reserva disponible</div><div class="metric-value">{usd(reserve_available)}</div><div class="subtle">withdrawable</div></div>
      <div class="card"><div class="metric-label">uPnL agregado</div><div class="metric-value">{signed_usd(total_pnl)}</div></div>
    </section>
    <section class="grid metrics">
      <div class="card"><div class="metric-label">Notional abierto</div><div class="metric-value">{usd(wallet['total_ntl_pos'])}</div></div>
      <div class="card"><div class="metric-label">Buffer/no disponible</div><div class="metric-value">{usd(locked_or_buffer)}</div></div>
      <div class="card"><div class="metric-label">Sesgo</div><div class="metric-value">{badge(wallet['direction_bias'])}</div></div>
      <div class="card"><div class="metric-label">ROI sobre margen</div><div class="metric-value">{signed_pct(total_pnl / total_capital if total_capital else 0)}</div></div>
    </section>
    <section class="card">
      <h2>Detalle</h2>
      <table><tbody>
        <tr><th>Equity real cuenta</th><td>{usd(wallet['account_value'])}</td></tr>
        <tr><th>Capital en posiciones</th><td>{usd(total_capital)}</td></tr>
        <tr><th>Reserva disponible</th><td>{usd(reserve_available)}</td></tr>
        <tr><th>Buffer/no disponible</th><td>{usd(locked_or_buffer)}</td></tr>
        <tr><th>Margen usado API</th><td>{usd(wallet['margin_used'])}</td></tr>
        <tr><th>Notional abierto</th><td>{usd(wallet['total_ntl_pos'])}</td></tr>
        <tr><th>Posiciones activas</th><td>{int(wallet['active_positions'])}</td></tr>
        <tr><th>Exposicion neta</th><td>{signed_full_usd(wallet['net_exposure'])}</td></tr>
        <tr><th>Exposicion long</th><td>{usd(wallet['long_value'])}</td></tr>
        <tr><th>Exposicion short</th><td>{usd(wallet['short_value'])}</td></tr>
        <tr><th>Diversificacion</th><td>{pct(wallet['diversification_score'])}</td></tr>
        <tr><th>Top coin</th><td>{html.escape(wallet['top_coin'] or '-')}</td></tr>
        <tr><th>Ultima lectura</th><td>{html.escape(peru_time_text(wallet['last_seen']))}</td></tr>
        <tr><th>Modo live</th><td>{'Seguida: posiciones ~' + str(TRACKED_REFRESH_INTERVAL) + 's, fills ~' + str(TRACKED_FILL_SYNC_INTERVAL) + 's, ledger ~' + str(TRACKED_LEDGER_SYNC_INTERVAL) + 's' if is_tracked else 'Normal'}</td></tr>
        <tr><th>Ultimo sync fills</th><td>{html.escape(fill_sync_status(fill_state))}</td></tr>
        <tr><th>Ultimo sync ledger</th><td>{html.escape(ledger_sync_status(ledger_state))}</td></tr>
      </tbody></table>
      <div class="subtle" style="margin-top:10px;">El modo live refresca posiciones, fills y ledger automaticamente para las wallets seguidas. Los trades cerrados y winrate se confirman con fills reales.</div>
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
      <h2>Posiciones actuales + fills</h2>
      <div class="subtle">Fuente viva: posiciones actuales de Hyperliquid. Los fills solo estiman apertura, escalados, fees y cobertura historica.</div>
      {render_open_trade_episodes_table(trade_stats['open'])}
    </section>
    <section class="card" style="margin-top:16px;">
      <h2>Trades cerrados reales</h2>
      <div class="subtle">Reconstruido desde userFillsByTime. PnL neto = closedPnl - fee total reportada por Hyperliquid.</div>
      {render_trade_episodes_table(trade_stats['recent'])}
    </section>
    <section class="card" style="margin-top:16px;">
      <div style="display:flex; justify-content:space-between; gap:12px; align-items:flex-start; flex-wrap:wrap;">
        <div>
          <h2>Reconciliacion PnL de cuenta</h2>
          <div class="subtle">USDC base de Hyperliquid perps. PnL operativo = cambio de accountValue - flujos externos; luego se separa entre PnL cerrado, cambio de uPnL abierto y residual.</div>
        </div>
        <form method="post" action="/reset-pnl">
          <button class="btn secondary" type="submit">Reset PnL</button>
        </form>
      </div>
      {render_account_pnl_chart(chart_snapshots, address)}
    </section>
    <section class="card" style="margin-top:16px;">
      <h2>Movimientos de capital</h2>
      <div class="grid three" style="margin-bottom:12px;">
        <div><div class="metric-label">Entradas USDC</div><div class="metric-value">{usd(ledger_summary['inflow'])}</div></div>
        <div><div class="metric-label">Salidas USDC</div><div class="metric-value">{signed_usd(ledger_summary['outflow'])}</div></div>
        <div><div class="metric-label">Flujo neto</div><div class="metric-value">{signed_usd(ledger_summary['net_flow'])}</div></div>
      </div>
      {render_ledger_table(ledger_rows)}
    </section>
    <section class="card" style="margin-top:16px;">
      <h2>Posiciones por moneda</h2>
      <div class="table-wrap"><table><thead><tr>
        {th('Coin', 'Mercado perpetuo de la posicion.')}
        {th('Lado', 'Long gana si el precio sube; Short gana si el precio baja.')}
        {th('Apertura est.', 'Estimacion reconstruida desde fills. Si la posicion nacio antes del historial sincronizado, se marca como parcial.')}
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
      </tr></thead><tbody>{''.join(rows) or '<tr><td colspan="13">Sin posiciones activas</td></tr>'}</tbody></table></div>
    </section>
    <section class="card" style="margin-top:16px;">
      <h2>Snapshots</h2>
      <div class="table-wrap"><table><thead><tr><th>Fecha</th><th>Equity</th><th>uPnL</th><th>Notional</th><th>Activas</th></tr></thead><tbody>{snap_rows or '<tr><td colspan="5">Sin snapshots</td></tr>'}</tbody></table></div>
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
    return render_layout("Wallet", body, "dashboard", message)


def trend_sentence(wallet):
    concentration = 1 - float(wallet["diversification_score"])
    if concentration >= 0.65:
        diversification = f"concentrada en {wallet['top_coin'] or 'una moneda'}"
    elif wallet["active_positions"] >= 5:
        diversification = "diversificada"
    else:
        diversification = "moderadamente diversificada"
    exposure = "sin gran exposicion" if wallet["gross_exposure"] <= 0 else f"con {usd(wallet['gross_exposure'])} en mercado"
    return f"{short_addr(wallet['address'])} esta {wallet['direction_bias'].lower()}, {diversification}, {exposure}; equity real observado {usd(wallet['account_value'])}."


def trends():
    top = q_all(
        """
        SELECT *
        FROM wallets
        ORDER BY account_value DESC
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
            SELECT address FROM wallets ORDER BY account_value DESC LIMIT 5
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
        f'<table style="margin-top:12px;"><tbody><tr><th>Equity real</th><td>{usd(row["account_value"])}</td></tr>'
        f'<tr><th>Margen pos.</th><td>{usd(row["margin_used"])}</td></tr>'
        f'<tr><th>Notional</th><td>{usd(row["total_ntl_pos"])}</td></tr>'
        f'<tr><th>Long</th><td>{usd(row["long_value"])}</td></tr><tr><th>Short</th><td>{usd(row["short_value"])}</td></tr>'
        f'<tr><th>Diversificacion</th><td>{pct(row["diversification_score"])}</td></tr></tbody></table></div>'
        for row in top
    )
    body = f"""
    <div class="topbar">
      <div>
        <h1>Tendencias top 5</h1>
        <div class="subtle">Criterio: equity real de cuenta reportado por Hyperliquid</div>
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
        if parsed.path == "/api/position-events":
            query = urllib.parse.parse_qs(parsed.query)
            limit = int(to_float(query.get("limit", ["5"])[0], 5))
            limit = min(25, max(1, limit))
            self.send_json({"events": sidebar_activity_events(limit)})
            return
        if parsed.path == "/api/whale-view":
            query = urllib.parse.parse_qs(parsed.query)
            self.send_json(whale_view_payload(query.get("coin", [""])[0]))
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
        if parsed.path == "/whale-view":
            query = urllib.parse.parse_qs(parsed.query)
            self.send_html(whale_view_page(query.get("coin", [""])[0]))
            return
        if parsed.path == "/trends":
            self.send_html(trends())
            return
        if parsed.path.startswith("/wallet/"):
            address = urllib.parse.unquote(parsed.path.split("/wallet/", 1)[1])
            message = urllib.parse.parse_qs(parsed.query).get("message", [""])[0]
            self.send_html(wallet_profile(address, message))
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
        if parsed.path == "/reset-pnl":
            count = reset_all_pnl_graphs()
            msg = f"Graficos de PnL reiniciados: {count} wallets con nuevo snapshot base. Ledger local limpiado."
            self.redirect("/?message=" + urllib.parse.quote(msg))
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
                    inserted = sync_wallet_fills(address, force_rebuild=True)
                    msg = f"Sync fills OK: {inserted} fills nuevos."
                except Exception as exc:
                    msg = f"Sync fills fallo: {str(exc)[:180]}"
                self.redirect(f"/wallet/{address}?message=" + urllib.parse.quote(msg))
            else:
                self.redirect("/wallets")
            return
        if parsed.path.startswith("/wallet/") and parsed.path.endswith("/sync-ledger"):
            address = urllib.parse.unquote(parsed.path.split("/wallet/", 1)[1].rsplit("/sync-ledger", 1)[0]).lower()
            if ADDRESS_RE.match(address):
                try:
                    inserted = sync_wallet_ledger(address)
                    msg = f"Sync ledger OK: {inserted} movimientos nuevos."
                except Exception as exc:
                    msg = f"Sync ledger fallo: {str(exc)[:180]}"
                self.redirect(f"/wallet/{address}?message=" + urllib.parse.quote(msg))
            else:
                self.redirect("/wallets")
            return
        if parsed.path.startswith("/wallet/") and parsed.path.endswith("/track"):
            address = urllib.parse.unquote(parsed.path.split("/wallet/", 1)[1].rsplit("/track", 1)[0]).lower()
            if ADDRESS_RE.match(address):
                tracked = form.get("tracked") == "1"
                set_wallet_tracked(address, tracked)
                msg = "Wallet marcada para seguimiento live." if tracked else "Wallet removida del seguimiento live."
                self.redirect(f"/wallet/{address}?message=" + urllib.parse.quote(msg))
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
