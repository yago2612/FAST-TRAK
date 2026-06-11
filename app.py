import base64
import hashlib
import hmac
import html
import json
import os
import re
import sqlite3
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
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin")
SECRET_KEY = os.getenv("SECRET_KEY", "change-me-for-production")
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


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


def pct(value):
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return "0.0%"


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
                position_value REAL NOT NULL DEFAULT 0,
                unrealized_pnl REAL NOT NULL DEFAULT 0,
                return_on_equity REAL NOT NULL DEFAULT 0,
                leverage TEXT,
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
            """
        )


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


def parse_wallet(address, state, source):
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
        position_value = abs(to_float(position.get("positionValue")))
        if position_value <= 0 and size:
            position_value = abs(size * to_float(position.get("entryPx")))
        side = "Long" if size > 0 else "Short" if size < 0 else "Flat"
        if side == "Long":
            long_value += position_value
        elif side == "Short":
            short_value += position_value
        coin_values[coin] = coin_values.get(coin, 0.0) + position_value
        leverage = position.get("leverage") or {}
        if isinstance(leverage, dict):
            leverage_label = f"{leverage.get('type', 'cross')} {leverage.get('value', '')}x".strip()
        else:
            leverage_label = str(leverage or "")
        parsed_positions.append(
            {
                "coin": coin,
                "side": side,
                "size": size,
                "entry_px": to_float(position.get("entryPx")),
                "position_value": position_value,
                "unrealized_pnl": to_float(position.get("unrealizedPnl")),
                "return_on_equity": to_float(position.get("returnOnEquity")),
                "leverage": leverage_label,
                "liquidation_px": to_float(position.get("liquidationPx")),
                "margin_used": to_float(position.get("marginUsed")),
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
        conn.execute("DELETE FROM positions WHERE wallet_address = ?", (wallet["address"],))
        for position in wallet["positions"]:
            conn.execute(
                """
                INSERT INTO positions (
                    wallet_address, coin, side, size, entry_px, position_value,
                    unrealized_pnl, return_on_equity, leverage, liquidation_px,
                    margin_used, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    wallet["address"],
                    position["coin"],
                    position["side"],
                    position["size"],
                    position["entry_px"],
                    position["position_value"],
                    position["unrealized_pnl"],
                    position["return_on_equity"],
                    position["leverage"],
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
    discovered = set(live_candidates)
    errors = list(live_summary.get("errors") or [])

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
            wallet = parse_wallet(address, state, "seed" if address in seeds else "subaccount")
            if wallet["account_value"] >= MIN_ACCOUNT_VALUE:
                save_wallet(wallet)
                saved_count += 1

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
            (finished, scanned_count, saved_count, len(discovered), "\n".join(errors[-25:]), run_id),
        )
    return {
        "scanned": scanned_count,
        "saved": saved_count,
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


def render_layout(title, body, active="dashboard", message=""):
    nav = [
        ("dashboard", "/", "Dashboard"),
        ("wallets", "/wallets", "Wallets"),
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


def wallet_link(address):
    safe = html.escape(address)
    return f'<a href="/wallet/{safe}">{short_addr(address)}</a>'


def render_wallet_table(rows, value_key="account_value"):
    if not rows:
        return '<div class="subtle">Sin datos guardados todavia.</div>'
    trs = []
    for row in rows:
        total = row["account_value"] + row["total_ntl_pos"]
        trs.append(
            "<tr>"
            f"<td>{wallet_link(row['address'])}</td>"
            f"<td>{usd(row['account_value'])}</td>"
            f"<td>{usd(row['total_ntl_pos'])}</td>"
            f"<td>{usd(total)}</td>"
            f"<td>{int(row['active_positions'])}</td>"
            f"<td>{badge(row['direction_bias'])}</td>"
            f"<td>{html.escape(row['top_coin'] or '-')}</td>"
            "</tr>"
        )
    return (
        '<div class="table-wrap"><table><thead><tr>'
        "<th>Wallet</th><th>Balance</th><th>Posiciones</th><th>Total</th>"
        "<th>Activas</th><th>Sesgo</th><th>Top coin</th>"
        "</tr></thead><tbody>"
        + "".join(trs)
        + "</tbody></table></div>"
    )


def dashboard(message=""):
    stats = q_one(
        """
        SELECT COUNT(*) wallet_count,
               COALESCE(SUM(account_value), 0) account_value,
               COALESCE(SUM(total_ntl_pos), 0) total_ntl_pos,
               COALESCE(SUM(long_value), 0) long_value,
               COALESCE(SUM(short_value), 0) short_value,
               COALESCE(AVG(active_positions), 0) avg_positions
        FROM wallets
        """
    )
    last_scan = q_one("SELECT * FROM scan_runs ORDER BY id DESC LIMIT 1")
    top_balance = q_all("SELECT * FROM wallets ORDER BY account_value DESC LIMIT 5")
    top_active = q_all("SELECT * FROM wallets ORDER BY active_positions DESC, account_value DESC LIMIT 5")
    top_positions = q_all("SELECT * FROM wallets ORDER BY total_ntl_pos DESC LIMIT 5")
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
        scan_copy = f"{last_scan['finished_at'] or last_scan['started_at']} | escaneadas {last_scan['scanned_count']} | guardadas {last_scan['saved_count']}"
    body = f"""
    <div class="topbar">
      <div>
        <h1>Dashboard general</h1>
        <div class="subtle">Ultimo escaneo: {html.escape(scan_copy)}</div>
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
      <div class="card"><div class="metric-label">Valor en posiciones</div><div class="metric-value">{usd(stats['total_ntl_pos'])}</div></div>
      <div class="card"><div class="metric-label">Posiciones promedio</div><div class="metric-value">{float(stats['avg_positions']):.1f}</div></div>
    </section>
    <section class="grid two">
      <div class="card"><h2>Top 5 por balance</h2>{render_wallet_table(top_balance)}</div>
      <div class="card"><h2>Top 5 por posiciones activas</h2>{render_wallet_table(top_active)}</div>
      <div class="card"><h2>Top 5 por valor en posiciones</h2>{render_wallet_table(top_positions)}</div>
      <div class="card">
        <h2>Macro de exposicion</h2>
        <div class="grid two" style="margin-bottom:14px;">
          <div><div class="metric-label">Long</div><div class="metric-value">{usd(stats['long_value'])}</div></div>
          <div><div class="metric-label">Short</div><div class="metric-value">{usd(stats['short_value'])}</div></div>
        </div>
        <div class="bars">{coin_bars or '<div class="subtle">Sin posiciones activas.</div>'}</div>
      </div>
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
        ORDER BY (w.account_value + w.total_ntl_pos) DESC
        LIMIT 250
        """,
        tuple(params),
    )
    total_value = sum(row["account_value"] + row["total_ntl_pos"] for row in rows)
    total_exposure = sum(row["gross_exposure"] for row in rows)
    total_active = sum(row["active_positions"] for row in rows)
    options = "".join(
        f'<option value="{value}" {"selected" if bias == value else ""}>{label}</option>'
        for value, label in [("", "Todos"), ("Alcista", "Alcistas"), ("Bajista", "Bajistas"), ("Neutral", "Neutrales")]
    )
    trs = []
    for row in rows:
        coins = ", ".join((row["coins"] or "").split(",")[:5]) or "-"
        total = row["account_value"] + row["total_ntl_pos"]
        trs.append(
            "<tr>"
            f"<td>{wallet_link(row['address'])}<div class='subtle'>{html.escape(row['address'])}</div></td>"
            f"<td>{usd(row['account_value'])}</td>"
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
      <div class="card"><div class="metric-label">Total balance + posiciones</div><div class="metric-value">{usd(total_value)}</div></div>
      <div class="card"><div class="metric-label">Exposicion bruta</div><div class="metric-value">{usd(total_exposure)}</div></div>
      <div class="card"><div class="metric-label">Posiciones activas</div><div class="metric-value">{int(total_active)}</div></div>
    </section>
    <section class="card">
      <h2>Listado</h2>
      <div class="table-wrap"><table><thead><tr>
        <th>Wallet</th><th>Balance</th><th>Posiciones</th><th>Total</th>
        <th>Long</th><th>Short</th><th>Activas</th><th>Sesgo</th><th>Coins</th><th>Actualizada</th>
      </tr></thead><tbody>{''.join(trs) or '<tr><td colspan="10">Sin wallets guardadas.</td></tr>'}</tbody></table></div>
    </section>
    """
    return render_layout("Wallets", body, "wallets")


def wallet_profile(address):
    address = address.lower()
    wallet = q_one("SELECT * FROM wallets WHERE address = ?", (address,))
    if not wallet:
        return render_layout("Wallet", "<h1>Wallet no encontrada</h1>", "dashboard")
    positions = q_all("SELECT * FROM positions WHERE wallet_address = ? ORDER BY position_value DESC", (address,))
    snapshots = q_all(
        "SELECT * FROM wallet_snapshots WHERE wallet_address = ? ORDER BY id DESC LIMIT 12",
        (address,),
    )
    rows = []
    for pos in positions:
        rows.append(
            "<tr>"
            f"<td>{html.escape(pos['coin'])}</td>"
            f"<td>{badge(pos['side'])}</td>"
            f"<td>{pos['size']:,.6f}</td>"
            f"<td>{usd(pos['position_value'])}</td>"
            f"<td>{usd(pos['entry_px'])}</td>"
            f"<td>{usd(pos['unrealized_pnl'])}</td>"
            f"<td>{pct(pos['return_on_equity'])}</td>"
            f"<td>{html.escape(pos['leverage'] or '-')}</td>"
            f"<td>{usd(pos['liquidation_px'])}</td>"
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
        <h1>{html.escape(short_addr(address))}</h1>
        <div class="subtle">{html.escape(address)}</div>
      </div>
      <a class="btn secondary" href="/">Volver</a>
    </div>
    <section class="grid metrics">
      <div class="card"><div class="metric-label">Balance</div><div class="metric-value">{usd(wallet['account_value'])}</div></div>
      <div class="card"><div class="metric-label">Posiciones</div><div class="metric-value">{usd(wallet['total_ntl_pos'])}</div></div>
      <div class="card"><div class="metric-label">Exposicion neta</div><div class="metric-value">{usd(wallet['net_exposure'])}</div></div>
      <div class="card"><div class="metric-label">Sesgo</div><div class="metric-value">{badge(wallet['direction_bias'])}</div></div>
    </section>
    <section class="grid two">
      <div class="card">
        <h2>Detalle</h2>
        <table><tbody>
          <tr><th>Withdrawable</th><td>{usd(wallet['withdrawable'])}</td></tr>
          <tr><th>Margen usado</th><td>{usd(wallet['margin_used'])}</td></tr>
          <tr><th>Exposicion long</th><td>{usd(wallet['long_value'])}</td></tr>
          <tr><th>Exposicion short</th><td>{usd(wallet['short_value'])}</td></tr>
          <tr><th>Diversificacion</th><td>{pct(wallet['diversification_score'])}</td></tr>
          <tr><th>Top coin</th><td>{html.escape(wallet['top_coin'] or '-')}</td></tr>
          <tr><th>Ultima lectura</th><td>{html.escape(wallet['last_seen'])}</td></tr>
        </tbody></table>
      </div>
      <div class="card">
        <h2>Snapshots</h2>
        <div class="table-wrap"><table><thead><tr><th>Fecha</th><th>Balance</th><th>Posiciones</th><th>Activas</th></tr></thead><tbody>{snap_rows or '<tr><td colspan="4">Sin snapshots</td></tr>'}</tbody></table></div>
      </div>
    </section>
    <section class="card" style="margin-top:16px;">
      <h2>Posiciones por moneda</h2>
      <div class="table-wrap"><table><thead><tr><th>Coin</th><th>Lado</th><th>Tamano</th><th>Valor</th><th>Entrada</th><th>uPnL</th><th>ROE</th><th>Lev</th><th>Liq.</th></tr></thead><tbody>{''.join(rows) or '<tr><td colspan="9">Sin posiciones activas</td></tr>'}</tbody></table></div>
    </section>
    """
    return render_layout("Wallet", body, "dashboard")


def trend_sentence(wallet):
    total = wallet["account_value"] + wallet["total_ntl_pos"]
    concentration = 1 - float(wallet["diversification_score"])
    if concentration >= 0.65:
        diversification = f"concentrada en {wallet['top_coin'] or 'una moneda'}"
    elif wallet["active_positions"] >= 5:
        diversification = "diversificada"
    else:
        diversification = "moderadamente diversificada"
    exposure = "sin gran exposicion" if wallet["gross_exposure"] <= 0 else f"con {usd(wallet['gross_exposure'])} en mercado"
    return f"{short_addr(wallet['address'])} esta {wallet['direction_bias'].lower()}, {diversification}, {exposure}; total observado {usd(total)}."


def trends():
    top = q_all(
        """
        SELECT *
        FROM wallets
        ORDER BY (account_value + total_ntl_pos) DESC
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
            SELECT address FROM wallets ORDER BY (account_value + total_ntl_pos) DESC LIMIT 5
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
        f'<div class="card"><h2>{wallet_link(row["address"])}</h2><div class="subtle">{html.escape(trend_sentence(row))}</div>'
        f'<table style="margin-top:12px;"><tbody><tr><th>Total</th><td>{usd(row["account_value"] + row["total_ntl_pos"])}</td></tr>'
        f'<tr><th>Long</th><td>{usd(row["long_value"])}</td></tr><tr><th>Short</th><td>{usd(row["short_value"])}</td></tr>'
        f'<tr><th>Diversificacion</th><td>{pct(row["diversification_score"])}</td></tr></tbody></table></div>'
        for row in top
    )
    body = f"""
    <div class="topbar">
      <div>
        <h1>Tendencias top 5</h1>
        <div class="subtle">Criterio: balance neto + valor en posiciones</div>
      </div>
    </div>
    <section class="grid metrics">
      <div class="card"><div class="metric-label">Lectura macro</div><div class="metric-value">{badge(macro)}</div></div>
      <div class="card"><div class="metric-label">Long top 5</div><div class="metric-value">{usd(total_long)}</div></div>
      <div class="card"><div class="metric-label">Short top 5</div><div class="metric-value">{usd(total_short)}</div></div>
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
        if parsed.path == "/":
            message = urllib.parse.parse_qs(parsed.query).get("message", [""])[0]
            self.send_html(dashboard(message))
            return
        if parsed.path == "/wallets":
            query = urllib.parse.parse_qs(parsed.query)
            self.send_html(wallets_page(query.get("q", [""])[0], query.get("bias", [""])[0]))
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
        if parsed.path == "/scan":
            result = scan_wallets(
                form.get("wallets", ""),
                use_live_discovery=form.get("use_live_discovery") == "1",
                coins_text=form.get("coins", ""),
                discovery_seconds=form.get("seconds") or None,
                max_candidates=form.get("max_candidates") or None,
            )
            msg = f"Escaneo listo: {result['scanned']} revisadas, {result['saved']} guardadas, {result['discovered']} candidatas descubiertas."
            if result.get("live", {}).get("candidates"):
                msg += f" Trades live: {result['live']['candidates']} candidatas desde {result['live']['trades']} trades."
            if result["errors"]:
                msg += f" Errores: {len(result['errors'])}."
            self.redirect("/?message=" + urllib.parse.quote(msg))
            return
        self.send_html(render_layout("404", "<h1>404</h1>", "dashboard"), 404)


def main():
    init_db()
    port = int(os.getenv("PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), AppHandler)
    print(f"{APP_NAME} running on http://localhost:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
