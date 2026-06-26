# Simple Tracker Whale

Sistema web simple para rastrear wallets de Hyperliquid desde trades en vivo y wallets semilla, guardar las cuentas con balance mayor o igual a USD 250,000 en SQLite y analizar sus posiciones.

## Ejecutar localmente

```bash
pip install -r requirements.txt
python app.py
```

Luego abre `http://localhost:8000`.

Credenciales por defecto:

- Usuario: `admin`
- Password: `admin`

## Escaneo

La API publica oficial de Hyperliquid permite consultar una wallet conocida con `clearinghouseState`, `portfolio` y `subAccounts`, pero no publica un listado global de todas las wallets.

El metodo principal ahora escucha el WebSocket publico de trades. Cada trade de Hyperliquid incluye `users: [buyer, seller]`, asi que el sistema:

1. Se suscribe a trades en vivo de coins liquidas.
2. Extrae comprador y vendedor de cada trade.
3. Rankea las direcciones candidatas por notional operado.
4. Consulta `clearinghouseState` para esas candidatas.
5. Guarda solo wallets con `accountValue >= 250000`.

Las wallets semilla siguen existiendo como complemento y para descubrir subcuentas.

Puedes pasar semillas de tres formas:

- Pegandolas en el formulario del dashboard.
- En la variable `HYPERLIQUID_SEED_WALLETS`.
- En `data/seed_wallets.txt`, una o varias direcciones por linea.

Solo se guardan en la base las wallets cuyo `accountValue` sea mayor o igual a `MIN_ACCOUNT_VALUE`, que por defecto es `250000`.

## Vistas

- Dashboard general: resumen macro, top wallets por balance/margen y formulario de escaneo.
- Wallets: listado filtrable por direccion, coin y sesgo.
- Actividad: aperturas y cierres detectados en wallets guardadas.
- Perfil de wallet: alias editable, balance, posiciones, exposicion, snapshots y detalle por moneda.
- Tendencias: analisis del top 5 por balance neto + margen usado en posiciones.

## Modo semi real-time

Mientras el servicio esta despierto, la app arranca un worker interno que:

- refresca wallets guardadas cada `AUTO_REFRESH_INTERVAL` segundos, por defecto 5;
- descubre nuevas candidatas desde trades cada `AUTO_DISCOVERY_INTERVAL` segundos;
- recalcula balance, posiciones, precio actual derivado, margen usado, uPnL y ROI.
- registra aperturas/cierres comparando contra un estado estable; los cierres requieren `POSITION_CLOSE_CONFIRMATIONS` lecturas consecutivas ausentes para evitar falsos positivos por respuestas incompletas.
- usa `metaAndAssetCtxs` para tomar el `markPx` oficial de Hyperliquid; solo usa `positionValue / tamano` como fallback.
- en el perfil de wallet, actualiza Mark px, Notional, uPnL y ROI con precios live desde WebSocket `allMids` cada `PRICE_UI_REFRESH_MS` milisegundos.
- sincroniza fills reales para reconstruir trades cerrados, winrate, PnL neto y fees reales. El primer sync toma los fills mas recientes con `userFills`; luego usa `userFillsByTime` incremental desde el ultimo fill sincronizado.
- por defecto, el sync automatico de fills se limita al top 10 por capital real aproximado (`account_value + margin_used`) para evitar sobrecargar el servicio.
- permite marcar wallets como "seguida" desde el perfil. Esas wallets se refrescan con prioridad cada `TRACKED_REFRESH_INTERVAL` segundos y sincronizan fills cada `TRACKED_FILL_SYNC_INTERVAL` segundos, hasta `TRACKED_MAX_WALLETS`.

En Render Free esto no es 24/7 garantizado porque el servicio puede dormirse, reiniciarse o perder SQLite local. Para monitoreo serio conviene Render pago + Postgres + worker separado.

## Variables de entorno

```bash
PORT=8000
ADMIN_USER=admin
ADMIN_PASSWORD=admin
SECRET_KEY=change-me
DATABASE_PATH=data/hyper_whales.sqlite3
MIN_ACCOUNT_VALUE=250000
DISCOVERY_COINS=BTC,ETH,SOL,HYPE,ETHFI
DISCOVERY_SECONDS=25
DISCOVERY_MAX_CANDIDATES=80
DISCOVERY_MIN_TRADE_NOTIONAL=25000
AUTO_DISCOVERY_ENABLED=1
AUTO_DISCOVERY_INTERVAL=180
AUTO_REFRESH_INTERVAL=5
AUTO_REFRESH_BATCH=5
TRACKED_REFRESH_INTERVAL=1
TRACKED_FILL_SYNC_INTERVAL=2
TRACKED_MAX_WALLETS=5
POSITION_CLOSE_CONFIRMATIONS=2
MARK_WS_ENABLED=1
PRICE_UI_REFRESH_MS=200
FILL_SYNC_ENABLED=1
FILL_SYNC_INTERVAL=60
FILL_SYNC_BATCH=10
FILL_SYNC_TOP_N=10
LEDGER_SYNC_ENABLED=1
LEDGER_SYNC_INTERVAL=120
LEDGER_INITIAL_LOOKBACK_DAYS=30
HYPERLIQUID_API_RETRIES=3
HYPERLIQUID_INFO_URL=https://api.hyperliquid.xyz/info
HYPERLIQUID_WS_URL=wss://api.hyperliquid.xyz/ws
HYPERLIQUID_SEED_WALLETS=0x...
```

Hyperliquid perps usa USDC como moneda base de margen/PnL/fees. El ledger se guarda en USDC para separar entradas/salidas de capital del PnL ajustado.

## Render

Puede correr en un Web Service gratuito de Render con:

```bash
python app.py
```

Limitacion importante: SQLite en el plan gratuito no es una base persistente confiable. Render puede reiniciar o redeplegar el servicio y perder datos del filesystem efimero. Para uso serio conviene usar PostgreSQL o un disco persistente de pago.
