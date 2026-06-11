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

- Dashboard general: resumen macro, top wallets y formulario de escaneo.
- Wallets: listado filtrable por direccion, coin y sesgo.
- Perfil de wallet: balance, posiciones, exposicion, snapshots y detalle por moneda.
- Tendencias: analisis del top 5 por balance neto + valor en posiciones.

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
HYPERLIQUID_INFO_URL=https://api.hyperliquid.xyz/info
HYPERLIQUID_WS_URL=wss://api.hyperliquid.xyz/ws
HYPERLIQUID_SEED_WALLETS=0x...
```

## Render

Puede correr en un Web Service gratuito de Render con:

```bash
python app.py
```

Limitacion importante: SQLite en el plan gratuito no es una base persistente confiable. Render puede reiniciar o redeplegar el servicio y perder datos del filesystem efimero. Para uso serio conviene usar PostgreSQL o un disco persistente de pago.
