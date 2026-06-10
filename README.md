# Simple Tracker Whale

Sistema web simple para rastrear wallets de Hyperliquid desde wallets semilla, guardar las cuentas con balance mayor o igual a USD 250,000 en SQLite y analizar sus posiciones.

## Ejecutar localmente

```bash
python app.py
```

Luego abre `http://localhost:8000`.

Credenciales por defecto:

- Usuario: `admin`
- Password: `admin`

## Escaneo

La API publica oficial de Hyperliquid permite consultar una wallet conocida con `clearinghouseState`, `portfolio` y `subAccounts`, pero no publica un listado global de todas las wallets. Por eso el sistema parte de wallets semilla y expande subcuentas encontradas.

Puedes pasar semillas de tres formas:

- Pegandolas en el formulario del dashboard.
- En la variable `HYPERLIQUID_SEED_WALLETS`.
- En `data/seed_wallets.txt`, una o varias direcciones por linea.

Solo se guardan en la base las wallets cuyo `accountValue` sea mayor o igual a `MIN_ACCOUNT_VALUE`, que por defecto es `250000`.

## Variables de entorno

```bash
PORT=8000
ADMIN_USER=admin
ADMIN_PASSWORD=admin
SECRET_KEY=change-me
DATABASE_PATH=data/hyper_whales.sqlite3
MIN_ACCOUNT_VALUE=250000
HYPERLIQUID_INFO_URL=https://api.hyperliquid.xyz/info
HYPERLIQUID_SEED_WALLETS=0x...
```

## Render

Puede correr en un Web Service gratuito de Render con:

```bash
python app.py
```

Limitacion importante: SQLite en el plan gratuito no es una base persistente confiable. Render puede reiniciar o redeplegar el servicio y perder datos del filesystem efimero. Para uso serio conviene usar PostgreSQL o un disco persistente de pago.
