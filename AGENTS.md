# AGENTS.md

Scanner de arbitraje triangular en Binance (Rust backend + frontend React/Vite). Sin README, sin tests, sin CI. Todo el código, output y commits en español — mantener esa convención.

## Comandos

- App (streams + servidor web): `cargo run --bin rust-ws` → sirve la web en `http://localhost:8080` (sirve `frontend/dist`, así que hacé `npm run build` antes o tras cambiarlo). Corre para siempre, Ctrl+C para salir. Requiere acceso a la API de Binance.
- Scratch (NO es la app): `cargo run --bin prueba` — demo de tokio con sleep infinito.
- `cargo run` pelado falla: hay 2 bins (`rust-ws`, `prueba`) y no hay `default-run`. El package se llama `rust-ws` (legado), no `binance-arbitrage`.
- Frontend: `npm run dev` en `frontend/` (hot reload en :5173, proxya `/ws` a :8080). `npm run build` genera `frontend/dist`.

## Arquitectura

1. `src/main.rs`: wiring + servidor axum. Fetchea `/api/v3/exchangeInfo`, detecta trios y arranca el server.
2. `src/engine.rs`: `detect_trios` (filtra a 10 monedas hardcodeadas, fuerza bruta O(n³)), `calculate_rate`, `spawn_trio_stream` (una tarea tokio + un WebSocket a Binance por trio, suscrito a `<symbol>@bookTicker`).
3. Flujo de datos: cada task publica `TrioUpdate { route, rate }` a un `tokio::sync::broadcast` **solo si el rate cambió** y actualiza un snapshot `HashMap<route, rate>`. `GET /ws` manda el snapshot al conectar y después cada update (protocolo `{"type":"snapshot","trios":[...]}` / `{"type":"update","trio":{...}}`).
4. `frontend/`: grid React (celda por trio, verde si rate > 1) con reconexión automática con backoff.

## Gotchas

- Binance manda los precios como strings; se parsean con `.parse::<f64>().unwrap()` (paniquea con data mala).
- `Symbol.book_ticker` (src/types.rs) es `#[serde(skip_deserializing)]`: viene vacío de exchangeInfo, lo llena solo la stream WS. El snapshot arranca con pocos trios y se completa solo.
- `#[serde(rename=...)]` mapea los campos camelCase de Binance a snake_case.
- Edition 2024 con let-chains (`if let ... && let`) — necesita rustc ≥ 1.85; no hay `rust-toolchain`.
- ServeDir apunta a `frontend/dist` relativo al CWD: correr cargo desde la raíz del repo.
- El snapshot se actualiza por la tasa de eventos de bookTicker; no mandar todo mensaje al navegador o se satura.
