use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use reqwest::Client;
use tokio::sync::{broadcast, RwLock};
use tower_http::services::ServeDir;

use engine::{detect_trios, fetch_exchange_info, spawn_trio_stream};
use types::TrioUpdate;

mod engine;
mod types;

const LISTEN_ADDR: &str = "0.0.0.0:8080";

#[derive(Clone)]
struct AppState {
    tx: broadcast::Sender<TrioUpdate>,
    snapshot: Arc<RwLock<HashMap<String, f64>>>,
}

#[tokio::main]
async fn main() {
    let http_client = Client::new();

    let exchange_info = fetch_exchange_info(http_client).await;
    println!("Symbols obtenidos: {}", exchange_info.symbols.len());

    let trios = detect_trios(&exchange_info.symbols);
    println!("Trios encontrados: {}", trios.len());

    let (tx, _) = broadcast::channel(1024);
    let snapshot: Arc<RwLock<HashMap<String, f64>>> = Arc::new(RwLock::new(HashMap::new()));

    for trio in trios {
        spawn_trio_stream(trio, tx.clone(), snapshot.clone());
    }

    let state = AppState { tx, snapshot };

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .fallback_service(ServeDir::new("frontend/dist"))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(LISTEN_ADDR)
        .await
        .expect("Fallo al bindear 0.0.0.0:8080");
    println!("Servidor web en http://{LISTEN_ADDR}");
    axum::serve(listener, app).await.expect("Fallo el server");
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    let trios: Vec<TrioUpdate> = {
        let snapshot = state.snapshot.read().await;
        snapshot
            .iter()
            .map(|(route, rate)| TrioUpdate {
                route: route.clone(),
                rate: *rate,
            })
            .collect()
    };

    let msg = serde_json::json!({ "type": "snapshot", "trios": trios });
    if socket.send(Message::Text(msg.to_string().into())).await.is_err() {
        return;
    }

    let mut rx = state.tx.subscribe();

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    None => break,
                    Some(Ok(Message::Close(_))) | Some(Err(_)) => break,
                    Some(Ok(_)) => {}
                }
            }
            update = rx.recv() => {
                match update {
                    Ok(trio) => {
                        let msg = serde_json::json!({ "type": "update", "trio": trio });
                        if socket.send(Message::Text(msg.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}
