use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use tokio::sync::{broadcast, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::types::{BaseUrl, BookTicker, ExchangeInfo, Symbol, TrioUpdate};

const HTTP_URL: &str = "https://api.binance.com/api/v3";
const STREAM_URL: &str = "wss://stream.binance.com:9443/stream";

impl BaseUrl {
    fn plus(mut self, path: &str) -> String {
        self.0.push_str(path);
        self.0
    }
}

pub async fn fetch_exchange_info(http_client: Client) -> ExchangeInfo {
    let http_url = BaseUrl(String::from(HTTP_URL));
    http_client
        .get(http_url.plus("/exchangeInfo"))
        .query(&[("showPermissionSets", false)])
        .query(&[("symbolStatus", "TRADING")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

pub fn detect_trios(symbols: &[Symbol]) -> Vec<(Symbol, Symbol, Symbol)> {
    let monedas = vec![
        "BTC", "ETH", "USDT", "SOL", "XRP", "BNB", "TRY", "USDC", "ADA", "AVAX",
    ];
    let symbols_filtrados: Vec<&Symbol> = symbols
        .iter()
        .filter(|s| {
            monedas.contains(&s.base_asset.as_str()) && monedas.contains(&s.quote_asset.as_str())
        })
        .collect();
    let mut symbols_ok: Vec<(Symbol, Symbol, Symbol)> = vec![];
    let mut seen: std::collections::HashSet<(String, String, String)> =
        std::collections::HashSet::new();
    println!("Symbols filtrados: {}", symbols_filtrados.len());
    let mut count = 0;
    for s1 in &symbols_filtrados {
        for s2 in &symbols_filtrados {
            for s3 in &symbols_filtrados {
                if s1.symbol == s3.symbol || s1.symbol == s2.symbol {
                    continue;
                }
                let mut trio_symbols = [s1.symbol.as_str(), s2.symbol.as_str(), s3.symbol.as_str()];
                trio_symbols.sort();
                let key = (
                    trio_symbols[0].to_string(),
                    trio_symbols[1].to_string(),
                    trio_symbols[2].to_string(),
                );
                if seen.contains(&key) {
                    continue;
                }

                let mut add = false;
                if s1.base_asset == s2.base_asset
                    && (s2.quote_asset == s3.base_asset || s2.quote_asset == s3.quote_asset)
                    && (s3.base_asset == s1.quote_asset || s3.quote_asset == s1.quote_asset)
                {
                    add = true;
                }

                if s1.base_asset == s2.quote_asset
                    && (s2.base_asset == s3.base_asset || s2.base_asset == s3.quote_asset)
                    && (s3.base_asset == s1.quote_asset || s3.quote_asset == s1.quote_asset)
                {
                    add = true;
                }

                if s1.quote_asset == s2.quote_asset
                    && (s2.base_asset == s3.base_asset || s2.base_asset == s3.quote_asset)
                    && (s3.base_asset == s1.base_asset || s3.quote_asset == s1.base_asset)
                {
                    add = true;
                }

                if s1.quote_asset == s2.base_asset
                    && (s2.quote_asset == s3.base_asset || s2.quote_asset == s3.quote_asset)
                    && (s3.base_asset == s1.base_asset || s3.quote_asset == s1.base_asset)
                {
                    add = true;
                }

                if add {
                    println!("{} {} {}", s1.symbol, s2.symbol, s3.symbol);
                    count += 1;
                    seen.insert(key);
                    symbols_ok.push(((**s1).clone(), (**s2).clone(), (**s3).clone()));
                }
            }
        }
    }
    println!("{}", count);
    symbols_ok
}

fn trio_route(s1: &Symbol, s2: &Symbol, s3: &Symbol) -> String {
    format!(
        "{}/{} -> {}/{} -> {}/{}",
        s1.base_asset,
        s1.quote_asset,
        s2.base_asset,
        s2.quote_asset,
        s3.base_asset,
        s3.quote_asset
    )
}

pub fn calculate_rate(s1: Symbol, s2: Symbol, s3: Symbol) -> f64 {
    let mut rate = 1.0;

    if s1.base_asset == s2.base_asset || s1.base_asset == s2.quote_asset {
        rate *= 1.0
            / s1.book_ticker
                .as_ref()
                .unwrap()
                .data
                .bid
                .parse::<f64>()
                .unwrap();
    } else {
        rate *= s1
            .book_ticker
            .as_ref()
            .unwrap()
            .data
            .ask
            .parse::<f64>()
            .unwrap();
    }

    if s2.base_asset == s3.base_asset || s2.base_asset == s3.quote_asset {
        rate *= 1.0
            / s2.book_ticker
                .as_ref()
                .unwrap()
                .data
                .bid
                .parse::<f64>()
                .unwrap();
    } else {
        rate *= s2
            .book_ticker
            .as_ref()
            .unwrap()
            .data
            .ask
            .parse::<f64>()
            .unwrap();
    }

    if s3.base_asset == s1.base_asset || s3.base_asset == s1.quote_asset {
        rate *= 1.0
            / s3.book_ticker
                .as_ref()
                .unwrap()
                .data
                .bid
                .parse::<f64>()
                .unwrap();
    } else {
        rate *= s3
            .book_ticker
            .as_ref()
            .unwrap()
            .data
            .ask
            .parse::<f64>()
            .unwrap();
    }

    rate
}

pub fn spawn_trio_stream(
    trio: (Symbol, Symbol, Symbol),
    tx: broadcast::Sender<TrioUpdate>,
    snapshot: Arc<RwLock<HashMap<String, f64>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let s1 = Arc::new(Mutex::new(trio.0.clone()));
        let s2 = Arc::new(Mutex::new(trio.1.clone()));
        let s3 = Arc::new(Mutex::new(trio.2.clone()));

        let route = trio_route(&trio.0, &trio.1, &trio.2);

        let (ws_stream, _) = connect_async(STREAM_URL).await.expect("Failed to connect");

        let (mut write, mut read) = ws_stream.split();

        let sub_msg = serde_json::json!({
            "method": "SUBSCRIBE",
            "params": [
                format!("{}@bookTicker", trio.0.symbol.to_lowercase()),
                format!("{}@bookTicker", trio.1.symbol.to_lowercase()),
                format!("{}@bookTicker", trio.2.symbol.to_lowercase()),
            ],
            "id": "e9d6b4349871b40611412680b3445foc"
        });

        if let Err(e) = write.send(Message::Text(sub_msg.to_string().into())).await {
            eprintln!("Send error: {}", e);
            return;
        }

        while let Some(msg) = read.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("Read error: {}", e);
                    break;
                }
            };

            if let Message::Text(text) = msg
                && let Ok(ticker) = serde_json::from_str::<BookTicker>(&text)
            {
                let sym = &ticker.data.symbol;
                if *sym == s1.lock().unwrap().symbol {
                    s1.lock().unwrap().book_ticker = Some(ticker);
                } else if *sym == s2.lock().unwrap().symbol {
                    s2.lock().unwrap().book_ticker = Some(ticker);
                } else if *sym == s3.lock().unwrap().symbol {
                    s3.lock().unwrap().book_ticker = Some(ticker);
                }
            }

            let rate = {
                let s1_locked = s1.lock().unwrap();
                let s2_locked = s2.lock().unwrap();
                let s3_locked = s3.lock().unwrap();
                if s1_locked.book_ticker.is_some()
                    && s2_locked.book_ticker.is_some()
                    && s3_locked.book_ticker.is_some()
                {
                    Some(calculate_rate(
                        (*s1_locked).clone(),
                        (*s2_locked).clone(),
                        (*s3_locked).clone(),
                    ))
                } else {
                    None
                }
            };

            if let Some(rate) = rate {
                let mut guard = snapshot.write().await;
                if guard.get(&route).copied() != Some(rate) {
                    guard.insert(route.clone(), rate);
                    drop(guard);
                    let _ = tx.send(TrioUpdate {
                        route: route.clone(),
                        rate,
                    });
                }
            }
        }
    })
}
