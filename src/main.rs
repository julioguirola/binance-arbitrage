use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use reqwest::Client;

use tokio_tungstenite::{connect_async, tungstenite::Message};
use types::{BaseUrl, BookTicker, ExchangeInfo, Symbol};

mod types;

const HTTP_URL: &str = "https://api.binance.com/api/v3";
const STREAM_URL: &str = "wss://stream.binance.com:9443/stream";

impl BaseUrl {
    fn plus(mut self, path: &str) -> String {
        self.0.push_str(path);
        self.0
    }
}

#[tokio::main]
async fn main() {
    let http_client = reqwest::Client::new();

    let exchange_info = fetch_exchange_info(http_client).await;
    println!("Symbols obtenidos: {}", exchange_info.symbols.len());

    let trios = detect_trios(&exchange_info.symbols);
    println!("Trios encontrados: {}", trios.len());

    evaluate(trios).await;
}

async fn fetch_exchange_info(http_client: Client) -> ExchangeInfo {
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

fn detect_trios(symbols: &[Symbol]) -> Vec<(Symbol, Symbol, Symbol)> {
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
    println!("Symbols filtrados: {}", symbols_filtrados.len());
    let mut count = 0;
    for s1 in &symbols_filtrados {
        for s2 in &symbols_filtrados {
            for s3 in &symbols_filtrados {
                if s1.symbol == s3.symbol || s1.symbol == s2.symbol {
                    continue;
                }
                if s1.base_asset == s2.base_asset
                    && (s2.quote_asset == s3.base_asset || s2.quote_asset == s3.quote_asset)
                    && (s3.base_asset == s1.quote_asset || s3.quote_asset == s1.quote_asset)
                {
                    println!("{} {} {}", s1.symbol, s2.symbol, s3.symbol);
                    count += 1;
                    symbols_ok.push(((**s1).clone(), (**s2).clone(), (**s3).clone()));
                }

                if s1.base_asset == s2.quote_asset
                    && (s2.base_asset == s3.base_asset || s2.base_asset == s3.quote_asset)
                    && (s3.base_asset == s1.quote_asset || s3.quote_asset == s1.quote_asset)
                {
                    println!("{} {} {}", s1.symbol, s2.symbol, s3.symbol);
                    count += 1;
                    symbols_ok.push(((**s1).clone(), (**s2).clone(), (**s3).clone()));
                }

                if s1.quote_asset == s2.quote_asset
                    && (s2.base_asset == s3.base_asset || s2.base_asset == s3.quote_asset)
                    && (s3.base_asset == s1.base_asset || s3.quote_asset == s1.base_asset)
                {
                    println!("{} {} {}", s1.symbol, s2.symbol, s3.symbol);
                    count += 1;
                    symbols_ok.push(((**s1).clone(), (**s2).clone(), (**s3).clone()));
                }

                if s1.quote_asset == s2.base_asset
                    && (s2.quote_asset == s3.base_asset || s2.quote_asset == s3.quote_asset)
                    && (s3.base_asset == s1.base_asset || s3.quote_asset == s1.base_asset)
                {
                    println!("{} {} {}", s1.symbol, s2.symbol, s3.symbol);
                    count += 1;
                    symbols_ok.push(((**s1).clone(), (**s2).clone(), (**s3).clone()));
                }
            }
        }
    }
    println!("{}", count);
    symbols_ok
}

async fn evaluate(trios: Vec<(Symbol, Symbol, Symbol)>) {
    let mut handles = vec![];

    for trio in trios {
        let s1 = Arc::new(Mutex::new(trio.0.clone()));
        let s2 = Arc::new(Mutex::new(trio.1.clone()));
        let s3 = Arc::new(Mutex::new(trio.2.clone()));

        let handle = tokio::spawn(async move {
            let (ws_stream, _) = connect_async(STREAM_URL).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            let sub_msg = serde_json::json!({
                "method": "SUBSCRIBE",
                "params": [
                    format!("{}@bookTicker", s1.lock().unwrap().symbol.to_lowercase()),
                    format!("{}@bookTicker", s2.lock().unwrap().symbol.to_lowercase()),
                    format!("{}@bookTicker", s3.lock().unwrap().symbol.to_lowercase()),
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

                let s1_locked = s1.lock().unwrap();
                let s2_locked = s2.lock().unwrap();
                let s3_locked = s3.lock().unwrap();

                if let (Some(b1), Some(b2), Some(b3)) = (
                    &s1_locked.book_ticker,
                    &s2_locked.book_ticker,
                    &s3_locked.book_ticker,
                ) && let (Ok(ask1), Ok(bid2), Ok(ask3)) = (
                    b1.data.ask.parse::<f64>(),
                    b2.data.bid.parse::<f64>(),
                    b3.data.ask.parse::<f64>(),
                ) {
                    let rate = ask1 * (1.0 / bid2) * ask3;
                    println!(
                        "{} -> {} -> {} = {}",
                        b1.data.symbol, b2.data.symbol, b3.data.symbol, rate
                    );
                }
            }
        });

        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }
}
