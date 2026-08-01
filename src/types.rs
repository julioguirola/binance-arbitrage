use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug)]
pub struct ExchangeInfo {
    pub symbols: Vec<Symbol>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Symbol {
    pub symbol: String,
    #[serde(rename = "baseAsset")]
    pub base_asset: String,
    #[serde(rename = "quoteAsset")]
    pub quote_asset: String,
    #[serde(skip_deserializing)]
    pub book_ticker: Option<BookTicker>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BookTicker {
    pub data: BookTickerData,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BookTickerData {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub bid: String,
    // #[serde(rename = "B")]
    // pub bid_qty: String,
    #[serde(rename = "a")]
    pub ask: String,
    // #[serde(rename = "A")]
    // pub ask_qty: String,
}

pub struct BaseUrl(pub String);

#[derive(Serialize, Clone, Debug)]
pub struct TrioUpdate {
    pub route: String,
    pub rate: f64,
}
