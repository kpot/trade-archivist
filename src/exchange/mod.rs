use std::collections::HashMap;

use async_trait::async_trait;
use chrono::TimeZone;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

use crate::{
    archivist::ExchangeName,
    order_book::{BookLogRecord, OrderBookState, UnixTimeMs},
};

pub mod binance;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DepthRecord {
    Update(UnixTimeMs, Vec<BookLogRecord>),
    FullBook(OrderBookState),
}

impl DepthRecord {
    pub fn general_time_ms(&self) -> UnixTimeMs {
        match self {
            DepthRecord::Update(t, _) => *t,
            DepthRecord::FullBook(book) => book.unix_time,
        }
    }

    pub fn general_time_utc(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc
            .timestamp_millis_opt(self.general_time_ms())
            .earliest()
            .expect("Record supposedly has a valid timestamp")
    }
}

#[async_trait]
pub trait ExchangeAPI {
    fn id() -> ExchangeName;
    fn trading_pairs() -> Vec<String>;
    async fn book_receiver(
        receivers: HashMap<String, mpsc::Sender<DepthRecord>>,
        mut shutdown: watch::Receiver<Option<()>>,
    );
}
