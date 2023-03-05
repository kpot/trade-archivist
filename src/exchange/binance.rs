use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::io;
use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use isahc;
use isahc::AsyncReadResponseExt;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::{mpsc::Sender, watch};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::archivist::ExchangeName;
use crate::errors::to_invalid_data_error;
use crate::order_book::{
    unix_now_millis, Amount, BookLogRecord, OrderBookItem, OrderBookState, Price, UnixTimeMs,
};

use super::{DepthRecord, ExchangeAPI};

const BINANCE_PAIRS: &[&str] = &[
    "LUNABTC", "SOLBTC", "SOLUSDT", "ETHBTC", "ETHUSDT", // former ETHUSD
    "ETHEUR", "NEOBTC", "NEOUSDT", // former NEOUSD
    "IOTXBTC", // former IOTBTC
    "XMRUSDT", // former XMRUSD
    "XMRBTC", "XMRBTC", "LTCBTC", "LTCUSDT", //former LTCUSD
    "XRPBTC", "XRPUSDT",
];

type UpdateIdType = u64;

#[allow(non_snake_case)]
#[derive(Deserialize, Debug)]
pub struct BinanceBook {
    lastUpdateId: UpdateIdType,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
}

//noinspection SpellCheckingInspection
#[allow(non_snake_case, dead_code)]
#[derive(Deserialize, Debug)]
pub struct BinanceBookDiff {
    e: String,                // Event type ("depthUpdate)
    E: f64,                   // Event time
    s: String,                // Symbol (like "BNBBTC")
    U: u64,                   // First update ID in event
    u: u64,                   // Final update ID in event
    b: Vec<(String, String)>, // Bids to be updated (Price, Quantity)
    a: Vec<(String, String)>, // Asks to be updated (Price, Quantity)
}

impl BinanceBookDiff {
    fn unix_time_ms(&self) -> UnixTimeMs {
        self.E as UnixTimeMs
    }
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
pub struct DepthStreamMessage {
    stream: String,
    data: BinanceBookDiff,
}

impl BinanceBookDiff {
    fn to_log_records(&self) -> io::Result<Vec<BookLogRecord>> {
        let mut result: Vec<BookLogRecord> = Vec::with_capacity(self.a.len() + self.b.len());
        for (str_price, str_amount) in &self.b {
            let amount = Amount::from_str(str_amount).map_err(to_invalid_data_error)?;
            result.push(BookLogRecord {
                amount,
                price: Price::from_str(str_price).map_err(to_invalid_data_error)?,
                count: if amount.is_zero() { 0 } else { 1 },
            });
        }
        for (str_price, str_amount) in &self.a {
            let amount = Amount::from_str(str_amount).map_err(to_invalid_data_error)?;
            result.push(BookLogRecord {
                amount,
                price: -Price::from_str(str_price).map_err(to_invalid_data_error)?,
                count: if amount.is_zero() { 0 } else { 1 },
            });
        }
        Ok(result)
    }
}

pub struct Binance {}

enum BinanceBookMessage {
    FullBook(String, OrderBookState, UpdateIdType),
    UpdateOnly(BinanceBookDiff),
    UnreadableBook(String, io::Error),
    UnreadableUpdate(io::Error),
}

const MIN_RECONNECT_DELAY_MS: u64 = 50;
const MAX_RECONNECT_DELAY_MS: u64 = 30000;

/// Keeps requesting a complete order book for a trading pair,
/// until it receives one.
/// Later this book will be the foundation for the following stream of changes.
/// Returns `std::io::Error` if the book is received successfully, but cannot
/// be parsed (which likely indicates some backward incompatible in the API).
async fn request_book(pair: &str) -> io::Result<(OrderBookState, UpdateIdType)> {
    let mut delay_ms: u64 = MIN_RECONNECT_DELAY_MS;
    let book_url = format!(
        "https://api.binance.com/api/v3/depth?symbol={}&limit=1000",
        pair
    );

    let string_tuple_to_book_item =
        |(price, amount): &(String, String)| -> io::Result<OrderBookItem> {
            Ok(OrderBookItem {
                price: Price::from_str(price).map_err(to_invalid_data_error)?,
                amount: Amount::from_str(amount).map_err(to_invalid_data_error)?,
            })
        };
    loop {
        log::info!("Requesting a new book from {}", book_url);
        match isahc::get_async(&book_url).await {
            Err(_) => {
                delay_ms = min(MAX_RECONNECT_DELAY_MS, delay_ms * 2);
            }
            Ok(mut response) => {
                if let Ok(content) = response.text().await {
                    let binance_book = serde_json::from_str::<BinanceBook>(&content)?;
                    let unix_time = unix_now_millis();
                    let mut book_state = OrderBookState::new(unix_time);
                    let asks: io::Result<Vec<OrderBookItem>> = binance_book
                        .asks
                        .iter()
                        .map(string_tuple_to_book_item)
                        .collect();
                    let bids: io::Result<Vec<OrderBookItem>> = binance_book
                        .bids
                        .iter()
                        .map(string_tuple_to_book_item)
                        .collect();
                    book_state.asks = asks?;
                    book_state.bids = bids?;
                    return Ok((book_state, binance_book.lastUpdateId));
                }
            }
        }
        // If we couldn't get a complete HTTP response, we'll try again later
        sleep(Duration::from_millis(delay_ms)).await;
    }
}

/// Main event loop for the `joined_streamer`, which mostly handles all the messages
/// coming from Binance, parses them, then normalizes, and then passes down to the final "sink",
/// responsible for all other exchanges too.
/// If fail, returns true if an attempt to re-connect makes sense (when the error is caused
/// merely by the connection issues).
async fn streamer_event_loop(
    ws_stream_url: &str,
    ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    sink: &mut mpsc::Sender<BinanceBookMessage>,
    shutdown: &mut watch::Receiver<Option<()>>,
) -> bool {
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                return false;
            }
            stream_iter_next = ws_stream.next() => {
                match stream_iter_next {
                    Some(Ok(Message::Text(ref text))) => {
                                // Dump the update into a single channel
                        match serde_json::from_str::<DepthStreamMessage>(text)
                            .map_err(to_invalid_data_error)
                        {
                            Ok(depth_msg) => {
                                let msg = BinanceBookMessage::UpdateOnly(depth_msg.data);
                                if sink.send(msg).await.is_err() {
                                    // the parent has stopped listening
                                    return false;
                                }
                            }
                            Err(error) => {
                                log::error!(
                                    "Unable to parse a depth update: {:#?}",
                                    error);
                                let msg = BinanceBookMessage::UnreadableUpdate(error);
                                sink.send(msg).await.ok();
                                return false;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        // Forced reconnect sent by the exchange
                        break;
                    }
                    Some(Ok(_)) => {}  // any other WebSocket messages
                    Some(Err(_)) => {
                        log::warn!(
                            "Unable to receive a complete message from WebSocket \
                             stream {}. This could be normal since Binance claims \
                             they disconnect everyone at least once every 24 hours.\
                             Will try to reconnect.", &ws_stream_url);
                        break;
                    }
                    None => {
                        log::warn!("Was disconnected from WebSocket stream at {}", &ws_stream_url);
                        break;
                    }
                }
            }
        }
    }
    true
}

/// Connects to a stream with updates for books of all trading pairs requested, at the same time
/// making multiple side request for complete books of the same pairs.
/// The results of all this activity will be dumped into a single channel monitored
/// by the parent task. This actually helps to reduce the complexity vs the case when one event
/// loop would have to joggle book requests, updates, reconnects, and book-related logic on top of
/// that. This function thus is responsible for connections, while its parent handles book-related
/// logic (like accumulating and applying updates to the book, etc.)
async fn joined_streamer(
    trading_pairs: Vec<String>,
    mut sink: mpsc::Sender<BinanceBookMessage>,
    mut shutdown: watch::Receiver<Option<()>>,
) {
    let channel_names = trading_pairs
        .iter()
        .map(|pair| format!("{}@depth", pair.to_lowercase()))
        .collect::<Vec<_>>()
        .join("/");

    let url = format!(
        "wss://stream.binance.com:9443/stream?streams={}",
        channel_names
    );
    let mut reconnect_delay: u64 = MIN_RECONNECT_DELAY_MS;

    'main_loop: loop {
        match connect_async(&url).await {
            Err(_) => {
                // Failed to connect
                reconnect_delay = min(reconnect_delay * 2, MAX_RECONNECT_DELAY_MS);
                log::warn!("Unable to connect to {}", &url);
            }
            Ok((mut ws_stream, _)) => {
                // Successful connection
                log::info!("Connected to WebSocket stream at {}", &url);
                reconnect_delay = MIN_RECONNECT_DELAY_MS;
                // Send requests to full copies of books for all trading pairs
                let book_requests: Vec<_> = trading_pairs
                    .iter()
                    .map(|pair| {
                        let pair_name = pair.clone();
                        let send_by = sink.clone();
                        tokio::spawn(async move {
                            match request_book(&pair_name).await {
                                Ok((book_state, update_id)) => {
                                    send_by
                                        .send(BinanceBookMessage::FullBook(
                                            pair_name, book_state, update_id,
                                        ))
                                        .await
                                        .ok();
                                }
                                Err(e) => {
                                    send_by
                                        .send(BinanceBookMessage::UnreadableBook(pair_name, e))
                                        .await
                                        .ok();
                                }
                            }
                        })
                    })
                    .collect();

                let stop_book_requests = || {
                    for book_request in &book_requests {
                        book_request.abort();
                    }
                };

                let should_retry =
                    streamer_event_loop(&url, &mut ws_stream, &mut sink, &mut shutdown).await;
                // Internal message loop receiving book updates
                // Killing requests for full books if any of them are still running
                stop_book_requests();
                if !should_retry {
                    break 'main_loop;
                }
            }
        }
        // Wait before attempting another connection
        log::warn!(
            "Waiting for {} ms before attempting another connection",
            reconnect_delay
        );
        sleep(Duration::from_millis(reconnect_delay)).await;
    } // loop
    log::warn!("Binance's joined streamer exited");
}

#[async_trait]
impl ExchangeAPI for Binance {
    fn id() -> ExchangeName {
        ExchangeName::Binance
    }

    //noinspection SpellCheckingInspection
    fn trading_pairs() -> Vec<String> {
        let set: HashSet<String> = BINANCE_PAIRS.iter().map(|item| item.to_string()).collect();
        set.into_iter().collect()
    }

    async fn book_receiver(
        receivers: HashMap<String, Sender<DepthRecord>>,
        mut shutdown: watch::Receiver<Option<()>>,
    ) {
        const RECEIVER_ATTACHED: &str = "A receiver must be assigned to each trading pair";
        let (tx, mut rx) = mpsc::channel(100);
        let streamer_task =
            tokio::spawn(joined_streamer(Self::trading_pairs(), tx, shutdown.clone()));
        let mut full_books_received: HashSet<String> = HashSet::new();
        let mut diff_buffer: HashMap<String, Vec<BinanceBookDiff>> = HashMap::new();
        'main_loop: loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    log::info!("Exiting book receiver.");
                    break;
                }
                Some(book_message) = rx.recv() => {
                    match book_message {
                        BinanceBookMessage::UnreadableBook(pair_name, error) => {
                            log::error!(
                                "Unable to parse received book for {}: {:#?}.\n \
                                 Possible changes in the exchange`s API? Exiting.",
                                pair_name,
                                error
                            );
                            break 'main_loop;
                        },
                        BinanceBookMessage::UnreadableUpdate(error) => {
                            log::error!(
                                "Unable to parse update for one of the books: {:#?}.\n \
                                 Possible changes in the exchange`s API? Exiting.",
                                error
                            );
                            break 'main_loop;
                        },
                        BinanceBookMessage::FullBook(pair_name, mut book_state, update_id) => {
                            log::debug!("Full {} book arrived", &pair_name);
                            // A full order book has arrived, we can merge it with buffered events
                            if let Some(buffered_diffs) = diff_buffer.get(&pair_name) {
                                // we need to re-play valid updates before sending the book further
                                let next_update_id = update_id + 1;
                                for diff in buffered_diffs {
                                    if diff.u >= next_update_id && diff.U <= next_update_id {
                                        // Diff captures events relevant for the books
                                        match diff.to_log_records() {
                                            Ok(diff_records) => {
                                                for log_record in diff_records.iter() {
                                                    book_state.apply_absolute_update(
                                                        diff.unix_time_ms(),
                                                        log_record
                                                    );
                                                }
                                            },
                                            Err(e) => {
                                                log::error!(
                                                    "Unable to parse some of the records \
                                                     of the book received: {:#?}.\nExiting.",
                                                    e
                                                );
                                                break 'main_loop;
                                            }
                                        }
                                    }
                                }
                                log::debug!(
                                    "{} book has been merged with {} buffered diffs",
                                    &pair_name,
                                    buffered_diffs.len()
                                );
                            }
                            let channel = receivers.get(&pair_name).expect(RECEIVER_ATTACHED);
                            if channel.send(
                                DepthRecord::FullBook(book_state))
                                    .await
                                    .is_err() {
                                log::error!("Unable to send a book to the archivist. Exiting.");
                                break;
                            }
                            full_books_received.insert(pair_name.clone());
                        }
                        BinanceBookMessage::UpdateOnly(diff) => {
                            let pair_name = &diff.s;
                            if full_books_received.contains(pair_name) {
                                // We have the full book already and can just re-transmit the updates
                                let channel = receivers.get(pair_name).expect(RECEIVER_ATTACHED);
                                match diff.to_log_records() {
                                    Ok(diff_records) => {
                                        let msg = DepthRecord::Update(
                                            diff.unix_time_ms(),
                                            diff_records
                                        );
                                        if channel.send(msg).await.is_err() {
                                            log::error!("Unable to send a book update \
                                                         to the archivist. Exiting.");
                                            break 'main_loop;
                                        }
                                    },
                                    Err(e) => {
                                        log::error!(
                                            "Can't parse some of the records \
                                             of the diff received: {:#?}.\nExiting.",
                                            e
                                        );
                                        break 'main_loop;
                                    }
                                }
                            } else {
                                diff_buffer
                                    .entry(diff.s.clone())
                                    .or_default()
                                    .push(diff);
                            }
                        }
                    }
                }
                else => {
                    log::error!("Abnormal interruption. Exiting.");
                    break;
                }
            }
        }
        streamer_task.await.ok();
    }
}
