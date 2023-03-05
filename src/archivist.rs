/// Archivist is service responsible both for recording all order book-related events in real time
/// and providing access to these recording via an API.
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bzip2::write::BzEncoder;
use chrono::{DateTime, Timelike, Utc};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::{signal, task};

use crate::api::{send_frame, FrameReader};
use crate::errors::wrap_lock_error;
use crate::exchange::binance::Binance;
use crate::exchange::{DepthRecord, ExchangeAPI};
use crate::order_book::navigation::{StorageNavigator, ARCHIVE_FILE_EXTENSION};
use crate::order_book::storage::{
    open_archive_for_writing, ArchiveFileEncoder, BookBlockHeader, BookBlockType,
};
use crate::order_book::{unix_now_millis, BookLogRecord, OrderBookState, UnixTimeMs};

/// Limits the size of individual messages coming to the API. This does not
/// limit the size of the response.
const API_MAX_FRAME_SIZE: usize = 1024;
/// Backlog for the channel receiving updates from the exchange
const EXCHANGE_UPDATES_BACKLOG: usize = 120;
/// Backlog for the channel receiving API calls for the Archivist
const ARCHIVIST_API_BACKLOG: usize = 120;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ExchangeName {
    Binance,
    Bittrex,
}

pub enum ArchivistAPIResponse {
    UnsavedHistory(Vec<DepthRecord>),
    ForwardedUpdate(DepthRecord),
}

#[derive(Debug)]
pub enum ArchiveCommand {
    GetUnsavedAndSubscribe(mpsc::Sender<ArchivistAPIResponse>),
}

/// Responsible for recording order book changes for a given trading pair,
/// and encapsulating all the details of serialization and writing,
/// plus creation of new archive files every hour (to make sure none
/// of the files stay open for too long and we don't lose much in case
/// of an unexpected shutdown).
struct ArchiveWriter {
    last_time_key: u32,
    storage_dir: PathBuf,
    pair_name: String,
    last_open_file: Option<ArchiveFileEncoder>,
}

impl ArchiveWriter {
    fn new(storage_dir: &str, pair_name: &str) -> Self {
        let storage_dir = std::path::Path::new(storage_dir).join(pair_name);
        std::fs::create_dir_all(&storage_dir)
            .expect("Cannot create directory for storing trade pair`s history");
        Self {
            storage_dir,
            last_time_key: u32::MAX,
            pair_name: pair_name.to_string(),
            last_open_file: None,
        }
    }

    fn time_key<Tz: chrono::TimeZone>(dt: &chrono::DateTime<Tz>) -> u32 {
        dt.hour()
    }

    fn is_new_file_required(&self, message: &DepthRecord) -> bool {
        let record_time = message.general_time_utc();
        Self::time_key(&record_time) != self.last_time_key
    }

    pub fn write(&mut self, msg: &DepthRecord) -> io::Result<()> {
        let open_file_ref = self.open_new_file_if_necessary(msg)?;
        match msg {
            DepthRecord::Update(unix_time, updates) => {
                if updates.len() >= u32::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Update contains too many records",
                    ));
                }
                BookBlockHeader {
                    record_type: BookBlockType::Update,
                    unix_time: *unix_time,
                    num_records: updates.len() as u32,
                }
                .write(open_file_ref)?;
                for diff in updates {
                    diff.write(open_file_ref)?;
                }
            }
            DepthRecord::FullBook(book) => {
                let total_records = book.asks.len() + book.bids.len();
                if total_records >= u32::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Full book contains too many records (bid + ask)",
                    ));
                }
                BookBlockHeader {
                    record_type: BookBlockType::Update,
                    unix_time: book.unix_time,
                    num_records: total_records as u32,
                }
                .write(open_file_ref)?;
                for item in book.bids.iter() {
                    BookLogRecord {
                        price: item.price,
                        count: 1,
                        amount: item.amount,
                    }
                    .write(open_file_ref)?;
                }
                for item in book.asks.iter() {
                    BookLogRecord {
                        price: item.price,
                        count: 1,
                        amount: -item.amount,
                    }
                    .write(open_file_ref)?;
                }
            }
        }
        Ok(())
    }

    fn open_new_file_if_necessary(
        &mut self,
        msg: &DepthRecord,
    ) -> io::Result<&mut BzEncoder<File>> {
        let record_datetime = msg.general_time_utc();
        let time_key = Self::time_key(&record_datetime);
        if self.last_time_key != time_key {
            self.last_open_file = None; // Ensures we've closed the previous file first
            let encoder = self.open_new_archive(record_datetime, time_key)?;
            self.last_open_file = Some(encoder);
        }
        self.last_time_key = time_key;
        Ok(self
            .last_open_file
            .as_mut()
            .expect("A file must be opened at this point, one way or another."))
    }

    fn open_new_archive(
        &mut self,
        record_datetime: DateTime<Utc>,
        time_key: u32,
    ) -> io::Result<BzEncoder<File>> {
        let new_file_name = format!(
            "{}-{}-{:02}-{:013}{}",
            &self.pair_name,
            record_datetime.format("%Y-%m-%d"),
            time_key,
            record_datetime.timestamp_millis(),
            ARCHIVE_FILE_EXTENSION
        );
        let month_dir_name = record_datetime.format("%Y-%m");
        let month_dir_path = self.storage_dir.join(month_dir_name.to_string());
        std::fs::create_dir_all(&month_dir_path)?;
        let new_file_path = month_dir_path.join(new_file_name);
        log::info!("Starting new file: {:?}", &new_file_path);
        open_archive_for_writing(new_file_path.as_path())
    }
}

struct Archivist {
    pair: String,
    writer: ArchiveWriter,
    full_book_arrived: bool,
    pub full_book: OrderBookState,
    pub unsaved_history: Vec<DepthRecord>,
}

impl Archivist {
    pub fn new(storage_dir: &str, pair: &str) -> Self {
        Self {
            pair: pair.to_string(),
            writer: ArchiveWriter::new(storage_dir, pair),
            full_book_arrived: false,
            full_book: OrderBookState::new(0),
            unsaved_history: Vec::with_capacity(2 * 60 * 60),
        }
    }

    pub fn process_record(&mut self, record: &DepthRecord) -> io::Result<()> {
        let is_new_file_required = self.writer.is_new_file_required(record);
        self.update_book(record);
        if is_new_file_required {
            let old_history_len = self.unsaved_history.len();
            self.unsaved_history.clear();
            // Ensure that we always start a new file with a complete book so it could
            // be later read independently
            let alternative_record = DepthRecord::FullBook(self.full_book.clone());
            self.writer.write(&alternative_record)?;
            self.unsaved_history.push(alternative_record);
            log::info!(
                "Unsaved history cache has been reset ({} records)",
                old_history_len
            );
        } else {
            self.writer.write(record)?;
            self.unsaved_history.push(record.clone());
        }
        Ok(())
    }

    fn update_book(&mut self, record: &DepthRecord) {
        match record {
            DepthRecord::FullBook(book) => {
                self.full_book = book.clone();
                self.full_book_arrived = true;
            }
            DepthRecord::Update(time, ref changes) => {
                assert!(
                    self.full_book_arrived,
                    "Exchange API client must always send a full book before any following updates"
                );
                for diff in changes {
                    self.full_book.apply_absolute_update(*time, diff);
                }
            }
        }
    }

    pub async fn message_loop(
        mut self,
        mut api_rx: mpsc::Receiver<ArchiveCommand>,
        mut rx: mpsc::Receiver<DepthRecord>,
        mut kill_rx: watch::Receiver<Option<()>>,
    ) {
        // Write log, keep data being logged in a buffer until that file is closed.
        // When request comes, return the buffer, then spawn a task to return the archived stuff.
        let mut update_receivers = Vec::<mpsc::Sender<ArchivistAPIResponse>>::new();
        'main_loop: loop {
            tokio::select!(
                // Handling shutdown signals
                _ = kill_rx.changed() => {
                    log::info!("{} archivist received a shutdown signal", &self.pair);
                    break 'main_loop;
                }
                // Handling messages about order book changes
                depth_msg = rx.recv() => {
                    if let Some(ref record) = depth_msg {
                        if let Err(some_error) = self.process_record(record) {
                            log::error!("Unable to process order book changed: {:#?}", some_error);
                            break 'main_loop;
                        }
                        let mut i: usize = 0;
                        while i < update_receivers.len() {
                            let msg = ArchivistAPIResponse::ForwardedUpdate(record.clone());
                            if update_receivers[i].send(msg).await.is_err() {
                                update_receivers.remove(i);
                            } else {
                                i += 1;
                            }
                        }
                    } else {
                        break 'main_loop;
                    }
                }
                // Handling external API messages
                optional_msg = api_rx.recv() => {
                    if let Some(msg) = optional_msg {
                        match msg {
                            ArchiveCommand::GetUnsavedAndSubscribe(sender) => {
                                let msg = ArchivistAPIResponse::UnsavedHistory(
                                    self.unsaved_history.clone()
                                );
                                if sender.send(msg).await.is_ok() {
                                    update_receivers.push(sender);
                                }
                            }
                        }
                    } else {
                        break 'main_loop;
                    }
                }
            );
        }
    }
}

/// Spawns a bunch of tasks that
///   1. start streaming order books from the Exchange `T`
///   2. serialize and save those streams into a bunch of archive files
///   3. provide an external API allowing to fetch any periods of the saved history up
///      to now, and, if needed, keep receiving new updates as they come from the Exchange.
pub async fn run_exchange_archivist<T>(
    storage_dir: String,
    listener: TcpListener,
    kill_receiver: watch::Receiver<Option<()>>,
    archive_nav: StorageNavigator,
) where
    T: ExchangeAPI,
{
    std::fs::read_dir(&storage_dir)
        .unwrap_or_else(|_| panic!("Cannot use directory \"{}\" as storage", &storage_dir));

    let mut archivist_channels = HashMap::<String, mpsc::Sender<DepthRecord>>::new();
    let mut archivist_api_channels = HashMap::<String, mpsc::Sender<ArchiveCommand>>::new();
    let mut spawned: HashMap<String, _> = HashMap::new();
    let trading_pairs = T::trading_pairs();
    for pair in &trading_pairs {
        let (exchange_updates_tx, exchange_updates_rx) = mpsc::channel(EXCHANGE_UPDATES_BACKLOG);
        let (api_tx, api_rx) = mpsc::channel(ARCHIVIST_API_BACKLOG);
        let trading_pair_archivist = Archivist::new(&storage_dir, pair);
        spawned.insert(
            format!("archivist for {}", &pair),
            tokio::spawn(trading_pair_archivist.message_loop(
                api_rx,
                exchange_updates_rx,
                kill_receiver.clone(),
            )),
        );
        archivist_channels.insert(pair.clone(), exchange_updates_tx);
        archivist_api_channels.insert(pair.clone(), api_tx);
    }
    spawned.insert(
        "Exchange API receiver".to_string(),
        tokio::spawn(T::book_receiver(archivist_channels, kill_receiver.clone())),
    );
    spawned.insert(
        "Public API Listener".to_string(),
        tokio::spawn(public_api_listener(
            listener,
            archive_nav,
            archivist_api_channels.clone(),
            kill_receiver.clone(),
        )),
    );

    for (task_name, task) in spawned.iter_mut() {
        match task.await {
            Ok(_) => {
                log::warn!("Task {} existed", task_name);
            }
            Err(_) => {
                log::warn!("Task {} was killed", task_name);
            }
        }
    }
}

pub async fn run_bitfinex() {}

/// All possible types of requests from API clients
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum APIRequest {
    GetHistoryRanges { pair: String },
    GetRecent { pair: String, hours: u32 },
    SubscribeToRecent { pair: String, hours: u32 },
    GetRange { pair: String, start: i64, end: i64 },
}

/// NOT an async task, that runs in a parallel thread and parses a bunch of archive files
/// matching given trading pair and, if needed, given start/end times. All parsed data
/// is then dumped into a `tokio::sync::mpsc` channel which acts as a buffered link to
/// the async world.
fn spawn_storage_reading_thread(
    pair: String,
    mut navigator: StorageNavigator,
    start_time: Option<UnixTimeMs>,
    end_time: Option<UnixTimeMs>,
) -> (mpsc::Receiver<DepthRecord>, JoinHandle<()>) {
    let (sink, parser_rx) = mpsc::channel(60);
    (
        parser_rx,
        task::spawn_blocking(move || {
            for depth_record in navigator.rescan_and_read_history(&pair, start_time, end_time) {
                let not_empty = match &depth_record {
                    DepthRecord::Update(_, records) => !records.is_empty(),
                    DepthRecord::FullBook(_) => true,
                };
                if not_empty {
                    sink.blocking_send(depth_record)
                        .expect("The sink channel is supposed to be operating");
                }
            }
        }),
    )
}

/// Receives from the given archivist its unsaved order book messages and re-transmits them
/// to the given output channel one by one. While doing so it keeps receiving new
/// updates from the archivist, which eventually all be delivered to the output too.
/// If `live_stream` is true, the task will keep forwarding new updates indefinitely, never exiting.
async fn unsaved_history_buffering_task(
    pair_archivist: Sender<ArchiveCommand>,
    output: mpsc::Sender<DepthRecord>,
    live_stream: bool,
) -> io::Result<()> {
    let (resp_sender, mut resp_receiver) = mpsc::channel(120);
    pair_archivist
        .send(ArchiveCommand::GetUnsavedAndSubscribe(resp_sender))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Archivist died mid-response"))?;
    // Creating a queue of all unsaved history records in a way that allows for simultaneous
    // adding and emptying from two parallel tasks. This allows both to send the updates
    // to the output channel and to accumulate new updates at the same time.
    let shared_unsaved_history =
        if let Some(ArchivistAPIResponse::UnsavedHistory(history)) = resp_receiver.recv().await {
            Arc::new(Mutex::new(VecDeque::from(history)))
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Not received unsaved history from the archivist as it was expected",
            ));
        };
    // Here we're trying to re-transmit all unsaved history while keeping this history
    // shared so if new updates arrive while we're retransmitting, they could also be accumulated.
    let (helper_task, mut retransmit_completion_status) =
        spawn_unsaved_history_retransmission(output.clone(), shared_unsaved_history.clone());
    // Waiting until the re-transmission is over, while still collecting new updates
    loop {
        tokio::select!(
            _ = retransmit_completion_status.changed() => {
                break;
            }
            archivist_update = resp_receiver.recv() => {
                if let Some(api_response) = archivist_update {
                    match api_response {
                        ArchivistAPIResponse::ForwardedUpdate(record) => {
                            shared_unsaved_history
                                .lock()
                                .map_err(wrap_lock_error)?
                                .push_back(record);
                        }
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "Only new updates are expected at this point"));
                        }
                    }
                }
            }
        );
    }
    helper_task.await??;
    if live_stream {
        // Now we switch to forwarding the updates
        while let Some(api_response) = resp_receiver.recv().await {
            match api_response {
                ArchivistAPIResponse::ForwardedUpdate(record) => {
                    output.send(record).await.map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "Unable to forward new updates")
                    })?;
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Only new updates are expected at this point",
                    ));
                }
            }
        }
    }
    Ok(())
}

/// A helper for `unsaved_history_buffering_task` which sends all records of the given
/// history of messages to the output channel, one by one. Once finished,
/// it changes the state of the `retransmit_status` channel to indicate that the job is done.
/// This method of notification is used because `tokio::sync::watch::Receiver::recv`
/// is cancellation-safe and can be safely used with `tokio::select` employed by the parent task.
fn spawn_unsaved_history_retransmission(
    output: Sender<DepthRecord>,
    shared_history: Arc<Mutex<VecDeque<DepthRecord>>>,
) -> (
    JoinHandle<io::Result<()>>,
    tokio::sync::watch::Receiver<bool>,
) {
    let (retransmit_tx, retransmit_status) = watch::channel(false);
    let task_handle = tokio::spawn(async move {
        loop {
            let next_to_send = shared_history.lock().map_err(wrap_lock_error)?.pop_front();
            if let Some(next_to_send) = next_to_send {
                output.send(next_to_send).await.map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "Cannot deliver unsaved history to the client handler task",
                    )
                })?;
            } else {
                break;
            }
        }
        retransmit_tx
            .send(true)
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))?;
        Ok(())
    });
    (task_handle, retransmit_status)
}

/// Spawned when API receives a new client connection on the given socket.
/// This function figures out what the client wants and runs a respective sub-function
/// to handle the needs.
pub async fn api_client_handler(
    mut socket: TcpStream,
    storage_nav: StorageNavigator,
    archivist_api_channels: HashMap<String, mpsc::Sender<ArchiveCommand>>,
    mut _shutdown: watch::Receiver<Option<()>>,
) -> io::Result<()> {
    let pair_archivist_channel = |request: &APIRequest, pair: &String| {
        archivist_api_channels.get(pair).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Request {:?} asks for an unsupported trading pair {}",
                    request.clone(),
                    pair.clone()
                ),
            )
        })
    };

    let mut api_frame_reader = FrameReader::new(Some(API_MAX_FRAME_SIZE));
    while let Some(ref request) = api_frame_reader.next::<APIRequest>(&mut socket).await? {
        match request {
            APIRequest::SubscribeToRecent { pair, hours } => {
                let pair_archivist = pair_archivist_channel(request, pair)?;
                stream_recent_archives(
                    pair,
                    pair_archivist,
                    storage_nav.clone(),
                    hours,
                    true,
                    &mut socket,
                )
                .await?;
            }
            APIRequest::GetRecent { pair, hours } => {
                let pair_archivist = pair_archivist_channel(request, pair)?;
                stream_recent_archives(
                    pair,
                    pair_archivist,
                    storage_nav.clone(),
                    hours,
                    false,
                    &mut socket,
                )
                .await?;
            }
            APIRequest::GetHistoryRanges { .. } => {
                todo!()
            }
            APIRequest::GetRange { .. } => {
                todo!()
            }
        }
    }
    Ok(())
}

async fn stream_recent_archives(
    pair: &str,
    pair_archivist: Sender<ArchiveCommand>,
    storage_nav: StorageNavigator,
    hours: &u32,
    live_stream: bool,
    socket: &mut TcpStream,
) -> io::Result<()> {
    // First we need to make sure we've received unsaved history
    let (buffering_tx, mut buffering_rx) = mpsc::channel(60);
    let buffering_task = tokio::spawn(unsaved_history_buffering_task(
        pair_archivist,
        buffering_tx,
        live_stream,
    ));
    // We need to parse old archive files and re-transmit them to the client. To ensure
    // that parsing and transmitting do not interfere, we spawn a new thread specifically
    // parsing, which will send us all extracted data back via a special channel.
    let start_time = Some(unix_now_millis() - (*hours as i64) * 60 * 60 * 1000);
    let (mut parser_rx, _storage_reader_task) =
        spawn_storage_reading_thread(String::from(pair), storage_nav.clone(), start_time, None);
    let mut last_parsed_record_time = 0i64;
    // Here we try to both retransmit all parsed records back to the client, and
    // while doing so, accumulate all new updates received from the exchange meanwhile.
    while let Some(ref depth_record) = parser_rx.recv().await {
        // Because the "unsaved" history is actually being saved, just not guaranteed
        // to be flushed on disk yet, we might end up reading some of the
        // successfully dumped records. To make sure we don't send them again later,
        // we always keep the last parsed record's time.
        last_parsed_record_time = match depth_record {
            DepthRecord::Update(update_time, _) => *update_time,
            DepthRecord::FullBook(book) => book.unix_time,
        };
        send_frame::<DepthRecord>(socket, depth_record).await?;
    }
    // Once we've done parsing the files, we should start retransmitting the unsaved history until its exhaustion.
    // While doing so, the buffering task spawned earlier keeps accumulating new updates,
    // and when live_stream is true, the loop below never ends.
    while let Some(ref depth_record) = buffering_rx.recv().await {
        let record_time = match depth_record {
            DepthRecord::Update(update_time, _) => *update_time,
            DepthRecord::FullBook(book) => book.unix_time,
        };
        if record_time > last_parsed_record_time {
            send_frame::<DepthRecord>(socket, depth_record).await?;
        }
    }
    if !live_stream {
        // If needed send an empty update to indicate the end of them
        send_frame(socket, &DepthRecord::Update(0, Vec::with_capacity(0))).await?;
    }
    buffering_task.await??;
    Ok(())
}

pub async fn public_api_listener(
    listener: TcpListener,
    storage_nav: StorageNavigator,
    archivist_api_channels: HashMap<String, mpsc::Sender<ArchiveCommand>>,
    mut shutdown: watch::Receiver<Option<()>>,
) {
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                break;
            }
            accept_result = listener.accept() => {
                if let Ok((socket, _)) = accept_result {
                    tokio::spawn(api_client_handler(
                        socket,
                        storage_nav.clone(),
                        archivist_api_channels.clone(),
                        shutdown.clone()
                    ));
                }
            }
        }
    }
    log::warn!("Public API listener has exited");
}

#[tokio::main(flavor = "current_thread")]
pub async fn run_archivist(
    exchange: ExchangeName,
    storage: String,
    listen: String,
    archive_nav: StorageNavigator,
) {
    let listener = TcpListener::bind(&listen)
        .await
        .expect("Unable to bind to the given address");
    let (kill_sender, kill_receiver) = watch::channel(None);

    let exchange = match exchange {
        ExchangeName::Binance => tokio::spawn(run_exchange_archivist::<Binance>(
            storage,
            listener,
            kill_receiver,
            archive_nav,
        )),
        ExchangeName::Bittrex => {
            todo!()
        }
    };

    match signal::ctrl_c().await {
        Ok(()) => {
            kill_sender
                .send(Some(()))
                .expect("Unable to send the shutdown signal");
        }
        Err(err) => {
            panic!("Unable to listen for the shutdown signal: {}", err);
        }
    }

    let _ = tokio::join!(exchange);
}
