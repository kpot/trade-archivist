# Trade-archivist

A receiving proxy for trading events from different cryptocurrency exchanges,
capable of real-time streaming, storing and retrieving history of those events 
through a unified network API.

Written as an asynchronous ([tokio](https://tokio.rs/)) client-server 
([Rust](https://www.rust-lang.org/)) application for my personal experiments 
in maching learning (time series analysis and reinforcement learning).

1. Collects realtime logs of all trading events from a set of cryptocurrency
   exchanges ([Binance](https://www.binance.com/en) only for now) for a given 
   set of trading pairs, and stores them in an series disk achives.
   The events come through (typically WebSocket) connections to the respective
   exchange's API.
2. Acts as a sever that provides a binary API for re-transmitting the trading 
   events as they occur, and also for requesting arbitrary slices 
   of the trading history (up to now). This can used for backtesting,
   training of various ML algorithms or streaming of the same realtime logs
   from the exchanges (in which case the server acts as a proxy with
   a unified API to all exchanges).
   This work is still in progress.


## Usage

``` shell
mkdir storage-dir
cargo run archive binance --storage ./storage-dir/ --listen 127.0.0.1:9000
```
The data will be stored within the `storage-dir` directory in a structure
that looks like

``` 
storage-dir
├── ETHBTC
│   └── 2023-02
│       ├── ETHBTC-2023-02-27-08-1677485354492.bz2
│       ├── ETHBTC-2023-02-27-09-1677488400546.bz2
│       ├── ETHBTC-2023-02-27-14-1677506706598.bz2
│       └── ...
├── ETHEUR
│   └── 2023-02
│       ├── ETHEUR-2023-02-27-08-1677485354623.bz2
│       ├── ETHEUR-2023-02-27-09-1677488400548.bz2
│       ├── ETHEUR-2023-02-27-14-1677506706583.bz2
│       └── ...
├── ETHUSDT
│   └── 2023-02
│       ├── ETHUSDT-2023-02-27-08-1677485354747.bz2
│       ├── ETHUSDT-2023-02-27-09-1677488400548.bz2
│       ├── ETHUSDT-2023-02-27-14-1677506706621.bz2
│       └── ...
...
```
