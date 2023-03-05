use std::cmp::Ordering;
use std::time::SystemTime;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub mod navigation;
pub mod storage;

pub const ESTIMATED_MAX_BOOK_SIZE: usize = 1200;

#[derive(Debug, PartialEq)]
pub enum BookBlockType {
    /// Snapshot block contains complete state of the order book, not just individual changes to it
    /// It's the foundation which all following Update blocks will transform.
    Snapshot,
    /// Update block contains a bunch of updates (`BookRecord` struct) telling how the order book
    /// was modified.
    Update,
}

pub type Price = Decimal;
pub type Amount = Decimal;
pub type UnixTimeMs = i64;

/// Returns current "UNIX time" in milliseconds (since 00:00:00 1 Jan 1970 UTC)
pub fn unix_now_millis() -> UnixTimeMs {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("System time is expected to be valid")
        .as_millis() as UnixTimeMs
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderBookItem {
    pub price: Price,
    pub amount: Amount,
}

/// Represents a state of a single price in the ask/bid side of the order book.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct BookLogRecord {
    pub price: Price,
    /// Number of orders at that price level.
    /// WARNING: Relevant only for Bitfinex. Other exchanges
    ///          will fill it with 0 (remove the position) or 1
    pub count: u32,
    /// Total amount available at that price level.
    /// 'bid' if amount >= 0 else 'ask'
    pub amount: Amount,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderBookState {
    pub unix_time: UnixTimeMs,
    /// Those who want to buy, ordered by price (descending order,
    /// those who pay more are higher).
    pub bids: Vec<OrderBookItem>,
    /// Those who want to sell, ordered by price (ascending order,
    /// those who sell cheaper are higher)
    pub asks: Vec<OrderBookItem>,
}

impl OrderBookState {
    pub fn new(time: UnixTimeMs) -> OrderBookState {
        OrderBookState {
            unix_time: time,
            bids: vec![],
            asks: vec![],
        }
    }

    pub fn fill(
        time: UnixTimeMs,
        bids: &[(Price, Amount)],
        asks: &[(Price, Amount)],
    ) -> OrderBookState {
        let init_item = |(price, amount): &(Price, Amount)| -> OrderBookItem {
            OrderBookItem {
                price: *price,
                amount: *amount,
            }
        };
        let bid_orders = bids.iter().map(init_item).collect();
        let ask_orders = asks.iter().map(init_item).collect();
        OrderBookState {
            unix_time: time,
            bids: bid_orders,
            asks: ask_orders,
        }
    }

    pub fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    fn detect_changes_in_orders(
        old_orders: &[OrderBookItem],
        new_orders: &[OrderBookItem],
        is_sorted_by_price_asc: bool,
        changes: &mut Vec<BookOp>,
    ) {
        // uses the fact that bids (buy orders) are ordered by descend of their prices
        let mut old_index: Option<usize> = if old_orders.is_empty() { None } else { Some(0) };
        let mut new_index: Option<usize> = if new_orders.is_empty() { None } else { Some(0) };
        let inc_index = |index: Option<usize>, max_size: usize| -> Option<usize> {
            match index {
                Some(i) if i + 1 < max_size => Some(i + 1),
                _ => None,
            }
        };
        loop {
            match (old_index, new_index) {
                (Some(o), None) => {
                    // new orders are exhausted, but some of the old remain, we must subtract them
                    for order in old_orders[o..].iter() {
                        changes.push(BookOp {
                            price: order.price,
                            delta: -order.amount,
                        });
                    }
                    break;
                }
                (None, Some(n)) => {
                    // Old orders are exhausted, but some of the new bids remain, we must add them
                    for order in &new_orders[n..] {
                        changes.push(BookOp {
                            price: order.price,
                            delta: order.amount,
                        });
                    }
                    break;
                }
                (Some(o), Some(n)) => {
                    // We have both old and new orders, likely misaligned, so we advance through
                    // both lists, attempting to "sync" them
                    let cursor_old = &old_orders[o];
                    let cursor_new = &new_orders[n];
                    match cursor_old.price.cmp(&cursor_new.price) {
                        Ordering::Equal => {
                            let delta = cursor_new.amount - cursor_old.amount;
                            if !delta.is_zero() {
                                changes.push(BookOp {
                                    price: cursor_new.price,
                                    delta,
                                });
                            }
                            new_index = inc_index(new_index, new_orders.len());
                            old_index = inc_index(old_index, old_orders.len());
                        }
                        Ordering::Less => {
                            if is_sorted_by_price_asc {
                                changes.push(BookOp {
                                    price: cursor_old.price,
                                    delta: -cursor_old.amount,
                                });
                                old_index = inc_index(old_index, old_orders.len());
                            } else {
                                changes.push(BookOp {
                                    price: cursor_new.price,
                                    delta: cursor_new.amount,
                                });
                                new_index = inc_index(new_index, new_orders.len());
                            }
                        }
                        Ordering::Greater => {
                            if is_sorted_by_price_asc {
                                changes.push(BookOp {
                                    price: cursor_new.price,
                                    delta: cursor_new.amount,
                                });
                                new_index = inc_index(new_index, new_orders.len());
                            } else {
                                changes.push(BookOp {
                                    price: cursor_old.price,
                                    delta: -cursor_old.amount,
                                });
                                old_index = inc_index(old_index, old_orders.len());
                            }
                        }
                    }
                }
                (None, None) => break,
            }
        }
    }

    pub fn collect_changes_since(&self, other: &Self, changes: &mut BookChanges) {
        changes.bids.clear();
        changes.asks.clear();
        changes.unix_time = self.unix_time;
        Self::detect_changes_in_orders(&other.bids, &self.bids, false, &mut changes.bids);
        Self::detect_changes_in_orders(&other.asks, &self.asks, true, &mut changes.asks);
    }

    pub fn apply_absolute_update(&mut self, unix_time: UnixTimeMs, record: &BookLogRecord) {
        self.unix_time = unix_time;
        let (side, asc_price_order) = if record.amount.is_sign_positive() {
            (&mut self.bids, false)
        } else {
            (&mut self.asks, true)
        };
        let price_key = if asc_price_order {
            record.price
        } else {
            -record.price
        };
        match side.binary_search_by_key(&price_key, |x| {
            if asc_price_order {
                x.price
            } else {
                -x.price
            }
        }) {
            Ok(matching_price_index) => {
                if record.count == 0 {
                    side.remove(matching_price_index);
                }
            }
            Err(index_for_price_insertion) => {
                if record.count != 0 {
                    side.insert(
                        index_for_price_insertion,
                        OrderBookItem {
                            price: record.price,
                            amount: record.amount.abs(),
                        },
                    );
                }
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct BookOp {
    pub price: Price,
    pub delta: Amount,
}

#[derive(Debug, PartialEq)]
pub struct BookChanges {
    pub unix_time: UnixTimeMs,
    pub bids: Vec<BookOp>,
    pub asks: Vec<BookOp>,
}
