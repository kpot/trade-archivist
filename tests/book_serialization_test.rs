use rust_decimal_macros::dec;
use std::io;
use std::io::Seek;
use trade_archivist::order_book::storage::{ArchiveFormat, BookBlockHeader, BookBlockType};
use trade_archivist::order_book::BookLogRecord;

/// Checks whether we can read a history record's header written by the old Python app
#[test]
fn read_book_py_history_header() {
    let data: [u8; 20] = [
        0, 0, 0, 0, 0, 0, 0, 0, 221, 71, 178, 249, 123, 113, 216, 65, 57, 48, 0, 0,
    ];
    let mut cursor = io::Cursor::new(&data);
    let header = BookBlockHeader::read_pystruct(&mut cursor).unwrap();
    assert_eq!(
        header,
        BookBlockHeader {
            record_type: BookBlockType::Snapshot,
            num_records: 12345,
            unix_time: 1640361958785
        }
    );
}

/// Checks whether we can write/read the modern history's header
#[test]
fn history_header_serialization() {
    let header = BookBlockHeader {
        record_type: BookBlockType::Update,
        unix_time: 1640361958785,
        num_records: 123456,
    };
    let mut cursor = io::Cursor::new(Vec::new());
    header.write(&mut cursor).unwrap();
    cursor.rewind().unwrap();
    let new_header = BookBlockHeader::read(&mut cursor, ArchiveFormat::Archivist).unwrap();
    assert_eq!(header, new_header);
}

/// Checks whether we can read a history record written with Python back in the days
#[test]
fn read_book_py_history_records() {
    let data2: [u8; 24] = [
        0, 0, 0, 0, 128, 28, 200, 64, 210, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 28, 200, 64,
    ];
    let mut cursor = io::Cursor::new(&data2);
    let header = BookLogRecord::read_pystruct(&mut cursor).unwrap();
    assert_eq!(
        header,
        BookLogRecord {
            price: dec!(12345.0),
            count: 1234,
            amount: dec!(12345.0),
        }
    );
}

/// Checks whether we can write/read a modern history record
#[test]
fn history_record_serialization() {
    let record = BookLogRecord {
        price: dec!(12345678.90),
        count: 1,
        amount: dec!(0.1234567),
    };
    let mut cursor = io::Cursor::new(Vec::new());
    record.write(&mut cursor).unwrap();
    cursor.rewind().unwrap();
    let new_header = BookLogRecord::read(&mut cursor, ArchiveFormat::Archivist).unwrap();
    assert_eq!(record, new_header);
}
