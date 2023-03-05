use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bzip2::bufread::BzDecoder;
use bzip2::write::BzEncoder;

use crate::errors::to_invalid_data_error;

use super::{Amount, BookLogRecord, OrderBookState, Price, UnixTimeMs};

#[derive(Debug, PartialEq)]
pub enum BookBlockType {
    /// Snapshot block contains complete state of the order book, not just individual changes to it
    /// It's the foundation which all following Update blocks will transform.
    Snapshot,
    /// Update block contains a bunch of updates (`BookRecord` struct) telling how the order book
    /// was modified.
    Update,
}

#[derive(Copy, Clone, Debug)]
pub enum ArchiveFormat {
    PyStruct,  // Obsolete format written by the old Python app
    Archivist, // New format, written by this code
}

impl ArchiveFormat {
    /// Identifies format of an archive, if possible, along with the time of its first record
    /// (so we could later avoid re-opening the file again later for the purpose of checking that).
    /// Returns an `std::io::Error` only if it cannot open the file at all.
    pub fn detect(path: &Path) -> io::Result<Option<(Self, UnixTimeMs)>> {
        let file = File::open(path)?;
        let mut reader = bzip2::bufread::BzDecoder::new(io::BufReader::new(file));
        Ok(match BookBlockHeader::read_archivist(&mut reader) {
            Ok(ref header) => Some((Self::Archivist, header.unix_time)),
            Err(_) => {
                drop(reader);
                let file = std::fs::File::open(path)?;
                let mut reader = bzip2::bufread::BzDecoder::new(io::BufReader::new(file));
                match BookBlockHeader::read_pystruct(&mut reader) {
                    Ok(ref header) => Some((Self::PyStruct, header.unix_time)),
                    Err(_) => None,
                }
            }
        })
    }
}

/// A header that comes before a bunch of `BookLogRecord` items.
/// It tells when the events happened, how many records describe
/// the changes and whether those records describe the whole
/// book, or just pieces of it
#[derive(Debug, PartialEq)]
pub struct BookBlockHeader {
    pub record_type: BookBlockType,
    pub unix_time: UnixTimeMs,
    pub num_records: u32,
}

impl BookBlockHeader {
    /// Reads what's been encoded with Python's `struct.pack('BdI',...)`
    pub fn read_pystruct<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        // Record type is a single byte but it is aligned to occupy 8 bytes
        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf8)?;
        let record_type = match buf8[0] {
            1 => BookBlockType::Update,
            0 => BookBlockType::Snapshot,
            _ => {
                return io::Result::Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid header code: {}", buf8[0]),
                ));
            }
        };
        reader.read_exact(&mut buf8)?;
        let unix_time = (1000.0 * f64::from_le_bytes(buf8)) as UnixTimeMs;
        let mut num_records_buf = [0u8; 4];
        reader.read_exact(&mut num_records_buf)?;
        let num_records = u32::from_le_bytes(num_records_buf);
        Ok(Self {
            record_type,
            unix_time,
            num_records,
        })
    }

    /// Reads what's been encoded with the code written in Rust
    pub fn read_archivist<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let version_marker = reader.read_u8()?;
        if version_marker != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Wrong version marker",
            ));
        }
        let mut result = Self {
            unix_time: 0,
            num_records: 0,
            record_type: match reader.read_u8() {
                Ok(0) => Ok(BookBlockType::Snapshot),
                Ok(1) => Ok(BookBlockType::Update),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid header code",
                )),
            }?,
        };
        result.unix_time = reader.read_i64::<LittleEndian>()?;
        result.num_records = reader.read_u32::<LittleEndian>()?;
        Ok(result)
    }

    pub fn read<R: io::Read>(reader: &mut R, format: ArchiveFormat) -> io::Result<BookBlockHeader> {
        match format {
            ArchiveFormat::PyStruct => Self::read_pystruct(reader),
            ArchiveFormat::Archivist => Self::read_archivist(reader),
        }
    }

    pub fn write<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u8(1)?; // version marker
        writer.write_u8(match self.record_type {
            BookBlockType::Snapshot => 0,
            BookBlockType::Update => 1,
        })?;
        writer.write_i64::<LittleEndian>(self.unix_time)?;
        writer.write_u32::<LittleEndian>(self.num_records)?;
        Ok(())
    }
}

impl BookLogRecord {
    /// The old Python version of this software was using standard Python `struct`
    /// module in creation of binary history files. This function can read those records.
    /// Modern functions read/write can store Decimal values directly and waste less space
    /// due to standard C structure's alignment forced by the Python's `struct` module.
    pub fn read_pystruct<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let mut result = BookLogRecord {
            price: Price::ZERO,
            count: 0,
            amount: Amount::ZERO,
        };
        let mut buf = [0u8; 8];
        let mut count_buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        result.price = Price::try_from(f64::from_le_bytes(buf)).map_err(to_invalid_data_error)?;
        reader.read_exact(&mut buf)?;
        count_buf.copy_from_slice(&buf[..4]);
        result.count = u32::from_le_bytes(count_buf);
        reader.read_exact(&mut buf)?;
        result.amount = Amount::try_from(f64::from_le_bytes(buf)).map_err(to_invalid_data_error)?;
        Ok(result)
    }

    /// Just like read_pystruct, except it completely ignores the content
    pub fn skip_pystruct<R: io::Read>(reader: &mut R, num: u32) -> io::Result<()> {
        let mut buf = [0u8; 3 * 8];
        for _ in 0..num {
            reader.read_exact(&mut buf)?;
        }
        Ok(())
    }

    pub fn read_archivist<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let mut result = BookLogRecord {
            price: Price::ZERO,
            count: 0,
            amount: Amount::ZERO,
        };
        let mut decimal_buffer = [0u8; 16];
        reader.read_exact(&mut decimal_buffer)?;
        result.price = Price::deserialize(decimal_buffer);
        result.count = reader.read_u32::<LittleEndian>()?;
        reader.read_exact(&mut decimal_buffer)?;
        result.amount = Amount::deserialize(decimal_buffer);
        Ok(result)
    }

    pub fn skip_archivist<R: io::Read>(reader: &mut R, num: u32) -> io::Result<()> {
        let mut buf = [0u8; 2 * 16 + 4];
        for _ in 0..num {
            reader.read_exact(&mut buf)?;
        }
        Ok(())
    }

    pub fn read<R: io::Read>(reader: &mut R, format: ArchiveFormat) -> io::Result<Self> {
        match format {
            ArchiveFormat::PyStruct => Self::read_pystruct(reader),
            ArchiveFormat::Archivist => Self::read_archivist(reader),
        }
    }

    pub fn skip<R: io::Read>(reader: &mut R, format: ArchiveFormat, num: u32) -> io::Result<()> {
        match format {
            ArchiveFormat::PyStruct => Self::skip_pystruct(reader, num),
            ArchiveFormat::Archivist => Self::skip_archivist(reader, num),
        }
    }

    pub fn write<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(self.price.serialize().as_ref())?;
        writer.write_u32::<LittleEndian>(self.count)?;
        writer.write_all(self.amount.serialize().as_ref())?;
        Ok(())
    }
}

pub type ArchiveFileDecoder = BzDecoder<io::BufReader<File>>;
pub type ArchiveFileEncoder = BzEncoder<File>;

pub fn open_archive_for_reading(path: &Path) -> io::Result<ArchiveFileDecoder> {
    let file = File::open(path)?;
    Ok(BzDecoder::new(io::BufReader::new(file)))
}

pub fn open_archive_for_writing(path: &Path) -> io::Result<ArchiveFileEncoder> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)?;
    Ok(BzEncoder::new(file, bzip2::Compression::default()))
}

/// Reads an order book's archive record by record and recreates a complete order book's state
/// at each step.
pub struct BookLogPlayer<R> {
    source: R,
    book_state: OrderBookState,
    format: ArchiveFormat,
}

impl<R: io::Read> BookLogPlayer<R> {
    pub fn new(reader: R, format: ArchiveFormat) -> BookLogPlayer<R> {
        BookLogPlayer {
            format,
            source: reader,
            book_state: OrderBookState::new(0),
        }
    }

    pub fn next_state(&mut self) -> Option<&OrderBookState> {
        let header = BookBlockHeader::read(&mut self.source, self.format);
        match header {
            Ok(header) => {
                if header.record_type == BookBlockType::Snapshot {
                    self.book_state.reset();
                }
                for _ in 0..header.num_records {
                    match BookLogRecord::read(&mut self.source, self.format) {
                        Ok(record) => {
                            self.book_state
                                .apply_absolute_update(header.unix_time, &record);
                        }
                        Err(_) => {
                            return None;
                        }
                    }
                }
                Some(&self.book_state)
            }
            Err(_) => None,
        }
    }
}

/// Returns times of the first and the last readable records from the given archives.
/// If the file is only partially readable, recovers what it can. Returns error only
/// if it can't read even a single record.
fn archive_time_range(path: &Path, format: ArchiveFormat) -> io::Result<(UnixTimeMs, UnixTimeMs)> {
    let mut reader = open_archive_for_reading(path)?;
    let header = BookBlockHeader::read(&mut reader, format)?;
    let start_time = header.unix_time;
    let mut end_time = start_time;
    if BookLogRecord::skip(&mut reader, format, header.num_records).is_err() {
        return Ok((start_time, end_time));
    }
    while let Ok(header) = BookBlockHeader::read(&mut reader, format) {
        end_time = header.unix_time;
        if BookLogRecord::skip(&mut reader, format, header.num_records).is_err() {
            return Ok((start_time, end_time));
        }
    }
    Ok((start_time, end_time))
}

#[derive(Clone)]
pub struct ArchiveFileInfo {
    pub path: PathBuf,
    pub format: ArchiveFormat,
    pub start_time: UnixTimeMs,
    end_time: Option<UnixTimeMs>,
}

impl ArchiveFileInfo {
    pub fn new(path: &Path, format: ArchiveFormat, start_time: UnixTimeMs) -> Self {
        Self {
            format,
            start_time,
            path: path.to_owned(),
            end_time: None,
        }
    }

    pub fn inspect_file(path: &Path) -> io::Result<Option<Self>> {
        Ok(ArchiveFormat::detect(path)?
            .map(|(format, start_time)| Self::new(path, format, start_time)))
    }

    pub fn get_end_time_and_cache(&mut self) -> io::Result<UnixTimeMs> {
        match self.end_time {
            None => {
                let (_start_time, end_time) = archive_time_range(&self.path, self.format)?;
                self.end_time = Some(end_time);
                Ok(end_time)
            }
            Some(end_time) => Ok(end_time),
        }
    }

    pub fn get_end_time(&self) -> io::Result<UnixTimeMs> {
        match self.end_time {
            None => {
                let (_start_time, end_time) = archive_time_range(&self.path, self.format)?;
                Ok(end_time)
            }
            Some(end_time) => Ok(end_time),
        }
    }
}
