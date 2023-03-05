use std::collections::HashMap;
use std::path::{Path, PathBuf};

use walkdir::WalkDir;

use crate::exchange::DepthRecord;
use crate::order_book::storage::{
    open_archive_for_reading, ArchiveFileDecoder, BookBlockHeader, BookBlockType,
};
use crate::order_book::{BookLogRecord, OrderBookState, UnixTimeMs, ESTIMATED_MAX_BOOK_SIZE};

use super::storage::ArchiveFileInfo;

pub(crate) const ARCHIVE_FILE_EXTENSION: &str = ".bz2";

/// Checks if a given path is a valid archive and finds out all about it
fn describe_if_archive<'a>(
    archive_path: &Path,
    trading_pairs: &'a [String],
) -> Option<(&'a String, ArchiveFileInfo)> {
    let name = archive_path.file_name()?.to_str()?;
    if name.ends_with(ARCHIVE_FILE_EXTENSION) {
        if let Some(pair) = trading_pairs
            .iter()
            .find(|pair| name.rfind(*pair).is_some() || name.rfind(&pair.to_lowercase()).is_some())
        {
            match ArchiveFileInfo::inspect_file(archive_path) {
                Err(e) => {
                    log::warn!(
                        "Unable to inspect file {}: {}",
                        archive_path.to_string_lossy(),
                        e
                    );
                    None
                }
                Ok(None) => None,
                Ok(Some(archive_info)) => Some((pair, archive_info)),
            }
        } else {
            None
        }
    } else {
        None
    }
}

#[derive(Clone)]
pub struct NavigatorDir {
    path: PathBuf,
    is_static: bool,
    is_scanned: bool,
}

/// Able to scan multiple directories, identifying order book archives in them, detecting
/// their formats, starting dates and the trading pair they belong to. Together with
/// the `MultiArchiveReader` this allows for reading of any arbitrary portions of trading
/// history.
#[derive(Clone)]
pub struct StorageNavigator {
    search_dirs: Vec<NavigatorDir>,
    trading_pairs: Vec<String>,
    inventory: HashMap<String, Vec<ArchiveFileInfo>>,
}

impl StorageNavigator {
    pub fn new(trading_pairs: &[String]) -> Self {
        Self {
            search_dirs: Vec::new(),
            trading_pairs: Vec::from(trading_pairs),
            inventory: HashMap::new(),
        }
    }

    pub fn add_dir(&mut self, path: &str, is_static: bool) {
        let path_obj = std::path::PathBuf::from(path);
        path_obj
            .read_dir()
            .unwrap_or_else(|_| panic!("Cannot use directory \"{}\" as storage", &path));
        self.search_dirs.push(NavigatorDir {
            is_static,
            is_scanned: false,
            path: path_obj,
        })
    }

    pub fn rescan(&mut self) -> &HashMap<String, Vec<ArchiveFileInfo>> {
        for dir in self.search_dirs.iter_mut() {
            if dir.is_scanned && dir.is_static {
                continue;
            }
            let root = WalkDir::new(&dir.path);
            let archives_in_directory = root
                .into_iter()
                .filter_map(|entry| entry.ok()) // Ignoring anything we have no access to
                .filter_map(|entry| {
                    describe_if_archive(entry.path(), self.trading_pairs.as_slice())
                });
            for (pair, archive_info) in archives_in_directory {
                match self.inventory.get_mut(pair) {
                    None => {
                        let new_vec = vec![archive_info];
                        self.inventory.insert(pair.to_string(), new_vec);
                    }
                    Some(records) => {
                        match records
                            .binary_search_by_key(&archive_info.start_time, |item| item.start_time)
                        {
                            Ok(index) => {
                                records[index] = archive_info;
                            }
                            Err(insertion_point) => {
                                records.insert(insertion_point, archive_info);
                            }
                        }
                    }
                };
            }
            dir.is_scanned = true;
        }
        &self.inventory
    }

    pub fn rescan_and_read_history(
        &mut self,
        pair: &str,
        start_time: Option<UnixTimeMs>,
        end_time: Option<UnixTimeMs>,
    ) -> MultiArchiveReader {
        self.rescan();
        let pair_archives = match self.inventory.get(pair) {
            Some(files) => files.as_slice(),
            None => &[],
        };
        MultiArchiveReader::new(pair_archives, start_time, end_time)
    }
}

pub struct MultiArchiveReader {
    archives: Vec<ArchiveFileInfo>,
    start_time: Option<UnixTimeMs>,
    end_time: Option<UnixTimeMs>,
    current_archive_index: usize,
    open_archive: Option<ArchiveFileDecoder>,
    record_buffer: Vec<BookLogRecord>,
}

impl MultiArchiveReader {
    pub fn new(
        files_to_read: &[ArchiveFileInfo],
        start_time: Option<UnixTimeMs>,
        end_time: Option<UnixTimeMs>,
    ) -> Self {
        let archives = Self::select_only_relevant_files(files_to_read, start_time, end_time);
        Self {
            start_time,
            end_time,
            archives,
            current_archive_index: 0,
            open_archive: None,
            record_buffer: Vec::with_capacity(ESTIMATED_MAX_BOOK_SIZE),
        }
    }

    /// Copies only files that fit into the given time range
    fn select_only_relevant_files(
        files_to_read: &[ArchiveFileInfo],
        start_time: Option<UnixTimeMs>,
        end_time: Option<UnixTimeMs>,
    ) -> Vec<ArchiveFileInfo> {
        if let (Some(start_time), Some(end_time)) = (start_time, end_time) {
            assert!(start_time <= end_time);
        }
        let mut start_index: usize = 0;
        let mut end_index: usize = files_to_read.len();
        // If start_time boundary is give, then we'll start from the last file which
        // has start_time < given start_time
        if let Some(start_time_value) = start_time {
            if let Some((index, _item)) = files_to_read
                .iter()
                .enumerate()
                .find(|(_, item)| start_time_value > item.start_time)
            {
                if index > 0 {
                    start_index = index - 1;
                }
            }
        }
        // When end_time boundary is given, we finish at the first file which
        // has end_time > given end_time (exclusive)
        if let Some(end_time_value) = end_time {
            if let Some((index, _item)) = files_to_read
                .iter()
                .enumerate()
                .find(|(_, item)| end_time_value < item.start_time)
            {
                end_index = index;
            }
        }
        let mut archives: Vec<ArchiveFileInfo> = files_to_read[start_index..end_index].to_vec();
        archives.sort_by_key(|item| item.start_time);
        archives
    }

    fn switch_to_next_file(&mut self) {
        self.open_archive = None;
        if self.current_archive_index < self.archives.len() {
            self.current_archive_index += 1;
        }
    }

    fn switch_to_end(&mut self) {
        self.open_archive = None;
        self.current_archive_index = self.archives.len();
    }

    fn current_archive_info(&self) -> Option<&ArchiveFileInfo> {
        self.archives.get(self.current_archive_index)
    }

    /// Ensures that there's an open archive to read the next record from,
    /// in which case returns true. Otherwise returns false.
    fn open_current_archive(&mut self) -> bool {
        if self.open_archive.is_none() {
            loop {
                let Some(archive_info) = self.current_archive_info() else {
                    return false;
                };
                match open_archive_for_reading(archive_info.path.as_path()) {
                    Ok(decoder) => {
                        self.open_archive = Some(decoder);
                        return true;
                    }
                    Err(_) => {
                        self.switch_to_next_file();
                    }
                }
            }
        }
        true
    }
}

impl Iterator for MultiArchiveReader {
    type Item = DepthRecord;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !self.open_current_archive() {
                return None;
            }
            let archive_info = &self.archives[self.current_archive_index];
            let open_archive = self
                .open_archive
                .as_mut()
                .expect("Current archive must be present in cache since it was sucessfuly opened");
            match BookBlockHeader::read(open_archive, archive_info.format) {
                Ok(header) => {
                    if let Some(start_time) = self.start_time {
                        if header.unix_time < start_time {
                            continue;
                        }
                    }
                    if let Some(end_time) = self.end_time {
                        if header.unix_time > end_time {
                            self.switch_to_end();
                            return None;
                        }
                    }
                    self.record_buffer.clear();
                    for _ in 0..header.num_records {
                        match BookLogRecord::read(open_archive, archive_info.format) {
                            Ok(record) => {
                                self.record_buffer.push(record);
                            }
                            Err(_) => {
                                // The file is broken, cannot read the remaining records
                                self.switch_to_next_file();
                                break;
                            }
                        }
                    }
                    if self.record_buffer.len() == header.num_records as usize {
                        return Some(match header.record_type {
                            BookBlockType::Snapshot => {
                                let mut full_book = OrderBookState::new(header.unix_time);
                                for record in &self.record_buffer {
                                    full_book.apply_absolute_update(header.unix_time, record);
                                }
                                DepthRecord::FullBook(full_book)
                            }
                            BookBlockType::Update => {
                                DepthRecord::Update(header.unix_time, self.record_buffer.clone())
                            }
                        });
                    }
                }
                Err(_) => {
                    // Switch to the next file if this cannot be read anymore
                    self.switch_to_next_file();
                }
            }
        }
    }
}
