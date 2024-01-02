use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use memmap2::MmapMut;
use prost::{DecodeError, EncodeError, Message};
use std::{
    borrow::BorrowMut,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    num::ParseIntError,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    vec,
};
use thiserror::Error;

use crate::log::log::Config;
use crate::log::log::{
    INDEX_ENTRY_LENGTH, INDEX_RECORD_OFFSET_LENGTH, POSITION_IN_STORE_FILE_LENGTH,
};
use crate::proto::{self, record::Record};
use std::io;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct IndexEntry {
    pub record_offset: u32,
    pub position: u64,
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("Index is full")]
    IndexFullError,
    #[error("Index entry {0} not found")]
    IndexEntryNotFound(u32),

    #[error(transparent)]
    IOError(#[from] std::io::Error),
}

#[derive(Debug)]
pub struct Index {
    pub file: File,
    pub size: u64,
    mmap: MmapMut,
    pub path: PathBuf,
}

impl Index {
    pub fn new(file_path: PathBuf, config: Arc<Config>) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&file_path)
            .expect("Unable to create or open file");

        let index_size = file.metadata().unwrap().len();

        file.set_len(config.get_max_index_bytes())
            .expect("Unable to truncate file");

        let mmap = unsafe { MmapMut::map_mut(&file).expect("Cannot create mmap file") };

        Self {
            file,
            size: index_size,
            mmap,
            path: file_path,
        }
    }

    pub fn close(&mut self) {
        let size = self.size;
        self.file.set_len(size).expect("Cannot truncate index file");
        self.mmap.flush().expect("Cannot flush mem map")
    }

    pub fn read_last_entry(&self) -> Option<IndexEntry> {
        if (self.size == 0) {
            return None;
        }

        // last entry should be index size / size of each index entry
        let index = (self.size / INDEX_ENTRY_LENGTH as u64) - 1;
        self.read(index)
    }

    pub fn read(&self, index_position: u64) -> Option<IndexEntry> {
        if self.size == 0 {
            return None;
        }

        let position_in_index_file = index_position * INDEX_ENTRY_LENGTH as u64;
        if position_in_index_file >= self.size {
            return None;
        }

        let record_offset = &self.mmap[position_in_index_file as usize
            ..(position_in_index_file + INDEX_RECORD_OFFSET_LENGTH as u64) as usize];

        let record_offset = byteorder::BigEndian::read_u32(record_offset);

        let start = (position_in_index_file + INDEX_RECORD_OFFSET_LENGTH as u64) as usize;
        let end = start + POSITION_IN_STORE_FILE_LENGTH as usize;

        let position_in_store_file = &self.mmap[start..end];
        let position_in_store_file = byteorder::BigEndian::read_u64(position_in_store_file);

        Some(IndexEntry {
            record_offset,
            position: position_in_store_file,
        })
    }

    pub fn write(&mut self, record_offset: u32, position: u64) -> Result<(), IndexError> {
        if self.mmap.len() < (self.size as usize + INDEX_ENTRY_LENGTH as usize) {
            // index file is full
            return Err(IndexError::IndexFullError);
        }

        let start = self.size;
        let end = self.size + 4 as u64;

        self.size += INDEX_ENTRY_LENGTH as u64; // new size should be the size of the index entry 4 + 8;

        let mut r = &mut self.mmap[start as usize..end as usize];

        byteorder::BigEndian::write_u32(&mut r, record_offset);

        // now let's write the position in store file
        let start = end;
        let end = start + 8 as u64;

        let mut r = &mut self.mmap[start as usize..end as usize];

        byteorder::BigEndian::write_u64(&mut r, position);
        Ok(())
    }

    fn delete(&mut self) {}
}

impl Drop for Index {
    fn drop(&mut self) {
        self.close()
    }
}

#[cfg(test)]
mod test {
    use crate::log::log::ConfigBuilder;

    use super::*;
    #[test]
    fn index_test() {
        let config = ConfigBuilder::new(1024, 1024, 0).build();
        let index_file = "index";

        let mut path = PathBuf::new();
        path.push(&index_file);

        let config = Arc::new(config);

        let mut index = Index::new(path, config);

        index.write(0, 10);
        index.write(1, 20);
        index.write(2, 30);
        index.write(3, 40);

        let result = index.read(1).unwrap();

        assert_eq!(result.record_offset, 1);
        assert_eq!(result.position, 20);

        index.close();

        let config = ConfigBuilder::new(1024, 1024, 0).build();

        let config = Arc::new(config);

        // test that we can rebuild state
        let mut path = PathBuf::new();
        path.push(&index_file);

        let mut index = Index::new(path, config);

        index.write(4, 50);
        index.write(5, 60);
        index.write(6, 70);
        index.write(7, 80);

        let result = index.read(7).unwrap();

        assert_eq!(result.record_offset, 7);
        assert_eq!(result.position, 80);

        // test that if we ask for an index that doesn't exist, we return none

        let result = index.read(8);
        assert!(result.is_none());

        std::fs::remove_file(index_file).unwrap();
    }
}
