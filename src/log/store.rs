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

use crate::log::log::LEN_WIDTH;
use crate::proto::{self, record::Record};
use std::io;
use std::sync::Arc;

use super::log::Config;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("Store is full")]
    StoreFullError,
    #[error("Store entry {0} not found")]
    StoreEntryNotFound(u64),

    #[error(transparent)]
    IOError(#[from] std::io::Error),
}
pub struct Store {
    pub file: File,
    pub size: usize,
    pub path: PathBuf,
    pub config: Arc<Config>,
}

impl Store {
    pub fn new(path: PathBuf, config: Arc<Config>) -> Store {
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        let file_size = file.metadata().unwrap().len();
        Self {
            file,
            size: file_size as usize,
            path,
            config,
        }
    }

    pub fn can_store_record(&self, record_len: usize) -> bool {
        self.size + (record_len + LEN_WIDTH as usize) < self.config.get_max_store_bytes() as usize
    }

    pub fn append(&mut self, value: Vec<u8>) -> Result<(usize, usize), StoreError> {
        let position = self.size;
        let mut buffer = BufWriter::new(&mut self.file);
        // 8 bytes for the length of the encoded record
        buffer.write_u64::<BigEndian>(value.len() as u64)?;
        let written = buffer.write(&value)?;
        let total_written = written + LEN_WIDTH as usize;
        self.size += total_written;
        buffer.flush();
        Ok((total_written, position))
    }

    pub fn read(&self, position: u64) -> Result<Vec<u8>, StoreError> {
        let mut buf: Vec<u8> = vec![0; LEN_WIDTH as usize];
        self.file.read_exact_at(&mut buf, position)?;
        let len_of_record = BigEndian::read_u64(&buf[..]);
        let mut record: Vec<u8> = vec![0; len_of_record as usize];
        self.file
            .read_exact_at(&mut record, position + LEN_WIDTH as u64)?; // add LEN_WIDTH, cos LEN_WIDTH holds the size of the record
        Ok(record)
    }
}

#[cfg(test)]
mod test {
    use crate::log::log::ConfigBuilder;

    use super::*;
    #[test]
    fn store_test() {
        let file_name = "tempfile_store_test";
        let mut path = PathBuf::new();
        path.push(&file_name);
        let config = ConfigBuilder::new(1024, 1024, 0).build();
        let mut store = Store::new(path, Arc::new(config));

        let record_1 = "hello_world1";
        let record_2 = "hello_world2";
        let record_3 = "hello_world3";

        let (_, position1) = store.append(record_1.as_bytes().to_vec()).unwrap();
        let (_, position2) = store.append(record_2.as_bytes().to_vec()).unwrap();
        let (_, position3) = store.append(record_3.as_bytes().to_vec()).unwrap();
        assert_eq!(
            &(store.read(position1 as u64).unwrap()),
            record_1.as_bytes()
        );
        assert_eq!(
            &(store.read(position2 as u64).unwrap()),
            record_2.as_bytes()
        );
        assert_eq!(
            &(store.read(position3 as u64).unwrap()),
            record_3.as_bytes()
        );

        std::fs::remove_file(file_name).unwrap();
    }

    #[test]
    fn knows_is_full(){
        let file_name = "tempfile_knows_is_full";
        let mut path = PathBuf::new();
        path.push(&file_name);
        let config = ConfigBuilder::new(1024, 20, 0).build();
        let mut store = Store::new(path, Arc::new(config));
        let record_1 = "hello_world1";
        let record_2 = "hello_world2";

        let (_, position1) = store.append(record_1.as_bytes().to_vec()).unwrap();

        let can_store = store.can_store_record(record_2.len());
        assert!(!can_store);

        std::fs::remove_file(file_name);


    }
}
