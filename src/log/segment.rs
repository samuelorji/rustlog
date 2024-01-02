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

use super::index::{Index, IndexError};
use super::log::Config;
use super::store::{Store, StoreError};
use crate::proto::{self, record::Record};
use std::io;
use std::sync::Arc;

#[derive(Error, Debug)]
pub enum SegmentError {
    #[error("Path {0} is not a directory")]
    SegmentPathNotADirectory(PathBuf),

    #[error("store full")]
    StoreFull(Record),

    #[error(transparent)]
    IndexErrors(#[from] IndexError),

    #[error(transparent)]
    StoreErrors(#[from] StoreError),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    DecodeError(#[from] DecodeError),

    #[error(transparent)]
    EncodeError(#[from] EncodeError),
}

pub struct Segment {
    pub store: Store,
    pub index: Index,
    pub base_offset: u64,
    pub next_offset: u64,
    pub config: Arc<Config>,
}

impl Segment {
    pub fn new(
        dir: PathBuf,
        base_offset: u64,
        config: Arc<Config>,
    ) -> Result<Segment, SegmentError> {
        if (!dir.is_dir()) {
            return Err(SegmentError::SegmentPathNotADirectory(dir));
        }

        let store = Store::new(dir.join(".store"), config.clone());
        let index = Index::new(dir.join(".index"), config.clone());
        let next_offset = index
            .read_last_entry()
            .map(|e| e.record_offset as u64 + 1)
            .unwrap_or(base_offset);

        Ok(Segment {
            store,
            index,
            base_offset,
            next_offset,
            config,
        })
        //todo!()
    }

    pub fn append(&mut self, mut record: proto::record::Record) -> Result<u64, SegmentError> {
        let record_offset = self.next_offset;

        if record.offset.is_none() {
            record.offset = Some(record_offset);
        }

        let mut record_buf: Vec<u8> = vec![];

        record.encode(&mut record_buf)?;



        if !self.store.can_store_record(record_buf.len()) {
            return Err(SegmentError::StoreFull(record));
        }

        let (total_written, position) = self.store.append(record_buf)?;

        // index offset is always relative to the base offset
        let index_offset = record_offset - self.base_offset;
        self.index.write(index_offset as u32, position as u64)?;

        self.next_offset += 1;

        Ok(record_offset)
    }

    pub fn read(&self, offset: u64) -> Result<Record, SegmentError> {
        // _, pos, err := s.index.Read(int64(off - s.baseOffset))

        let pos: u64 = offset - self.base_offset;
        if let Some(entry) = self.index.read(pos) {
            if let Ok(record) = self.store.read(entry.position) {
                let record: Record = prost::Message::decode(&record[..])?;
                return Ok(record);
            } else {
                return Err(SegmentError::StoreErrors(StoreError::StoreEntryNotFound(
                    entry.position,
                )));
            }
        } else {
            return Err(SegmentError::IndexErrors(IndexError::IndexEntryNotFound(
                pos as u32,
            )));
        }
    }

    pub fn close(&mut self) {
        self.index.close();
    }

    pub fn remove(&mut self) {
        self.close();

        std::fs::remove_file(self.index.path.clone()).expect("Cannot delete index file");
        std::fs::remove_file(self.store.path.clone()).expect("Cannot delete store file");
    }

    pub fn is_maxed(&self) -> bool {
        self.store.size >= self.config.get_max_store_bytes() as usize
            || self.index.size >= self.config.get_max_index_bytes()
    }

    // nearestMultiple(j uint64, k uint64) returns the nearest and lesser multiple of k in j,
    // for example nearestMultiple(9, 4) == 8. We take the lesser multiple to make sure
    // we stay under the user’s disk capacity.
    fn nearest_multiple(j: u64, k: u64) -> u64 {
        if j >= 0 {
            (j / k) * k
        } else {
            ((j - k + 1) / k) * k
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::index::IndexError;
    use super::super::log::INDEX_ENTRY_LENGTH;
    use super::*;
    use crate::log::log::ConfigBuilder;
    use crate::proto::record::Record;
    use std::sync::Arc;

    #[test]
    fn segment_test() {
        let dir = "segment-dir-segment_test";
        std::fs::create_dir(dir).expect("Cannot create segment directory");

        let config = ConfigBuilder::new((INDEX_ENTRY_LENGTH * 3) as u64, 1024, 0).build();
        let config = Arc::new(config);

        let record: Record = Record {
            value: "hello world".as_bytes().to_vec(),
            offset: None,
        };

        let mut path = PathBuf::new();
        path.push(dir);

        let mut segment = Segment::new(path.clone(), 16, config).expect("Cannot create Segment");

        assert_eq!(segment.next_offset, 16);
        assert_eq!(segment.is_maxed(), false);

        for i in 0..3 {
            let offset = segment.append(record.clone()).unwrap();
            assert_eq!(16 + i, offset);

            let record_1 = segment.read(offset).unwrap();
            assert_eq!(record_1.value, record.clone().value);
        }

        let result = segment.append(record.clone());
        assert!(matches!(
            result,
            Err(SegmentError::IndexErrors(IndexError::IndexFullError))
        ));

        // index should be full
        assert!(segment.is_maxed());

        let config = ConfigBuilder::new(1024, (&record.value.len() * 3) as u64, 0).build();

        let config = Arc::new(config);

        let mut segment =
            Segment::new(path.clone(), 16, config.clone()).expect("Cannot create Segment");

        // store should be full
        assert!(segment.is_maxed());

        // clear segment
        segment.remove();

        let mut segment = Segment::new(path.clone(), 16, config).expect("Cannot create Segment");

        // store and index should NOT be full
        assert!(!segment.is_maxed());

        segment.remove();

        std::fs::remove_dir(dir).expect("Cannot delete")
    }
}
