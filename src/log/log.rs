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
use super::segment::{Segment, SegmentError};
use super::store::{Store, StoreError};
use crate::proto::{self, record::Record};
use std::io;
use std::sync::Arc;

pub const LEN_WIDTH: u8 = 8; // number of bytes used to store the position of a record
pub const INDEX_RECORD_OFFSET_LENGTH: u8 = 4; // should u32
pub const POSITION_IN_STORE_FILE_LENGTH: u8 = 8; // u64
pub const INDEX_ENTRY_LENGTH: u8 = INDEX_RECORD_OFFSET_LENGTH + POSITION_IN_STORE_FILE_LENGTH;

#[derive(Clone)]
struct SegmentConfig {
    max_index_bytes: u64,
    max_store_bytes: u64,
    initial_offset: u64,
    max_record_size_kb: u16,
}

#[derive(Clone)]
pub struct Config {
    segment: SegmentConfig,
}

impl Config {
    pub fn get_max_index_bytes(&self) -> u64 {
        self.segment.max_index_bytes
    }
    pub fn get_max_store_bytes(&self) -> u64 {
        self.segment.max_store_bytes
    }
}

pub struct ConfigBuilder {
    max_index_bytes: u64,
    max_store_bytes: u64,
    initial_offset: u64,
    max_record_size_kb: u16,
}

impl ConfigBuilder {
    pub fn new(max_index_bytes: u64, max_store_bytes: u64, initial_offset: u64) -> Self {
        // assert!(
        //     max_index_bytes > 1024,
        //     "max index size must be greater than 1Kb"
        // );
        // assert!(
        //     max_store_bytes > 10240,
        //     "max store size must be greater than 10Kb"
        // );
        // assert!(
        //     initial_offset > 10240,
        //     "max store size must be greater than 10Kb"
        // );
        Self {
            max_index_bytes,
            max_store_bytes,
            initial_offset,
            max_record_size_kb: 400,
        }
    }

    pub fn with_max_record_size_kb(mut self, max: u16) -> Self {
        self.max_record_size_kb = max;
        self
    }

    pub fn build(self) -> Config {
        Config {
            segment: SegmentConfig {
                max_index_bytes: self.max_index_bytes,
                max_store_bytes: self.max_store_bytes,
                initial_offset: self.initial_offset,
                max_record_size_kb: self.max_record_size_kb,
            },
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            segment: SegmentConfig {
                max_index_bytes: 1024,
                max_store_bytes: 1024,
                initial_offset: 0,
                max_record_size_kb: 400,
            },
        }
    }
}

#[derive(Error, Debug)]
pub enum LogError {
    #[error("Invalid Segment file {0}")]
    InvalidSegmentFile(PathBuf),

    #[error("Record too large")]
    RecordTooLarge,

    #[error(transparent)]
    ParseIntError(#[from] ParseIntError),

    #[error(transparent)]
    IndexErrors(#[from] IndexError),

    #[error(transparent)]
    StoreErrors(#[from] StoreError),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    SegmentErrors(#[from] SegmentError),
}
pub struct Log {
    dir: PathBuf, // where we store segments
    config: Arc<Config>,
    active_segment: usize,
    segments: Vec<Segment>,
}

impl Log {
    fn new(dir: PathBuf, config: Option<Config>) -> Result<Self, LogError> {
        if (!dir.exists()) {
            std::fs::create_dir(&dir)?
        };
        let mut l = Log {
            dir,
            config: Arc::new(config.unwrap_or_else(|| Default::default())),
            active_segment: 0,
            segments: vec![],
        };

        l.setup()?;
        Ok(l)
    }

    fn setup(&mut self) -> Result<(), LogError> {
        let mut base_offsets: Vec<u64> = vec![];

        // read all segment files
        for files in std::fs::read_dir(&self.dir)? {
            let file = files?;
            let path = file.path();

            let r = path
                .file_stem()
                .and_then(|file_name| file_name.to_str())
                .map_or_else(
                    || Err(LogError::InvalidSegmentFile(file.path())),
                    |stripped| Ok(stripped),
                )?;

            let base_offset = r.parse::<u64>()?;
            base_offsets.push(base_offset);
        }

        // arrange base offsets in ascending order

        base_offsets.sort();

        for offset in base_offsets {
            self.new_segment(offset)?;
        }
        if self.segments.is_empty() {
            // create a new segment
            self.new_segment(self.config.segment.initial_offset)?;
        }

        Ok(())
    }

    fn new_segment(&mut self, offset: u64) -> Result<(), LogError> {
        // create segment directory under log directory
        let segment_dir = self.dir.join(offset.to_string());
        if !segment_dir.exists() {
            std::fs::create_dir(&segment_dir)?;
        }
        let segment = Segment::new(segment_dir, offset, self.config.clone())?;
        let len_segments = self.segments.len();
        self.segments.push(segment);
        self.active_segment = len_segments;

        Ok(())
    }

    pub fn append(&mut self, record: Record) -> Result<u64, LogError> {
        if record.value.len() > (self.config.segment.max_record_size_kb as usize) {
            return Err(LogError::RecordTooLarge);
        }
        let mut active_segment = &mut self.segments[self.active_segment];

        match active_segment.append(record) {
            Ok(offset) => {
                if active_segment.is_maxed() {
                    self.new_segment(offset + 1)?;
                }
                Ok(offset)
            }
            Err(e ) => {
                match e {
                    SegmentError::StoreFull(record) => {
                        let offset = self.segments[self.active_segment].next_offset;
                        let _  = self.new_segment(offset)?;
                        let r = self.segments[self.active_segment].append(record)?;
                        Ok(r)
                    },
                    x =>   Err(LogError::SegmentErrors(x))
                }
            }
        }
    }

    pub fn read(&self, offset: u64) -> Result<Record, LogError> {
        let mut active_segment: usize = 0;
        // we iterate over the segments until we find the
        //first segment whose base offset is less than or equal to the offset we’re looking

        for (i, segment) in self.segments.iter().enumerate() {
            if self.segments[i].base_offset <= offset && offset < self.segments[i].next_offset {
                active_segment = i;
                break;
            }
        }
        let record = self.segments[active_segment].read(offset)?;
        Ok(record)
    }

    fn close(&mut self) {
        for segment in &mut self.segments {
            segment.close();
        }
    }

    fn remove(&mut self) -> Result<(), LogError> {
        self.close();

        let _ = std::fs::remove_dir(self.dir.clone())?;
        Ok(())
    }

    fn reset(&mut self) -> Result<(), LogError> {
        self.remove()?;
        self.setup()
    }

    fn lowest_offset(&self) -> Result<u64, LogError> {
        Ok(self.segments[0].base_offset)
    }

    fn highest_offset(&self) -> Result<u64, LogError> {
        let offset = self
            .segments
            .last()
            .map(|last_segment| last_segment.next_offset - 1)
            .unwrap_or(0);
        Ok(offset)
    }

    fn truncate(&mut self, lowest: u64) {
        let mut segments: Vec<Segment> = vec![];

        let mut segment_index_to_remove: Vec<usize> = vec![];

        for (i, mut segment) in &mut self.segments.iter_mut().enumerate() {
            if segment.next_offset <= lowest + 1 {
                segment.remove();
                segment_index_to_remove.push(i)
            }
        }

        for index in segment_index_to_remove {
            self.segments.remove(index);
        }
    }
}

impl Drop for Log {
    fn drop(&mut self) {
        self.close()
    }
}

#[cfg(test)]
mod test {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::{
        fs::{File, OpenOptions},
        io::{Cursor, Read},
    };

    use crate::proto::record::Record;

    use super::{Config, Index, Store};

    #[test]
    fn log_test_append_read() {
        // test append and read a record
        use super::*;
        let mut log_dir = PathBuf::new();
        log_dir.push("log_dir_append_read");
        let config = Config {
            segment: SegmentConfig {
                max_index_bytes: 1024,
                max_store_bytes: 1024,
                initial_offset: 0,
                max_record_size_kb: 400,
            },
        };

        // let config  = Arc::new(config);

        let mut log = Log::new(log_dir.clone(), Some(config)).expect("cannot create log");

        let record = crate::proto::record::Record {
            value: "hello world".as_bytes().to_vec(),
            offset: None,
        };

        let offset = log.append(record.clone()).unwrap();

        let read_record = log.read(offset).unwrap();
        assert_eq!(record.value, read_record.value);

        std::fs::remove_dir_all(log_dir).expect("cannot remove dir");
    }

    #[test]
    fn log_test_out_of_range() {
        use super::IndexError::{self, *};
        use super::LogError::{IndexErrors, SegmentErrors};
        use super::*;
        let mut log_dir = PathBuf::new();
        log_dir.push("log_dir_test_out_of_range");
        let config = Config {
            segment: SegmentConfig {
                max_index_bytes: 1024,
                max_store_bytes: 1024,
                initial_offset: 0,
                max_record_size_kb: 400,
            },
        };
        let mut log = Log::new(log_dir.clone(), Some(config)).expect("cannot create log");
        let res = log.read(1);
        assert!(matches!(
            res,
            Err(SegmentErrors(SegmentError::IndexErrors(
                IndexError::IndexEntryNotFound(1)
            )))
        ));
        std::fs::remove_dir_all(log_dir).expect("cannot remove dir");
    }
    #[test]
    fn log_test_init_existing() {
        use super::IndexError::{self, *};
        use super::LogError::{IndexErrors, SegmentErrors};
        use super::*;
        let mut log_dir = PathBuf::new();
        log_dir.push("log_dir_init_existing");
        let config = Config {
            segment: SegmentConfig {
                max_index_bytes: 1024,
                max_store_bytes: 100,
                initial_offset: 0,
                max_record_size_kb: 400,
            },
        };
        let mut log = Log::new(log_dir.clone(), Some(config.clone())).expect("cannot create log");
        let record: Record = Record {
            value: "hello world".as_bytes().to_vec(),
            offset: None,
        };

        for i in 0..3 {
            log.append(record.clone()).unwrap();
        }
        assert_eq!(log.lowest_offset().unwrap(), 0);
        assert_eq!(log.highest_offset().unwrap(), 2);

        log.close(); // apparently, shadowed variables are not dropped, so explicitly close

        let mut log = Log::new(log_dir.clone(), Some(config)).expect("cannot create log");
        assert_eq!(log.lowest_offset().unwrap(), 0);
        assert_eq!(log.highest_offset().unwrap(), 2);
        std::fs::remove_dir_all(log_dir).expect("cannot remove dir");
    }

    #[test]
    fn log_test_read_write_plenty() {
        use super::IndexError::{self, *};
        use super::LogError::{IndexErrors, SegmentErrors};
        use super::*;
        let mut log_dir = PathBuf::new();
        log_dir.push("log_dir_write_plenty");
        let config = Config {
            segment: SegmentConfig {
                max_index_bytes: 1024,
                max_store_bytes: 1024, // use a small store size
                initial_offset: 0,
                max_record_size_kb: 400
            },
        };
        let mut log = Log::new(log_dir.clone(), Some(config.clone())).expect("cannot create log");

        for i in 0..30{
            let record: Record = Record {
                value: format!("hello world{}", i).into_bytes(),
                offset: None,
            };
            log.append(record).unwrap();
        }

        let mut c = ConfigBuilder::new(0, 0, 0);
        let d = c.with_max_record_size_kb(78);
        let e = d.build();

        let record = log.read(29).unwrap();

        assert_eq!(record.offset, Some(29));
        assert_eq!(String::from_utf8(record.value).unwrap().as_str(), "hello world29");
        std::fs::remove_dir_all(log_dir).expect("cannot remove dir");
    }

    #[test]
    fn test_create_new_segment() {
        use super::IndexError::{self, *};
        use super::LogError::{IndexErrors, SegmentErrors};
        use super::*;
        let mut log_dir = PathBuf::new();
        log_dir.push("log_dir_create_new_segment");
        let config = Config {
            segment: SegmentConfig {
                max_index_bytes: 1024,
                max_store_bytes: 50, // use a small store size of 40 bytes
                initial_offset: 0,
                max_record_size_kb: 400,
            },
        };
        let mut log = Log::new(log_dir.clone(), Some(config.clone())).expect("cannot create log");
        
        // this record "hello world 1" is serialized into 16 bytes (when the offset is added)
        // plus the len of the record (8 bytes) totalling 24 bytes
        let record: Record = Record {
            value: "hello world1".as_bytes().to_vec(),
            offset: None,
        };
        log.append(record).unwrap(); // this should succeed

         // there should be one segment
         assert_eq!(std::fs::read_dir(&log_dir).unwrap().count(), 1); 

        
        // 9 + 8 = 17
        // active segment store should be 24 + 17 = 41 bytes, space for 9 bytes left (record of size 1 + 8 bytes for len of record)
        let record_2 = Record {
            value: "hello".as_bytes().to_vec(),
            offset: None,
        }; 

        log.append(record_2).unwrap(); // this should succeed

         // there should still be one segment
         assert_eq!(std::fs::read_dir(&log_dir).unwrap().count(), 1); 

        

        // now if we add something more than 1 bytes, it should result in the creation of a new segment as the old one should not be able to carry it
        // despite there being space

       
        // 2 + 8 = 10 (greater than the 9 bytes left in segment, should result in creation of a new segment)
        let record_3 = Record {
            value: "he".as_bytes().to_vec(),
            offset: None,
        }; 

        log.append(record_3).unwrap(); // this should succeed, but result in the creation of a new segment

        // there should be 2 segments
        assert_eq!(std::fs::read_dir(&log_dir).unwrap().count(), 2); 

        std::fs::remove_dir_all(log_dir);


    }
}
