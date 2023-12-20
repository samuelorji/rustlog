use crate::models::Record;
pub struct Log {
    records: Vec<Record>,
}

impl Log {
    pub fn new() -> Self {
        Log { records: vec![] }
    }

    pub fn append(&mut self, mut record: Record) -> usize {
        let offset = self.records.len();
        record.offset = Some(offset);
        self.records.push(record);

        offset
    }

    pub fn read(&self, offset: usize) -> Option<Record> {
        self.records.get(offset).map(|record| record.clone())
    }
}
