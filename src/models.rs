use serde::{Deserialize, Serialize};

use crate::proto::record::Record;

//#[derive(Serialize, Deserialize, Debug)]
pub struct ProduceRequest {
    pub record: Record,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ProduceResponse {
    pub offset: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConsumeRequest {
    pub offset: usize,
}

//#[derive(Serialize, Debug)]
pub struct ConsumeResponse {
    pub record: Record,
}
