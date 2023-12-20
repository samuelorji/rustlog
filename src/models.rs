use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Record {
    value: String,
    pub offset: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Serialize, Debug)]
pub struct ConsumeResponse {
    pub record: Record,
}
