use crate::models::{ConsumeRequest, ConsumeResponse, ProduceRequest, ProduceResponse};
use crate::log::Log;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use std::sync::Mutex;

#[post("/")]
pub async fn add_record(
    record: web::Json<ProduceRequest>,
    log: web::Data<Mutex<Log>>,
) -> impl Responder {
    let offset = log.lock().unwrap().append(record.0.record);
    let prod_response = ProduceResponse { offset };
    HttpResponse::Ok().json(prod_response)
}

#[get("/")]
pub async fn get_record(
    record: web::Json<ConsumeRequest>,
    log: web::Data<Mutex<Log>>,
) -> impl Responder {
    let offset = log.lock().unwrap().read(record.0.offset);
    match offset {
        Some(record) => {
            let resp = ConsumeResponse { record };
            HttpResponse::Ok().json(resp)
        }
        None => HttpResponse::NotFound().finish(),
    }
}
