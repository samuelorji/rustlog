use std::time::Duration;

// use crate::models::{ConsumeRequest, ConsumeResponse, ProduceRequest, ProduceResponse};
// use crate::log::Log;
use actix_web::{
    get, post,
    web::{self, Bytes},
    App, HttpMessage, HttpRequest, HttpResponse, HttpServer, Responder,
};
// use std::sync::Mutex;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Stuff {
    username: String,
    password: String,
}
#[post("/post/")]
pub async fn test(bytes: Bytes) -> impl Responder {
    //println!("got record {:?}, now sleeping for 2 seconds",record.0 );
    let body = String::from_utf8(bytes.to_vec()).expect("cannot parse string");

    println!("body is {}", body);
    std::thread::sleep(Duration::from_secs(2));
    "Hello from Rust service"
}
// #[post("/")]
// pub async fn add_record(
//     //record: web::Json<ProduceRequest>,
//     log: web::Data<Mutex<Log>>,
// ) -> impl Responder {
//     // let offset = log.lock().unwrap().append(record.0.record);
//     // let prod_response = ProduceResponse { offset };
//     // HttpResponse::Ok().json(prod_response)
//     HttpResponse::NotFound().finish()
// }

// #[get("/")]
// pub async fn get_record(
//     record: web::Json<ConsumeRequest>,
//     log: web::Data<Mutex<Log>>,
// ) -> impl Responder {
//     let offset = log.lock().unwrap().read(record.0.offset);
//     match offset {
//         Some(record) => {
//             let resp = ConsumeResponse { record };
//             HttpResponse::NotFound().finish()
//         }
//         None => HttpResponse::NotFound().finish(),
//     }
// }
