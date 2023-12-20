mod routes;
mod models;
mod log;
use std::sync::Mutex;

use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use log::Log;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let log = Log::new();

    let log = web::Data::new(Mutex::new(log));
    HttpServer::new(move || {
        App::new()
        .service(routes::add_record)
        .service(routes::get_record)
        .app_data(log.clone())
        // .service(hello)
        // .service(echo)
        // .route("/hey", web::get().to(manual_hello))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
