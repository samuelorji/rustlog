mod index;
mod log;
mod models;
mod proto;
mod routes;
use std::sync::Mutex;

use actix_web::{get, post, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use routes::test;
// use log::Log;

async fn not_found(request: HttpRequest) -> impl Responder {
    println!("request is {:?}", &request);
    "404"
}
// #[actix_web::main]
// async fn main() -> std::io::Result<()> {
//     // let log = Log::new();

//     // let log = web::Data::new(Mutex::new(log));
//     HttpServer::new(move || {
//         App::new()
//             .service(routes::test)
//             .default_service(web::to(not_found))
//         // .service(routes::add_record)
//         // .service(routes::get_record)
//         //.app_data(log.clone())
//         // .service(hello)
//         // .service(echo)
//         // .route("/hey", web::get().to(manual_hello))
//     })
//     .bind(("127.0.0.1", 8080))?
//     .run()
//     .await
// }

use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tokio::{join, sync::RwLock};

struct MyThing {
    n: usize,
}

impl MyThing {
    fn new(x: usize) -> MyThing {
        MyThing { n: x }
    }

    fn read(&self) -> usize {
        self.n
    }

    fn write(&mut self) -> usize {
        self.n += 1;
        self.n
    }
}

async fn rr(v: Arc<RwLock<MyThing>>) {
    loop {
        let read = v.read().await;

        let n = (*read).read();
        println!("\x1b[93mReading Value : {}\x1b[0m", n);
        drop(read);
        // sleep(Duration::from_millis(50)).await;
    }
}

async fn ww(w: Arc<RwLock<MyThing>>) {
    loop {
        let mut write = w.write().await;

        let n = (*write).write();

        println!("\x1b[31mUpdated value: {}\x1b[0m", n);
        drop(write);
        // sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::main]
async fn main() {
    let x = Arc::new(RwLock::new(MyThing::new(11)));
    let w = x.clone();
    let w1 = x.clone();
    let r = x.clone();
    let r1 = x.clone();
    let r2 = x.clone();
    let r3 = x.clone();
    let r4 = x.clone();
    let r5 = x.clone();
    let h = tokio::spawn(async move { ww(w).await });

    let h1 = tokio::spawn(async move { ww(w1).await });
    let j = tokio::spawn(async move { rr(r).await });
    let j1 = tokio::spawn(async move { rr(r1).await });
    let j2 = tokio::spawn(async move { rr(r2).await });

    let j3 = tokio::spawn(async move { rr(r3).await });
    let j4 = tokio::spawn(async move { rr(r4).await });
    let j5 = tokio::spawn(async move { rr(r5).await });

    tokio::join!(h);
    tokio::join!(h1);

    tokio::join!(j);
    tokio::join!(j1);
    tokio::join!(j2);
    tokio::join!(j3);
    tokio::join!(j4);
    tokio::join!(j5);
}
