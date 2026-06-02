use std::io::{Read, Write};
use std::net::TcpListener;

fn handle_health(mut stream: std::net::TcpStream) {
    let mut buf = [0; 1024];
    let _ = stream.read(&mut buf);
    let response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\n\r\n{\"status\":\"ok\"}";
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn main() {
    let port: u16 = std::env::var("ADMIN_SERVICE_PORT")
        .unwrap_or_else(|_| "8082".into())
        .parse()
        .unwrap_or(8082);

    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).expect("bind failed");
    eprintln!("admin-service listening on {}", addr);

    for stream in listener.incoming() {
        match stream {
            Ok(s) => handle_health(s),
            Err(e) => eprintln!("connection error: {}", e),
        }
    }
}
