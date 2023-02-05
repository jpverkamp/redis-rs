
use std::net::SocketAddr;
use std::str::FromStr;

use redis_rs::RedisType;

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use tracing_subscriber;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:6379";

    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Listening on {addr}");
    
    loop {
        let (stream, addr) = listener.accept().await?;
        tracing::debug!("Accepted connection from {addr:?}");
        tokio::spawn(async move {
            if let Err(e) = handle(stream, addr).await {
                tracing::warn!("An error occurred: {e:?}");
            }
        });
    }
}

async fn handle(mut stream: TcpStream, addr: SocketAddr) -> std::io::Result<()> {
    tracing::info!("[{addr}] Accepted connection");

    let mut buf = [0; 1024];
    loop {
        let bytes_read = stream.read(&mut buf).await?;
        if bytes_read == 0 {
            break;
        }
        tracing::debug!("[{addr}] Received {bytes_read} bytes");

        let string = String::from_utf8_lossy(&buf[0..bytes_read]);
        let data = match RedisType::from_str(&string) {
            Ok(data) => data,
            Err(err) => {
                tracing::warn!("[{addr}] Error parsing input: {err:?}");
                continue;
            },
        };
        tracing::debug!("[{addr} Received {data:?}");

        stream.write_all(data.to_string().as_bytes()).await?;
    }

    tracing::info!("[{addr}] Ending connection");

    Ok(())
}
