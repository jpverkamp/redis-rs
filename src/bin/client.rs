
use std::io::{self, BufRead, stdout, Write};
use std::str::FromStr;

use redis_rs::RedisType;

use tokio::net::{TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use tracing_subscriber;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:6379";
    let mut stream = TcpStream::connect(addr).await?;
    tracing::info!("Connecting to {addr}");

    let stdin = io::stdin();
    let mut stdin_iterator = stdin.lock().lines();
    let mut buf = [0; 1024];

    // To match the protocol, always encode strings as bulk string even when it's not necessary
    // TODO: Do this better :)
    unsafe {
        redis_rs::ALWAYS_USE_BULK_STRING = true;
    }
    
    loop {
        print!("redis-rs> ");
        stdout().flush()?;

        match stdin_iterator.next() {
            Some(Ok(line)) => {
                tracing::debug!("Input read: {line}");

                // Parse the input into a collection of bulk strings
                let mut values = Vec::new();
                for arg in line.split_ascii_whitespace().into_iter() {
                    values.push(RedisType::String { value: String::from(arg) });
                }

                // Bundle into an array
                let array = RedisType::from(values);
                tracing::debug!("Input parsed: {array}");

                // Send them to the server
                stream.write_all(array.to_string().as_bytes()).await?;

                // Wait for an read a response back from the server
                let bytes_read = stream.read(&mut buf).await?;
                if bytes_read == 0 {
                    break;
                }
                tracing::debug!("Received {bytes_read} bytes from server");

                // Parse the response from the server
                let string = String::from_utf8_lossy(&buf[0..bytes_read]);
                let data = match RedisType::from_str(&string) {
                    Ok(data) => data,
                    Err(err) => {
                        tracing::warn!("Error parsing response from server: {err:?}");
                        continue;
                    },
                };
                
                // Print out the response from the server
                // TODO: Do something else with this? 
                println!("{data:?}");
            },
            Some(Err(e)) => {
                tracing::warn!("Error reading from stdin: {e:?}");
            },
            None => {
                tracing::info!("Reached end of stdin");
                break;
            }
        }
    }

    Ok(())
}

