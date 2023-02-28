use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;

use redis_rs::RedisType;

use lazy_static::lazy_static;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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
    let mut state = State::default();

    loop {
        let bytes_read = stream.read(&mut buf).await?;
        if bytes_read == 0 {
            break;
        }
        tracing::debug!("[{addr}] Received {bytes_read} bytes");

        let string = String::from_utf8_lossy(&buf[0..bytes_read]);
        let command = match RedisType::from_str(&string) {
            Ok(RedisType::Array { value }) => value,
            Ok(data) => {
                tracing::warn!("[{addr}] Error, input should be array, got: {data:?}");
                continue;
            }
            Err(err) => {
                tracing::warn!("[{addr}] Error parsing input: {err:?}");
                continue;
            }
        };

        if command.len() < 1 {
            tracing::warn!("[{addr}] Input command was empty");
            continue;
        }

        let args = &command[1..];
        let command = match &command[0] {
            RedisType::String { value } => value.to_ascii_uppercase().to_owned(),
            _ => {
                tracing::warn!(
                    "[{addr}] Input command must be a string, got {:?}",
                    command[0]
                );
                continue;
            }
        };
        tracing::debug!("[{addr} Received: {command} {args:?}");

        match COMMANDS.get(command.as_str()) {
            Some(command) => {
                let response = match command.f.as_ref()(&mut state, args) {
                    Ok(value) => value,
                    Err(value) => RedisType::Error { value },
                };
                stream.write_all(response.to_string().as_bytes()).await?;
            }
            None => {
                tracing::warn!("[{addr}] Unimplemented command: {command} {args:?}");
                stream
                    .write_all(
                        RedisType::Error {
                            value: format!("Unimplemented command: {command}").to_owned(),
                        }
                        .to_string()
                        .as_bytes(),
                    )
                    .await?;
                continue;
            }
        }
    }

    tracing::info!("[{addr}] Ending connection");

    Ok(())
}

#[derive(Debug, Default)]
pub struct State {
    keystore: HashMap<String, String>,
}

#[derive()]
pub struct Command {
    f: Box<fn(&mut State, &[RedisType]) -> Result<RedisType, String>>,
}

lazy_static! {
    static ref COMMANDS: HashMap<&'static str, Command> = {
        let mut m = HashMap::new();

        m.insert("COMMAND", Command {
            f: Box::new(|_state, _args| {
                // TODO: Assume for now it's run as COMMAND DOCS with no more parameters
                // Eventually we'll want to serialize and send `COMMANDS` back
                Ok(RedisType::Array { value: vec![] })
            })
        });

        m.insert("SET", Command {
            f: Box::new(|state, args| {
                if args.len() < 2 {
                    return Err("Expected: SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]".to_string());
                }

                if args.len() > 2 {
                    return Err("Expected: SET key value; additional parameters are not yet supported".to_string());
                }

                let key = match &args[0] {
                    RedisType::String { value } => value.to_owned(),
                    _ => return Err("SET: Unknown key format".to_string())
                };

                let value = match &args[1] {
                    RedisType::String { value } => value.to_owned(),
                    _ => return Err("SET: Unknown value format".to_string())
                };

                state.keystore.insert(key, value);
                Ok(RedisType::String { value: "OK".to_owned() })
            })
        });

        m.insert("GET", Command {
            f: Box::new(|state, args| {
                if args.len() != 1 {
                    return Err("Expected: GET $key".to_string());
                }

                let key = match &args[0] {
                    RedisType::String { value } => value.to_owned(),
                    _ => return Err("Expected: GET $key:String".to_string())
                };

                Ok(match state.keystore.get(&key) {
                    Some(value) => RedisType::String { value: value.to_owned() },
                    None => RedisType::NullString,
                })
            })
        });

        m
    };
}
