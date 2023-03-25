use lazy_static::lazy_static;
use priority_queue::PriorityQueue;
use redis_rs::RedisType;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tracing_subscriber;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:6379";

    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Listening on {addr}");

    let state = Arc::new(Mutex::new(State::default()));

    loop {
        let (stream, addr) = listener.accept().await?;
        let thread_state = state.clone();

        tracing::debug!("Accepted connection from {addr:?}");
        tokio::spawn(async move {
            if let Err(e) = handle(stream, addr, thread_state).await {
                tracing::warn!("An error occurred: {e:?}");
            }
        });
    }
}

async fn handle(
    mut stream: TcpStream,
    addr: SocketAddr,
    state: Arc<Mutex<State>>,
) -> std::io::Result<()> {
    tracing::info!("[{addr}] Accepted connection");

    let mut buf = [0; 1024];

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
    ttl: PriorityQueue<String, SystemTime>,
}

#[derive()]
pub struct Command {
    help: String,
    f: Box<fn(&mut State, &[RedisType]) -> Result<RedisType, String>>,
}

lazy_static! {
    static ref COMMANDS: HashMap<&'static str, Command> = {
        let mut m = HashMap::new();

        macro_rules! assert_n_args {
            ($args:ident, $n:literal) => {
                if $args.len() != $n {
                    return Err(String::from(format!("Expected {} args, got {}", $n, $args.len())));
                }
            }
        }

        macro_rules! assert_n_or_more_args {
            ($args:ident, $n:literal) => {
                if $args.len() < $n {
                    return Err(String::from(format!("Expected at least {} args, got {}", $n, $args.len())));
                }
            }
        }

        macro_rules! get_string_arg {
            ($args:ident, $index:expr) => {
                {
                    if $index >= $args.len() {
                        return Err(String::from("Not enough args"));
                    }

                    match $args[$index].clone() {
                        RedisType::String{value} => value,
                        RedisType::Integer{value} => value.to_string(),
                        _ => return Err(String::from(format!("Attempted to use {} as a string", $args[$index]))),

                    }
                }
            }
        }

        // TODO: should this be case insensitive?
        macro_rules! is_string_eq {
            ($args:ident, $index:expr, $value:literal) => {
               get_string_arg!($args, $index).to_ascii_uppercase() == $value.to_ascii_uppercase()
            }
        }

        macro_rules! get_integer_arg {
            ($args:ident, $index:expr) => {
                {
                    if $index >= $args.len() {
                        return Err(String::from("Not enough args"));
                    }

                    match $args[$index].clone() {
                        RedisType::String{value} => {
                            match value.parse() {
                                Ok(value) => value,
                                Err(_) => return Err(String::from(format!("Attempted to use {} as an integer", $args[$index]))),
                            }
                        },
                        RedisType::Integer{value} => value,
                        _ => return Err(String::from(format!("Attempted to use {} as an integer", $args[$index]))),
                    }
                }
            }
        }

        macro_rules! get_float_arg {
            ($args:ident, $index:expr) => {
                {
                    if $index >= $args.len() {
                        return Err(String::from("Not enough args"));
                    }

                    match $args[$index].clone() {
                        RedisType::String{value} => {
                            match value.parse() {
                                Ok(value) => value,
                                Err(_) => return Err(String::from(format!("Attempted to use {} as a float", $args[$index]))),
                            }
                        },
                        RedisType::Integer{value} => value as f64,
                        _ => return Err(String::from(format!("Attempted to use {} as a float", $args[$index]))),
                    }
                }
            }
        }

        macro_rules! get_expiration {
            ($args:ident, $index:expr) => {
                if is_string_eq!($args, $index, "EX") {
                    // Seconds from now
                    let value = get_integer_arg!($args, $index + 1);
                    Some((
                        SystemTime::now()
                        + Duration::from_secs(value as u64)
                    ))
                } else if is_string_eq!($args, $index, "PX") {
                    // Milliseconds from now
                    let value = get_integer_arg!($args, $index + 1);
                    Some((
                        SystemTime::now()
                        + Duration::from_millis(value as u64)
                    ))
                } else if is_string_eq!($args, $index, "EXAT") {
                    // Seconds since epoch
                    let value = get_integer_arg!($args, $index + 1);
                    Some(UNIX_EPOCH + Duration::from_secs(value as u64))
                } else if is_string_eq!($args, $index, "PXAT") {
                    // Milliseconds since epoch
                    let value = get_integer_arg!($args, $index + 1);
                    Some(UNIX_EPOCH + Duration::from_millis(value as u64))
                } else {
                    None
                }
            }
        }

        m.insert("COMMAND", Command {
            f: Box::new(|_state, _args| {
                // TODO: Assume for now it's run as COMMAND DOCS with no more parameters
                // Eventually we'll want to serialize and send `COMMANDS` back
                Ok(RedisType::Array { value: vec![] })
            })
        });

        m.insert("SET", Command {
            help: String::from("\
SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]

Sets key to a given value.

NX|XX - only set if the key does not / does already exist.
EX|PX|EXAT|PXAT - key expires after seconds/milliseconds or at a Unix timestamp in seconds/milliseconds
KEEPTTL - retain the previously set TTL
GET - return the previous value, returns NIL and doesn't return if the key wasn't set

Returns OK if SET succeeded, nil if SET was not performed for NX|XX or because of GET, the old value if GET was specified. 
            "),
            f: Box::new(|state, args| {
                assert_n_or_more_args!(args, 2);
                let key = get_string_arg!(args, 0);
                let value = get_string_arg!(args, 1);

                let mut nx = false;
                let mut xx = false;
                let mut keepttl = false;
                let mut get = false;

                let mut expiration = None;

                let mut i = 2;
                loop {
                    if i >= args.len() {
                        break;
                    } else if is_string_eq!(args, i, "NX") {
                        nx = true;
                        i += 1;
                    } else if is_string_eq!(args, i, "XX") {
                        xx = true;
                        i += 1;
                    } else if is_string_eq!(args, i, "KEEPTTL") {
                        keepttl = true;
                        i += 1;
                    } else if is_string_eq!(args, i, "GET") {
                        get = true;
                        i += 1;
                    } else if let Some(ex) = get_expiration!(args, i) {
                        expiration = Some(ex);
                        i+= 2;
                    } else {
                        return Err(String::from(format!("Unexpected parameter: {:?}", args[i])));
                    }
                }

                if nx && xx {
                    return Err(String::from("SET: Cannot set both NX and XX"));
                }

                if keepttl && expiration.is_some() {
                    return Err(String::from("SET: Cannot set more than one of EX/PX/EXAT/PXAT/KEEPTTL"));
                }

                if expiration.is_some() {
                    tracing::debug!("Setting expiration for key {} to {:?}", key, expiration);
                    state.ttl.push(key.clone(), expiration.unwrap());
                } else if keepttl {
                    // do nothing
                } else {
                    state.ttl.remove(&key);
                }

                if nx && state.keystore.contains_key(&key) {
                    return Ok(RedisType::NullString);
                }

                if xx && !state.keystore.contains_key(&key) {
                    return Ok(RedisType::NullString);
                }

                let result = if get {
                    Ok(match state.keystore.get(&key) {
                        Some(value) => RedisType::String { value: value.to_owned() },
                        None => RedisType::NullString,
                    })
                } else {
                    Ok(RedisType::String { value: "OK".to_owned() })
                };

                state.keystore.insert(key, value);
                result
            })
        });


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
