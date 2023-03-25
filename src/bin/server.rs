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

    let ttl_state = state.clone();
    tokio::spawn(async move {
        loop {
            let now = SystemTime::now();
            loop {
                let evict = match ttl_state.lock().await.ttl.peek() {
                    Some((_, eviction_time)) => *eviction_time < now,
                    None => false,
                };

                if evict {
                    let mut ttl_state = ttl_state.lock().await;
                    let (key, _) = ttl_state.ttl.pop().unwrap();
                    tracing::debug!("Evicting {key} from keystore");
                    ttl_state.keystore.remove(&key);
                } else {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

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
            help: String::from("Return an array with details about every Redis command"),
            f: Box::new(|_state, args| {
                assert_n_args!(args, 1);
                if !is_string_eq!(args, 0, "DOCS") {
                    return Err(String::from("Only DOCS is supported"));
                }

                // TODO: Eventually we'll want to serialize and send `COMMANDS` back
                Ok(RedisType::Array { value: vec![] })
            })
        });

        m.insert("APPEND", Command {
            help: String::from("\
APPEND key value

Append value to the string stored at key. If key is not set, SET it now. 
            "),
            f: Box::new(|state, args| {
                assert_n_args!{args, 2};
                let key = get_string_arg!(args, 0);
                let value = get_string_arg!(args, 1);

                if let Some(current) = state.keystore.get_mut(&key) {
                    current.push_str(&value);
                } else {
                    state.keystore.insert(key.clone(), value);
                }

                Ok(RedisType::Integer{ value: state.keystore.get(&key).unwrap().to_string().len() as i64 })
            })
        });

        m.insert("DECR", Command {
            help: String::from("\
DECR key

Decrement the number stored at key by one.

If the key does not exist, it is set to 0 before performing the operation. An error is returned if the key contains a value of the wrong type or contains a string that can not be represented as integer. This operation is limited to 64 bit signed integers. 
            "),
            f: Box::new(|state, args| {
                assert_n_args!{args, 1};
                let key = get_string_arg!(args, 0);

                if let Some(current) = state.keystore.get_mut(&key) {
                    match current.parse::<i64>() {
                        Ok(value) => {
                            *current = (value - 1).to_string();
                            Ok(RedisType::Integer{ value: value - 1 })
                        },
                        Err(_) => Err(String::from("Value is not an integer or out of range")),
                    }
                } else {
                    state.keystore.insert(key.clone(), "-1".to_owned());
                    Ok(RedisType::Integer{ value: -1 })
                }
            })
        });

        m.insert("DECRBY", Command {
            help: String::from("\
DECRBY key decrement

Decrement the number stored at key by decrement.

If the key does not exist, it is set to 0 before performing the operation. An error is returned if the key contains a value of the wrong type or contains a string that can not be represented as integer. This operation is limited to 64 bit signed integers. 
            "),
            f: Box::new(|state, args| {
                assert_n_args!{args, 2};
                let key = get_string_arg!(args, 0);
                let decrement = get_integer_arg!(args, 1);

                if let Some(current) = state.keystore.get_mut(&key) {
                    match current.parse::<i64>() {
                        Ok(value) => {
                            *current = (value - decrement).to_string();
                            Ok(RedisType::Integer{ value: value - decrement })
                        },
                        Err(_) => Err(String::from("Value is not an integer or out of range")),
                    }
                } else {
                    state.keystore.insert(key.clone(), (0 - decrement).to_string());
                    Ok(RedisType::Integer{ value: 0 - decrement })
                }
            })
        });

        m.insert("GET", Command {
            help: String::from(""),
            f: Box::new(|state, args| {
                assert_n_args!(args, 1);
                let key = get_string_arg!(args, 0);

                Ok(match state.keystore.get(&key) {
                    Some(value) => RedisType::String { value: value.to_owned() },
                    None => RedisType::NullString,
                })
            })
        });

        m.insert("GETDEL", Command {
            help: String::from("\
GETDEL key

Get the value of key and delete it. 
            "),
            f: Box::new(|state, args| {
                assert_n_args!(args, 1);
                let key = get_string_arg!(args, 0);

                Ok(match state.keystore.remove(&key) {
                    Some(value) => RedisType::String { value: value.to_owned() },
                    None => RedisType::NullString,
                })
            })
        });

        m.insert("GETEX", Command {
            help: String::from("\
GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST]

Get the value of key and set its expiration time. 
            "),
            f: Box::new(|state, args| {
                assert_n_or_more_args!(args, 1);
                let key = get_string_arg!(args, 0);

                let mut persist = false;
                let mut expiration = None;

                if args.len() > 1 {
                    if is_string_eq!(args, 1, "PERSIST") {
                        persist = true;
                    } else if let Some(ex) = get_expiration!(args, 1) {
                        expiration = Some(ex);
                    } else {
                        return Err(String::from("Invalid argument"));
                    }
                }

                if persist && expiration.is_some() {
                    return Err(String::from("Cannot set multiple of PERSIST, EX, PX, EXAT, PXAT"));
                }

                if expiration.is_some() {
                    tracing::debug!("Setting expiration for key {} to {:?}", key, expiration);
                    state.ttl.push(key.clone(), expiration.unwrap());
                } else if persist {
                    state.ttl.remove(&key);
                }

                Ok(match state.keystore.remove(&key) {
                    Some(value) => RedisType::String { value: value.to_owned() },
                    None => RedisType::NullString,
                })
            })
        });

        m.insert("GETRANGE", Command {
            help: String::from("\
GETRANGE key start end

Get a substring of the string stored at a key."
            ),
            f: Box::new(|state, args| {
                assert_n_args!(args, 3);
                let key = get_string_arg!(args, 0);
                let mut start = get_integer_arg!(args, 1);
                let mut end = get_integer_arg!(args, 2);

                Ok(match state.keystore.get(&key) {
                    Some(value) => {
                        start = start.max(0).min(value.len() as i64 - 1);
                        end = end.max(0).min(value.len() as i64 - 1);

                        if start > end {
                            RedisType::String { value: String::new() }
                        } else {
                            RedisType::String { value: value[start as usize..end as usize].to_owned() }
                        }
                    },
                    None => RedisType::NullString,
                })
            })
        });

        m.insert("GETSET", Command {
            help: String::from("\
GETSET key value

Set key to hold the string value and return its old value. 
            "),
            f: Box::new(|state, args| {
                assert_n_args!(args, 2);
                let key = get_string_arg!(args, 0);
                let value = get_string_arg!(args, 1);

                Ok(match state.keystore.insert(key.clone(), value.clone()) {
                    Some(old_value) => RedisType::String { value: old_value },
                    None => RedisType::NullString,
                })
            })
        });

        m.insert("INCR", Command {
            help: String::from("\
INCR key

Increment the number stored at key by one.

If the key does not exist, it is set to 0 before performing the operation. An error is returned if the key contains a value of the wrong type or contains a string that can not be represented as integer. This operation is limited to 64 bit signed integers. 
            "),
            f: Box::new(|state, args| {
                assert_n_args!{args, 1};
                let key = get_string_arg!(args, 0);

                if let Some(current) = state.keystore.get_mut(&key) {
                    match current.parse::<i64>() {
                        Ok(value) => {
                            *current = (value + 1).to_string();
                            Ok(RedisType::Integer{ value: value + 1 })
                        },
                        Err(_) => Err(String::from("Value is not an integer or out of range")),
                    }
                } else {
                    state.keystore.insert(key.clone(), "1".to_owned());
                    Ok(RedisType::Integer{ value: 1 })
                }
            })
        });

        m.insert("INCRBY", Command {
            help: String::from("\
INCRBY key increment

Increment the number stored at key by increment.
"),
            f: Box::new(|state, args| {
                assert_n_args!{args, 2};
                let key = get_string_arg!(args, 0);
                let increment = get_integer_arg!(args, 1);

                if let Some(current) = state.keystore.get_mut(&key) {
                    match current.parse::<i64>() {
                        Ok(value) => {
                            *current = (value + increment).to_string();
                            Ok(RedisType::Integer{ value: value + increment })
                        },
                        Err(_) => Err(String::from("Value is not an integer or out of range")),
                    }
                } else {
                    state.keystore.insert(key.clone(), increment.to_string());
                    Ok(RedisType::Integer{ value: increment })
                }
            })
        });

        m.insert("INCRBYFLOAT", Command {
            help: String::from("\
INCRBYFLOAT key increment

Increment the string representing a floating point number stored at key by the specified increment. 
            "),
            f: Box::new(|state, args| {
                assert_n_args!{args, 2};
                let key = get_string_arg!(args, 0);
                let increment = get_float_arg!(args, 1);

                if let Some(current) = state.keystore.get_mut(&key) {
                    match current.parse::<f64>() {
                        Ok(value) => {
                            *current = (value + increment).to_string();
                            Ok(RedisType::String{ value: (value + increment).to_string() })
                        },
                        Err(_) => Err(String::from("Value is not a float")),
                    }
                } else {
                    state.keystore.insert(key.clone(), increment.to_string());
                    Ok(RedisType::String{ value: increment.to_string() })
                }
            })
        });

        m.insert("MGET", Command {
            help: String::from("\
MGET key [key ...]

Get the values of all the given keys.

For every key that does not hold a string value or does not exist, the special value nil is returned.
            "),
            f: Box::new(|state, args| {
                assert_n_or_more_args!(args, 1);

                let mut values = Vec::new();

                for i in 0..args.len() {
                    let key = get_string_arg!(args, i);
                    match state.keystore.get(&key) {
                        Some(value) => values.push(RedisType::String { value: value.to_owned() }),
                        None => values.push(RedisType::NullString),
                    }
                }

                Ok(RedisType::Array { value: values })
            })
        });

        m.insert("MSET", Command {
            help: String::from("\
MSET key value [key value ...]

Set multiple keys to multiple values.
            "),
            f: Box::new(|state, args| {
                assert_n_or_more_args!(args, 2);

                for i in (0..args.len()).step_by(2) {
                    let key = get_string_arg!(args, i);
                    let value = get_string_arg!(args, i + 1);
                    state.keystore.insert(key, value);
                }

                Ok(RedisType::String { value: "OK".to_owned() })
            })
        });
        
        m.insert("MSETNX", Command {
            help: String::from("\
MSETNX key value [key value ...]

Set multiple keys to multiple values, only if none of the keys exist.
            "),
            f: Box::new(|state, args| {
                assert_n_or_more_args!(args, 2);

                for i in (0..args.len()).step_by(2) {
                    let key = get_string_arg!(args, i);
                    if state.keystore.contains_key(&key) {
                        return Ok(RedisType::Integer { value: 0 });
                    }
                }

                for i in (0..args.len()).step_by(2) {
                    let key = get_string_arg!(args, i);
                    let value = get_string_arg!(args, i + 1);
                    state.keystore.insert(key, value);
                }

                Ok(RedisType::Integer { value: 1 })
            })
        });

        m.insert("PSETEX", Command {
            help: String::from("\
PSETEX key milliseconds value

Set the value and expiration in milliseconds of a key.
            "),
            f: Box::new(|state, args| {
                assert_n_args!{args, 3};
                let key = get_string_arg!(args, 0);
                let milliseconds = get_integer_arg!(args, 1);
                let value = get_string_arg!(args, 2);

                let expiration = SystemTime::now() + Duration::from_millis(milliseconds as u64);

                state.ttl.push(key.clone(), expiration);
                state.keystore.insert(key, value);
                
                Ok(RedisType::String { value: "OK".to_owned() })
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

        m.insert("SETEX", Command {
            help: String::from("\
SETEX key seconds value

Set the value and expiration of a key.
            "),
            f: Box::new(|state, args| {
                assert_n_args!{args, 3};
                let key = get_string_arg!(args, 0);
                let seconds = get_integer_arg!(args, 1);
                let value = get_string_arg!(args, 2);

                let expiration = SystemTime::now() + Duration::from_secs(seconds as u64);

                state.ttl.push(key.clone(), expiration);
                state.keystore.insert(key, value);
                
                Ok(RedisType::String { value: "OK".to_owned() })
            })
        }); 

        m.insert("SETNX", Command {
            help: String::from("\
SETNX key value

Set the value of a key, only if the key does not exist.
            "),
            f: Box::new(|state, args| {
                assert_n_args!{args, 2};
                let key = get_string_arg!(args, 0);
                let value = get_string_arg!(args, 1);

                if state.keystore.contains_key(&key) {
                    Ok(RedisType::Integer { value: 0 })
                } else {
                    state.keystore.insert(key, value);
                    Ok(RedisType::Integer { value: 1 })
                }
            })
        });

        m.insert("SETRANGE", Command {
            help: String::from("\
SETRANGE key offset value

Overwrite part of a string at key starting at the specified offset.
            "),
            f: Box::new(|state, args| {
                assert_n_args!{args, 3};
                let key = get_string_arg!(args, 0);
                let offset = get_integer_arg!(args, 1);
                let value = get_string_arg!(args, 2);

                let mut current_value = match state.keystore.get(&key) {
                    Some(value) => value.to_owned(),
                    None => String::new(),
                };

                if offset > current_value.len() as i64 {
                    current_value.push_str(&" ".repeat((offset - current_value.len() as i64) as usize));
                }

                current_value.replace_range(offset as usize.., &value);

                state.keystore.insert(key, current_value.clone());

                Ok(RedisType::Integer { value: current_value.len() as i64 })
            })
        });

        m.insert("STRLEN", Command {
            help: String::from("\
STRLEN key

Get the length of the value stored in a key.
            "),
            f: Box::new(|state, args| {
                assert_n_args!{args, 1};
                let key = get_string_arg!(args, 0);

                let value = match state.keystore.get(&key) {
                    Some(value) => value,
                    None => return Ok(RedisType::Integer { value: 0 }),
                };

                Ok(RedisType::Integer { value: value.len() as i64 })
            })
        });

        m
    };
}
