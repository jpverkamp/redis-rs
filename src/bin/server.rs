use redis_rs::RedisType;
use std::{process::ExitCode, str::FromStr};

fn main() -> Result<(), ExitCode> {
    let s = RedisType::from_str("$-1\r\n\r\n").unwrap();
    println!("{s:?}");
    println!("{s}");

    Ok(())
}
