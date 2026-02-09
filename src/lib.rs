use std::{fmt::Display, str::FromStr};

// Force output as bulk string rather than simple string
// Default to false so simple strings are used when appropriate
pub static mut ALWAYS_USE_BULK_STRING: bool = false;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum RedisType {
    NullString,
    NullArray,
    String { value: String },
    Error { value: String },
    Integer { value: i64 },
    Array { value: Vec<RedisType> },
}

impl From<Option<String>> for RedisType {
    fn from(value: Option<String>) -> Self {
        match value {
            Some(value) => RedisType::String {
                value: value.to_owned(),
            },
            None => RedisType::NullString,
        }
    }
}

impl From<String> for RedisType {
    fn from(value: String) -> Self {
        RedisType::String {
            value: value.to_owned(),
        }
    }
}

impl From<i64> for RedisType {
    fn from(value: i64) -> Self {
        RedisType::Integer { value }
    }
}

impl From<Vec<RedisType>> for RedisType {
    fn from(value: Vec<RedisType>) -> Self {
        RedisType::Array { value }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum RedisTypeParseError {
    MissingPrefix,
    InvalidPrefix,
    InvalidSuffix,
    InvalidArrayLength,
    LeftOverData,
}

impl FromStr for RedisType {
    type Err = RedisTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        fn parse(s: &str) -> Result<(&str, RedisType), RedisTypeParseError> {
            let bytes = s.as_bytes();

            if s.len() == 0 {
                return Err(RedisTypeParseError::MissingPrefix);
            }

            if !s.contains("\r\n") {
                return Err(RedisTypeParseError::InvalidSuffix);
            }

            let crlf = s.find("\r\n").unwrap();
            let payload = &s[1..crlf];
            let mut rest = &s[crlf + 2..];

            match bytes[0] as char {
                '+' => Ok((
                    rest,
                    RedisType::String {
                        value: String::from(payload),
                    },
                )),
                '-' => Ok((
                    rest,
                    RedisType::Error {
                        value: String::from(payload),
                    },
                )),
                // TODO: Better error handling for failing to parse
                ':' => Ok((
                    rest,
                    RedisType::Integer {
                        value: String::from(payload).parse::<i64>().unwrap(),
                    },
                )),
                '*' => {
                    // TODO: Validate that array length parsed correctly
                    let len = String::from(payload).parse::<i64>().unwrap();

                    // Special case: bulk string with -1 length is actually a 'null' array
                    // This is historical
                    if len < 0 {
                        Ok((rest, RedisType::NullArray))
                    } else {
                        let mut value = Vec::new();

                        for _ in 0..len {
                            let (next, el) = parse(rest)?;
                            value.push(el);
                            rest = next;
                        }

                        Ok((rest, RedisType::Array { value }))
                    }
                }
                '$' => {
                    let len = String::from(payload).parse::<i64>().unwrap(); // TODO: Validate

                    // Special case: bulk string with -1 length is actually a 'null' value
                    // I'm just treating any negative as this case
                    if len < 0 {
                        Ok((rest, RedisType::NullString))
                    } else {
                        let len = len as usize;
                        let value = String::from(&rest[0..len]);
                        rest = &rest[len + 2..];

                        Ok((rest, RedisType::String { value }))
                    }
                }
                _ => Err(RedisTypeParseError::InvalidPrefix),
            }
        }

        match parse(s) {
            Ok((rest, result)) if rest.len() == 0 => Ok(result),
            Ok(_) => Err(RedisTypeParseError::LeftOverData),
            Err(e) => Err(e),
        }
    }
}

impl Display for RedisType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let crlf = "\r\n";

        match self {
            RedisType::NullString => write!(f, "$-1{}", crlf),
            RedisType::NullArray => write!(f, "*-1{}", crlf),
            RedisType::String { value } => {
                if value.len() == 0 {
                    // Empty strings
                    write!(f, "$0{}{}", crlf, crlf)
                } else if unsafe { ALWAYS_USE_BULK_STRING }
                    || (value
                        .chars()
                        .any(|c| c.is_control() || c == '\r' || c == '\n'))
                {
                    // Bulk strings
                    // TODO: Are there any other interesting cases?
                    write!(f, "${}{}{}{}", value.len(), crlf, value, crlf)
                } else {
                    // Simple strings
                    write!(f, "+{}{}", value, crlf)
                }
            }
            RedisType::Error { value } => write!(f, "-{}{}", value, crlf),
            RedisType::Integer { value } => write!(f, ":{}{}", value, crlf),
            RedisType::Array { value } => {
                write!(f, "*{}{}", value.len(), crlf)?;

                for el in value {
                    write!(f, "{}", el)?;
                }

                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::RedisType;

    macro_rules! make_tests {
        ($name:tt, $string:expr, $redis:expr) => {
            paste::item! {
                #[test]
                fn [< test_ $name _from_str >]() {
                    assert_eq!(RedisType::from_str($string).unwrap(), $redis);
                }

                #[test]
                fn [< test_ $name _to_string >]() {
                    assert_eq!($redis.to_string(), $string);
                }

                #[test]
                fn [< test_ $name _inverse_to_from  >]() {
                    assert_eq!(RedisType::from_str(&$redis.to_string()).unwrap(), $redis);
                }

                #[test]
                fn [< test_ $name _inverse_from_to >]() {
                    assert_eq!(RedisType::from_str($string).unwrap().to_string(), $string);
                }
            }
        };
    }

    make_tests!(null, "$-1\r\n", RedisType::NullString);
    make_tests!(null_array, "*-1\r\n", RedisType::NullArray);

    make_tests!(
        simple_string,
        "+Hello world\r\n",
        RedisType::String {
            value: "Hello world".to_owned()
        }
    );

    make_tests!(
        empty_string,
        "$0\r\n\r\n",
        RedisType::String {
            value: "".to_owned()
        }
    );

    make_tests!(
        bulk_string,
        "$5\r\nYo\0\r\n\r\n",
        RedisType::String {
            value: "Yo\0\r\n".to_owned()
        }
    );

    make_tests!(
        err,
        "-ERR Goodbye world\r\n",
        RedisType::Error {
            value: "ERR Goodbye world".to_owned()
        }
    );

    make_tests!(
        positive_integer,
        ":42\r\n",
        RedisType::Integer { value: 42 }
    );

    make_tests!(
        negative_integer,
        ":-1337\r\n",
        RedisType::Integer { value: -1337 }
    );

    make_tests!(
        array,
        "*3\r\n+Hello world\r\n:42\r\n-ERR Goodbye world\r\n",
        RedisType::Array {
            value: vec![
                RedisType::String {
                    value: "Hello world".to_owned()
                },
                RedisType::Integer { value: 42 },
                RedisType::Error {
                    value: "ERR Goodbye world".to_owned()
                },
            ]
        }
    );

    make_tests!(
        null_in_array,
        "*3\r\n$3\r\nYo\0\r\n$-1\r\n-ERR Goodbye world\r\n",
        RedisType::Array {
            value: vec![
                RedisType::String {
                    value: "Yo\0".to_owned()
                },
                RedisType::NullString,
                RedisType::Error {
                    value: "ERR Goodbye world".to_owned()
                },
            ]
        }
    );

    make_tests!(
        nested_array,
        "*4\r\n+Hello world\r\n:42\r\n-ERR Goodbye world\r\n*3\r\n+Hello world\r\n:42\r\n-ERR Goodbye world\r\n",
        RedisType::Array {
            value: vec![
                RedisType::String {
                    value: "Hello world".to_owned()
                },
                RedisType::Integer { value: 42 },
                RedisType::Error {
                    value: "ERR Goodbye world".to_owned()
                },
                RedisType::Array {
                    value: vec![
                        RedisType::String {
                            value: "Hello world".to_owned()
                        },
                        RedisType::Integer { value: 42 },
                        RedisType::Error {
                            value: "ERR Goodbye world".to_owned()
                        },
                    ]
                }
            ]
        }
    );
}
