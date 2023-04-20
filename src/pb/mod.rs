pub mod abi;

use abi::{command_request::RequestData, *};
use http::StatusCode;

use crate::KvError;

impl Kvpair {
    /// 创建一个新的 kv pair
    pub fn new(key: impl Into<String>, value: Value) -> Self{
        Self {
            key: key.into(),
            value: Some(value),
        }
    }
}

impl CommandRequest {
    /// 创建 HSET 命令
    pub fn new_hset(table: impl Into<String>, key: impl Into<String>, value: Value) -> Self {
        Self {
            request_data: Some(RequestData::Hset(
                Hset{
                    table: table.into(),
                    pair: Some(Kvpair::new(key, value)),
                }
            )),
        }
    }

    pub fn new_hget(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self { 
            request_data: Some(RequestData::Hget(
                Hget { 
                    table: table.into(), 
                    key: key.into(),
                 }
                )),
             }
    }

    pub fn new_hgetall(table: impl Into<String>) -> Self {
        Self { 
            request_data: Some(RequestData::Hgetall(
                Hgetall { 
                    table: table.into(),
                 })),
                 }
    }

    pub fn new_hdel(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self { 
            request_data: Some(RequestData::Hdel(
                Hdel { 
                    table: table.into(), 
                    key: key.into(), 
                }
            )) 
        }
    }

    pub fn new_hexist(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self { request_data: Some(RequestData::Hexist(
            Hexist { 
                table: table.into(), 
                key: key.into(),
            }
        ))
         }
    }

    pub fn new_hmget(table: impl Into<String>, keys: Vec<String>) -> Self {
        Self { request_data: Some(RequestData::Hmget(
            Hmget { 
                table: table.into(), 
                keys: keys,
             }
        )) }
    }

    pub fn new_hmset(table: impl Into<String>, pairs: Vec<Kvpair>) -> Self {
        Self { request_data: Some(RequestData::Hmset(
            Hmset { 
                table: table.into(), 
                pairs: pairs,
            }
        )) }
    }

    pub fn new_hmexist(table: impl Into<String>, keys: Vec<String>) -> Self {
        Self { request_data: Some(RequestData::Hmexist(
            Hmexist { 
                table: table.into(),
                 keys: keys,
                 }
        )) }
    }

    pub fn new_hmdel(table: impl Into<String>, keys: Vec<String>) -> Self {
        Self { request_data: Some(RequestData::Hmdel(
            Hmdel { 
                table:table.into(),
                 keys: keys,
                 }
        )) }
    }
}

/// 从 String 转换成 Value
impl From<String> for Value {
    fn from(s: String) -> Self {
        Self { value: Some(value::Value::String(s)) }
    }
}

/// 从 &str 转换成 Value
impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self { value: Some(value::Value::String(s.into())) }
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Self { value: Some(value::Value::Integer(i)) }
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Self { value: Some(value::Value::Bool(b)) }
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Self { value: Some(value::Value::Float(f)) }
    }
}

impl From<Value> for CommandResponse {
    fn from(v: Value) -> Self {
        Self { 
            status: StatusCode::OK.as_u16() as _, 
            values: vec![v], 
            ..Default::default()
        }
    }
}

impl From<Vec<Value>> for CommandResponse {
    fn from(v: Vec<Value>) -> Self {
        Self { 
            status: StatusCode::OK.as_u16() as _, 
            values: v, 
            ..Default::default()
        }
    }
}

impl From<Vec<Kvpair>> for CommandResponse {
    fn from(v: Vec<Kvpair>) -> Self {
        Self { 
            status: StatusCode::OK.as_u16() as _, 
            pairs: v, 
            ..Default::default()
        }
    }
}

impl From<KvError> for CommandResponse {
    fn from(e: KvError) -> Self {
        let mut res = Self {
            status: StatusCode::INTERNAL_SERVER_ERROR.as_u16() as _,
            message: e.to_string(),
            values: vec![],
            pairs: vec![],
        };
        match e {
            KvError::NotFound(_, _) => res.status = StatusCode::NOT_FOUND.as_u16() as _,
            KvError::InvalidCommand(_) => res.status = StatusCode::BAD_REQUEST.as_u16() as _,
            _ => {}
        }

        res
    }
}