//! Values and errors carried through the live event transport.
//!
//! These used to live in `hftbacktest::types`. They describe how a venue reports a failure and
//! the free-form value that travels with it, so they belong with the domain model rather than
//! with the backtest engine. `hftbacktest` re-exports them, which keeps its public API unchanged.

use std::collections::HashMap;

use anyhow::Error;
use bincode::{Decode, Encode};

#[derive(Clone, Debug, Decode, Encode)]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
    Empty,
}

impl Value {
    pub fn get_str(&self) -> Option<&str> {
        if let Value::String(val) = self {
            Some(val.as_str())
        } else {
            None
        }
    }

    pub fn get_int(&self) -> Option<i64> {
        if let Value::Int(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    pub fn get_float(&self) -> Option<f64> {
        if let Value::Float(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    pub fn get_bool(&self) -> Option<bool> {
        if let Value::Bool(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    pub fn get_list(&self) -> Option<&Vec<Value>> {
        if let Value::List(val) = self {
            Some(val)
        } else {
            None
        }
    }

    pub fn get_map(&self) -> Option<&HashMap<String, Value>> {
        if let Value::Map(val) = self {
            Some(val)
        } else {
            None
        }
    }
}

impl From<anyhow::Error> for Value {
    fn from(value: Error) -> Self {
        // todo!: improve this to deliver detailed error information.
        Value::String(value.to_string())
    }
}

/// Error conveyed through [`LiveEvent`].
#[derive(Clone, Debug, Decode, Encode)]
pub struct LiveError {
    pub kind: ErrorKind,
    pub value: Value,
}

impl LiveError {
    /// Constructs an instance of `LiveError`.
    pub fn new(kind: ErrorKind) -> LiveError {
        Self {
            kind,
            value: Value::Empty,
        }
    }

    /// Constructs an instance of `LiveError` with a value that contains detailed error information.
    pub fn with(kind: ErrorKind, value: Value) -> LiveError {
        Self { kind, value }
    }

    /// Returns a reference to the value that contains detailed error information.
    pub fn value(&self) -> &Value {
        &self.value
    }
}

/// Error type assigned to [`LiveError`].
#[derive(Clone, Copy, Eq, PartialEq, Debug, Decode, Encode)]
pub enum ErrorKind {
    ConnectionInterrupted,
    CriticalConnectionError,
    OrderError,
    Custom(i64),
}
