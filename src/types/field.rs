use crate::common::{Error, Result};
use crate::errinput;
use crate::types::DataType;
use serde::{Deserialize, Serialize};
use std::ops::{Add, Div, Mul, Rem, Sub};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Field {
    Null,
    Boolean(bool),
    Integer(i32),
    Float(f32),
    String(String),
}

impl PartialEq for Field {
    fn eq(&self, other: &Field) -> bool {
        match self {
            Field::Null => match other {
                Field::Null => true,
                _ => false,
            },
            Field::Boolean(b) => match other {
                Field::Boolean(b2) => b == b2,
                _ => false,
            },
            Field::Integer(i) => match other {
                Field::Integer(i2) => i == i2,
                _ => false,
            },
            // match on NaN as well as equality
            Field::Float(f) => match other {
                Field::Float(f2) => (f == f2) || (f.is_nan() && f2.is_nan()),
                _ => false,
            },
            Field::String(s) => match other {
                Field::String(s2) => s == s2,
                _ => false,
            },
        }
    }
}

impl Eq for Field {} // implement Eq trait for Field, uses PartialEq

impl std::hash::Hash for Field {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Field::Null => 0.hash(state),
            Field::Boolean(b) => b.hash(state),
            Field::Integer(i) => i.hash(state),
            Field::Float(f) => {
                if f.is_nan() {
                    0.hash(state);
                } else {
                    f.to_bits().hash(state);
                }
            }
            Field::String(s) => s.hash(state),
        }
    }
}

// for use in sorting
impl Ord for Field {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Field::Null, Field::Null) => std::cmp::Ordering::Equal,
            (Field::Null, _) => std::cmp::Ordering::Less,
            (_, Field::Null) => std::cmp::Ordering::Greater,
            (Field::Boolean(b), Field::Boolean(b2)) => b.cmp(b2),
            (Field::Integer(i), Field::Integer(i2)) => i.cmp(i2),

            (Field::Float(f), Field::Float(f2)) => match (f.is_nan(), f2.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => f.partial_cmp(f2).unwrap_or(std::cmp::Ordering::Equal),
            },
            (Field::String(s), Field::String(s2)) => s.cmp(s2),
            (Field::Boolean(_), _) => std::cmp::Ordering::Less,
            (Field::Integer(_), Field::Boolean(_)) => std::cmp::Ordering::Greater,
            (Field::Integer(_), _) => std::cmp::Ordering::Less,
            (Field::Float(_), Field::Boolean(_)) => std::cmp::Ordering::Greater,
            (Field::Float(_), Field::Integer(_)) => std::cmp::Ordering::Greater,
            (Field::Float(_), _) => std::cmp::Ordering::Less,
            (Field::String(_), _) => std::cmp::Ordering::Greater,
        }
    }
}

impl PartialOrd for Field {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Add for Field {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let tmp = self.checked_add(&other);
        tmp.unwrap_or_else(|_e| Field::Null)
    }
}

impl Sub for Field {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        let tmp = self.checked_sub(&other);
        tmp.unwrap_or_else(|_e| Field::Null)
    }
}

impl Mul for Field {
    type Output = Self;

    fn mul(self, other: Self) -> Self {
        let tmp = self.checked_mul(&other);
        tmp.unwrap_or_else(|_e| Field::Null)
    }
}

impl Div for Field {
    type Output = Self;

    fn div(self, other: Self) -> Self {
        let tmp = self.checked_div(&other);
        tmp.unwrap_or_else(|_e| Field::Null)
    }
}

impl Rem for Field {
    type Output = Self;

    fn rem(self, other: Self) -> Self {
        let tmp = self.checked_mod(&other);
        tmp.unwrap_or_else(|_e| Field::Null)
    }
}

impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Boolean(true) => f.write_str("TRUE"),
            Self::Boolean(false) => f.write_str("FALSE"),
            Self::Integer(integer) => integer.fmt(f),
            Self::Float(float) => write!(f, "{float:?}"),
            Self::String(string) => write!(f, "'{}'", string.escape_debug()),
        }
    }
}

impl From<f32> for Field {
    fn from(v: f32) -> Self {
        Field::Float(v)
    }
}

impl From<i32> for Field {
    fn from(v: i32) -> Self {
        Field::Integer(v)
    }
}

impl From<String> for Field {
    fn from(v: String) -> Self {
        Field::String(v)
    }
}

impl From<&str> for Field {
    fn from(v: &str) -> Self {
        Field::String(v.to_owned())
    }
}

impl From<bool> for Field {
    fn from(v: bool) -> Self {
        Field::Boolean(v)
    }
}

impl Field {
    // default constructor
    pub fn new(d: DataType) -> Field {
        match d {
            DataType::Bool => Field::from(false),
            DataType::Int => Field::from(0i32),
            DataType::Float => Field::from(0.0),
            DataType::Text => Field::from("".to_string()),
            DataType::Invalid => Field::Null,
        }
    }
    pub fn get_type(&self) -> DataType {
        match self {
            Field::Null => DataType::Invalid,
            Field::Boolean(_) => DataType::Bool,
            Field::Integer(_) => DataType::Int,
            Field::Float(_) => DataType::Float,
            Field::String(_) => DataType::Text,
        }
    }
    // size in bytes
    pub fn get_size(&self) -> u16 {
        match self {
            Field::Null => 0,
            Field::Boolean(_) => 1,
            Field::Integer(_) => 4,
            Field::Float(_) => 4,
            Field::String(s) => s.len() as u16,
        }
    }
    pub fn to_string(&self) -> String {
        match self {
            Field::Null => "NULL".to_string(),
            Field::Boolean(b) => b.to_string(),
            Field::Integer(i) => i.to_string(),
            Field::Float(f) => f.to_string(),
            Field::String(s) => s.clone(),
        }
    }
    pub fn checked_add(&self, other: &Field) -> Result<Field> {
        use Field::*;
        match (&self, other) {
            (Integer(lhs), Integer(rhs)) => match lhs.checked_add(*rhs) {
                Some(v) => Ok(Integer(v)),
                None => Result::from(Error::OverflowError),
            },
            (Integer(lhs), Float(rhs)) => {
                let result = (*lhs as f32) + rhs;
                Ok(Float(result))
            }
            (Float(lhs), Integer(rhs)) => Ok(Float(lhs + (*rhs as f32))),
            (Float(lhs), Float(rhs)) => Ok(Float(lhs + rhs)),
            (Null, Integer(_)) | (Null, Float(_)) => Ok(Null),
            (Integer(_), Null) | (Float(_), Null) => Ok(Null),
            (Null, Null) => Ok(Null),
            _ => {
                let msg = format!("Cannot add {:?} and {:?}", self, other);
                Result::from(Error::InvalidData(msg))
            }
        }
    }

    pub fn checked_sub(&self, other: &Field) -> Result<Field> {
        use Field::*;
        match (&self, other) {
            (Integer(lhs), Integer(rhs)) => match lhs.checked_sub(*rhs) {
                Some(v) => Ok(Integer(v)),
                None => Result::from(Error::OverflowError),
            },
            (Integer(lhs), Float(rhs)) => {
                let result = (*lhs as f32) - rhs;
                Ok(Float(result))
            }
            (Float(lhs), Integer(rhs)) => Ok(Float(lhs - (*rhs as f32))),
            (Float(lhs), Float(rhs)) => Ok(Float(lhs - rhs)),
            (Null, Integer(_)) | (Null, Float(_)) => Ok(Null),
            (Integer(_), Null) | (Float(_), Null) => Ok(Null),
            (Null, Null) => Ok(Null),
            _ => {
                let msg = format!("Cannot subtract {:?} and {:?}", self, other);
                Result::from(Error::InvalidData(msg))
            }
        }
    }

    pub fn checked_mul(&self, other: &Field) -> Result<Field> {
        use Field::*;
        match (&self, other) {
            (Integer(lhs), Integer(rhs)) => match lhs.checked_mul(*rhs) {
                Some(v) => Ok(Integer(v)),
                None => Result::from(Error::OverflowError),
            },
            (Integer(lhs), Float(rhs)) => {
                let result = (*lhs as f32) * rhs;
                Ok(Float(result))
            }
            (Float(lhs), Integer(rhs)) => Ok(Float(lhs * (*rhs as f32))),
            (Float(lhs), Float(rhs)) => Ok(Float(lhs * rhs)),
            (Null, Integer(_)) | (Null, Float(_)) => Ok(Null),
            (Integer(_), Null) | (Float(_), Null) => Ok(Null),
            (Null, Null) => Ok(Null),
            _ => {
                let msg = format!("Cannot multiply {:?} and {:?}", self, other);
                Result::from(Error::InvalidData(msg))
            }
        }
    }

    pub fn checked_div(&self, other: &Field) -> Result<Field> {
        use Field::*;
        match (&self, other) {
            (Integer(lhs), Integer(rhs)) => match lhs.checked_div(*rhs) {
                Some(v) => Ok(Integer(v)),
                None => Result::from(Error::OverflowError),
            },
            (Integer(lhs), Float(rhs)) => {
                let result = (*lhs as f32) / rhs;
                Ok(Float(result))
            }
            (Float(lhs), Integer(rhs)) => Ok(Float(lhs / (*rhs as f32))),
            (Float(lhs), Float(rhs)) => Ok(Float(lhs / rhs)),
            (Null, Integer(_)) | (Null, Float(_)) => Ok(Null),
            (Integer(_), Null) | (Float(_), Null) => Ok(Null),
            (Null, Null) => Ok(Null),
            _ => {
                let msg = format!("Cannot divide {:?} and {:?}", self, other);
                Result::from(Error::InvalidData(msg))
            }
        }
        //  _ =>  Null,
    }

    /// Exponentiates two values. Errors when invalid.
    pub fn checked_pow(&self, other: &Self) -> Result<Self> {
        use Field::*;
        Ok(match (self, other) {
            (Integer(lhs), Integer(rhs)) if *rhs >= 0 => {
                let rhs = (*rhs)
                    .try_into()
                    .or_else(|_| errinput!("integer overflow"))?;
                match lhs.checked_pow(rhs) {
                    Some(i) => Integer(i),
                    None => return errinput!("integer overflow"),
                }
            }
            (Integer(lhs), Integer(rhs)) => Float((*lhs as f32).powf(*rhs as f32)),
            (Integer(lhs), Float(rhs)) => Float((*lhs as f32).powf(*rhs)),
            (Float(lhs), Integer(rhs)) => Float((lhs).powi(*rhs as i32)),
            (Float(lhs), Float(rhs)) => Float((lhs).powf(*rhs)),
            (Integer(_) | Float(_), Null) => Null,
            (Null, Integer(_) | Float(_) | Null) => Null,
            (lhs, rhs) => return errinput!("can't exponentiate {lhs} and {rhs}"),
        })
    }

    pub fn checked_mod(&self, other: &Field) -> Result<Field> {
        use Field::*;
        match (&self, other) {
            (Integer(lhs), Integer(rhs)) => match lhs.checked_rem(*rhs) {
                Some(v) => Ok(Integer(v)),
                None => Result::from(Error::OverflowError),
            },
            (Integer(lhs), Float(rhs)) => {
                let result = (*lhs as f32) % rhs;
                Ok(Float(result))
            }
            (Float(lhs), Integer(rhs)) => Ok(Float(lhs % (*rhs as f32))),
            (Float(lhs), Float(rhs)) => Ok(Float(lhs % rhs)),
            (Null, Integer(_)) | (Null, Float(_)) => Ok(Null),
            (Integer(_), Null) | (Float(_), Null) => Ok(Null),
            (Null, Null) => Ok(Null),
            _ => {
                let msg = format!("Cannot mod {:?} and {:?}", self, other);
                Result::from(Error::InvalidData(msg))
            }
        }
        //  _ =>  Null,
    }

    pub fn is_null(&self) -> bool {
        match self {
            Field::Null => true,
            _ => false,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Field::Null => vec![0],
            Field::Boolean(b) => {
                if *b {
                    vec![1]
                } else {
                    vec![0]
                }
            }
            Field::Integer(i) => i.to_le_bytes().to_vec(),
            Field::Float(f) => f.to_le_bytes().to_vec(),
            Field::String(s) => s.as_bytes().to_vec(),
        }
    }

    pub fn deserialize(data: &[u8], data_type: DataType) -> Field {
        match data_type {
            DataType::Bool => {
                if data[0] == 0 {
                    Field::Boolean(false)
                } else {
                    Field::Boolean(true)
                }
            }
            DataType::Int => Field::Integer(i32::from_le_bytes(data.try_into().unwrap())),
            DataType::Float => Field::Float(f32::from_le_bytes(data.try_into().unwrap())),
            DataType::Text => Field::String(String::from_utf8(data.to_vec()).unwrap()),
            _ => Field::Null,
        }
    }

    /// Returns true if the value is undefined (NULL or NaN).
    pub fn is_undefined(&self) -> bool {
        *self == Self::Null || matches!(self, Self::Float(f) if f.is_nan())
    }
}

/// A column label, used in query results and plans.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Label {
    /// No label.
    None,
    /// An unqualified column name.
    Unqualified(String),
    /// A fully qualified table/column name.
    Qualified(String, String),
}

impl std::fmt::Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, ""),
            Self::Unqualified(name) => write!(f, "{name}"),
            Self::Qualified(table, column) => write!(f, "{table}.{column}"),
        }
    }
}

impl Label {
    /// Formats the label as a short column header.
    pub fn as_header(&self) -> &str {
        match self {
            Self::Qualified(_, column) | Self::Unqualified(column) => column.as_str(),
            Self::None => "?",
        }
    }
}

impl From<Option<String>> for Label {
    fn from(name: Option<String>) -> Self {
        name.map(Label::Unqualified).unwrap_or(Label::None)
    }
}

#[allow(unused_imports)]
mod tests {
    use crate::types::field::Field;
    use crate::types::DataType;

    #[test]
    pub fn test_init() {
        let v = Field::Null;
        assert_eq!(v, Field::Null);
    }

    #[test]
    pub fn test_field_by_type() {
        let v = Field::Integer(10);
        match v {
            Field::Integer(i) => assert_eq!(i, 10),
            _ => panic!("Expected Integer"),
        }

        let v = Field::Float(7.0);
        match v {
            Field::Float(f) => assert_eq!(f, 7.0),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    pub fn test_addition() {
        let lhs = Field::Integer(10);
        let rhs = Field::Integer(7);

        let result = lhs + rhs;

        match result {
            Field::Integer(i) => assert_eq!(i, 17),
            _ => panic!("Expected Integer"),
        }

        let lhs = Field::Float(10.0);
        let rhs = Field::Float(7.0);

        let result = lhs + rhs;

        match result {
            Field::Float(f) => assert_eq!(f, 17.0),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    pub fn test_comparison() {
        let lhs = Field::Integer(10);
        let rhs = Field::Integer(7);

        assert!(lhs > rhs);

        let lhs = Field::Float(10.0);
        let rhs = Field::Float(7.0);

        assert!(lhs > rhs);
    }

    #[test]
    pub fn test_serialization() {
        let v = Field::Integer(10);
        let serialized = v.serialize();
        let deserialized = Field::deserialize(&serialized, DataType::Int);

        assert_eq!(v, deserialized);

        let s = Field::String("testing, 1, 2, 3".to_string());
        let serialized = s.serialize();
        let deserialized = Field::deserialize(&serialized, DataType::Text);
        assert_eq!(s, deserialized);
    }
}
