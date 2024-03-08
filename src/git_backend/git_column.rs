use std::io::{Error, ErrorKind};

use gitql_ast::types::DataType;

use pgwire::api::portal::Format;
use pgwire::api::results::FieldInfo;
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};

use crate::git_backend::git_schema::TABLES_FIELDS_TYPES;

pub fn encode_column(string: &str, index: usize) -> PgWireResult<FieldInfo> {
    match TABLES_FIELDS_TYPES[string] {
        DataType::Text => Ok(FieldInfo::new(
            String::from(string),
            None,
            None,
            Type::TEXT,
            Format::UnifiedText.format_for(index),
        )),
        DataType::Integer => Ok(FieldInfo::new(
            String::from(string),
            None,
            None,
            Type::INT8,
            Format::UnifiedText.format_for(index),
        )),
        DataType::Float => Ok(FieldInfo::new(
            String::from(string),
            None,
            None,
            Type::FLOAT8,
            Format::UnifiedText.format_for(index),
        )),
        DataType::Boolean => Ok(FieldInfo::new(
            String::from(string),
            None,
            None,
            Type::BOOL,
            Format::UnifiedText.format_for(index),
        )),
        DataType::Time => Ok(FieldInfo::new(
            String::from(string),
            None,
            None,
            Type::TIME,
            Format::UnifiedText.format_for(index),
        )),
        DataType::Date => Ok(FieldInfo::new(
            String::from(string),
            None,
            None,
            Type::DATE,
            Format::UnifiedText.format_for(index),
        )),
        DataType::DateTime => Ok(FieldInfo::new(
            String::from(string),
            None,
            None,
            Type::TIME,
            Format::UnifiedText.format_for(index),
        )),
        _ => Err(PgWireError::IoError(Error::new(
            ErrorKind::Other,
            "Failed to get type of data",
        ))),
    }
}
