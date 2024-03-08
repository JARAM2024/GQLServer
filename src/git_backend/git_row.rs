use std::sync::Arc;

use futures::{stream, Stream};

use gitql_ast::object::GitQLObject;
use gitql_ast::value::Value;

use pgwire::api::results::{DataRowEncoder, FieldInfo};
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;

pub fn encode_row(
    groups: &GitQLObject,
    fields_info: Arc<Vec<FieldInfo>>,
) -> impl Stream<Item = PgWireResult<DataRow>> {
    let mut elements = vec![];

    for row in &groups.groups[0].rows {
        let mut encoder = DataRowEncoder::new(fields_info.clone().into());
        for value in row.values.iter() {
            match value {
                Value::Text(text) => encoder.encode_field(&text).unwrap(),
                Value::Integer(int) => encoder.encode_field(&int).unwrap(),
                Value::Float(float) => encoder.encode_field(&float).unwrap(),
                Value::Boolean(bool) => encoder.encode_field(&bool).unwrap(),
                Value::Time(time) => encoder.encode_field(&time).unwrap(),
                Value::Date(date) => encoder.encode_field(&date).unwrap(),
                Value::DateTime(date) => encoder.encode_field(&date).unwrap(),
                _ => encoder.encode_field(&None::<i8>).unwrap(),
            }
        }
        elements.push(encoder.finish());
    }

    stream::iter(elements.into_iter())
}
