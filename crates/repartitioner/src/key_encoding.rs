#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KeyValue {
    Null,
    Utf8(String),
    Int64(i64),
    UInt64(u64),
    Boolean(bool),
    Date32(i32),
    TimestampMicros(i64),
    Decimal(String),
}

impl From<String> for KeyValue {
    fn from(value: String) -> Self {
        KeyValue::Utf8(value)
    }
}

pub fn encode_key_value(value: &KeyValue) -> String {
    match value {
        KeyValue::Null => "null".to_string(),
        KeyValue::Utf8(value) => format!("utf8:{}:{value}", value.len()),
        KeyValue::Int64(value) => format!("int64:{value}"),
        KeyValue::UInt64(value) => format!("uint64:{value}"),
        KeyValue::Boolean(value) => format!("bool:{value}"),
        KeyValue::Date32(value) => format!("date32:{value}"),
        KeyValue::TimestampMicros(value) => format!("timestamp_micros:{value}"),
        KeyValue::Decimal(value) => format!("decimal:{}:{value}", value.len()),
    }
}

pub fn encode_key_part(column: &str, value: &KeyValue) -> String {
    format!("{}:{column}#{}", column.len(), encode_key_value(value))
}

pub fn key_value_to_string(value: &KeyValue) -> Option<String> {
    match value {
        KeyValue::Null => None,
        KeyValue::Utf8(value) => Some(value.clone()),
        KeyValue::Int64(value) => Some(value.to_string()),
        KeyValue::UInt64(value) => Some(value.to_string()),
        KeyValue::Boolean(value) => Some(value.to_string()),
        KeyValue::Date32(value) => Some(value.to_string()),
        KeyValue::TimestampMicros(value) => Some(value.to_string()),
        KeyValue::Decimal(value) => Some(value.clone()),
    }
}

pub fn key_value_type_name(value: &KeyValue) -> &'static str {
    match value {
        KeyValue::Null => "null",
        KeyValue::Utf8(_) => "utf8",
        KeyValue::Int64(_) => "int64",
        KeyValue::UInt64(_) => "uint64",
        KeyValue::Boolean(_) => "boolean",
        KeyValue::Date32(_) => "date32",
        KeyValue::TimestampMicros(_) => "timestamp_micros",
        KeyValue::Decimal(_) => "decimal",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_null_and_empty_string_differently() {
        assert_eq!(encode_key_value(&KeyValue::Null), "null");
        assert_eq!(encode_key_value(&KeyValue::Utf8(String::new())), "utf8:0:");
    }

    #[test]
    fn encodes_values_with_delimiters_unambiguously() {
        assert_eq!(
            encode_key_part("user=id", &KeyValue::Utf8("a=b|c".to_string())),
            "7:user=id#utf8:5:a=b|c"
        );
    }
}
