use std::collections::BTreeMap;

use crate::key_encoding::{encode_key_part, KeyValue};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dataset {
    pub rows: Vec<Row>,
}

impl Dataset {
    pub fn new(rows: Vec<Row>) -> Self {
        Self { rows }
    }

    pub fn empty() -> Self {
        Self { rows: Vec::new() }
    }

    pub fn from_key_values<V>(
        key_column: impl Into<String>,
        values: impl IntoIterator<Item = V>,
    ) -> Self
    where
        V: Into<String>,
    {
        let key_column = key_column.into();
        let rows = values
            .into_iter()
            .map(|value| Row::from_key_value(key_column.clone(), KeyValue::Utf8(value.into())))
            .collect();

        Self { rows }
    }

    pub fn row_count(&self) -> u64 {
        self.rows.len() as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    key_values: BTreeMap<String, KeyValue>,
}

impl Row {
    pub fn new(key_values: BTreeMap<String, KeyValue>) -> Self {
        Self { key_values }
    }

    pub fn from_key_value(key_column: impl Into<String>, value: KeyValue) -> Self {
        Self {
            key_values: BTreeMap::from([(key_column.into(), value)]),
        }
    }

    pub fn key_values(&self) -> &BTreeMap<String, KeyValue> {
        &self.key_values
    }

    pub fn partition_key(&self, key_columns: &[String]) -> Option<String> {
        key_columns
            .iter()
            .map(|column| {
                self.key_values
                    .get(column)
                    .map(|value| encode_key_part(column, value))
            })
            .collect::<Option<Vec<_>>>()
            .map(|parts| parts.join("|"))
    }
}
