use std::collections::BTreeMap;

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
            .map(|value| Row::from_key_value(key_column.clone(), value.into()))
            .collect();

        Self { rows }
    }

    pub fn row_count(&self) -> u64 {
        self.rows.len() as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    key_values: BTreeMap<String, String>,
}

impl Row {
    pub fn new(key_values: BTreeMap<String, String>) -> Self {
        Self { key_values }
    }

    pub fn from_key_value(key_column: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key_values: BTreeMap::from([(key_column.into(), value.into())]),
        }
    }

    pub fn key_values(&self) -> &BTreeMap<String, String> {
        &self.key_values
    }

    pub fn partition_key(&self, key_columns: &[String]) -> Option<String> {
        key_columns
            .iter()
            .map(|column| {
                self.key_values
                    .get(column)
                    .map(|value| format!("{column}={value}"))
            })
            .collect::<Option<Vec<_>>>()
            .map(|parts| parts.join("|"))
    }
}
