use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeavyHitter {
    pub key: String,
    pub frequency: u64,
}

pub fn detect_heavy_hitters(
    key_frequencies: &BTreeMap<String, u64>,
    alpha: f64,
) -> Vec<HeavyHitter> {
    if key_frequencies.is_empty() {
        return Vec::new();
    }

    let total_frequency: u64 = key_frequencies.values().sum();
    let mean_frequency = total_frequency as f64 / key_frequencies.len() as f64;
    let threshold = alpha * mean_frequency;

    key_frequencies
        .iter()
        .filter(|(_, frequency)| **frequency as f64 > threshold)
        .map(|(key, frequency)| HeavyHitter {
            key: key.clone(),
            frequency: *frequency,
        })
        .collect()
}
