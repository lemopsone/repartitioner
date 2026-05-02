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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_frequent_key_in_synthetic_skew() {
        let frequencies = BTreeMap::from([
            ("user_id=heavy".to_string(), 20),
            ("user_id=a".to_string(), 2),
            ("user_id=b".to_string(), 2),
            ("user_id=c".to_string(), 2),
        ]);

        let hitters = detect_heavy_hitters(&frequencies, 2.0);

        assert_eq!(
            hitters,
            vec![HeavyHitter {
                key: "user_id=heavy".to_string(),
                frequency: 20,
            }]
        );
    }

    #[test]
    fn ignores_uniform_distribution() {
        let frequencies = BTreeMap::from([
            ("user_id=a".to_string(), 5),
            ("user_id=b".to_string(), 5),
            ("user_id=c".to_string(), 5),
        ]);

        let hitters = detect_heavy_hitters(&frequencies, 2.0);

        assert!(hitters.is_empty());
    }
}
