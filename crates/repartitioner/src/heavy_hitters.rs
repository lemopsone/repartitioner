use std::collections::BTreeMap;

use crate::manifest::HeavyKeyReason;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeavyHitter {
    pub key: String,
    pub frequency: u64,
    pub detection_reasons: Vec<HeavyKeyReason>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceSavingSummary {
    pub frequencies: BTreeMap<String, u64>,
    pub max_error: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SpaceSavingCounter {
    count: u64,
    error: u64,
}

pub fn space_saving<I>(keys: I, capacity: usize) -> SpaceSavingSummary
where
    I: IntoIterator<Item = String>,
{
    let capacity = capacity.max(1);
    let mut counters = BTreeMap::<String, SpaceSavingCounter>::new();
    let mut max_error = 0;

    for key in keys {
        if let Some(counter) = counters.get_mut(&key) {
            counter.count += 1;
            continue;
        }

        if counters.len() < capacity {
            counters.insert(key, SpaceSavingCounter { count: 1, error: 0 });
            continue;
        }

        let (min_key, min_counter) = counters
            .iter()
            .min_by_key(|(candidate_key, counter)| (counter.count, *candidate_key))
            .map(|(candidate_key, counter)| (candidate_key.clone(), *counter))
            .expect("space-saving counters should not be empty");
        counters.remove(&min_key);
        max_error = max_error.max(min_counter.count);
        counters.insert(
            key,
            SpaceSavingCounter {
                count: min_counter.count + 1,
                error: min_counter.count,
            },
        );
    }

    SpaceSavingSummary {
        frequencies: counters
            .into_iter()
            .map(|(key, counter)| (key, counter.count))
            .collect(),
        max_error,
    }
}

pub fn detect_heavy_hitter_candidates(
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
            detection_reasons: vec![HeavyKeyReason::AboveMeanThreshold],
        })
        .collect()
}

pub fn detect_final_heavy_keys(
    key_frequencies: &BTreeMap<String, u64>,
    alpha: f64,
    target_partition_rows: u64,
) -> Vec<HeavyHitter> {
    if key_frequencies.is_empty() {
        return Vec::new();
    }

    let total_frequency: u64 = key_frequencies.values().sum();
    let mean_frequency = total_frequency as f64 / key_frequencies.len() as f64;
    let mean_threshold = alpha * mean_frequency;

    key_frequencies
        .iter()
        .filter_map(|(key, frequency)| {
            let mut detection_reasons = Vec::new();
            if *frequency as f64 > mean_threshold {
                detection_reasons.push(HeavyKeyReason::AboveMeanThreshold);
            }
            if *frequency > target_partition_rows {
                detection_reasons.push(HeavyKeyReason::ExceedsTargetPartitionRows);
            }

            (!detection_reasons.is_empty()).then(|| HeavyHitter {
                key: key.clone(),
                frequency: *frequency,
                detection_reasons,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::HeavyKeyReason;

    #[test]
    fn detects_frequent_key_in_synthetic_skew() {
        let frequencies = BTreeMap::from([
            ("user_id=heavy".to_string(), 20),
            ("user_id=a".to_string(), 2),
            ("user_id=b".to_string(), 2),
            ("user_id=c".to_string(), 2),
        ]);

        let hitters = detect_heavy_hitter_candidates(&frequencies, 2.0);

        assert_eq!(
            hitters,
            vec![HeavyHitter {
                key: "user_id=heavy".to_string(),
                frequency: 20,
                detection_reasons: vec![HeavyKeyReason::AboveMeanThreshold],
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

        let hitters = detect_heavy_hitter_candidates(&frequencies, 2.0);

        assert!(hitters.is_empty());
    }

    #[test]
    fn detects_final_heavy_keys_that_only_exceed_target_partition_rows() {
        let frequencies = BTreeMap::from([
            ("user_id=a".to_string(), 2500),
            ("user_id=b".to_string(), 2500),
            ("user_id=c".to_string(), 2500),
            ("user_id=d".to_string(), 2500),
        ]);

        let hitters = detect_final_heavy_keys(&frequencies, 2.0, 100);

        assert_eq!(hitters.len(), 4);
        assert!(hitters.iter().all(
            |heavy| heavy.detection_reasons == vec![HeavyKeyReason::ExceedsTargetPartitionRows]
        ));
    }

    #[test]
    fn space_saving_keeps_synthetic_heavy_key_with_small_capacity() {
        let keys = std::iter::repeat_n("heavy".to_string(), 100)
            .chain((0..900).map(|index| format!("key_{index}")));

        let summary = space_saving(keys, 16);

        assert!(summary.frequencies.contains_key("heavy"));
        assert!(summary.frequencies["heavy"] >= 100);
        assert!(summary.max_error > 0);
    }
}
