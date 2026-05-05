pub const HASH_FUNCTION_NAME: &str = "fnv1a64_seeded";

pub fn hash_bytes_seeded(seed: u64, bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64 ^ seed;

    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    hash
}

pub fn hash_key(seed: u64, key: &str) -> u64 {
    hash_bytes_seeded(seed, key.as_bytes())
}

pub fn partition_id(key: &str, partition_count: usize, seed: u64) -> usize {
    if partition_count == 0 {
        return 0;
    }

    (hash_key(seed, key) as usize) % partition_count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_key_and_seed_always_map_to_same_partition() {
        let first = partition_id("user_id=42", 16, 42);
        let second = partition_id("user_id=42", 16, 42);

        assert_eq!(first, second);
    }

    #[test]
    fn different_seeds_can_produce_different_partition_assignments() {
        let has_different_assignment = (0..128).any(|index| {
            let key = format!("user_id={index}");
            partition_id(&key, 64, 42) != partition_id(&key, 64, 43)
        });

        assert!(has_different_assignment);
    }
}
