import bisect
import math
import random
from collections import Counter
from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass(frozen=True)
class HeavyKeySpec:
    key: str
    fraction: float


def uniform_keys(rows: int, key_cardinality: int, seed: int, shuffle: bool = True) -> list[str]:
    validate_positive(rows=rows, key_cardinality=key_cardinality)
    keys = normal_key_pool(key_cardinality)
    values = [keys[index % key_cardinality] for index in range(rows)]
    return maybe_shuffle(values, seed, shuffle)


def single_heavy_keys(
    rows: int,
    key_cardinality: int,
    seed: int,
    heavy_key: str,
    heavy_fraction: float,
    tail_distribution: str,
    zipf_exponent: float,
    shuffle: bool = True,
) -> list[str]:
    return custom_heavy_keys(
        rows=rows,
        key_cardinality=key_cardinality,
        seed=seed,
        heavy_specs=[HeavyKeySpec(heavy_key, heavy_fraction)],
        tail_distribution=tail_distribution,
        zipf_exponent=zipf_exponent,
        shuffle=shuffle,
    )


def multi_heavy_keys(
    rows: int,
    key_cardinality: int,
    seed: int,
    heavy_key_count: int,
    heavy_fraction: float,
    heavy_weights: Sequence[float] | None,
    tail_distribution: str,
    zipf_exponent: float,
    shuffle: bool = True,
) -> list[str]:
    validate_positive(heavy_key_count=heavy_key_count)
    validate_fraction("heavy_fraction", heavy_fraction)
    keys = [f"heavy_{index:08d}" for index in range(heavy_key_count)]
    weights = normalized_weights(heavy_weights, heavy_key_count)
    specs = [
        HeavyKeySpec(key=key, fraction=heavy_fraction * weight)
        for key, weight in zip(keys, weights)
    ]
    return custom_heavy_keys(
        rows=rows,
        key_cardinality=key_cardinality,
        seed=seed,
        heavy_specs=specs,
        tail_distribution=tail_distribution,
        zipf_exponent=zipf_exponent,
        shuffle=shuffle,
    )


def custom_heavy_keys(
    rows: int,
    key_cardinality: int,
    seed: int,
    heavy_specs: Sequence[HeavyKeySpec],
    tail_distribution: str,
    zipf_exponent: float,
    shuffle: bool = True,
) -> list[str]:
    validate_positive(rows=rows, key_cardinality=key_cardinality)
    if not heavy_specs:
        raise ValueError("at least one heavy key must be configured")
    if len({spec.key for spec in heavy_specs}) != len(heavy_specs):
        raise ValueError("heavy keys must be unique")

    fractions = [spec.fraction for spec in heavy_specs]
    for index, fraction in enumerate(fractions):
        validate_fraction(f"heavy_specs[{index}].fraction", fraction)

    heavy_fraction = sum(fractions)
    if heavy_fraction > 1.0:
        raise ValueError("sum of heavy key fractions must not exceed 1.0")

    counts = allocate_counts(rows, fractions)
    values: list[str] = []
    for spec, count in zip(heavy_specs, counts):
        values.extend([spec.key] * count)

    tail_rows = rows - len(values)
    values.extend(
        tail_keys(
            rows=tail_rows,
            key_cardinality=key_cardinality,
            seed=seed + 17,
            distribution=tail_distribution,
            zipf_exponent=zipf_exponent,
            shuffle=False,
        )
    )
    return maybe_shuffle(values, seed, shuffle)


def zipf_keys(
    rows: int,
    key_cardinality: int,
    seed: int,
    exponent: float,
    shuffle: bool = True,
) -> list[str]:
    validate_positive(rows=rows, key_cardinality=key_cardinality)
    if exponent <= 0:
        raise ValueError("zipf exponent must be greater than zero")

    rng = random.Random(seed)
    keys = normal_key_pool(key_cardinality)
    cumulative: list[float] = []
    total = 0.0
    for rank in range(1, key_cardinality + 1):
        total += 1.0 / (rank**exponent)
        cumulative.append(total)

    values = []
    for _ in range(rows):
        pick = rng.random() * total
        values.append(keys[bisect.bisect_left(cumulative, pick)])
    return maybe_shuffle(values, seed + 31, shuffle)


def group_by_keys(
    rows: int,
    key_cardinality: int,
    seed: int,
    distribution: str,
    heavy_fraction: float,
    heavy_key_count: int,
    zipf_exponent: float,
    shuffle: bool = True,
) -> list[str]:
    if distribution == "uniform":
        return uniform_keys(rows, key_cardinality, seed, shuffle)
    if distribution == "single-heavy":
        return single_heavy_keys(
            rows,
            key_cardinality,
            seed,
            "group_heavy_00000000",
            heavy_fraction,
            "uniform",
            zipf_exponent,
            shuffle,
        )
    if distribution == "multi-heavy":
        return multi_heavy_keys(
            rows,
            key_cardinality,
            seed,
            heavy_key_count,
            heavy_fraction,
            None,
            "uniform",
            zipf_exponent,
            shuffle,
        )
    if distribution == "zipf":
        return zipf_keys(rows, key_cardinality, seed, zipf_exponent, shuffle)
    raise ValueError(f"unsupported group distribution: {distribution}")


def join_side_keys(
    rows: int,
    key_cardinality: int,
    seed: int,
    distribution: str,
    heavy_fraction: float,
    heavy_key_count: int,
    zipf_exponent: float,
    shuffle: bool = True,
) -> list[str]:
    if distribution == "single-heavy":
        return single_heavy_keys(
            rows,
            key_cardinality,
            seed,
            "join_heavy_00000000",
            heavy_fraction,
            "uniform",
            zipf_exponent,
            shuffle,
        )
    if distribution == "multi-heavy":
        return multi_heavy_keys(
            rows,
            key_cardinality,
            seed,
            heavy_key_count,
            heavy_fraction,
            None,
            "uniform",
            zipf_exponent,
            shuffle,
        )
    if distribution == "zipf":
        return zipf_keys(rows, key_cardinality, seed, zipf_exponent, shuffle)
    if distribution == "uniform":
        return uniform_keys(rows, key_cardinality, seed, shuffle)
    raise ValueError(f"unsupported join distribution: {distribution}")


def key_frequencies(keys: Iterable[str]) -> Counter[str]:
    return Counter(keys)


def parse_heavy_specs(value: str) -> list[HeavyKeySpec]:
    specs = []
    for item in split_csv(value):
        if ":" not in item:
            raise ValueError("heavy specs must use KEY:FRACTION entries")
        key, raw_fraction = item.split(":", 1)
        key = key.strip()
        if not key:
            raise ValueError("heavy key name must not be empty")
        specs.append(HeavyKeySpec(key=key, fraction=float(raw_fraction)))
    if not specs:
        raise ValueError("heavy spec list must not be empty")
    return specs


def parse_weights(value: str | None) -> list[float] | None:
    if value is None:
        return None
    weights = [float(item) for item in split_csv(value)]
    if not weights:
        raise ValueError("weights must not be empty")
    return weights


def normal_key_pool(key_cardinality: int) -> list[str]:
    validate_positive(key_cardinality=key_cardinality)
    return [f"key_{index:08d}" for index in range(key_cardinality)]


def tail_keys(
    rows: int,
    key_cardinality: int,
    seed: int,
    distribution: str,
    zipf_exponent: float,
    shuffle: bool,
) -> list[str]:
    validate_positive(key_cardinality=key_cardinality)
    if rows < 0:
        raise ValueError("tail rows must not be negative")
    if rows == 0:
        return []
    if distribution == "uniform":
        return uniform_keys(rows, key_cardinality, seed, shuffle)
    if distribution == "zipf":
        return zipf_keys(rows, key_cardinality, seed, zipf_exponent, shuffle)
    raise ValueError("tail distribution must be either 'uniform' or 'zipf'")


def allocate_counts(total: int, fractions: Sequence[float]) -> list[int]:
    raw_counts = [total * fraction for fraction in fractions]
    counts = [math.floor(count) for count in raw_counts]
    remaining = round(sum(raw_counts)) - sum(counts)
    order = sorted(
        range(len(raw_counts)),
        key=lambda index: raw_counts[index] - counts[index],
        reverse=True,
    )
    for index in order[:remaining]:
        counts[index] += 1
    return counts


def normalized_weights(weights: Sequence[float] | None, expected_count: int) -> list[float]:
    if weights is None:
        return [1.0 / expected_count] * expected_count
    if len(weights) != expected_count:
        raise ValueError("number of heavy weights must match --heavy-keys")
    if any(weight < 0 for weight in weights):
        raise ValueError("heavy weights must not be negative")
    total = sum(weights)
    if total <= 0:
        raise ValueError("sum of heavy weights must be greater than zero")
    return [weight / total for weight in weights]


def maybe_shuffle(values: list[str], seed: int, shuffle: bool) -> list[str]:
    if shuffle:
        random.Random(seed).shuffle(values)
    return values


def split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def validate_positive(**values: int) -> None:
    for name, value in values.items():
        if value <= 0:
            raise ValueError(f"{name} must be greater than zero")


def validate_fraction(name: str, value: float) -> None:
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{name} must be between 0.0 and 1.0")
