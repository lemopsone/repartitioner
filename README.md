# Repartitioner

Прототип для дипломного проекта по методу адаптивного партиционирования данных
для предотвращения перекоса в распределённых системах обработки больших данных.

Rust-ядро реализовано как внешний инструмент предобработки. Оно читает входной
dataset, считает статистики ключей и партиций, строит адаптивный план
партиционирования, при необходимости физически переписывает dataset и сохраняет
JSON-метаданные для последующих экспериментов. Apache Spark используется только
в Python-слое benchmark-ов.

## Область поддержки

Полностью поддержано в прототипе:

- adaptive key placement с selective salting;
- no-op решение, если физическая перезапись не нужна;
- Parquet input и Parquet output;
- Spark-compatible Hive-style директории `rp_partition=<id>`;
- технические колонки `_rp_partition_id`, `_rp_salt`, `_rp_is_heavy_key`;
- Spark groupBy benchmark в режимах baseline, physical-only и method-aware.

Частично поддержано или является экспериментальным:

- адаптер CSV-ввода для статистики и планирования;
- сценарий CSV-ввод -> Parquet-вывод;
- method-aware Spark join benchmark для безопасных single-key сценариев;
- approximate heavy hitter detection только для выявления heavy keys.

Не реализовано как самостоятельная функциональность:

- CSV-вывод;
- изменение Spark internals;
- полное автоматическое переписывание Spark-операторов;
- standalone стратегия планирования `file_size_balancing`;
- полноценное split/coalesce планирование входных файлов.

Rolling выходных файлов по `storage.target_file_size_mb` реализован как
оптимизация Parquet writer-а, а не как отдельный метод балансировки.

## Минимальный конфиг

```yaml
dataset:
  input: "./data/input.parquet"
  input_format: "parquet"

output:
  path: "./data/output_partitioned"
  format: "parquet"

partitioning:
  key_columns: ["user_id"]
  target_partition_size_mb: 128
  max_partitions: 128
  strategy: "adaptive_hash_salt"
  heavy_key_alpha: 2.0
  seed: 42

job:
  type: "group_by"
  downstream_engine: "spark"

resources:
  local_threads: 8
  memory_limit_mb: 4096
```

Legacy-конфиги с `dataset.output` и `dataset.format` остаются валидными.

## Проверки

```bash
cargo fmt --check
cargo test
cargo build -p repartitioner
.venv/bin/python -m compileall experiments spark_pipeline datasets
```

Дополнительные материалы:

- `docs/architecture.md`
- `docs/method-spec.md`
- `experiments/README.md`
- `spark_pipeline/README.md`
