# Набор скриптов для проведения исследования

Для использования необходимо установить зависимости

```bash
pip install pyarrow
# или
pip install -r experiments/requirements.txt
```

Генерация датасетов:

```bash
python3 experiments/generate_uniform.py --output data/uniform.parquet --rows 100000
python3 experiments/generate_heavy_key.py --output data/heavy.parquet --rows 100000 --heavy-fraction 0.5
python3 experiments/generate_multi_heavy_key.py --output data/multi-heavy.parquet --rows 100000 --heavy-keys 4
python3 experiments/generate_zipf.py --output data/zipf.parquet --rows 100000 --zipf-exponent 1.2
```

Запуск Rust-ядра через Python-обертку с генерацией отчета:

```bash
python3 experiments/run_preprocessor.py \
  --input data/heavy.parquet \
  --output data/heavy_partitioned \
  --result reports/heavy-result.json \
  --max-partitions 16
```

Сборка метаданных и результатов с полученного перераспределения:

```bash
python3 experiments/collect_results.py \
  --output-dataset data/heavy_partitioned \
  --input-metadata data/heavy.parquet.json \
  --result reports/heavy-result.json
```

## Полный набор экспериментов

Единый сценарный runner для раздела исследования:

```bash
.venv/bin/python experiments/run_suite.py \
  --run-dir experiments/runs/main-suite \
  --release
```

По умолчанию запускается матрица:

- сценарии groupBy: `uniform_no_skew`, `single_heavy`, `multi_heavy`,
  `zipf`, `normal_key_hash_skew`;
- сценарии join: `small_right_join`, `shared_heavy_join`,
  `one_sided_heavy_join`;
- количество строк: `10000`, `100000`, `1000000`;
- доля heavy key: `0.1`, `0.25`, `0.5`, `0.75`;
- максимум партиций: `4`, `8`, `16`, `32`;
- целевой размер партиции: `16`, `64`, `128` МБ.

Для быстрой проверки runner-а:

```bash
.venv/bin/python experiments/run_suite.py \
  --run-dir /tmp/repartitioner-suite-smoke \
  --rows 10000 \
  --distributions uniform_no_skew,single_heavy,small_right_join \
  --heavy-fractions 0.5 \
  --max-partitions 4 \
  --target-partition-size-mb 16 \
  --limit 2
```

Если Spark в окружении временно недоступен, можно проверить только генерацию и
preprocessor:

```bash
.venv/bin/python experiments/run_suite.py \
  --run-dir /tmp/repartitioner-suite-no-spark \
  --rows 10000 \
  --limit 2 \
  --skip-spark
```

Сценарный runner сохраняет для каждого сценария:

- сгенерированный dataset и `input.parquet.json`;
- для join-сценариев — правую сторону `right.parquet`;
- YAML-конфиг preprocessor-а как `config.yaml`;
- metadata результата: `_partition_plan.json`, `_stats.json`,
  `_manifest.json`;
- JSON-результат preprocessor-а;
- JSON/CSV-отчёт Spark benchmark-а как `spark_report.json` и
  `spark_report.csv`;
- локальный `summary.csv` для конкретного сценария.

Итоговые файлы:

- `summary.csv` — таблица для графиков ВКР;
- `summary.json` — те же результаты плюс список неуспешных сценариев.

В `summary.csv` отдельно фиксируются времена только Spark и end-to-end времена:

- `preprocessing_total_seconds`;
- `preprocessing_writing_seconds`;
- `before_max_mean_ratio`;
- `after_max_mean_ratio`;
- `skew_reduction_ratio`;
- `spark_baseline_seconds`;
- `spark_physical_only_seconds`;
- `spark_method_aware_seconds`;
- `spark_method_aware_join_seconds`;
- `end_to_end_physical_only_seconds`;
- `end_to_end_method_aware_seconds`.

Дополнительно сохраняются:

- `before_max_partition_size`;
- `after_max_partition_size`;
- `before_mean_partition_size`;
- `after_mean_partition_size`;
- `before_cv`;
- `after_cv`;
- `target_rows_satisfied_after`;
- `rewrite_required`;
- `cost_estimated_rows_written`;
- `cost_estimated_bytes_written`;
- `heavy_hitter_count`;
- `output_partitions`;
- `output_file_count`;
- `partitioning_strategy`.

Поля корректности Spark:

- `group_by_exact_correctness` — точное совпадение counts по каждому ключу
  для method-aware groupBy;
- `join_checksum_correctness` — совпадение checksum результата join с
  baseline;
- `method_aware_join_applied`, `method_aware_join_skipped`,
  `method_aware_join_skip_reason`, `method_aware_join_strategy` — статус
  применения method-aware join.

Для no-op сценариев method-aware Spark-режим может быть `null`, потому что
preprocessor не материализует технические колонки и Spark читает исходный input
как reused dataset.

Сценарий `uniform_no_skew` нужен как контрольный пример: если данные уже
распределены равномерно, heavy hitters отсутствуют и ограничение `L` выполняется,
метод не обязан физически переписывать dataset. В таком запуске ожидается
`rewrite_required=false`, `action=no_op`, а `preprocessing_writing_seconds`
отражает только запись metadata и должен быть близок к нулю.

Сценарий `normal_key_hash_skew` генерирует набор normal keys, которые
детерминированно попадают в одну hash-партицию при фиксированном seed. Он нужен
для проверки load-aware assignment без heavy hitters.

Join-сценарии разделены по ожидаемой рекомендации planner-а:

- `small_right_join` — правая сторона мала, ожидается рекомендация
  `broadcast_join`;
- `shared_heavy_join` — heavy key присутствует на обеих сторонах, ожидается
  `salted_heavy_key_join`;
- `one_sided_heavy_join` — heavy key выражен только на одной стороне, ожидается
  `heavy_key_isolation_join`.

## Границы интерпретации

Набор экспериментов оценивает прототип, а не произвольную Spark-интеграцию:

- Rust-ядро не зависит от Spark.
- Parquet используется как основной формат прототипа.
- CSV-ввод можно использовать для демонстрации слоя адаптеров, но CSV-вывод
  отсутствует.
- `file_size_balancing` как отдельная стратегия не участвует в наборе
  экспериментов; в отчётах отражается только rolling выходных файлов на уровне
  writer-а.
- Method-aware join является экспериментальным режимом Spark benchmark-а и может
  быть пропущен с `skip_reason`, если metadata или схема не позволяют выполнить
  его безопасно.
