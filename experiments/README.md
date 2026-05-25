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

По умолчанию генераторы добавляют к каждой записи 8 числовых payload-колонок
`payload_0..payload_7`. Количество можно изменить через `--payload-columns`.

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

Для матрицы, описанной в исследовательском разделе ВКР
(`1000000`, затем `5000000..25000000` с шагом `5000000`; четыре вида
перекоса; операторы `scan`, `filter`, `group_by`, `join`), можно использовать:

```bash
python3 experiments/run_research.py \
  --data-dir data/research \
  --reports-dir reports/research \
  --min-partitions 16 \
  --max-partitions 16 \
  --local-threads 8 \
  --part-rows 1000000 \
  --payload-columns 8 \
  --shuffle-partitions 16 \
  --spark-driver-memory 8g \
  --spark-executor-memory 8g \
  --parquet-batch-size 1024 \
  --auto-broadcast-threshold-bytes -1 \
  --dataset-repetitions 5 \
  --spark-repetitions 3 \
  --trim-fraction 0.2 \
  --correctness-level basic \
  --spark-mode suite
```

Скрипт сохраняет сводную таблицу `reports/research/summary.csv` с двумя
вариантами для каждого опыта:

- `baseline` — Spark читает исходный dataset;
- `repartitioner` — Spark читает предобработанный dataset.

Для графиков `rho` важно задавать больше одной целевой партиции. Если оставить
`--min-partitions 1`, небольшие dataset могут оказаться меньше
`--target-partition-size-mb`, и планировщик выберет одну партицию. Тогда
`rho = 1.0` для baseline и repartitioner на таких точках. Для исследования с
фиксированным числом downstream partitions используйте, например,
`--min-partitions 16 --max-partitions 16`.

Поля таблицы включают `spark_time_seconds`, `rho`,
`preprocessing_seconds` и `total_with_preprocessing_seconds`.
Для каждого `--dataset-repetitions` генерируется новый dataset с новым seed.
Для каждого такого dataset Spark-измерения выполняются `--spark-repetitions`
раз. По умолчанию используется `--spark-mode suite`: один Python-процесс
создаёт один `SparkSession` и последовательно выполняет все Spark-замеры из
`reports/research/spark/benchmark_tasks.json`. Это уменьшает шум от запуска JVM,
Spark и Hadoop. Для старого поведения, где каждый замер запускает отдельный
Spark-процесс, можно указать `--spark-mode per_process`.
Генераторы пишут исходные dataset как директории с несколькими Parquet
part-файлами; размер part-файла по строкам задаётся `--part-rows`. Это нужно,
чтобы Rust preprocessor и Spark могли читать исходные данные параллельно.
Для больших прогонов используется `--correctness-level basic`: benchmark
сверяет размеры результатов, но не запускает дорогие checksum-проверки над
полным результатом join. Полную проверку (`--correctness-level full`) лучше
оставлять для smoke-run на малых данных.
Для join-нагрузки auto-broadcast по умолчанию отключён
(`--auto-broadcast-threshold-bytes -1`), а AQE выключен. Vectorized Parquet
reader и Hadoop vectored IO также выключены по умолчанию, чтобы широкие
payload-датасеты не давали резких всплесков Java heap при чтении локального
Parquet. Heap Spark-процесса задаётся через `--spark-driver-memory` и
`--spark-executor-memory`. Если нужно сравнить с обычными оптимизациями Spark,
можно указать `--enable-aqe`, `--enable-vectorized-parquet`,
`--enable-vectored-io` и положительный broadcast threshold.
Для операторов `scan` и `filter` Rust preprocessor по умолчанию принимает
no-op решение: физический rewrite не выполняется, metadata содержит
`input_reused = true`, а Spark benchmark читает исходный dataset. Это нужно,
поскольку эти операторы не создают key-based shuffle и не выигрывают от борьбы
с перекосом.
`summary.csv` содержит усреднённые значения, а все отдельные прогоны сохраняются
в `summary_raw.csv`. Параметр `--trim-fraction 0.2` отбрасывает по 20%
минимальных и максимальных значений перед вычислением среднего, если повторов
достаточно. `rho` усредняется по независимым dataset-повторам.
PNG-графики автоматически создаются в `reports/research/plots`. Каталог можно
переопределить через `--plots-dir`, а построение отключить через `--no-plots`.

Для быстрой проверки:

```bash
python3 experiments/run_research_smoke.py \
  --rows 100000 \
  --skews uniform heavy_key \
  --workloads scan group_by \
  --reports-dir reports/smoke \
  --dataset-repetitions 2 \
  --spark-repetitions 3
```

`run_research_smoke.py` создаёт dataset во временном каталоге ОС и удаляет его
после завершения прогона. CSV/JSON-отчёты и графики остаются в `--reports-dir`.

Повторное построение графиков по уже готовому CSV:

```bash
python3 experiments/plot_research.py \
  --summary reports/research/summary.csv \
  --plots-dir reports/research/plots
```

Для каждой пары `(вид перекоса, оператор)` создаются два графика:

- `*_time.png` — зависимость времени Spark-оператора от объёма dataset;
- `*_max_partition_rows.png` — максимальный размер партиции в строках;
- `*_max_partition_bytes.png` — оценка максимального размера партиции в байтах;
- `*_p95_partition_rows.png` — 95-й процентиль размера партиции в строках;
- `*_p95_partition_bytes.png` — оценка 95-го процентиля размера партиции в байтах;
- `*_cv.png` — коэффициент вариации размеров партиций;
- `*_skew_reduction_factor.png` — во сколько раз уменьшен максимум относительно baseline;
- `*_skew_remaining_ratio.png` — доля оставшегося перекоса относительно baseline;
- `*_largest_partition_share.png` — доля всего dataset в самой большой партиции;
- `*_max_minus_mean_rows.png` — разница между максимальной и средней партицией;
- `*_max_over_target_rows.png` — отношение максимальной партиции к целевому размеру;
- `*_tau.png` — вспомогательная метрика `rho = max_partition / mean_partition`.

## Исследование качества переразбиения

Для отдельного исследования на фиксированном объёме данных можно менять долю
одного тяжёлого ключа и смотреть, насколько метод снижает отношение
максимального размера партиции к среднему:

```bash
python3 experiments/run_partition_quality.py \
  --data-dir /media/lemopsone/Useful/repartitioner_quality_data \
  --reports-dir reports/partition-quality \
  --rows 10000000 \
  --partitions 16 \
  --payload-columns 0 \
  --local-threads 16 \
  --release
```

По умолчанию проверяются доли тяжёлого ключа
`0%, 5%, 10%, 20%, ..., 90%`. Скрипт сохраняет:

- `reports/partition-quality/summary.csv`;
- `reports/partition-quality/quality.png`.

Метрика на графике:

```text
rho = max_partition_rows / mean_partition_rows
```

Для варианта `baseline` используется не физическая нарезка исходных Parquet
файлов, а детерминированная оценка hash-разбиения по ключу на то же число
партиций. Поэтому точки baseline стабильны и отражают именно перекос, который
получает key-based shuffle. Сгенерированные dataset после каждой точки
удаляются, если не указан `--keep-data`.

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
