# Spark benchmark

Скрипты для прогона тестовых Spark pipeline-ов с целью исследования
эффективности разрабатываемого метода.

Установка зависимостей:

```bash
pip install -r spark_pipeline/requirements.txt
```

Два прогона (JOIN + groupBy):

```bash
python3 spark_pipeline/benchmark.py \
  --original data/heavy.parquet \
  --preprocessed data/heavy_partitioned \
  --json-report reports/spark-heavy.json \
  --csv-report reports/spark-heavy.csv \
  --shuffle-partitions 16
```

Для `group_by` отчёт содержит три режима:

- `baseline`: `original.groupBy(key_column).count()`.
- `physical_only`: `preprocessed.groupBy(key_column).count()`.
- `method_aware`: двухстадийная агрегация по техническим колонкам партиции,
  salt и исходному ключу.

`method_aware` включается автоматически, если в предобработанном dataset есть
`_partition_plan.json`. Его также можно запросить явно:

```bash
python3 spark_pipeline/run_groupby.py \
  --original data/heavy.parquet \
  --preprocessed data/heavy_partitioned \
  --json-report reports/groupby-heavy.json \
  --csv-report reports/groupby-heavy.csv \
  --include-method-aware
```

Для method-aware groupBy частичная стадия использует
`recommended_downstream_plan.partial_group_keys` из `_partition_plan.json`, если
они доступны в DataFrame. Обычно это `_rp_partition_id`, `_rp_salt` и исходный
ключ. Если `_rp_salt` отсутствует, benchmark не падает, но помечает режим как
деградировавший через `extra.salt_column_used = false`,
`extra.method_aware_degraded = true` и
`extra.degraded_reason = "salt_column_missing"`.

Колонка партиции берётся из `technical_columns.partition_column`. Если
техническая колонка не записана, benchmark пробует Hive-style колонки
`rp_partition` и `ap_partition`, если Spark видит их при чтении dataset.

В JSON/CSV отчётах поле `mode` разделяет `baseline`, `physical_only` и
`method_aware`. Для groupBy блок `correctness` сохраняет старые проверки размеров,
а также содержит строгую сверку counts по каждому ключу:
`exact_group_counts_match`, `group_count_diff_rows` и checksum по `(key,
count)`. Для join блок `correctness` сохраняет проверки размеров и добавляет
`checksum_matches_baseline` по стабильному набору логических колонок результата.
Технические `_rp_*`/Hive partition columns не участвуют в join checksum.

Для join-aware режимов heavy keys берутся из структурированных полей metadata
`join_plan.left_heavy_key_values`, `right_heavy_key_values` и
`shared_heavy_key_values`. Старые encoded поля сохраняются, но benchmark не
должен парсить их вручную. Composite heavy keys в method-aware join должны
обрабатываться отдельной логикой; helper для одного ключевого столбца явно сообщает об ошибке,
если получает несколько частей ключа.

Join benchmark является экспериментальной downstream-проверкой рекомендаций из
`_partition_plan.json`. Он не означает, что Rust-ядро зависит от Spark или
переписывает Spark-операторы автоматически.

Только groupBy:

```bash
python3 spark_pipeline/run_groupby.py \
  --original data/heavy.parquet \
  --preprocessed data/heavy_partitioned \
  --json-report reports/groupby-heavy.json \
  --csv-report reports/groupby-heavy.csv
```

Только JOIN:

```bash
python3 spark_pipeline/run_join.py \
  --original data/heavy.parquet \
  --preprocessed data/heavy_partitioned \
  --join-right data/dimension.parquet \
  --json-report reports/join-heavy.json \
  --csv-report reports/join-heavy.csv
```

Для `join` отчёт также может содержать режим `method_aware`. Он включается
флагом `--include-method-aware` или автоматически при наличии
`_partition_plan.json`. Поддерживаемые стратегии берутся из
`recommended_downstream_plan.strategy`:

- `broadcast_join`: Spark broadcast join правой стороны.
- `physical_repartitioning`: обычный join без переписывания оператора.
- `salted_heavy_key_join`: split normal/heavy left side и join heavy keys по
  исходному ключу и `_rp_salt`.
- `heavy_key_isolation_join`: аналогичная изоляция heavy-key subset по union
  heavy keys.

Если метод-aware join нельзя выполнить безопасно, отчёт содержит строку
`mode = method_aware`, `skipped = true` и `skip_reason`, например
`composite_join_key_unsupported`, `missing_technical_columns` или
`partition_plan_job_type_not_join`.

Ограничения текущей реализации:

- method-aware groupBy рассчитан на наличие `_rp_partition_id` и `_rp_salt`;
  при отсутствии salt режим помечается как деградировавший;
- method-aware join поддерживает join key из одного столбца;
- composite join keys не исполняются молча и должны попадать в skipped result;
- CSV-вывод не поддерживается Rust preprocessor-ом, поэтому benchmark ожидает
  предобработанный dataset, совместимый с Parquet.

Если не указывать флаг `--join-right`, скрипт собирает правую часть из оригинального датасета
до начала замеров времени. Эти действия не попадают в генерируемые отчеты.
