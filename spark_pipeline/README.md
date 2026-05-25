# Spark benchmark

Скрипты для прогона тестовых Spark pipeline-ов с целью исследования
эффективности разрабатываемого метода.

Установка зависимостей:

```bash
pip install -r spark_pipeline/requirements.txt
```

Spark/Hadoop для этих скриптов нужно запускать на JDK 17. На Linux Mint:

```bash
sudo apt install openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
java -version
```

Если запустить benchmark на слишком новой Java, например JDK 24/25, Hadoop
может упасть с ошибкой `Subject.getSubject is not supported`. Скрипты проверяют
это до создания `SparkSession` и выводят понятную ошибку.

Все четыре оператора (`scan`, `filter`, `group_by`, `join`):

```bash
python3 spark_pipeline/benchmark.py \
  --original data/heavy.parquet \
  --preprocessed data/heavy_partitioned \
  --json-report reports/spark-heavy.json \
  --csv-report reports/spark-heavy.csv \
  --shuffle-partitions 16
```

По умолчанию benchmark отключает auto-broadcast join
(`spark.sql.autoBroadcastJoinThreshold = -1`) и AQE
(`spark.sql.adaptive.enabled = false`), чтобы shuffle-нагрузки были
повторяемыми и перекос не скрывался оптимизациями Spark. Для широких payload
dataset также отключены vectorized Parquet reader и Hadoop vectored IO; это
уменьшает риск `Java heap space` при чтении локального Parquet. Heap задаётся
через `--driver-memory` и `--executor-memory`.

Для обратного режима можно передать `--auto-broadcast-threshold-bytes <bytes>`,
`--enable-aqe`, `--enable-vectorized-parquet` и `--enable-vectored-io`.

Текущие workload-и намеренно читают payload-колонки:

- `scan`: `count(*)` и суммы по `value`, `payload_0..payload_N`;
- `filter`: фильтр по hash ключа, затем `count(*)` и суммы по payload;
- `group_by`: `groupBy(key)` с `count(*)` и суммами по payload;
- `join`: shuffle join по ключу, затем `count`, `countDistinct(key)` и суммы
  по payload результата.

Для исследовательского прогона предпочтительно использовать suite-режим из
`experiments/run_research.py`: он создаёт один `SparkSession` и выполняет все
замеры последовательно внутри одного Spark-процесса. Внутренний скрипт
`spark_pipeline/benchmark_suite.py` принимает JSON со списком задач и обычно не
запускается вручную.

Только scan:

```bash
python3 spark_pipeline/run_scan.py \
  --original data/heavy.parquet \
  --preprocessed data/heavy_partitioned \
  --json-report reports/scan-heavy.json \
  --csv-report reports/scan-heavy.csv
```

Только filter:

```bash
python3 spark_pipeline/run_filter.py \
  --original data/heavy.parquet \
  --preprocessed data/heavy_partitioned \
  --json-report reports/filter-heavy.json \
  --csv-report reports/filter-heavy.csv
```

Для `group_by` отчёт содержит три режима:

- `baseline`: `original.groupBy(key_column)` с `count` и payload-агрегатами.
- `physical_only`: `preprocessed.groupBy(key_column)` с теми же агрегатами.
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
`exact_group_counts_match`, `group_count_diff_rows` и checksum по ключу,
`count` и payload-агрегатам. Для join блок `correctness` сохраняет проверки размеров и добавляет
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
