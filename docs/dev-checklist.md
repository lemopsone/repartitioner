# Dev checklist

## RP-00. Baseline текущей реализации

Цель раздела - зафиксировать минимальный набор проверок и карту текущего
pipeline перед дальнейшими изменениями. Этот документ не описывает новую
бизнес-логику метода; он служит контрольной точкой для разработки.

## Карта pipeline

Текущий CLI-процесс выполняет стадии в следующем порядке:

```text
read_dataset
-> compute_statistics
-> build_plan
-> assign_partitions
-> write_output
```

Назначение стадий:

- `read_dataset`: читает входной dataset через выбранный input adapter и
  извлекает строки с ключами партиционирования.
- `compute_statistics`: считает частоты ключей, кандидатов в heavy hitters и
  базовые метрики перекоса.
- `build_plan`: строит план адаптивного hash/salt-разбиения.
- `assign_partitions`: назначает каждой записи выходную `rp_partition`.
- `write_output`: записывает Spark-compatible Parquet dataset и JSON-метаданные.

## Форматы ввода и вывода

Актуальная конфигурация разделяет формат входа и выхода:

```yaml
dataset:
  input: "./data/input.csv"
  input_format: "csv"

output:
  path: "./data/output_partitioned"
  format: "parquet"
```

Старые конфиги с `dataset.output` и `dataset.format` остаются валидными.
Семантика совместимости:

- `dataset.format` используется как `dataset.input_format`;
- `dataset.output` используется как `output.path`;
- если `output.format` не указан, он наследуется из `dataset.format`.

Поддержка форматов:

- Parquet: чтение и запись.
- CSV: чтение для статистики и планирования; запись CSV не реализована.

## Область применимости прототипа

При обновлении документации и текстов ВКР важно не расширять заявленную область
реализации сверх фактического состояния кода:

- основной метод: adaptive key placement, selective salting и optional no-op;
- Parquet adapter: полноценное чтение и запись;
- CSV adapter: только чтение;
- Spark groupBy benchmark: baseline, physical-only и method-aware;
- Spark join benchmark: baseline, physical-only и экспериментальный
  method-aware для безопасных single-key сценариев;
- `file_size_balancing`: не standalone strategy, а перспектива; сейчас
  реализован только rolling выходных Parquet-файлов в writer-е.

## Базовые команды проверки

Проверка форматирования Rust-кода:

```bash
cargo fmt --check
```

Полный набор Rust-тестов:

```bash
cargo test
```

Проверка сборки CLI:

```bash
cargo build -p repartitioner
```

Проверка Python-скриптов на синтаксические ошибки:

```bash
python -m compileall experiments spark_pipeline datasets
```

Если в локальном окружении команда `python` отсутствует, использовать
интерпретатор из виртуального окружения:

```bash
.venv/bin/python -m compileall experiments spark_pipeline datasets
```

## Baseline RP-00

На момент фиксации RP-00 выполнены проверки:

- `cargo fmt --check` - проходит.
- `cargo test` - проходит.
- `cargo build -p repartitioner` - проходит.
- `.venv/bin/python -m compileall experiments spark_pipeline datasets` - проходит.
