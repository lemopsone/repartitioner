# Dev checklist

## AP-00. Baseline текущей реализации

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

- `read_dataset`: читает входной Parquet dataset и извлекает строки с ключами
  партиционирования.
- `compute_statistics`: считает частоты ключей, кандидатов в heavy hitters и
  базовые метрики перекоса.
- `build_plan`: строит план адаптивного hash/salt-разбиения.
- `assign_partitions`: назначает каждой записи выходную `ap_partition`.
- `write_output`: записывает Spark-compatible Parquet dataset и JSON-метаданные.

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

## Baseline AP-00

На момент фиксации AP-00 выполнены проверки:

- `cargo fmt --check` - проходит.
- `cargo test` - проходит.
- `cargo build -p repartitioner` - проходит.
- `.venv/bin/python -m compileall experiments spark_pipeline datasets` - проходит.

