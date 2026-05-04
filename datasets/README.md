# Генерация синтетических датасетов

Набор python-генераторов данных для создания датасетов с выбранной стратегией ассиметрии данных.

## Установка

Требуется Python 3.10+.

```bash
pip install -r datasets/requirements.txt
```

## CLI

Примеры запуска:

```bash
python3 datasets/generate.py uniform --output data/uniform.parquet --rows 100000
python3 datasets/generate.py single-heavy --output data/heavy.parquet --rows 100000 --heavy-fraction 0.55
python3 datasets/generate.py multi-heavy --output data/multi-heavy --rows 100000 --files 4 --heavy-keys 5
python3 datasets/generate.py zipf --output data/zipf.parquet --rows 100000 --zipf-exponent 1.3
python3 datasets/generate.py custom-heavy --output data/custom.parquet --heavy-spec hot_a:0.30,hot_b:0.20
python3 datasets/generate.py group-by --output data/groupby.parquet --group-distribution zipf
python3 datasets/generate.py join-pair --output data/join_pair --rows 100000 --right-rows 25000
```

Every command writes Parquet plus JSON side metadata. For a single file, metadata is written as `<file>.json`;
Каждый режим работы на выходе создает Parquet + JSON-метаданные. При создании одного файла, метаданные хранятся в файле `<file.json>`;
для директории с данными, метаданные хранятсяв `_dataset_metadata.json`.

## Набор колонок

По умолчанию:

- `user_id`; для отдельных сценариев требующих ключей аггрегации или соединения: `group_key`/`join_key`;
- `row_id`;
- `value`;
- `event_time`.

Параметры конфигурации:

```bash
python3 datasets/generate.py single-heavy \
  --output data/orders.parquet \
  --rows 1000000 \
  --key-columns tenant_id,user_id \
  --metric-columns amount,cost \
  --categorical-columns region,device \
  --payload-bytes 64 \
  --files 8
```

## Доступные варианты ассиметрии

- `uniform`: равномерные частоты ключей;
- `single-heavy`: один тяжелый ключ с регулируемой частотой;
- `multi-heavy`: несколько тяжелых ключей с опционально настраиваемыми весами;
- `zipf`: частоты ключей распределены по экспоненциальному закону;
- `custom-heavy`: явное задание `KEY:FRACTION` для тяжелых ключей;
- `group-by`: данные специально для группировки, подлежащие настраиваемому uniform/heavy/Zipf распределению ключей.
- `join-pair`: левые + правые Parquet файлы с совпадающими перекошенными join-ключами для вычислительных заданий требующих JOIN.
