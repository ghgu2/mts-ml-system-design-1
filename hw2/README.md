# Задание 2: загрузка Kafka → ClickHouse

Решение рассчитано на выполнение всех пунктов задания, включая оптимизацию хранения и доказательство ускорения запроса. Пункты 2 и 3 в условии повторяются: для них подготовлены отдельные SQL-файлы и CSV-результаты.

## Что соответствует требованиям

| Пункт | Реализация |
|---|---|
| 1. Отдельный скрипт CSV → Kafka | [load_csv.py](load_csv.py), сообщения JSONEachRow в топике `hw2-transactions` |
| 2. DDL и получение данных из Kafka | [01_baseline.sql](sql/01_baseline.sql) и [02_optimized.sql](sql/02_optimized.sql): Kafka Engine, MergeTree и materialized views |
| 2–3. Категория максимальной транзакции по штату | [03_max_transaction.sql](sql/03_max_transaction.sql), [04_max_transaction_optimized.sql](sql/04_max_transaction_optimized.sql), [результирующий CSV](results/full_dataset/max_transaction_by_state.csv) |
| 4. Оптимизация хранения и запросов | [02_optimized.sql](sql/02_optimized.sql): LowCardinality, codecs, сортировка, AggregatingMergeTree; [измерения](results/full_dataset/REPORT.md) |
| Воспроизводимый запуск | [Docker Compose](../docker-compose.hw2.yml), [Dockerfile](Dockerfile), [requirements.txt](requirements.txt), включённая CSV-выборка |

## Быстрый запуск

Нужен работающий Docker Engine / Docker Desktop с Docker Compose v2. Рекомендуется выделить Docker не менее 4 ГБ памяти. Проверено на ARM64; образы поддерживают также AMD64. HTTP-порт ClickHouse `8123` должен быть свободен. При необходимости задайте другой порт через `HW2_HTTP_PORT=18123`.

Все команды ниже выполняются **из корня репозитория**:

```bash
git clone https://github.com/ghgu2/mts-ml-system-design-1.git
cd mts-ml-system-design-1
docker compose -f docker-compose.hw2.yml run --build --rm pipeline
```

Первый запуск скачивает образы и устанавливает зависимости внутри контейнера. Compose ждёт готовности Kafka, создаёт топик и запускает ClickHouse, который автоматически применяет оба DDL-файла. После этого скрипт:

1. Проверяет CSV и независимо вычисляет ожидаемые максимумы через Python `Decimal`.
2. Отправляет все строки в Kafka и проверяет подтверждение доставки каждого сообщения.
3. Ждёт, пока обе исходные таблицы получат нужное количество уникальных строк.
4. Сравнивает три SQL-результата с расчётом по исходному CSV.
5. Сохраняет результирующие CSV, замеры и отчёт.

Успешный запуск завершается строкой `PASS. CSV results and benchmark report saved to /results` и кодом `0`. Проверяется 10 000 исходных транзакций и 50 штатов/округов. Ошибка в данных, доставке или результате завершает команду с ненулевым кодом.

Включённый [hw2_sample.csv](../examples/hw2_sample.csv) содержит первые 10 000 строк предоставленного `teta-ml-1-2025/test.csv`. Это реальные строки учебного датасета; синтетическое увеличение выборки для замеров не используется. Полные исходные датасеты в Git не добавлены.

Контейнеры Kafka и ClickHouse остаются работать после завершения проверки:

```bash
docker compose -f docker-compose.hw2.yml ps -a
docker compose -f docker-compose.hw2.yml logs --tail 50 kafka clickhouse
```

`kafka-setup` должен завершиться с кодом `0`; Kafka и ClickHouse должны быть `healthy`.

## Результаты

После быстрого запуска в `hw2/results/` появляются:

| Файл | Содержание |
|---|---|
| `max_transaction_by_state.csv` | Результат оптимизированного запроса: штат, категория, максимальная сумма, номер исходной строки |
| `max_transaction_baseline.csv` | Тот же результат исходного запроса |
| `max_transaction_storage.csv` | Тот же результат запроса по оптимизированной таблице без агрегатов |
| `reference.csv` | Независимый расчёт по CSV |
| `benchmark.csv` | Медианы времени, объёма чтения и памяти для трёх запросов |
| `benchmark_runs.csv` | Все замеры и идентификаторы запросов из `system.query_log` |
| `storage.csv` | Размеры активных частей таблиц текущей загрузки |
| `run_metadata.json` | UUID загрузки, число строк, SHA-256 файла, версия ClickHouse и параметры проверки |
| `REPORT.md` | Описание проверки, сравнение и ограничения замеров |

Пример строки результата полного `test.csv` можно посмотреть в [готовом CSV](results/full_dataset/max_transaction_by_state.csv). Сохранённые результаты полного датасета лежат отдельно в [results/full_dataset](results/full_dataset), поэтому обычный демонстрационный запуск их не перезаписывает.

**Смысл запроса:** категория одной транзакции с наибольшим `amount` в каждом `us_state`. Это не максимальная сумма транзакций по категории. При одинаковом `amount` выбирается первая строка исходного CSV; номер `source_row` начинается с `1`, без заголовка. Сумма хранится как `Decimal(18, 2)`, чтобы сохранить копейки. В наборе присутствует округ DC, поэтому полный результат содержит 51 запись.

## Запуск на полном датасете

Положите `test.csv` в отдельный каталог. Например, если `teta-ml-1-2025` находится рядом с репозиторием:

```bash
HW2_DATA_DIR=../teta-ml-1-2025 docker compose -f docker-compose.hw2.yml run --rm pipeline \
  python run_pipeline.py --csv /data/test.csv --output-dir /results/full_dataset --repeats 21
```

`HW2_DATA_DIR` можно заменить абсолютным путём к каталогу с CSV. Поддерживается также `train.csv`; дополнительная колонка `target` не включается в поток аналитических транзакций. Все 17 признаков транзакции сохраняются в обеих таблицах.

Для проверки небольшой части файла используйте `--limit 1000`. Без `--limit` загружается весь CSV. `--timeout 180` задаёт время ожидания загрузки в ClickHouse после отправки; `--repeats` задаёт число измерений каждого запроса (минимум 3, по умолчанию 11).

### Сохранённый замер полного test.csv

262 144 строки; 51 штат/округ; ClickHouse 25.8.33.6; ARM64. 21 измерение каждого варианта, два прогрева, один поток, query cache отключён. Порядок трёх вариантов перемешивается в каждом повторе.

| Вариант | Медиана времени сервера | Прочитано строк | Прочитано байт |
|---|---:|---:|---:|
| Исходная таблица | 20,016 мс | 262 144 | 15 866 252 |
| Оптимизированное хранение | 18,906 мс | 262 144 | 8 912 896 |
| Предварительная агрегация | 1,362 мс | 51 | 8 321 |

Исходная таблица занимает 20 481 609 байт; оптимизированная вместе с агрегатами — 10 808 640 байт. Предварительная агрегация ускорила этот локальный запрос в 14,70 раза. Полная методика, сырые замеры и ограничения приведены в [REPORT.md](results/full_dataset/REPORT.md).

## Архитектура и оптимизация

```text
CSV → load_csv.py → Kafka: hw2-transactions
                        ↓ Kafka Engine + transactions_ingest_mv
              transactions_baseline (MergeTree)
                        ↓ transactions_optimize_mv
              transactions (MergeTree)
                        ↓ state_max_mv
              state_max (AggregatingMergeTree)
```

В ClickHouse нет отдельного Python consumer: Kafka Engine читает сообщения, а materialized views сохраняют и преобразуют поступающие блоки. Цепочка создаётся от последнего назначения к первому, чтобы уже первый прочитанный блок попал во все таблицы.

Исходная таблица хранит строки без ключа сортировки (`ORDER BY tuple()`) и со стандартным сжатием. В оптимизированной таблице изменён DDL:

- Штаты, категории и другие повторяющиеся строки используют `LowCardinality(String)`.
- Используются `ZSTD(1)`, `Delta` для времени и номера строки, `T64` для населения и `Gorilla` для координат.
- Сортировка `(us_state, transaction_time, source_row)` группирует близкие значения и полезна для запросов по штатам и времени. В запросе максимума по всем штатам исходные строки всё ещё нужно читать: само изменение сортировки не превращает этот запрос в чтение 51 строки.
- `state_max` хранит объединяемые состояния `argMax`, рассчитанные при вставке. `argMaxMerge` правильно объединяет состояния и до завершения фоновых слияний.

Отдельный [05_max_transaction_storage.sql](sql/05_max_transaction_storage.sql) показывает эффект хранения без предварительной агрегации. Это позволяет отличить небольшой выигрыш исходного SQL от основного ускорения за счёт агрегатов. Агрегация добавляет работу при вставке и небольшую дополнительную таблицу; скорость вставки в этом эксперименте не сравнивается.

В обеих исходных таблицах одинаковые строки и числовые типы. Одна загрузка получает новый `run_id` (UUID) и отдельный раздел таблицы; все проверочные запросы фильтруют этот UUID. Разделение по загрузкам используется для учебного сравнения и повторных запусков, а не как рекомендация для непрерывного production-потока. Время исходного файла интерпретируется как UTC без преобразования часового пояса.

`OPTIMIZE ... FINAL` перед замерами объединяет части во всех трёх таблицах, чтобы сравнение не зависело от числа Kafka-блоков. Для корректности обычного запроса принудительное слияние не требуется. Размеры считаются по активным частям текущей загрузки в `system.parts`; в итог оптимизированной схемы включён размер `state_max`. Baseline оставлен в стенде для сравнения, поэтому весь стенд занимает сумму размеров трёх таблиц.

## Отдельный запуск producer и SQL

Producer можно запустить отдельно, как требует пункт 1:

```bash
docker compose -f docker-compose.hw2.yml run --rm pipeline \
  python load_csv.py --csv /data/hw2_sample.csv
```

Скрипт напечатает `run_id`. После загрузки подставьте его вместо UUID ниже. SQL возвращает `CSVWithNames`, то есть CSV с заголовком:

```bash
docker compose -f docker-compose.hw2.yml exec -T clickhouse \
  clickhouse-client --user hw2 --password hw2-local \
  --param_run_id=00000000-0000-0000-0000-000000000000 --format CSVWithNames \
  --queries-file /dev/stdin < hw2/sql/04_max_transaction_optimized.sql
```

Для сохранения результата добавьте перенаправление `> max_transaction_by_state.csv`. Аналогично можно выполнить `03_max_transaction.sql` или `05_max_transaction_storage.sql`. Для автоматического ожидания доставки и сравнения результатов используйте основной `run_pipeline.py`.

HTTP ClickHouse доступен на [localhost:8123](http://localhost:8123) с пользователем `hw2` и паролем `hw2-local`; это локальные учебные настройки. Kafka доступна внутри Compose-сети по адресу `kafka:9092`.

## Проверки и повторные запуски

Unit-проверки не требуют локального Python:

```bash
docker compose -f docker-compose.hw2.yml run --build --no-deps --rm \
  -v ./hw2/tests:/app/tests:ro pipeline python -m unittest discover -s tests -v
```

Проверяются равные максимумы, точность денежных сумм выше диапазона точного `Float64`, ведущие нули почтового индекса, ограничение выборки, пустой файл и некорректные строки.

Проверка этих случаев через реальную Kafka и ClickHouse:

```bash
HW2_DATA_DIR=./hw2/tests docker compose -f docker-compose.hw2.yml run --rm pipeline \
  python run_pipeline.py --csv /data/edge_cases.csv --output-dir /tmp/edge_cases --repeats 3
```

Обычную команду запуска можно повторять: новый UUID отделяет результаты от предыдущих загрузок. Kafka producer включает idempotence и проверяет все подтверждения. Kafka Engine обеспечивает доставку с возможными повторами при авариях (at-least-once); проверка количества и уникальности строк обнаруживает такие повторы и завершает запуск с ошибкой. Сквозная exactly-once доставка здесь не заявляется. Параллельные запуски должны использовать разные `--output-dir`, чтобы не перезаписывать отчёт друг друга.

Для автоматизации этих же проверок подготовлен [шаблон GitHub Actions](ci-workflow.example.yml). Он не активирован: используемый при публикации GitHub-токен не имеет права `workflow`. При необходимости владелец репозитория может добавить шаблон как `.github/workflows/hw2.yml` через интерфейс GitHub или с соответствующим доступом. Обычный запуск и проверки из README от GitHub Actions не зависят.

Остановка сохраняет данные:

```bash
docker compose -f docker-compose.hw2.yml down
```

Чтобы начать с пустого стенда, следующая команда **удаляет только тома второго задания и их данные**:

```bash
docker compose -f docker-compose.hw2.yml down -v
docker compose -f docker-compose.hw2.yml run --build --rm pipeline
```

SQL из `docker-entrypoint-initdb.d` выполняется только при первом создании базы. После изменения DDL используйте пустой стенд либо примените миграцию вручную. Первое задание запускается своим `docker-compose.yml`; его данные и сервисы относятся к другому Compose-проекту.

## Документация используемых механизмов

- [Kafka Engine и materialized views](https://clickhouse.com/docs/integrations/connectors/data-ingestion/kafka/kafka-table-engine).
- [AggregatingMergeTree](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/aggregatingmergetree).
- [LowCardinality](https://clickhouse.com/docs/reference/data-types/lowcardinality).
- [Официальный Docker-образ Apache Kafka](https://kafka.apache.org/39/getting-started/docker/).
