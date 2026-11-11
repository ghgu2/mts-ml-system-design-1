# Домашние задания по MLOps

## Задание 11 — мониторинг антифрода

Симулятор непрерывно отправляет транзакции в Kafka. Скоринг публикует результаты в топик `scoring`, а `scoring_writer` сохраняет их в PostgreSQL. Prometheus опрашивает оба сервиса раз в 2 секунды и Node Exporter раз в 5 секунд. Grafana при запуске загружает три дашборда, два источника данных и правила оповещения.

### Запуск

Из корня репозитория при работающем Docker и Docker Compose 2.24.4 или новее:

```bash
docker compose -f docker-compose.yml -f docker-compose.hw11.yml up --build -d
docker compose -f docker-compose.yml -f docker-compose.hw11.yml ps -a
```

Compose-проект `mlops-hw11` использует отдельные тома и порты. Первый запуск создаёт базу `fraud_monitoring` в PostgreSQL и обучает исходную модель на включённом `examples/hw3_train.csv`. Дождитесь завершения `volume-init`, `bootstrap`, `airflow-init`, `kafka-setup` и `scores-init` с кодом 0. Затем дайте потоку 20–30 секунд, чтобы появились значения на графиках. После пересоздания контейнера скоринга Kafka может потребоваться до минуты для восстановления потока.

| Сервис | Адрес |
|---|---|
| Grafana | http://localhost:3000 — `admin` / `fraud-local` |
| Prometheus | http://localhost:9090 · [цели сбора](http://localhost:9090/targets) |
| Airflow | http://localhost:18081 — `admin` / `admin` |
| Kafka UI | http://localhost:28080 |
| Streamlit для своих CSV | http://localhost:28501 |

PostgreSQL доступен внутри сети Compose. Grafana читает `scoring.scores` через роль `scores_reader`. Пароли для локального стенда задаются переменными `HW11_GRAFANA_PASSWORD`, `HW11_READER_PASSWORD` и `HW11_WRITER_PASSWORD`. Пароль администратора Grafana применяется при первом создании её тома.

### Дашборды и метрики

| Дашборд | Метрики и панели |
|---|---|
| [Обработка транзакций](http://localhost:3000/d/hw11-scoring) | Скорость из `fraud_scoring_processed_total`, время обработки из `fraud_scoring_processing_seconds`, доля фрода за 30 секунд из скоростей `fraud_scoring_flagged_total` и `fraud_scoring_processed_total`, гистограмма `fraud_scoring_score`, отметка смены версии из `fraud_scoring_model_switch_timestamp_seconds`. Скоринг также отдаёт долю фрода с запуска `fraud_scoring_fraud_ratio` и счётчик смен версии `fraud_scoring_model_switches_total`. |
| [Запись результатов](http://localhost:3000/d/hw11-writer) | Скорость записи из `fraud_writer_scores_written_total`, время записи из `fraud_writer_write_seconds`, фродовые флаги из `fraud_writer_flagged_written_total`, гистограмма `fraud_writer_score`. |
| [Системные метрики](http://localhost:3000/d/hw11-system) | `node_cpu_seconds_total`, `node_memory_*`, `node_network_*_bytes_total`, `node_filesystem_*_bytes` и `node_disk_*_bytes_total`. На macOS Node Exporter работает внутри Linux VM Docker. Панель сети показывает интерфейсы, видимые контейнеру экспортера. |

Дополнительная метрика скоринга `fraud_scoring_kafka_publish_seconds` показывает время от отправки результата до подтверждения Kafka. Панель «Время подтверждения записи в Kafka» показывает p95. Если растёт и общее время обработки, и этот p95, можно проверить задержку брокера отдельно от препроцессинга и инференса.

На дашборде обработки результаты по версиям модели, плотность скоров, TPS с фильтрами, доля фрода по категориям и средний скор по мерчантам берутся из PostgreSQL. Фильтры «Штат» и «Мерчант» можно комбинировать и применять к двум панелям. В график по категориям попадают 1000 последних сохранённых транзакций по времени публикации. Доля фрода везде означает долю выставленных моделью флагов. Штат, мерчант и категория передаются заголовками Kafka, поэтому JSON результата скоринга сохраняет свой формат. Повторная доставка с тем же ID транзакции сохраняет одну строку.

### Проверка

1. В [Prometheus Targets](http://localhost:9090/targets) должны быть три цели в состоянии UP. В [Prometheus](http://localhost:9090) выполните `fraud_scoring_processed_total` и `fraud_writer_scores_written_total` дважды с паузой в несколько секунд. Оба счётчика должны вырасти. В Grafana откройте три дашборда и проверьте, что графики обновляются.
2. На дашборде обработки выберите штат, затем мерчанта с транзакциями в этом штате. Обе панели с фильтрами должны пересчитаться. В панели «Результаты по версиям модели» должны появляться строки по мере записи скоров.
3. Для проверки смены модели запустите команду ниже. Она вызывает существующие DAG Airflow, ждёт принятого кандидата и сверяет результаты скоринга до и после публикации. Если quality gate отклоняет кандидата, проверка пробует следующие месяцы. На панели «Смена версии модели» после подхвата появится отметка примерно на 30 секунд с новой версией в легенде. Она остаётся видимой в истории графика. Таблица результатов покажет обе версии, пока они входят в выбранный диапазон.

```bash
docker compose -f docker-compose.yml -f docker-compose.hw11.yml run --rm verify
```

В разделе [Alerting → Alert rules](http://localhost:3000/alerting/list) есть два правила. Оба оценивают скорость за последние 30 секунд каждые 10 секунд и переходят в Alerting после 30 секунд за порогом.

| Правило | Порог | Как проверить |
|---|---|---|
| Поток скоринга остановился | Скорость ниже 0,05 транзакции/с | Выполните `stop simulator` с теми же Compose-файлами. Через 60–80 секунд проверьте состояние, затем выполните `start simulator`. |
| Запись скоров отстаёт | Скорость скоринга выше скорости записи более чем на 0,5 результата/с | При работающем симуляторе выполните `stop scoring_writer`. После срабатывания выполните `start scoring_writer`. Writer дочитает топик с подтверждённого смещения и заполнит пропущенный интервал. |

Остановка с сохранением данных:

```bash
docker compose -f docker-compose.yml -f docker-compose.hw11.yml down
```

[Код семинара 11](https://github.com/NikitaMalykhin/mlops_realtime_fraud_detector_monitoring) использован как справочник по составу стенда.

## Задание 3: переобучение в Airflow

Три DAG поставляют новые размеченные месяцы, переобучают CatBoost на скользящем окне и публикуют HTML-отчёты Data Quality. Новая модель попадает в скоринг только тогда, когда Average Precision и ROC-AUC на одном проверочном месяце не хуже действующей модели. Скоринг подхватывает модель и её препроцессинг без перезапуска и указывает `model_version` в Kafka.

```bash
docker compose up --build -d
docker compose ps -a
docker compose run --rm --no-deps verify
```

В репозитории уже есть размеченный пример из 12 000 строк. Для быстрого запуска не нужно скачивать датасет или устанавливать локальный Python. Нужен работающий Docker с Compose v2 и желательно 8 ГБ памяти для Docker.

- [Airflow](http://localhost:8081) — `admin` / `admin`, DAG `retrain_fraud_model`, `supply_labeled_month`, `check_data_quality`.
- [HTML-отчёты качества данных](http://localhost:8082).
- [Kafka UI](http://localhost:18080) — топики `transactions` и `scoring`.
- [Отправка своих транзакций](http://localhost:18501).
- [Подробное описание, восстановление после ошибок и проверки](hw3/README.md).
- [Результаты сквозного запуска и автоматических проверок](hw3/results/README.md).

`verify` запускает настоящие DAG через API Airflow и отправляет пробные транзакции до и после смены модели. Результаты доступны на [странице проверки](http://localhost:8082/verification/). Копия проверенного запуска сохраняется в `hw3/results/`. Обработка в Kafka также идёт автоматически: симулятор отправляет одну транзакцию в секунду.

ДЗ‑1 сохранено в `docker-compose.hw1.yml`, ДЗ‑2 — в `docker-compose.hw2.yml`. У ДЗ‑3 отдельное имя Compose-проекта `mlops-hw3`, тома и порты; предыдущие задания можно запускать независимо. Исходные состояния также доступны в коммитах `0152082` (ДЗ‑1) и `bed253c` (ДЗ‑2).

## Задание 2: Kafka → ClickHouse и оптимизация

Решение пунктов 1–4 задания: отдельный Python producer загружает CSV в Kafka, ClickHouse получает поток через Kafka Engine и materialized view. SQL находит категорию **одной максимальной транзакции** по каждому штату. Оптимизированная схема использует словарное кодирование, сжатие, сортировку и предварительную агрегацию; результаты сверяются с исходным CSV.

Из корня репозитория при запущенном Docker с Compose v2:

```bash
docker compose -f docker-compose.hw2.yml run --build --rm pipeline
```

Команда поднимает Kafka и ClickHouse, загружает включённые в репозиторий 10 000 строк из `test.csv`, проверяет доставку и результаты, сохраняет CSV и замеры в `hw2/results/`. Локальный Python и скачивание датасета для этого запуска не нужны.

На полном `test.csv` (262 144 строки) проверено совпадение трёх SQL-запросов с независимым расчётом для 51 штата/округа. В локальном замере предварительная агрегация ускорила запрос в **14,70 раза**; оптимизированная таблица вместе с агрегатами занимает **52,8%** от исходного размера. Методика и ограничения приведены в отчёте.

- [Подробный запуск, SQL и соответствие требованиям](hw2/README.md).
- [Результирующий CSV полного test.csv](hw2/results/full_dataset/max_transaction_by_state.csv).
- [Отчёт с замерами полного test.csv](hw2/results/full_dataset/REPORT.md).

Для первого и второго заданий используются отдельные Compose-проекты и тома.

## Задание 1: скоринг фродовых транзакций в Kafka

Сервис для домашнего задания по MLOps. Streamlit отправляет строки `test.csv` по одной в топик `transactions`. Контейнер `fraud_detector` подготавливает признаки, применяет готовую CatBoost-модель **на CPU** и пишет результат в топик `scoring`.

Обучение модели при запуске не производится. Модель `fraud_detector/models/my_catboost.cbm` взята из учебного примера. Файл `fraud_detector/models/preprocessing.json` содержит небольшие статистики для подготовки признаков, заранее вычисленные из предоставленного `train.csv`. Поэтому скачивать датасет для запуска сервиса не требуется.

## Требования и запуск

- Docker с работающим Docker Engine и Docker Compose v2
- Свободные порты 8501, 8080 и 9095

Скачайте или клонируйте репозиторий и перейдите в его корневую папку. Затем выполните:

```bash
docker compose -f docker-compose.hw1.yml up --build -d
docker compose -f docker-compose.hw1.yml ps
```

Первый запуск скачает образы Kafka, Zookeeper и Python и соберёт два контейнера Python. Подождите, пока `kafka-setup` завершится с кодом `0`, а `fraud_detector` и `interface` перейдут в состояние `Up`. Логи:

```bash
docker compose -f docker-compose.hw1.yml logs -f fraud_detector interface
```

## Проверка

1. Откройте [интерфейс отправки](http://localhost:8501).
2. Загрузите включённый в репозиторий файл `examples/sample.csv` или исходный `test.csv` из датасета соревнования. Выберите 10 строк и нажмите **«Отправить транзакции»**.
3. Проверьте логи `fraud_detector`: для каждой строки должна появиться запись `Scored ...`.
4. Откройте [Kafka UI](http://localhost:8080), выберите топик `scoring` и посмотрите сообщения. Также можно получить одно сообщение командой:

```bash
docker compose -f docker-compose.hw1.yml exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic scoring --from-beginning --max-messages 1 --timeout-ms 15000
```

Пример результата:

```json
{"transaction_id":"a7f18ec9-6a10-4404-a891-0181d76cb22f","score":0.0234,"fraud_flag":0}
```

`score` — вероятность фрода от 0 до 1; `fraud_flag` равен 1 при `score > 0.98`. Идентификатор каждой транзакции генерируется интерфейсом. Топики создаются автоматически сервисом `kafka-setup`.

Для остановки:

```bash
docker compose -f docker-compose.hw1.yml down
```

## Состав

- `interface` — Streamlit, загрузка CSV и отправка транзакций;
- `fraud_detector` — отдельные модули подготовки признаков, скоринга и Kafka consumer/producer;
- `kafka`, `zookeeper`, `kafka-setup`, `kafka-ui` — потоковая инфраструктура;
- `scripts/build_preprocessing_artifacts.py` — воспроизводимое создание `preprocessing.json` из `train.csv` (для обычного запуска не нужно).

При необходимости пересоздать статистики из исходного обучающего файла:

```bash
python3 scripts/build_preprocessing_artifacts.py /путь/к/train.csv
```
