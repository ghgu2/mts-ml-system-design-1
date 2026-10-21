# Домашние задания по MLOps

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
