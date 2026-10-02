# Скоринг фродовых транзакций в Kafka

Сервис для домашнего задания по MLOps. Streamlit отправляет строки `test.csv` по одной в топик `transactions`. Контейнер `fraud_detector` подготавливает признаки, применяет готовую CatBoost-модель **на CPU** и пишет результат в топик `scoring`.

Обучение модели при запуске не производится. Модель `fraud_detector/models/my_catboost.cbm` взята из учебного примера. Файл `fraud_detector/models/preprocessing.json` содержит небольшие статистики для подготовки признаков, заранее вычисленные из предоставленного `train.csv`. Поэтому скачивать датасет для запуска сервиса не требуется.

## Требования и запуск

- Docker с работающим Docker Engine и Docker Compose v2
- Свободные порты 8501, 8080 и 9095

Скачайте или клонируйте репозиторий и перейдите в его корневую папку. Затем выполните:

```bash
docker compose up --build -d
docker compose ps
```

Первый запуск скачает образы Kafka, Zookeeper и Python и соберёт два контейнера Python. Подождите, пока `kafka-setup` завершится с кодом `0`, а `fraud_detector` и `interface` перейдут в состояние `Up`. Логи:

```bash
docker compose logs -f fraud_detector interface
```

## Проверка

1. Откройте [интерфейс отправки](http://localhost:8501).
2. Загрузите включённый в репозиторий файл `examples/sample.csv` или исходный `test.csv` из датасета соревнования. Выберите 10 строк и нажмите **«Отправить транзакции»**.
3. Проверьте логи `fraud_detector`: для каждой строки должна появиться запись `Scored ...`.
4. Откройте [Kafka UI](http://localhost:8080), выберите топик `scoring` и посмотрите сообщения. Также можно получить одно сообщение командой:

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic scoring --from-beginning --max-messages 1 --timeout-ms 15000
```

Пример результата:

```json
{"transaction_id":"a7f18ec9-6a10-4404-a891-0181d76cb22f","score":0.0234,"fraud_flag":0}
```

`score` — вероятность фрода от 0 до 1; `fraud_flag` равен 1 при `score > 0.98`. Идентификатор каждой транзакции генерируется интерфейсом. Топики создаются автоматически сервисом `kafka-setup`.

Для остановки:

```bash
docker compose down
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
