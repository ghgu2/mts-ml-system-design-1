"""Measure server execution and per-run storage for three equivalent queries."""

import csv
import json
import random
import statistics
import time
import uuid
from pathlib import Path

QUERIES = {
    "baseline": "03_max_transaction.sql",
    "optimized_storage": "05_max_transaction_storage.sql",
    "preaggregated": "04_max_transaction_optimized.sql",
}
TABLES = ("transactions_baseline", "transactions", "state_max")
SQL_DIR = Path(__file__).parent / "sql"


def measure(client, run_id: str, repeats: int):
    if repeats < 3:
        raise ValueError("At least three benchmark repetitions are required")
    statements = {
        name: (SQL_DIR / file).read_text().strip().rstrip(";") +
        "\nSETTINGS max_threads = 1, use_query_cache = 0, log_queries = 1"
        for name, file in QUERIES.items()
    }
    # Normalize part count before measuring; this is only a benchmark step.
    for table in TABLES:
        client.query(f"OPTIMIZE TABLE hw2.{table} FINAL")
    for _ in range(2):
        for statement in statements.values():
            client.query(statement, params={"run_id": run_id}, output_format="JSONEachRow")
    samples = []
    rng = random.Random(42)
    for repetition in range(1, repeats + 1):
        order = list(statements)
        rng.shuffle(order)
        for name in order:
            query_id = "hw2-benchmark-" + str(uuid.uuid4())
            started = time.perf_counter()
            _, summary = client.query(statements[name], params={"run_id": run_id},
                                      query_id=query_id, output_format="JSONEachRow")
            samples.append({
                "variant": name, "repetition": repetition, "query_id": query_id,
                "http_elapsed_ms": (time.perf_counter() - started) * 1000,
                "server_elapsed_ms": int(summary["elapsed_ns"]) / 1_000_000,
            })
    client.query("SYSTEM FLUSH LOGS")
    logs = client.rows(
        "SELECT query_id, query_duration_ms, read_rows, read_bytes, memory_usage "
        "FROM system.query_log WHERE type = 'QueryFinish' "
        "AND query_id IN {ids:Array(String)}",
        params={"ids": repr([sample["query_id"] for sample in samples])},
    )
    by_id = {row["query_id"]: row for row in logs}
    for sample in samples:
        if sample["query_id"] not in by_id:
            raise RuntimeError("Benchmark query missing from system.query_log")
        sample.update({key: int(value) for key, value in by_id[sample["query_id"]].items()
                       if key != "query_id"})
    statistics_rows = []
    for name in QUERIES:
        selected = [sample for sample in samples if sample["variant"] == name]
        statistics_rows.append({
            "variant": name, "repetitions": repeats,
            "median_server_ms": round(statistics.median(row["server_elapsed_ms"] for row in selected), 3),
            "median_http_ms": round(statistics.median(row["http_elapsed_ms"] for row in selected), 3),
            "median_read_rows": int(statistics.median(row["read_rows"] for row in selected)),
            "median_read_bytes": int(statistics.median(row["read_bytes"] for row in selected)),
            "median_memory_bytes": int(statistics.median(row["memory_usage"] for row in selected)),
        })
    storage = []
    for table in TABLES:
        partitions = client.rows(
            f"SELECT DISTINCT _partition_id AS partition_id FROM hw2.{table} "
            "WHERE run_id = {run_id:UUID}", params={"run_id": run_id},
        )
        row = client.rows(
            "SELECT table, sum(rows) AS rows, sum(data_compressed_bytes) AS compressed_bytes, "
            "sum(data_uncompressed_bytes) AS uncompressed_bytes, "
            "sum(bytes_on_disk) AS bytes_on_disk FROM system.parts "
            "WHERE active AND database = 'hw2' AND table = {table:String} "
            "AND partition_id IN {partitions:Array(String)} GROUP BY table",
            params={"table": table, "partitions": repr([row["partition_id"] for row in partitions])},
        )[0]
        storage.append({key: value if key == "table" else int(value) for key, value in row.items()})
    return statistics_rows, samples, storage


def save_csv(path: Path, rows: list[dict]):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def save_report(output: Path, metadata: dict, metrics: list[dict], samples: list[dict], storage: list[dict]):
    save_csv(output / "benchmark.csv", metrics)
    save_csv(output / "benchmark_runs.csv", samples)
    save_csv(output / "storage.csv", storage)
    (output / "run_metadata.json").write_text(json.dumps(metadata, indent=2, ensure_ascii=False) + "\n")
    baseline, optimized, aggregated = metrics
    speedup = baseline["median_server_ms"] / aggregated["median_server_ms"]
    baseline_bytes = storage[0]["bytes_on_disk"]
    optimized_bytes = storage[1]["bytes_on_disk"] + storage[2]["bytes_on_disk"]
    report = [
        "# Проверка и сравнение оптимизации\n",
        f"Источник: `{metadata['dataset_file']}`; SHA-256: `{metadata['dataset_sha256']}`.\n",
        f"Загружено **{metadata['rows']:,}** строк; **{metadata['states']}** штатов/округов. "
        f"ClickHouse `{metadata['clickhouse_version']}`; архитектура `{metadata['architecture']}`.\n",
        f"Идентификатор загрузки: `{metadata['run_id']}`.\n",
        "Проверено: количество строк, уникальность source_row, совпадение всех трёх SQL-результатов "
        "с независимым расчётом по исходному CSV. Денежные суммы рассчитаны через Decimal; "
        "при равенстве выбрана первая строка CSV.\n",
        "## Время и объём чтения\n",
        "Два прогрева, затем перемешанный порядок запросов; один поток; query cache отключён. "
        "Медиана server_elapsed_ms из X-ClickHouse-Summary, read_rows/read_bytes/memory_usage "
        "из system.query_log. Перед замерами части каждой таблицы объединены через OPTIMIZE FINAL.\n",
        "| Вариант | Повторы | Время сервера, мс | Прочитано строк | Прочитано байт | Память, байт |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    labels = {"baseline": "Исходная таблица", "optimized_storage": "Оптимизированное хранение",
              "preaggregated": "Предварительная агрегация"}
    for row in metrics:
        report.append(f"| {labels[row['variant']]} | {row['repetitions']} | {row['median_server_ms']} "
                      f"| {row['median_read_rows']} | {row['median_read_bytes']} | {row['median_memory_bytes']} |")
    report.extend([
        f"\nВ этом запуске ускорение запроса с предварительной агрегацией: **{speedup:.2f}×**.\n",
        "## Хранение\n",
        "Одинаковый набор исходных признаков сохраняется в обеих таблицах. "
        "Размеры относятся только к разделу текущей загрузки и активным частям.\n",
        "| Таблица | Строк после слияния | Сжатые данные, байт | На диске, байт |",
        "|---|---:|---:|---:|",
    ])
    for row in storage:
        report.append(f"| {row['table']} | {row['rows']} | {row['compressed_bytes']} | {row['bytes_on_disk']} |")
    report.extend([
        f"\nИсходная таблица: **{baseline_bytes:,} байт**; оптимизированная вместе с агрегатами: "
        f"**{optimized_bytes:,} байт** ({optimized_bytes / baseline_bytes:.1%} от исходной).\n",
        "Baseline сохраняется для сравнения. Поэтому фактический учебный стенд занимает сумму "
        "трёх таблиц; в обычном рабочем варианте baseline не нужен. Kafka и служебные логи "
        "в размеры таблиц не входят.\n",
        "## Ограничения\n",
        "Это локальный замер с прогретыми данными, а не гарантия такого же ускорения на другой машине. "
        "Предварительная агрегация переносит вычисления на вставку и хранит дополнительную таблицу; "
        "скорость вставки этим экспериментом не измеряется. На малой выборке накладные расходы "
        "могут перевесить выигрыш. Все измерения сохранены в benchmark_runs.csv.\n",
    ])
    (output / "REPORT.md").write_text("\n".join(report), encoding="utf-8")
