"""Upload a sample of test.csv and publish transactions to Kafka."""

import json
import os
import uuid

import pandas as pd
import streamlit as st
from kafka import KafkaProducer

BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
REQUIRED_COLUMNS = {
    "transaction_time", "merch", "cat_id", "amount", "gender",
    "one_city", "us_state", "lat", "lon", "population_city",
    "jobs", "merchant_lat", "merchant_lon",
}

st.title("Скоринг транзакций")
st.write("Загрузите examples/sample.csv или test.csv и отправьте небольшую выборку в Kafka.")
uploaded_file = st.file_uploader("CSV с транзакциями", type="csv")
row_limit = st.number_input("Сколько строк отправить", min_value=1, max_value=1000, value=10)

if uploaded_file is not None:
    try:
        data = pd.read_csv(uploaded_file)
        missing = REQUIRED_COLUMNS - set(data.columns)
        if missing:
            st.error("Не хватает колонок: " + ", ".join(sorted(missing)))
        else:
            st.write(f"В файле {len(data):,} транзакций. Будут отправлены первые {min(row_limit, len(data))}.")
            if st.button("Отправить транзакции", disabled=data.empty):
                producer = KafkaProducer(
                    bootstrap_servers=BROKERS,
                    value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
                )
                sample = data.head(row_limit).to_dict(orient="records")
                progress = st.progress(0)
                for index, transaction in enumerate(sample, start=1):
                    payload = {
                        "transaction_id": str(uuid.uuid4()),
                        "data": transaction,
                    }
                    producer.send(TOPIC, value=payload).get(timeout=15)
                    progress.progress(index / len(sample))
                producer.flush(timeout=15)
                producer.close()
                st.success(f"Отправлено {len(sample)} транзакций в топик {TOPIC}.")
    except Exception as error:
        st.error(f"Не удалось обработать файл или отправить сообщения: {error}")
