FROM apache/airflow:2.10.5-python3.11
USER airflow
RUN pip install --no-cache-dir "apache-airflow==2.10.5" \
    "numpy==1.26.4" "pandas==2.1.4" "scipy==1.14.1" \
    "scikit-learn==1.5.2" "catboost==1.2.8" "pyarrow==17.0.0" \
    "joblib==1.4.2" "cloudpickle==2.2.1" "confluent-kafka==2.10.0" \
    && pip check
COPY --chown=airflow:root hw3/hw3_pipeline /opt/airflow/hw3_pipeline
COPY --chown=airflow:root hw3/dags /opt/airflow/dags
COPY --chown=airflow:root hw3/tests /opt/airflow/tests
COPY --chown=airflow:root fraud_detector/src /opt/airflow/fraud_src
ENV PYTHONPATH=/opt/airflow:/opt/airflow/fraud_src
