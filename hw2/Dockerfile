FROM python:3.12.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py ./
COPY sql ./sql
CMD ["python", "run_pipeline.py", "--csv", "/data/hw2_sample.csv", "--output-dir", "/results"]
