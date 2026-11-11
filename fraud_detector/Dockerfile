FROM python:3.12-slim AS scorer
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY src ./src
COPY models ./models
CMD ["python", "app/app.py"]

FROM scorer AS monitored
RUN pip install --no-cache-dir prometheus-client==0.22.1

FROM scorer AS runtime
