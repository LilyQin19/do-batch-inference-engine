FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

COPY data/sample_batch.json ./data/sample_batch.json

EXPOSE 8000
ENV BATCHENGINE_DB_PATH=/app/data/jobs.db \
    BATCHENGINE_RESULTS_DIR=/app/data/results

CMD ["uvicorn", "batchengine.main:app", "--host", "0.0.0.0", "--port", "8000"]
