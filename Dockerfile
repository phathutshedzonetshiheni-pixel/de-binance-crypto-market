# Crypto DE pipeline — batch ingestion, streaming, dbt (BigQuery).
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY docker/entrypoint.sh /usr/local/bin/docker-entrypoint.sh
# Normalize CRLF when the repo is checked out on Windows
RUN sed -i 's/\r$//' /usr/local/bin/docker-entrypoint.sh && chmod +x /usr/local/bin/docker-entrypoint.sh

COPY ingestion/ ./ingestion/
COPY orchestration/ ./orchestration/
COPY transform/ ./transform/
COPY tests/ ./tests/

# Image uses env-driven profile (see transform/profiles.yml.example); avoid baking local paths.
RUN cp transform/profiles.yml.example transform/profiles.yml

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
# Override with docker compose or `docker run ... python ingestion/...`
CMD ["python", "--version"]
