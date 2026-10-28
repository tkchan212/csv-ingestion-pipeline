FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install .

COPY config ./config

RUN useradd --create-home app
USER app

ENTRYPOINT ["csv-ingestor"]
CMD ["--config", "/app/config/pipeline.yaml"]
