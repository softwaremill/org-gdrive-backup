FROM python:3.13.0-slim
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /bin/
WORKDIR /app

RUN apt-get update && apt-get install -y \
    gzip \
    pigz \
    pv \
    lz4 \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml pyproject.toml
COPY uv.lock uv.lock
RUN uv sync --frozen
COPY . .

ENV PATH="/app/.venv/bin:$PATH"
CMD ["python3", "main.py"]
