FROM python:3.13.0-slim
WORKDIR /app

RUN apt-get update && apt-get install -y \
    gzip \
    pigz \
    pv \
    lz4 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "main.py"]