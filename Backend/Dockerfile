# Use python slim image
FROM python:3.11-slim

# --- metadata ---
LABEL maintainer="Hugo <seu-email@exemplo.com>"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# --- instalando deps do SO (se precisar build de pacotes) ---
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia requirements primeiro para cache de layer
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copia app
COPY app.py /app/app.py

# Porta padrão
ENV PORT=8080
EXPOSE 8080

# Comando de execução (Gunicorn). Ajuste workers conforme CPU.
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "3", "app:app"]
