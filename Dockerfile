FROM quay.io/astronomer/astro-runtime:9.0.0

# Устанавливаем PostgreSQL client (выполняем от root)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        postgresql-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Устанавливаем только нужные пакеты
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

# Возвращаемся к пользователю astro
USER astro