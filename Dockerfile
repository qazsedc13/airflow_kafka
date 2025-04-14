FROM quay.io/astronomer/astro-runtime:8.4.0

# Install Kerberos and other system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        krb5-user \
        libkrb5-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Удаляем все предустановленные пакеты
USER root
RUN pip freeze | xargs pip uninstall -y

# Устанавливаем только нужные пакеты
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

USER astro