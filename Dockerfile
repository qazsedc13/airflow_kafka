FROM quay.io/astronomer/astro-runtime:9.0.0

# Устанавливаем только нужные пакеты
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

USER astro