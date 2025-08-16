
# Airflow + Kafka интеграция

Проект демонстрирует интеграцию Apache Airflow с Apache Kafka для построения ETL/ELT pipelines с обработкой событий в реальном времени.

## Особенности проекта

- Полностью контейнеризированное решение (Docker)
- Готовые DAGs для работы с Kafka:
  - `produce_consume_treats.py` - производитель и потребитель сообщений Kafka
  - `listen_to_the_stream.py` - слушатель Kafka потока
- Встроенная Kafka UI для мониторинга (порт 8888)
- Подключение к дополнительной PostgreSQL базе данных
- Предварительно настроенные Airflow Connections для Kafka и PostgreSQL

## Технологический стек

- Apache Airflow (Astro Runtime 9.0.0)
- Apache Kafka
- Confluent Kafka
- PostgreSQL (основная и дополнительная БД)
- Kafka UI

## Быстрый старт

### Предварительные требования

- Установленный Docker и Docker Compose
- Astro CLI (рекомендуется)
- Git

### Запуск проекта

1. Клонируйте репозиторий:
   ```bash
   git clone https://github.com/qazsedc13/airflow_kafka.git
   cd airflow_kafka
   ```

2. Запустите проект (с использованием Astro CLI):
   ```bash
   astro dev start
   ```

   Или с помощью Docker Compose:
   ```bash
   docker-compose up -d
   ```

3. После запуска будут доступны:
   - Airflow UI: http://localhost:8080
   - Kafka UI: http://localhost:8888

4. В Airflow UI:
   - Разверните DAG `produce_consume_treats`
   - Запустите его вручную
   - Наблюдайте за обработкой сообщений в Kafka

## Конфигурация

### Основные сервисы

- **Kafka**: доступна на портах 9092 и 19092
- **Zookeeper**: порт 2181
- **PostgreSQL (основная)**: порт 5432 (внутри контейнера)
- **PostgreSQL (kap_247_db)**: порт 5433 (наружу), 5432 (внутри)

### Airflow Connections

Предварительно настроены соединения:
- `kafka_synapce` - для работы с Kafka
- `kafka_listener` - альтернативное соединение с Kafka
- `kap_247_db` - соединение с дополнительной PostgreSQL БД

### Переменные окружения

Настройки можно изменить в файлах:
- `airflow_settings.yaml` - настройки Airflow
- `docker-compose.override.yml` - настройки сервисов

## Разработка

### Dev Container

Проект поддерживает работу в Dev Container (VS Code) с предустановленным:
- Astro CLI v1.9.0
- Все необходимые зависимости

### Установка дополнительных пакетов

Добавьте необходимые Python пакеты в `requirements.txt` и системные зависимости в `packages.txt`.

## Лицензия

Проект распространяется под MIT License.

## Ресурсы

- [Официальная документация Airflow](https://airflow.apache.org/docs/)
- [Документация Kafka](https://kafka.apache.org/documentation/)
- [Astro CLI документация](https://docs.astronomer.io/astro/cli/install-cli)
- [Примеры интеграции Airflow и Kafka](https://docs.astronomer.io/learn/airflow-kafka)
```
