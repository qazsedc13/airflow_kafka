# Airflow Kafka Integration (Astro)

Проект на базе Astro CLI для разработки и тестирования интеграции Apache Airflow с Apache Kafka.

## Описание
Данный репозиторий содержит набор DAG для работы с Kafka, включая задачи по публикации сообщений (Produce) и их чтению (Consume). Проект настроен для использования с Astro CLI, что упрощает локальную разработку и развертывание в облаке Astronomer.

## Технологический стек
- **Оркестрация:** Apache Airflow (развернутый через Astro CLI)
- **Брокер сообщений:** Apache Kafka
- **Язык программирования:** Python
- **Провайдеры Airflow:** 
  - `apache-airflow-providers-apache-kafka`
  - `apache-airflow-providers-amazon`
  - `apache-airflow-providers-apache-spark`
- **Библиотеки:** `confluent-kafka`, `kafka-python`, `boto3`

## Установка и запуск

1. Установите [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli).
2. Склонируйте репозиторий:
   ```bash
   git clone git@github.com:qazsedc13/airflow_kafka.git
   ```
3. Запустите проект локально:
   ```bash
   astro dev start
   ```
4. После успешного запуска Airflow будет доступен по адресу [http://localhost:8080](http://localhost:8080) (логин/пароль: `admin`/`admin`).

## Примеры использования
В проекте реализовано множество примеров в папке `dags/`:
- `kafka-example`: Базовый пример использования `ProduceToTopicOperator` и `ConsumeFromTopicOperator`.
- `job_kafka_produce_json.py`: Демонстрация отправки структурированных JSON-данных в Kafka.
- `job_kafka_read_json3.py` / `job_kafka_read_json4.py`: Примеры чтения и обработки JSON-сообщений.
- `produce_consume_treats.py`: Сложный сценарий взаимодействия с Kafka.

## Структура проекта
- `dags/` — исходный код всех DAG-файлов.
- `tests/` — тесты на целостность DAG и корректность логики.
- `requirements.txt` — список Python-зависимостей.
- `packages.txt` — системные пакеты, необходимые для работы (например, библиотеки для Kafka).
- `airflow_settings.yaml` — локальные настройки подключений и переменных Airflow.
- `.astro/` — конфигурационные файлы Astro CLI.

## Зависимости и требования
- Docker Engine 20.10.0+
- Astro CLI
- Настроенный брокер Kafka (в примерах используется `kafka:19092`).
