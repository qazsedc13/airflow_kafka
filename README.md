# Airflow + Kafka Integration (Astro Edition)

![License](https://img.shields.io/github/license/qazsedc13/airflow_kafka)
![Airflow Version](https://img.shields.io/badge/Airflow-2.7.2-blue)
![Astro Runtime](https://img.shields.io/badge/Astro%20Runtime-9.0.0-orange)
![Docker](https://img.shields.io/badge/Docker-Compose-green)

Комплексное решение для интеграции Apache Airflow с Apache Kafka, предназначенное для построения отказоустойчивых ETL/ELT конвейеров с обработкой потоковых данных в реальном времени. Проект базируется на экосистеме Astronomer (Astro CLI) и включает в себя инструменты для мониторинга и отладки.

---

## 🚀 Основные возможности

- **Стриминг данных**: Готовые шаблоны для реализации паттерна Producer-Consumer внутри Airflow DAG.
- **Мониторинг**: Встроенный интерфейс Kafka UI для визуального контроля топиков, групп потребителей и сообщений.
- **Гибкость БД**: Интеграция с основной мета-базой и дополнительной аналитической БД PostgreSQL (`kap_247_db`).
- **Dev-среда**: Полная поддержка VS Code Dev Containers для быстрой настройки рабочего окружения.
- **Автоматизация**: Предварительно настроенные Airflow Connections для Kafka и PostgreSQL через `airflow_settings.yaml`.

---

## 🛠 Технологический стек

- **Orchestration**: Apache Airflow 2.7.2 (Astro Runtime 9.0.0).
- **Messaging**: Apache Kafka, Zookeeper.
- **Database**: PostgreSQL 15 (Metadata & Data storage).
- **BI/Monitoring**: Kafka UI.
- **Python Libraries**: `confluent-kafka`, `kafka-python`, `psycopg2-binary`.

---

## 📦 Структура проекта

- `dags/` — исходный код всех DAG-файлов.
  - `produce_consume_treats.py` — основной пример Producer/Consumer.
  - `job_kafka_produce_json_*.py` — генерация сложных JSON-событий.
  - `listen_to_the_stream.py` — пример бесконечного слушателя потока.
- `.astro/` — конфигурация и тесты Astro CLI.
- `tests/` — модульные тесты для проверки целостности DAG.
- `airflow_settings.yaml` — декларативное описание подключений и переменных.
- `docker-compose.override.yml` — дополнительные настройки инфраструктуры (Kafka UI, доп. БД).
- `requirements.txt` / `packages.txt` — зависимости Python и системные пакеты.

---

## ⚙️ Быстрый старт

### Предварительные требования
- Docker Engine 20.10+
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) (рекомендуется)

### 1. Клонирование репозитория
```bash
git clone git@github.com:qazsedc13/airflow_kafka.git
cd airflow_kafka
```

### 2. Запуск проекта
С использованием Astro CLI (автоматически подхватит все настройки):
```bash
astro dev start
```
Или через стандартный Docker Compose:
```bash
docker-compose up -d
```

### 3. Доступ к интерфейсам
| Сервис | URL | Логин / Пароль |
| :--- | :--- | :--- |
| **Airflow UI** | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` |
| **Kafka UI** | [http://localhost:8888](http://localhost:8888) | - |
| **Kafka Broker** | `localhost:9092` | - |

---

## 🔧 Конфигурация

### Соединения (Connections)
В проекте уже настроены следующие ID соединений (см. `airflow_settings.yaml`):
- `kafka_synapce`: Основное подключение к брокеру Kafka.
- `kap_247_db`: Дополнительная база данных PostgreSQL.

### Порты сервисов
- **Kafka**: 9092 (host), 19092 (internal).
- **Zookeeper**: 2181.
- **PostgreSQL**: 5432 (internal), 5433 (external для `kap_247_db`).

---

## 🛠 Разработка и тестирование

**Запуск тестов:**
```bash
astro dev pytest
```

**Проверка системных пакетов:**
Запустите DAG `system_packages_check` для верификации установленных библиотек в контейнере.

**Dev Container:**
Если вы используете VS Code, просто откройте папку проекта и согласитесь на запуск в контейнере. Все зависимости будут установлены автоматически.

---

## ⚠️ Устранение неполадок

- **Kafka UI пустой**: Убедитесь, что контейнер `kafka` находится в статусе `healthy`.
- **Ошибка подключения в DAG**: Проверьте, что в Airflow UI создан Connection с ID `kafka_synapce`.
- **Проблемы с памятью**: Для стабильной работы Kafka и Airflow рекомендуется выделить минимум 6GB RAM в настройках Docker.

---

## 📄 Лицензия

Проект распространяется под лицензией **MIT**.

---

## 📚 Ресурсы

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Astro Runtime Release Notes](https://docs.astronomer.io/astro/runtime-release-notes)
- [Confluent Kafka Python Guide](https://docs.confluent.io/kafka-python/current/overview.html)
