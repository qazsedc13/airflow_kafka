# Airflow + Kafka + PostgreSQL (Event Processing Edition)

![License](https://img.shields.io/github/license/qazsedc13/airflow_kafka)
![Airflow Version](https://img.shields.io/badge/Airflow-2.7.2-blue)
![Astro Runtime](https://img.shields.io/badge/Astro%20Runtime-9.0.0-orange)
![Docker](https://img.shields.io/badge/Docker-Compose-green)

Специализированная ветка проекта `airflow_kafka`, ориентированная на обработку структурированных событий (Events) и интеграцию с PostgreSQL. Включает в себя расширенные примеры для работы с типами событий `BULKAGENTSERVICESEVENT` и `CONTENTEVENT`, а также утилиты для взаимодействия с брокером сообщений.

---

## 🚀 Основные возможности

- **Специфическая обработка событий**: Реализация логики для публикации и чтения сложных JSON-событий (Agent Services, Content Events).
- **Интеграция с БД**: Настроенные механизмы для сохранения результатов обработки потоковых данных в PostgreSQL.
- **Модульность**: Вынос общей логики работы с Kafka в отдельный модуль `kafka_utils.py`.
- **Мониторинг**: Встроенный Kafka UI для отладки и просмотра содержимого топиков.
- **Автоматизация**: Декларативное управление подключениями через `airflow_settings.yaml`.

---

## 🛠 Технологический стек

- **Orchestration**: Apache Airflow 2.7.2 (Astro Runtime 9.0.0).
- **Messaging**: Apache Kafka, Zookeeper.
- **Database**: PostgreSQL 15 (Metadata & Event storage).
- **BI/Monitoring**: Kafka UI.
- **Python Libraries**: `confluent-kafka`, `faker`, `psycopg2-binary`.

---

## 📦 Структура проекта

- `dags/` — специализированные DAG-файлы для обработки событий.
  - `job_kafka_produce_json_BULKAGENTSERVICESEVENT.py` — генерация событий сервисов агентов.
  - `job_kafka_produce_json_CONTENTEVENT.py` — генерация событий контента.
  - `job_kafka_read_BULKAGENTSERVICESEVENT.py` — потребление и обработка данных BULKAGENTSERVICES.
  - `job_kafka_read_CONTENTEVENT.py` — потребление и обработка данных CONTENT.
  - `kafka_utils.py` — вспомогательные функции для работы с Kafka (shared logic).
- `airflow_settings.yaml` — описание подключений (Connections) и переменных (Variables).
- `docker-compose.override.yml` — конфигурация инфраструктуры (Kafka UI на порту 8888, PostgreSQL).
- `requirements.txt` — зависимости (включая `faker` для генерации тестовых данных).

---

## ⚙️ Быстрый старт

### Предварительные требования
- Docker Engine 20.10+
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)

### 1. Клонирование и переход в ветку
```bash
git clone git@github.com:qazsedc13/airflow_kafka.git
cd airflow_kafka
git checkout airflow_kafka_postgres
```

### 2. Запуск проекта
```bash
astro dev start
```

### 3. Доступ к интерфейсам
| Сервис | URL | Логин / Пароль |
| :--- | :--- | :--- |
| **Airflow UI** | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` |
| **Kafka UI** | [http://localhost:8888](http://localhost:8888) | - |

---

## 🔧 Конфигурация

### Ключевые соединения
Убедитесь, что в `airflow_settings.yaml` настроены:
- `kafka_synapce`: Хост `kafka:19092`.
- `kap_247_db`: Параметры доступа к PostgreSQL.

---

## ⚠️ Устранение неполадок

- **Ошибки в логах Consumer**: Проверьте, что Producer успешно отправил сообщения в топики `BULKAGENTSERVICESEVENT` или `CONTENTEVENT`.
- **Проблемы с Kafka UI**: Если интерфейс недоступен, проверьте статус контейнеров: `astro dev ps`.

---

## 📄 Лицензия

Проект распространяется под лицензией **MIT**.

---

## 📚 Ресурсы

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Astronomer Registry](https://registry.astronomer.io/)
- [Kafka Python Client](https://github.com/confluentinc/confluent-kafka-python)
