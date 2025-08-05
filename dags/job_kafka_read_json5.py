from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.hooks.base import BaseHook  # для получения Connection из Airflow

# Импорт Kafka
from kafka import KafkaConsumer  # <-- Вот этого не хватало

import json
import logging
import socket

# Логгер
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': 60,
}

# Конфигурация Kafka (лучше вынести в Variable или Connection)
KAFKA_BROKERS = ['kafka-broker1:9092', 'kafka-broker2:9092']
KAFKA_TOPIC = 'SCPL.BULKAGENTSERVICESEVENT.V1'
GROUP_ID = 'airflow_kafka_to_postgres_group'

def consume_and_process_messages(**kwargs):
    log.info("Запуск Kafka Consumer")

    # Получаем настройки из Connection
    conn = BaseHook.get_connection('kafka_synapce')
    config = conn.extra_dejson

    bootstrap_servers = config.get("bootstrap.servers", "localhost:19092")
    group_id = config.get("group.id", "airflow_kafka_to_postgres_group")
    auto_offset_reset = config.get("auto.offset.reset", "earliest")
    enable_auto_commit = config.get("enable.auto.commit", False)

    # Тест подключения к каждому брокеру
    for broker in bootstrap_servers.split(','):
        host, port = broker.split(':')
        try:
            with socket.create_connection((host, int(port)), timeout=5):
                log.info(f"✅ Подключение к {broker} прошло успешно")
        except Exception as e:
            log.error(f"❌ Ошибка подключения к {broker}: {e}")
            raise AirflowException(f"Не могу подключиться к Kafka-брокеру {broker}")

    # Создаём KafkaConsumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        max_poll_records=1000,
        session_timeout_ms=45000,
        heartbeat_interval_ms=10000,
    )

def process_messages(messages):
    """
    Парсинг и запись сообщений в БД
    """

    pg_hook = PostgresHook(postgres_conn_id='kap_247_db')

    insert_query = """
    INSERT INTO kap_247_scpl.bulkagentservicesevent_tmp (
        message_id, tenant_id, division_id, service_id,
        user_id, last_name, first_name, patronymic, employee_id,
        tenant_user_id, proficiency_level, connection_type, interaction_search_tactic
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = []
    processed_count = 0

    for msg in messages:
        try:
            payload = msg.value.get("payload", {})
            users = payload.get("user_info", [])
            message_id = payload.get("message_id")
            tenant_id = payload.get("tenant_id")
            division_id = payload.get("division_id")
            service_id = payload.get("service_id")

            for user in users:
                record = (
                    message_id,
                    tenant_id,
                    division_id,
                    service_id,
                    user.get("user_id"),
                    user.get("last_name"),
                    user.get("first_name"),
                    user.get("patronymic"),
                    user.get("employee_id"),
                    user.get("tenant_user_id"),
                    user.get("proficiency_level"),
                    user.get("connection_type"),
                    user.get("interaction_search_tactic"),
                )
                records.append(record)
                processed_count += 1
        except Exception as e:
            log.warning(f"Ошибка при обработке сообщения: {e}")

    if records:
        pg_hook.insert_rows(
            table="kap_247_scpl.bulkagentservicesevent_tmp",
            rows=records,
            target_fields=[
                "message_id", "tenant_id", "division_id", "service_id",
                "user_id", "last_name", "first_name", "patronymic", "employee_id",
                "tenant_user_id", "proficiency_level", "connection_type", "interaction_search_tactic"
            ],
            commit_every=500
        )
        log.info(f"Записано {processed_count} записей в БД")

with DAG(
    dag_id='kafka_to_postgres_bulk_agent_service_events',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
    tags=['kafka', 'postgres', 'scpl']
) as dag:

    consume_task = PythonOperator(
        task_id='read_from_kafka',
        python_callable=consume_and_process_messages,
        provide_context=True,
    )