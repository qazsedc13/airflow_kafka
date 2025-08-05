import json  # Добавляем этот импорт в начало файла
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pendulum import datetime
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
import uuid
import logging
from confluent_kafka import Consumer, KafkaException

# Константы
CONN_ID_KAP = 'kap_247_db'
KAFKA_TOPIC_C = "SCPL.CONTENTEVENT.V1"
SCHEMA_NAME = "kap_247_scpl"
TMP_TABLE = "contentevent_tmp"
MAIN_TABLE = "CONTENTEVENT_air"
DATA_RETENTION_DAYS = 30
PROCESSING_BATCH_SIZE = 500
NO_MESSAGES_WAIT_SECONDS = 30

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# SQL для создания таблиц
CREATE_TMP_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TMP_TABLE} (
    id SERIAL PRIMARY KEY,
    message_key VARCHAR(255) NOT NULL,
    text_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP WITH TIME ZONE,
    processing_status VARCHAR(20) DEFAULT 'NEW'
);
"""

CREATE_MAIN_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{MAIN_TABLE} (
    id SERIAL PRIMARY KEY,
    tmp_record_id INTEGER REFERENCES {SCHEMA_NAME}.{TMP_TABLE}(id),
    event_type VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    message_id UUID NOT NULL,
    interaction_id UUID NOT NULL,
    content_id UUID,
    sender_type VARCHAR(50),
    user_id UUID,
    content_timestamp TIMESTAMP WITH TIME ZONE,
    content_text TEXT,
    content_type VARCHAR(50),
    workitem_id UUID,
    segment_id VARCHAR(100),
    routing_point_id VARCHAR(100),
    workflow_id VARCHAR(100),
    service_id UUID,
    client_id VARCHAR(100),
    client_name VARCHAR(255),
    client_token VARCHAR(255),
    client_url VARCHAR(500),
    client_phone_number VARCHAR(50),
    client_email VARCHAR(255),
    client_ext_messenger_id VARCHAR(100),
    tenant_id UUID NOT NULL,
    division_id UUID,
    division_name VARCHAR(255),
    full_division_names VARCHAR(255),
    routing_point_name VARCHAR(255),
    service_name VARCHAR(255),
    last_name VARCHAR(100),
    first_name VARCHAR(100),
    patronymic VARCHAR(100),
    employee_id VARCHAR(50),
    file_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_{MAIN_TABLE}_tmp_id ON {SCHEMA_NAME}.{MAIN_TABLE}(tmp_record_id);
CREATE INDEX IF NOT EXISTS idx_{MAIN_TABLE}_timestamp ON {SCHEMA_NAME}.{MAIN_TABLE}(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_{MAIN_TABLE}_tenant ON {SCHEMA_NAME}.{MAIN_TABLE}(tenant_id);
CREATE INDEX IF NOT EXISTS idx_{MAIN_TABLE}_interaction ON {SCHEMA_NAME}.{MAIN_TABLE}(interaction_id);
CREATE INDEX IF NOT EXISTS idx_{MAIN_TABLE}_event_type ON {SCHEMA_NAME}.{MAIN_TABLE}(event_type);
"""

def _get_engine(conn_id_str):
    """Создаёт и возвращает SQLAlchemy engine"""
    conn = BaseHook.get_connection(conn_id_str)
    return create_engine(
        f'postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}',
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        pool_recycle=3600
    )

def _ensure_tables_exist():
    """Создаёт таблицы если они не существуют"""
    engine = _get_engine(CONN_ID_KAP)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))
        conn.execute(text(CREATE_TMP_TABLE_SQL))
        conn.execute(text(CREATE_MAIN_TABLE_SQL))
    engine.dispose()

def consume_function(message, rqUid):
    """
    Обрабатывает сообщения из kafka и сохраняет во временную таблицу
    """
    engine = _get_engine(CONN_ID_KAP)
    try:
        if not message or not hasattr(message, 'value'):
            logger.error('Получено некорректное сообщение')
            return
        
        msg_value = message.value()
        if not msg_value:
            logger.warning('Получено пустое сообщение')
            return
        
        decode_message = msg_value.decode('utf-8')
        message_key = message.key().decode('utf-8') if message.key() else str(uuid.uuid4())

        logger.info(f"Получено сообщение. Key: {message_key}, Size: {len(decode_message)} bytes")

        with engine.begin() as conn:
            conn.execute(
                text(f"""
                INSERT INTO {SCHEMA_NAME}.{TMP_TABLE}
                (message_key, text_json, processing_status)
                VALUES (:key, :message, 'PROCESSED')
                """),
                {"key": message_key, "message": decode_message}
            )
    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {str(e)}", exc_info=True)
    finally:
        engine.dispose()

@task
def process_messages():
    """Переносит данные из временной таблицы в основную"""
    engine = _get_engine(CONN_ID_KAP)
    
    with engine.begin() as conn:
        total = conn.execute(text(f"""
            SELECT COUNT(*) 
            FROM {SCHEMA_NAME}.{TMP_TABLE}
            WHERE processing_status = 'PROCESSED'
            AND processed_at IS NULL
        """)).scalar()
        logger.info(f"Найдено {total} записей для обработки в БД")
        
        if total == 0:
            logger.info("Нет новых записей для обработки")
            return
            
        processed = 0
        while processed < total:
            batch = conn.execute(text(f"""
                WITH batch AS (
                    SELECT id, text_json
                    FROM {SCHEMA_NAME}.{TMP_TABLE}
                    WHERE processing_status = 'PROCESSED'
                    AND processed_at IS NULL
                    ORDER BY created_at
                    LIMIT {PROCESSING_BATCH_SIZE}
                    FOR UPDATE SKIP LOCKED
                )
                INSERT INTO {SCHEMA_NAME}.{MAIN_TABLE} (
                    tmp_record_id, event_type, event_timestamp, message_id, interaction_id,
                    content_id, sender_type, user_id, content_timestamp, content_text,
                    content_type, workitem_id, segment_id, routing_point_id, workflow_id,
                    service_id, client_id, client_name, client_token, client_url,
                    client_phone_number, client_email, client_ext_messenger_id, tenant_id,
                    division_id, division_name, full_division_names, routing_point_name,
                    service_name, last_name, first_name, patronymic, employee_id, file_id
                )
                SELECT 
                    batch.id,
                    batch.text_json::jsonb->'payload'->>'event_type',
                    (batch.text_json::jsonb->'payload'->>'timestamp')::timestamp,
                    (batch.text_json::jsonb->'payload'->>'message_id')::uuid,
                    (batch.text_json::jsonb->'payload'->>'interaction_id')::uuid,
                    NULLIF(batch.text_json::jsonb->'payload'->>'content_id', '')::uuid,
                    batch.text_json::jsonb->'payload'->>'sender_type',
                    NULLIF(batch.text_json::jsonb->'payload'->>'user_id', '')::uuid,
                    NULLIF(batch.text_json::jsonb->'payload'->>'content_timestamp', '')::timestamp,
                    batch.text_json::jsonb->'payload'->>'content_text',
                    batch.text_json::jsonb->'payload'->>'content_type',
                    NULLIF(batch.text_json::jsonb->'payload'->>'workitem_id', '')::uuid,
                    batch.text_json::jsonb->'payload'->>'segment_id',
                    batch.text_json::jsonb->'payload'->>'routing_point_id',
                    batch.text_json::jsonb->'payload'->>'workflow_id',
                    NULLIF(batch.text_json::jsonb->'payload'->>'service_id', '')::uuid,
                    batch.text_json::jsonb->'payload'->>'client_id',
                    batch.text_json::jsonb->'payload'->>'client_name',
                    batch.text_json::jsonb->'payload'->>'client_token',
                    batch.text_json::jsonb->'payload'->>'client_url',
                    batch.text_json::jsonb->'payload'->>'client_phone_number',
                    batch.text_json::jsonb->'payload'->>'client_email',
                    batch.text_json::jsonb->'payload'->>'client_ext_messenger_id',
                    (batch.text_json::jsonb->'payload'->>'tenant_id')::uuid,
                    NULLIF(batch.text_json::jsonb->'payload'->>'division_id', '')::uuid,
                    batch.text_json::jsonb->'payload'->>'division_name',
                    batch.text_json::jsonb->'payload'->>'full_division_names',
                    batch.text_json::jsonb->'payload'->>'routing_point_name',
                    batch.text_json::jsonb->'payload'->>'service_name',
                    batch.text_json::jsonb->'payload'->>'last_name',
                    batch.text_json::jsonb->'payload'->>'first_name',
                    batch.text_json::jsonb->'payload'->>'patronymic',
                    batch.text_json::jsonb->'payload'->>'employee_id',
                    batch.text_json::jsonb->'payload'->>'file_id'
                FROM batch
                RETURNING tmp_record_id
            """)).rowcount
            
            updated = conn.execute(text(f"""
                UPDATE {SCHEMA_NAME}.{TMP_TABLE}
                SET processed_at = CURRENT_TIMESTAMP
                WHERE id IN (
                    SELECT id 
                    FROM {SCHEMA_NAME}.{TMP_TABLE}
                    WHERE processing_status = 'PROCESSED'
                    AND processed_at IS NULL
                    ORDER BY created_at
                    LIMIT {PROCESSING_BATCH_SIZE}
                    FOR UPDATE SKIP LOCKED
                )
            """)).rowcount
            
            processed += updated
            logger.info(f"Обработано {updated} записей. Всего: {processed}/{total}")

@task
def cleanup_old_data():
    """Очищает старые обработанные данные"""
    engine = _get_engine(CONN_ID_KAP)
    with engine.begin() as conn:
        deleted = conn.execute(text(f"""
            DELETE FROM {SCHEMA_NAME}.{TMP_TABLE}
            WHERE processed_at < NOW() - INTERVAL '{DATA_RETENTION_DAYS} days'
            RETURNING id
        """)).rowcount
        logger.info(f"Удалено {deleted} старых записей")

def kafka_consumer_operator(**context):
    """Кастомный оператор для чтения из Kafka"""
    conn = BaseHook.get_connection('kafka_synapce')
    extra_config = {}
    try:
        extra_config = json.loads(conn.extra or '{}')
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка парсинга extra конфига Kafka: {str(e)}")
    
    conf = {
        'bootstrap.servers': f"{conn.host}:{conn.port}",
        'group.id': f"airflow_{context['dag'].dag_id}_{context['ts_nodash']}",
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False,
        **extra_config
    }
    
    logger.info(f"Подключаемся к Kafka с конфигом: {str(conf)}")
    
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC_C])
    
    try:
        processed_count = 0
        while processed_count < PROCESSING_BATCH_SIZE * 200:
            msg = consumer.poll(timeout=NO_MESSAGES_WAIT_SECONDS)
            
            if msg is None:
                logger.info("Новых сообщений в топике не обнаружено")
                break
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    logger.info("Достигнут конец раздела топика")
                    continue
                else:
                    logger.error(f"Ошибка Kafka: {msg.error()}")
                    raise KafkaException(msg.error())
            
            try:
                # Обработка сообщения
                consume_function(msg, context['ti'].xcom_pull(task_ids='generate_rq_uid'))
                processed_count += 1
                if processed_count % 100 == 0:
                    logger.info(f"Обработано сообщений: {processed_count}")
            except Exception as e:
                logger.error(f"Ошибка обработки сообщения: {str(e)}", exc_info=True)
                continue
            
    except Exception as e:
        logger.error(f"Критическая ошибка в consumer: {str(e)}", exc_info=True)
        raise
    finally:
        consumer.close()
        logger.info("Consumer закрыт")

@dag(
    start_date=datetime(2025, 4, 9),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    render_template_as_native_obj=True,
    tags=['kafka', 'integr', 'scpl', 'content'],
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(minutes=30)
    }
)
def job_kafka_read_CONTENTEVENT():
    @task
    def setup_database():
        """Создаёт таблицы если они не существуют"""
        _ensure_tables_exist()
    
    @task
    def generate_rq_uid():
        """Генерирует уникальный ID для отслеживания"""
        return str(uuid.uuid4())
    
    # Заменяем ConsumeFromTopicOperator на PythonOperator
    consume_task = PythonOperator(
        task_id="consume",
        python_callable=kafka_consumer_operator,
        provide_context=True
    )
    
    # Порядок выполнения задач
    setup_db = setup_database()
    rq_uid = generate_rq_uid()
    process_task = process_messages()
    cleanup_task = cleanup_old_data()
    
    setup_db >> rq_uid >> consume_task >> process_task >> cleanup_task

job_kafka_read_CONTENTEVENT()