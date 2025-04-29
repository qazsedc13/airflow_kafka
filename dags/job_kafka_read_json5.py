"""
DAG для массовой обработки событий из Kafka с сохранением в PostgreSQL:
1. Итеративно обрабатывает сообщения порциями
2. Сохраняет позицию обработки между запусками
3. Автоматически продолжает обработку, пока не обработает все данные
4. Контролирует время выполнения в пределах окна хранения Kafka
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable, TaskInstance
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from sqlalchemy import create_engine, text
import uuid
import logging
from typing import Dict, Any

# Константы
CONN_ID_KAP = 'kap_247_db'
KAFKA_TOPIC_C = "SCPL.BULKAGENTSERVICESEVENT.V1"
SCHEMA_NAME = "kap_247_scpl"
TMP_TABLE = "bulkagentservicesevent_tmp"
MAIN_TABLE = "BULKAGENTSERVICESEVENT"
DATA_RETENTION_DAYS = 30
BATCH_LIMIT = 50000  # Макс. сообщений за один запуск DAG
CHUNK_SIZE = 5000    # Размер пачки для обработки в одной транзакции
OFFSET_STORAGE_KEY = f"{KAFKA_TOPIC_C}_last_offset"
DEFAULT_OFFSET = "earliest"
MAX_PROCESSING_HOURS = 3.5  # Оставляем запас 30 минут до истечения 4 часов

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
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
    processing_status VARCHAR(20) DEFAULT 'NEW',
    kafka_offset BIGINT
);
CREATE INDEX IF NOT EXISTS idx_{TMP_TABLE}_offset ON {SCHEMA_NAME}.{TMP_TABLE}(kafka_offset);
"""

CREATE_MAIN_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{MAIN_TABLE} (
    id SERIAL PRIMARY KEY,
    tmp_record_id INTEGER REFERENCES {SCHEMA_NAME}.{TMP_TABLE}(id),
    event_type VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    tenant_id UUID NOT NULL,
    division_id UUID NOT NULL,
    division_name VARCHAR(255),
    service_id UUID NOT NULL,
    service_name VARCHAR(255),
    message_id UUID NOT NULL,
    service_kind VARCHAR(50),
    user_id UUID NOT NULL,
    tenant_user_id UUID NOT NULL,
    proficiency_level INTEGER,
    connection_type VARCHAR(50),
    interaction_search_tactic VARCHAR(50),
    last_name VARCHAR(100),
    first_name VARCHAR(100),
    patronymic VARCHAR(100),
    employee_id VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_{MAIN_TABLE}_timestamp ON {SCHEMA_NAME}.{MAIN_TABLE}(event_timestamp);
"""

def _get_engine(conn_id_str: str):
    """Создаёт оптимизированный engine для массовой вставки"""
    conn = BaseHook.get_connection(conn_id_str)
    return create_engine(
        f'postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}',
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        pool_recycle=3600,
        executemany_mode='values',
        executemany_values_page_size=10000
    )

def _ensure_tables_exist():
    """Создаёт таблицы с дополнительными индексами"""
    engine = _get_engine(CONN_ID_KAP)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))
        conn.execute(text(CREATE_TMP_TABLE_SQL))
        conn.execute(text(CREATE_MAIN_TABLE_SQL))
    engine.dispose()

def get_last_offset() -> str:
    """Возвращает последний успешно обработанный offset"""
    return Variable.get(OFFSET_STORAGE_KEY, default_var=DEFAULT_OFFSET)

def store_last_offset(offset: int):
    """Сохраняет последний обработанный offset в Variable"""
    Variable.set(OFFSET_STORAGE_KEY, str(offset))

def need_more_processing(**context) -> str:
    """
    Определяет, нужно ли продолжать обработку.
    Возвращает task_id следующего шага.
    """
    ti = context['ti']
    processed_count = ti.xcom_pull(task_ids='process_messages', key='processed_count') or 0
    
    # Проверяем время выполнения
    start_time_str = Variable.get("processing_start_time", default_var=None)
    if start_time_str:
        elapsed = datetime.now() - datetime.fromisoformat(start_time_str)
        if elapsed > timedelta(hours=MAX_PROCESSING_HOURS):
            logger.warning(f"Превышено максимальное время обработки ({MAX_PROCESSING_HOURS}ч)")
            return "finalize_processing"
    
    # Если обработали меньше чем BATCH_LIMIT, значит достигли конца
    if processed_count < BATCH_LIMIT * 0.9:  # 10% допуск для неравномерного распределения
        logger.info("Достигнут конец потока сообщений")
        return "finalize_processing"
    
    return "continue_processing"

def consume_function(messages: list, rqUid: str) -> Dict[str, Any]:
    """Обрабатывает пачку сообщений и возвращает статистику"""
    if not messages:
        return {'processed': 0, 'last_offset': None}
    
    engine = _get_engine(CONN_ID_KAP)
    processed = 0
    last_offset = None
    
    try:
        with engine.begin() as conn:
            # Подготавливаем данные для массовой вставки
            records = []
            for message in messages:
                if not message or not hasattr(message, 'value'):
                    continue
                
                msg_value = message.value()
                if not msg_value:
                    continue
                
                try:
                    decoded_message = msg_value.decode('utf-8')
                    message_key = message.key().decode('utf-8') if message.key() else str(uuid.uuid4())
                    records.append({
                        "key": message_key,
                        "message": decoded_message,
                        "offset": message.offset()
                    })
                    last_offset = message.offset()
                except Exception as e:
                    logger.error(f"Ошибка декодирования сообщения: {str(e)}")
                    continue
            
            # Массовая вставка
            if records:
                conn.execute(
                    text(f"""
                    INSERT INTO {SCHEMA_NAME}.{TMP_TABLE} 
                    (message_key, text_json, processing_status, kafka_offset)
                    VALUES (:key, :message, 'PROCESSED', :offset)
                    """),
                    [{"key": r['key'], "message": r['message'], "offset": r['offset']} for r in records]
                )
                processed = len(records)
                
                # Сохраняем последний offset
                if last_offset is not None:
                    store_last_offset(last_offset)
        
        logger.info(f"Успешно обработано {processed} сообщений. Последний offset: {last_offset}")
        return {'processed': processed, 'last_offset': last_offset}
    
    except Exception as e:
        logger.error(f"Критическая ошибка обработки: {str(e)}", exc_info=True)
        raise

@task
def setup_database():
    """Инициализирует структуру БД"""
    _ensure_tables_exist()
    logger.info("Проверка/создание структуры БД завершена")

@task
def generate_rq_uid():
    """Генерирует уникальный ID для трейсинга"""
    return str(uuid.uuid4())

@task
def start_processing_session():
    """Инициализирует сессию обработки"""
    Variable.set("processing_start_time", str(datetime.now()))
    logger.info("Начата новая сессия обработки")
    return get_last_offset()

@task
def process_messages(start_offset: str) -> Dict[str, Any]:
    """Задача для обработки порции сообщений"""
    logger.info(f"Начинаем обработку с offset: {start_offset}")
    
    result = ConsumeFromTopicOperator(
        task_id="consume_messages",
        kafka_config_id="kafka_synapce",
        topics=[KAFKA_TOPIC_C],
        apply_function=consume_function,
        apply_function_kwargs={"rqUid": "{{ ti.xcom_pull(task_ids='generate_rq_uid')}}"},
        consumer_config={
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "group.id": "airflow_consumer_group_v2",
            "max.poll.records": CHUNK_SIZE,
            "fetch.max.bytes": 104857600,  # 100MB
            "max.partition.fetch.bytes": 10485760  # 10MB
        },
        start_offset=start_offset,
        max_messages=BATCH_LIMIT,
        max_batch_size=CHUNK_SIZE,
        poll_timeout=300
    ).execute(context={})
    
    logger.info(f"Результат обработки: {result}")
    return {'processed_count': result.get('processed', 0), 'last_offset': result.get('last_offset')}

@task
def finalize_processing():
    """Финализирует сессию обработки"""
    Variable.delete("processing_start_time")
    logger.info("Сессия обработки завершена")

@task
def log_progress():
    """Логирует текущий прогресс обработки"""
    offset = get_last_offset()
    engine = _get_engine(CONN_ID_KAP)
    with engine.connect() as conn:
        total_processed = conn.execute(text(f"""
            SELECT COUNT(*) FROM {SCHEMA_NAME}.{TMP_TABLE}
            WHERE processing_status = 'PROCESSED'
        """)).scalar()
        
        pending = conn.execute(text(f"""
            SELECT MIN(kafka_offset), MAX(kafka_offset), COUNT(*)
            FROM {SCHEMA_NAME}.{TMP_TABLE}
            WHERE processing_status = 'PROCESSED'
            AND processed_at IS NULL
        """)).fetchone()
    
    logger.info(
        f"Прогресс обработки:\n"
        f"- Всего обработано: {total_processed}\n"
        f"- Ожидают переноса в основную таблицу: {pending[2]}\n"
        f"- Текущий offset: {offset}\n"
        f"- Диапазон offsets для обработки: {pending[0]}..{pending[1]}"
    )

@task
def transfer_to_main_table():
    """Переносит данные из временной таблицы в основную"""
    engine = _get_engine(CONN_ID_KAP)
    
    with engine.begin() as conn:
        # Логирование статистики перед обработкой
        total = conn.execute(text(f"""
            SELECT COUNT(*) 
            FROM {SCHEMA_NAME}.{TMP_TABLE}
            WHERE processing_status = 'PROCESSED'
            AND processed_at IS NULL
        """)).scalar()
        
        if total == 0:
            logger.info("Нет новых записей для переноса")
            return 0
        
        logger.info(f"Начинаем перенос {total} записей в основную таблицу")
        
        # Обработка пачками
        processed = 0
        while processed < total:
            chunk = min(CHUNK_SIZE, total - processed)
            inserted = conn.execute(text(f"""
                WITH batch AS (
                    SELECT id, text_json, kafka_offset
                    FROM {SCHEMA_NAME}.{TMP_TABLE}
                    WHERE processing_status = 'PROCESSED'
                    AND processed_at IS NULL
                    ORDER BY kafka_offset
                    LIMIT {chunk}
                    FOR UPDATE SKIP LOCKED
                )
                INSERT INTO {SCHEMA_NAME}.{MAIN_TABLE} (
                    tmp_record_id, event_type, event_timestamp, tenant_id, 
                    division_id, division_name, service_id, service_name,
                    message_id, service_kind, user_id, tenant_user_id,
                    proficiency_level, connection_type, interaction_search_tactic,
                    last_name, first_name, patronymic, employee_id
                )
                SELECT 
                    batch.id,
                    batch.text_json::jsonb->'payload'->>'event_type',
                    (batch.text_json::jsonb->'payload'->>'timestamp')::timestamp,
                    (batch.text_json::jsonb->'payload'->>'tenant_id')::uuid,
                    (batch.text_json::jsonb->'payload'->>'division_id')::uuid,
                    batch.text_json::jsonb->'payload'->>'division_name',
                    (batch.text_json::jsonb->'payload'->>'service_id')::uuid,
                    batch.text_json::jsonb->'payload'->>'service_name',
                    (batch.text_json::jsonb->'payload'->>'message_id')::uuid,
                    batch.text_json::jsonb->'payload'->>'service_kind',
                    (user_info->>'user_id')::uuid,
                    (user_info->>'tenant_user_id')::uuid,
                    (user_info->>'proficiency_level')::integer,
                    user_info->>'connection_type',
                    user_info->>'interaction_search_tactic',
                    user_info->>'last_name',
                    user_info->>'first_name',
                    user_info->>'patronymic',
                    user_info->>'employee_id'
                FROM 
                    batch,
                    jsonb_array_elements(batch.text_json::jsonb->'payload'->'user_info') user_info
                RETURNING id
            """)).rowcount
            
            # Помечаем как обработанные
            updated = conn.execute(text(f"""
                UPDATE {SCHEMA_NAME}.{TMP_TABLE}
                SET processed_at = CURRENT_TIMESTAMP
                WHERE id IN (
                    SELECT id 
                    FROM {SCHEMA_NAME}.{TMP_TABLE}
                    WHERE processing_status = 'PROCESSED'
                    AND processed_at IS NULL
                    ORDER BY kafka_offset
                    LIMIT {chunk}
                    FOR UPDATE SKIP LOCKED
                )
            """)).rowcount
            
            processed += updated
            logger.info(f"Перенесено {updated} записей. Всего: {processed}/{total}")
        
        return processed

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

@dag(
    start_date=datetime(2025, 4, 9),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    dagrun_timeout=timedelta(minutes=45),
    render_template_as_native_obj=True,
    tags=['kafka', 'postgres', 'scpl', 'production', 'bulk_processing'],
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(minutes=15)
    }
)
def job_kafka_bulk_processing():
    init_db = setup_database()
    rq_uid = generate_rq_uid()
    start_session = start_processing_session()
    
    process = process_messages(start_session)
    check_continue = BranchPythonOperator(
        task_id="check_continue_processing",
        python_callable=need_more_processing,
        provide_context=True
    )
    continue_op = DummyOperator(task_id="continue_processing")
    finalize = finalize_processing()
    
    log_progress_task = log_progress()
    transfer_data = transfer_to_main_table()
    cleanup = cleanup_old_data()
    
    # Основной workflow
    init_db >> rq_uid >> start_session >> process >> check_continue
    check_continue >> [continue_op, finalize]
    continue_op >> process  # Создаем цикл для итеративной обработки
    
    # Финализирующие задачи
    finalize >> log_progress_task >> transfer_data >> cleanup

job_kafka_bulk_processing()