from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from confluent_kafka import Consumer, KafkaException
import logging

# Константы
KAFKA_BOOTSTRAP_SERVERS = "kafka:19092"
KAFKA_TOPIC = "SCPL.CONTENTEVENT.V1"
GROUP_ID = "test",#"pkape_airflow_kafka"
AUTO_OFFSET_RESET = "earliest"

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def consume_message():
    """
    Обрабатывает сообщения из kafka и логирует их
    """
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': AUTO_OFFSET_RESET
    }

    try:
        c = Consumer(conf)
        c.subscribe([KAFKA_TOPIC])

        logger.info(f"Начало чтения из топика {KAFKA_TOPIC}")

        timeout_seconds = 60
        max_message = 10
        message_received = 0

        start_time = datetime.now()

        while (datetime.now() - start_time).seconds < timeout_seconds and message_received < max_message:
            msg = c.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())
            
            try:
                message_value = msg.value().decode('utf-8')
                logger.info(f"Получено сообщение: {message_value}")
                message_received += 1
            except Exception as e:
                logger.error(f"Ошибка декодирования сообщения: {e}")

        logger.info(f"Обработано {message_received} сообщений")

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        if 'c' in locals():
            c.close()
            logger.info("Consumer закрыт")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'job_kafka_read_test',
    default_args=default_args,
    description='Тестовый даг для чтения из кафки',
    schedule_interval='@once',
    start_date=datetime(2025,8,1),
    tags=['kafka', 'test'],
)

read_kafka_task = PythonOperator(
    task_id='read_kafka_messages',
    python_callable=consume_message,
    dag=dag,
)

read_kafka_task