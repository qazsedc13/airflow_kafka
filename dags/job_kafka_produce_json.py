from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

def get_kafka_config():
    """Получаем конфигурацию из соединения kafka_default типа Generic"""
    conn = BaseHook.get_connection('kafka_default')
    extra_config = json.loads(conn.extra or '{}')
    return {
        'bootstrap.servers': f"{conn.host}:{conn.port}",
        **extra_config
    }

def produce_messages(**context):
    conf = get_kafka_config()
    producer = Producer(conf)
    topic = "SCPL.BULKAGENTSEVENT.V1"
    
    test_messages = [
        {
            "eventId": f"event_{i}",
            "agentId": f"agent_{i}",
            "timestamp": datetime.now().isoformat(),
            "status": "ACTIVE" if i % 2 == 0 else "INACTIVE",
            "payload": {
                "metric1": i * 10,
                "metric2": f"value_{i}"
            }
        }
        for i in range(1, 6)
    ]
    
    for msg in test_messages:
        producer.produce(
            topic=topic,
            key=str(msg["eventId"]).encode('utf-8'),
            value=json.dumps(msg).encode('utf-8'),
            callback=lambda err, msg: print(f"Ошибка: {err}") if err else None
        )
        print(f"Отправлено в {topic}: {msg['eventId']}")
    
    producer.flush()

with DAG(
    dag_id='produce_agents_events',
    default_args=default_args,
    description='Отправка тестовых событий агентов в Kafka',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    tags=['kafka', 'agents'],
    catchup=False,
) as dag:

    send_messages = PythonOperator(
        task_id='send_agents_events',
        python_callable=produce_messages,
    )