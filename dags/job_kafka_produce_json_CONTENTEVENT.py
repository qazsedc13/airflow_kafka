from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
import json
import uuid
import random
from faker import Faker

# Инициализируем Faker для генерации случайных данных
fake = Faker('ru_RU')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

def get_kafka_config():
    """Получаем конфигурацию из соединения kafka_default типа Generic"""
    conn = BaseHook.get_connection('kafka_synapce')
    extra_config = json.loads(conn.extra or '{}')
    return {
        'bootstrap.servers': f"{conn.host}:{conn.port}",
        **extra_config
    }

def generate_event_schema():
    """Генерируем схему события для CONTENTEVENT"""
    return {
        "type": "struct",
        "fields": [
            {
                "type": "string",
                "field": "timestamp",
                "optional": False
            },
            {
                "type": "string",
                "field": "message_id",
                "optional": False
            },
            {
                "type": "string",
                "field": "interaction_id",
                "optional": False
            },
            {
                "type": "string",
                "field": "event_type",
                "optional": False
            },
            {
                "type": "string",
                "field": "content_id"
            },
            {
                "type": "string",
                "field": "sender_type"
            },
            {
                "type": "string",
                "field": "user_id"
            },
            {
                "type": "string",
                "field": "content_timestamp"
            },
            {
                "type": "string",
                "field": "content_text"
            },
            {
                "type": "string",
                "field": "content_type"
            },
            {
                "type": "string",
                "field": "workitem_id"
            },
            {
                "type": "string",
                "field": "segment_id"
            },
            {
                "type": "string",
                "field": "routing_point_id"
            },
            {
                "type": "string",
                "field": "workflow_id"
            },
            {
                "type": "string",
                "field": "service_id"
            },
            {
                "type": "string",
                "field": "client_id"
            },
            {
                "type": "string",
                "field": "client_name"
            },
            {
                "type": "string",
                "field": "client_token"
            },
            {
                "type": "string",
                "field": "client_url"
            },
            {
                "type": "string",
                "field": "client_phone_number"
            },
            {
                "type": "string",
                "field": "client_email"
            },
            {
                "type": "array",
                "field": "custom_data",
                "items": {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "string",
                            "field": "title"
                        },
                        {
                            "type": "string",
                            "field": "content"
                        },
                        {
                            "type": "string",
                            "field": "link"
                        },
                        {
                            "type": "string",
                            "field": "key"
                        },
                    ],
                    "optional": False
                },
            },
            {
                "type": "string",
                "field": "tenant_id",
                "optional": False
            },
            {
                "type": "string",
                "field": "division_id"
            },
            {
                "type": "string",
                "field": "division_name"
            },
            {
                "type": "string",
                "field": "full_division_names"
            },
            {
                "type": "string",
                "field": "routing_point_name"
            },
            {
                "type": "string",
                "field": "service_name"
            },
            {
                "type": "string",
                "field": "last_name"
            },
            {
                "type": "string",
                "field": "first_name"
            },
            {
                "type": "string",
                "field": "patronymic"
            },
            {
                "type": "string",
                "field": "employee_id"
            },
            {
                "type": "string",
                "field": "file_id"
            },
            {
                "type": "string",
                "field": "client_ext_messenger_id"
            }
        ],
        "optional": False,
        "name": "content_events"
    }

def generate_custom_data(count=0):
    """Генерируем custom_data для сообщения"""
    if count == 0:
        return []
    
    custom_data = []
    for _ in range(count):
        custom_data.append({
            "title": fake.sentence(),
            "content": fake.text(),
            "link": fake.uri_path(),
            "key": str(uuid.uuid4())
        })
    return custom_data

def generate_message(event_type):
    """Генерируем сообщение в зависимости от типа события"""
    message = {
        "timestamp": datetime.now().isoformat(),
        "message_id": str(uuid.uuid4()),
        "interaction_id": str(uuid.uuid4()),
        "event_type": event_type,
        "tenant_id": str(uuid.uuid4()),
        "custom_data": generate_custom_data(random.randint(0, 1)),  # Чаще пустой массив
        "file_id": "",
        "client_ext_messenger_id": ""
    }
    
    if event_type == "EVENTS_NEW_MESSAGE":
        sender_type = random.choice(["SENDERS_TYPES_SYSTEM", "SENDERS_TYPES_AGENT", "SENDERS_TYPES_CLIENT"])
        message.update({
            "content_id": str(uuid.uuid4()),
            "sender_type": sender_type,
            "user_id": str(uuid.uuid4()) if sender_type != "SENDERS_TYPES_SYSTEM" else "",
            "content_timestamp": datetime.now().isoformat(),
            "content_text": random.choice([
                "Из чата вышел участник",
                "Добрый день! Как я могу вам помочь?",
                "Пожалуйста, уточните ваш вопрос",
                "Ваш запрос принят в обработку",
                "Спасибо за обращение!"
            ]),
            "content_type": random.choice(["CONTENT_TYPES_TEXT", "CONTENT_TYPES_IMAGE", "CONTENT_TYPES_VIDEO"]),
            "workitem_id": str(uuid.uuid4()),
            "service_id": str(uuid.uuid4()),
            "division_id": str(uuid.uuid4()) if random.random() > 0.3 else "",
            "division_name": "ПАО Сбербанк" if random.random() > 0.3 else "",
            "full_division_names": "ПАО Сбербанк" if random.random() > 0.3 else "",
            "service_name": random.choice(["Reporting AKA-chat", "Support Chat", "Main Service"]),
            "last_name": fake.last_name() if random.random() > 0.5 else "",
            "first_name": fake.first_name() if random.random() > 0.5 else "",
            "patronymic": fake.middle_name() if random.random() > 0.7 else "",
            "employee_id": str(random.randint(100000, 999999)) if random.random() > 0.5 else "",
            "client_id": "",
            "client_name": "",
            "client_token": "",
            "client_url": "",
            "client_phone_number": "",
            "client_email": ""
        })
    elif event_type == "EVENTS_CLIENT_INFO":
        message.update({
            "content_id": "",
            "sender_type": "SENDERS_TYPES_UNSPECIFIED",
            "user_id": "",
            "content_timestamp": "",
            "content_text": "",
            "content_type": "CONTENT_TYPES_UNSPECIFIED",
            "workitem_id": "",
            "service_id": "",
            "client_id": str(random.randint(1000, 9999)),
            "client_name": fake.name() if random.random() > 0.5 else "",
            "client_token": "",
            "client_url": random.choice(["", "https://jivo.chat/" + str(uuid.uuid4())[:8]]),
            "client_phone_number": fake.phone_number() if random.random() > 0.5 else "",
            "client_email": fake.email() if random.random() > 0.5 else "",
            "division_id": "",
            "division_name": "",
            "full_division_names": "",
            "service_name": "",
            "last_name": "",
            "first_name": "",
            "patronymic": "",
            "employee_id": "",
            "segment_id": "",
            "routing_point_id": "",
            "workflow_id": "",
            "routing_point_name": ""
        })
    
    # Общие пустые поля для всех типов сообщений
    empty_fields = [
        "segment_id", "routing_point_id", "workflow_id", 
        "routing_point_name", "client_token"
    ]
    
    for field in empty_fields:
        if field not in message:
            message[field] = ""
    
    return message

def produce_messages(**context):
    conf = get_kafka_config()
    producer = Producer(conf)
    topic = "SCPL.CONTENTEVENT.V1"
    
    event_types = [
        "EVENTS_NEW_MESSAGE",
        "EVENTS_CLIENT_INFO"
    ]
    
    test_messages = []
    
    for i in range(1000):  # Генерируем 1000 сообщений
        event_type = random.choices(
            event_types, 
            weights=[0.7, 0.3],  # 70% EVENTS_NEW_MESSAGE, 30% EVENTS_CLIENT_INFO
            k=1
        )[0]
        
        payload = generate_message(event_type)
        
        message = {
            "payload": payload,
            "schema": generate_event_schema()
        }
        
        test_messages.append(message)
    
    for msg in test_messages:
        producer.produce(
            topic=topic,
            key=str(msg["payload"]["message_id"]).encode('utf-8'),
            value=json.dumps(msg, ensure_ascii=False).encode('utf-8'),
            callback=lambda err, msg: print(f"Ошибка: {err}") if err else None
        )
        print(f"Отправлено в {topic}: {msg['payload']['message_id']} ({msg['payload']['event_type']})")
    
    producer.flush()

with DAG(
    dag_id='job_kafka_produce_json_CONTENTEVENT',
    default_args=default_args,
    description='Отправка тестовых событий контента в Kafka (адаптировано под ИФТ стенд)',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    tags=['kafka', 'content', 'ИФТ'],
    catchup=False,
) as dag:

    send_messages = PythonOperator(
        task_id='send_content_events',
        python_callable=produce_messages,
    )