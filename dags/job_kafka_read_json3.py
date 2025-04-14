"""
Даг готовит витрины: '''###pkap_247_sch.billing_act_sent_info###'''
"""

import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pendulum import datetime
from airflow.hooks.base import BaseHook
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import json
from sqlalchemy import create_engine, text
import pandas as pd
import codecs
import os

import uuid


CONN_ID_PKAP = 'pkap_247_db'
CONN_ID_KAP = 'kap_247_db'
KAFKA_TOPIC_C = "SCPL.BULKAGENTSERVICESEVENT.V1"

def _get_engine(conn_id_str):
    """
    Функция для создания движка базы данных.

    Args:
        conn_id_str (str): Идентификатор подключения.

    Returns:
        Engine: Движок базы данных.
    """
    # Загружаем настройки
    connection = BaseHook.get_connection(conn_id_str)

    user = connection.login
    password = connection.password
    host = connection.host
    port = connection.port
    dbname = connection.schema

    return create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}?target_session_attrs=read-write',
                         pool_pre_ping=True)


def consume_function(message, rqUid):
    """
    Функция для обработки сообщений из Kafka.

    Args:
        message: Сообщение из Kafka.
        rqUid: Идентификатор запроса.
    """
    if not isinstance(message, object):
        logging.error('Получено некорректное сообщение')
        logging.info('Идентификатор запроса: %s', rqUid)
    elif message.value() is None:
        logging.warning('Получено пустое сообщение')
        logging.info('Идентификатор запроса: %s', rqUid)
    elif len(message.value()) == 0:
        logging.warning('Получено сообщение с длиной 0')
        logging.info('Идентификатор запроса: %s', rqUid)
    else:
        logging.info('Полное сообщение: %s', message.value())
        logging.info('Идентификатор запроса: %s', rqUid)
    

@dag(
    start_date=datetime(2025, 4, 9),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    tags=['test', 'kafka']
)
def job_kafka_read_json3():
    """
    DAG для чтения сообщений из Kafka и обработки их с помощью функции consume_function.
    """
    @task
    def rqUid_generator():
        """Генерируем идентификатор"""
        return str(uuid.uuid4())
    
    # Убираем скобки () у оператора
    consume = ConsumeFromTopicOperator(
        task_id="consume",
        kafka_config_id="kafka_synapce",
        commit_cadence='never',
        topics=[KAFKA_TOPIC_C],
        apply_function=consume_function,
        apply_function_kwargs={
            "rqUid": "{{ ti.xcom_pull(task_ids='rqUid_generator')}}"
        },
        poll_timeout=10,
        max_messages=20,
        max_batch_size=20,
    )

    # Правильный синтаксис для установки зависимостей
    rqUid_generator() >> consume


job_kafka_read_json3_dag = job_kafka_read_json3()