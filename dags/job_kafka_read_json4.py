###pkap_247_sch.billing_act_sent_info###

import logging
from datetime import datetime as dt  # Изменение импорта
from airflow.decorators import dag, task
from pendulum import datetime
from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from confluent_kafka import Consumer, KafkaException
import uuid
from typing import Any
import time  # Добавляем для получения timestamp

KAFKA_TOPIC_C = "SCPL.BULKAGENTSERVICESEVENT.V1"

class KafkaMessageSensor(BaseSensorOperator):
    """
    Улучшенный кастомный сенсор для проверки наличия сообщений в Kafka
    """
    template_fields = ("kafka_config_id", "topic")

    def __init__(
        self,
        kafka_config_id: str,
        topic: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.kafka_config_id = kafka_config_id
        self.topic = topic

    def poke(self, context: Any) -> bool:
        try:
            conn = BaseHook.get_connection(self.kafka_config_id)
            self.log.info(f"Connecting to Kafka at {conn.host}")
            
            conf = {
                'bootstrap.servers': conn.host,
                'group.id': f'airflow-kafka-sensor-group',
                'auto.offset.reset': 'earliest',
                'session.timeout.ms': 6000,
                'enable.auto.commit': False
            }

            if conn.login:
                conf.update({
                    'security.protocol': 'SASL_PLAINTEXT',
                    'sasl.mechanism': 'PLAIN',
                    'sasl.username': conn.login,
                    'sasl.password': conn.password
                })

            consumer = Consumer(conf)
            self.log.info(f"Subscribing to topic {self.topic}")
            consumer.subscribe([self.topic])
            
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    self.log.info("No messages available")
                    return False
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        self.log.info("Reached end of partition")
                        return False
                    self.log.error(f"Kafka error: {msg.error()}")
                    return False
                
                self.log.info(f"Found message: {msg.value()}")
                return True
            finally:
                consumer.close()
        except Exception as e:
            self.log.error(f"Error in Kafka sensor: {str(e)}")
            return False

def consume_function(message, rqUid):
    """Обрабатывает сообщения из Kafka."""
    if not isinstance(message, object):
        logging.error('Получено некорректное сообщение')
    elif message.value() is None:
        logging.warning('Получено пустое сообщение')
    elif len(message.value()) == 0:
        logging.warning('Получено сообщение с длиной 0')
    else:
        logging.info('Полное сообщение: %s', message.value())
    logging.info('Идентификатор запроса: %s', rqUid)

@dag(
    start_date=datetime(2025, 4, 9),  # Здесь pendulum.datetime
    schedule="0 0 * * *",
    catchup=False,
    tags=['kafka', 'scheduled']
)
def job_kafka_read_json4():
    @task
    def rqUid_generator():
        """Генерирует UUID для трейсинга."""
        return str(uuid.uuid4())

    wait_for_kafka_data = KafkaMessageSensor(
        task_id="wait_for_kafka_data",
        kafka_config_id="kafka_synapce",
        topic=KAFKA_TOPIC_C,
        timeout=300,  # 5 минут для теста
        poke_interval=10,  # Проверка каждые 10 секунд
        mode="reschedule",
        )

    consume = ConsumeFromTopicOperator(
        task_id="consume",
        kafka_config_id="kafka_synapce",
        commit_cadence='never',
        topics=[KAFKA_TOPIC_C],
        apply_function=consume_function,
        apply_function_kwargs={"rqUid": "{{ ti.xcom_pull(task_ids='rqUid_generator')}}"},
        poll_timeout=10,
        max_messages=20,
        max_batch_size=20,
    )

    rqUid_generator() >> wait_for_kafka_data >> consume

job_kafka_read_json4_dag = job_kafka_read_json4()