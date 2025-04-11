"""
### Produce to and Consume from a Kafka Cluster

This DAG will produce messages of several elements to a Kafka cluster and consume
them.
"""

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
import json
import random
import uuid

KAFKA_TOPIC_C = "SCPL.BULKAGENTSEVENT.V1"

def consume_function(message):
    "Takes in consumed messages and prints its contents to the logs."
    print('Полное сообщение выглядит так: ', message.value())
    key = json.loads(message.key())
    message_content = json.loads(message.value())
    print(
        f"key #{key}: message_content #{message_content}"
    )


@dag(
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def job_kafka_read_json():
    

    consume = ConsumeFromTopicOperator(
        task_id="consume",
        kafka_config_id="kafka_default",
        topics=[KAFKA_TOPIC_C],
        apply_function=consume_function,
        poll_timeout=20,
        max_messages=1000,
        max_batch_size=20,
    )

    consume


job_kafka_read_json()
