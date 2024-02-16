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

NUMBER_OF_ATTEMPS = 3
KAFKA_TOPIC_P = "in_ckr"
KAFKA_TOPIC_C = "result_ckr"


def consume_function(message):
    "Takes in consumed messages and prints its contents to the logs."
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
def produce_consume2():
    
    consume = ConsumeFromTopicOperator(
        task_id="consume",
        kafka_config_id="kafka_default",
        topics=[KAFKA_TOPIC_C],
        apply_function=consume_function,
        poll_timeout=90,
        max_messages=1000,
        max_batch_size=20,
    )

    consume


produce_consume2()
