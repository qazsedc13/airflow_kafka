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


def prod_function(num_attemps):
    """Produces `num_treats` messages containing the pet's name, a randomly picked
    pet mood post treat and whether or not it was the last treat in a series."""

    for i in range(int(num_attemps)):
        rqUid = str(uuid.uuid4())
        cdr = ["2024-01-31T;20;asdfe74554;VOICE_IN;;;;;;;;;;;;;;;;;;;{\"VOICE_IN\":{\"HT_MIN_FACT\":\"28\"}}","2024-01-31T;20;asdfe74554;CHAT;;;;;;;;;;;;;;;;;;;{\"CHAT\":{\"HT_MIN_FACT\":\"100\"}}"]
        print(json.dumps({"rqUid": rqUid, "cdr": cdr}))

        # Формирование json на отправку
        final_attemps = False
        if i + 1 == num_attemps:
            final_attemps = True
        yield (
            json.dumps(i),
            json.dumps(
                {"rqUid": rqUid, "cdr": cdr}
            ),
        )


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
def produce_consume1():
    
    produce = ProduceToTopicOperator(
        task_id="produce",
        kafka_config_id="kafka_default",
        topic=KAFKA_TOPIC_P,
        producer_function=prod_function,
        producer_function_args=[NUMBER_OF_ATTEMPS],
        poll_timeout=10,
    )

    consume = ConsumeFromTopicOperator(
        task_id="consume",
        kafka_config_id="kafka_default",
        topics=[KAFKA_TOPIC_P],
        apply_function=consume_function,
        poll_timeout=20,
        max_messages=1000,
        max_batch_size=20,
    )

    produce >> consume


produce_consume1()
