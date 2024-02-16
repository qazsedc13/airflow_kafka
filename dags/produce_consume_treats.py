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

# Change these variables
YOUR_NAME = "Nikolay"
YOUR_PET_NAME = "Nevermore"
NUMBER_OF_TREATS = 5
# if your pet is very hungry, consider also changing `max_messages`
# in the `consume_treats` task

# only change the topic name if you are using your own Kafka cluster/topic
KAFKA_TOPIC = "my_topic"


def prod_function(num_treats, pet_name):
    """Produces `num_treats` messages containing the pet's name, a randomly picked
    pet mood post treat and whether or not it was the last treat in a series."""

    for i in range(num_treats):
        final_treat = False
        pet_mood_post_treat = random.choices(
            # change these weights to make an event with zoomy or bouncy mood more
            # or less likely
            ["content", "happy", "zoomy", "bouncy"],
            weights=[1, 1, 1, 1],
            k=1,
        )[0]
        if i + 1 == num_treats:
            final_treat = True
        yield (
            json.dumps(i),
            json.dumps(
                {
                    "pet_name": pet_name,
                    "pet_mood_post_treat": pet_mood_post_treat,
                    "final_treat": final_treat,
                }
            ),
        )


def consume_function(message, name):
    "Takes in consumed messages and prints its contents to the logs."
    key = json.loads(message.key())
    message_content = json.loads(message.value())
    pet_name = message_content["pet_name"]
    pet_mood_post_treat = message_content["pet_mood_post_treat"]
    print(
        f"Message #{key}: Hello {name}, your pet {pet_name} has consumed another treat and is now {pet_mood_post_treat}!"
    )


@dag(
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def produce_consume_treats():
    @task
    def get_your_pet_name(pet_name=None):
        return pet_name

    @task
    def get_number_of_treats(num_treats=None):
        return num_treats

    @task
    def get_pet_owner_name(your_name=None):
        return your_name

    produce_treats = ProduceToTopicOperator(
        task_id="produce_treats",
        kafka_config_id="kafka_default",
        topic=KAFKA_TOPIC,
        producer_function=prod_function,
        producer_function_args=[get_number_of_treats(NUMBER_OF_TREATS)],
        producer_function_kwargs={"pet_name": get_your_pet_name(YOUR_PET_NAME)},
        poll_timeout=10,
    )

    consume_treats = ConsumeFromTopicOperator(
        task_id="consume_treats",
        kafka_config_id="kafka_default",
        topics=[KAFKA_TOPIC],
        apply_function=consume_function,
        apply_function_kwargs={"name": get_pet_owner_name(YOUR_NAME)},
        poll_timeout=20,
        max_messages=1000,
    )

    produce_treats >> consume_treats


produce_consume_treats()
