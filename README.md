
## Option 2: Use the Astro CLI

Download the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) to run Airflow locally in Docker. `astro` is the only package you will need to install.

1. Run `git clone https://github.com/qazsedc13/airflow_kafka.git` on your computer to create a local clone of this repository.
2. Install the Astro CLI by following the steps in the [Astro CLI documentation](https://docs.astronomer.io/astro/cli/install-cli). Docker Desktop/Docker Engine is a prerequisite, but you don't need in-depth Docker knowledge to run Airflow with the Astro CLI.
3. Run `astro dev start` in your cloned repository.
4. After your Astro project has started. View the Airflow UI at `localhost:8080`.
5. Unpause all DAGs. Manually run the `produce_consume_treats` DAG to see the pipeline in action. Note that a random function is used to generate parts of the message to Kafka which determines if the `listen_for_mood` task will trigger the downstream `walking_your_pet` DAG. You might need to run the `produce_consume_treats` several times to see the full pipeline in action!

# Resources

- [Use Apache Kafka with Apache Airflow tutorial](https://docs.astronomer.io/learn/airflow-kafka).
- [Apache Kafka Airflow provider documentation](https://airflow.apache.org/docs/apache-airflow-providers-apache-kafka/stable/index.html).
- [Apache Kafka documentation](https://kafka.apache.org/documentation/). 
- [Apache Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html).
- [Airflow Quickstart](https://docs.astronomer.io/learn/airflow-quickstart).