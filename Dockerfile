FROM quay.io/astronomer/astro-runtime:8.4.0

USER root
RUN apt-get update && apt-get install -y python3.9 python3.9-dev
USER astro