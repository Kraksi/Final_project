FROM bitnami/spark:3.4.1

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-dev build-essential libsnappy-dev && \
    rm -rf /var/lib/apt/lists/*

COPY ./docker_data/requirements-docker.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /spark

ENV PATH="/opt/bitnami/spark/bin:${PATH}"

USER 1001
