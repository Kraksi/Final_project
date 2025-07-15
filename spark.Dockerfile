FROM bde2020/spark-master:2.4.0-hadoop2.7

USER root

RUN apk update && \
    apk add --no-cache python3 py3-pip python3-dev build-base musl-dev linux-headers

RUN pip3 install --upgrade pip

COPY ./docker_data/requirements-docker.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

USER root



