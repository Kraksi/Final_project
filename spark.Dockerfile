FROM bitnami/spark:3.5.1

USER root

RUN install_packages python3 python3-pip

COPY ./docler_data/requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

USER 1001
