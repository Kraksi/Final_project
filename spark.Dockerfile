FROM bitnami/spark:3.4.1

USER root

# Устанавливаем Python 3 и pip (bitnami образ уже содержит python)
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-dev build-essential libsnappy-dev && \
    rm -rf /var/lib/apt/lists/*

# Обновляем pip и ставим зависимости
COPY ./docker_data/requirements-docker.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Указываем рабочую директорию
WORKDIR /spark

# Добавляем Spark bin в PATH (на всякий случай)
ENV PATH="/opt/bitnami/spark/bin:${PATH}"

USER 1001
