# COVID-19 Analysis Pipeline

Проект автоматизирует обработку и анализ медицинских данных COVID-19 с использованием Hadoop, Spark и PySpark. В результате работы строятся отчёты и визуализации по эпидемиологическим данным, выгружаются parquet-файлы для дальнейшей аналитики.

ℹ️ Описание проекта

Проект представляет собой ETL-пайплайн для анализа медицинских изображений и метаданных COVID-19. Основные этапы пайплайна:

#### 1. Загрузка сырых данных в HDFS

#### 2. Предобработка CSV-файлов (очистка, унификация диагнозов)

#### 3. Создание Hive-таблиц с партиционированием и бакетированием

#### 4. SQL-аналитика (окна, join-операции, агрегации)

#### 5. PySpark UDF-обработка

#### 6. Машинное обучение (кластеризация KMeans)

#### 7. Выгрузка результатов в Parquet

#### 8. Визуализация данных в Jupyter Notebook

# Цель проекта — подготовить качественные данные для аналитических отчётов и BI-платформ.

## 📂 Структура проекта
```bash
FINAL_PROJECT/
│
├── docker_data/
│   ├── images/                         # Исходные изображения (если используются)
│   ├── clean_metadata.py               # Скрипт очистки CSV-файла
│   ├── covid_full_pipeline.py          # Основной скрипт PySpark-пайплайна
│   ├── metadata.csv                    # Исходный CSV с метаданными
│   ├── metadata_cleaned.csv            # Очищенный CSV-файл
│   ├── requirements-docker.txt         # Python-зависимости для установки в контейнере
│
├── results/                            # Итоговые parquet-файлы после пайплайна
│   ├── covid_only.parquet
│   ├── kmeans_clusters.parquet
│   ├── metadata_with_udf.parquet
│   ├── query1_top_patients.parquet
│   ├── query2_views_join.parquet
│   ├── query3_covid_stats.parquet
│
├── venv/                               # Виртуальное окружение Python (локально)
│
├── .gitignore
├── docker-compose.yml                  # Конфигурация сервисов Docker
├── init_hdfs.py                        # Автоматизация загрузки файлов в HDFS
├── report.ipynb                        # Jupyter Notebook для визуализации и анализа
├── requirements.txt                    # Python-зависимости для Jupyter
├── spark.Dockerfile                    # Dockerfile для Spark
└── README.md
```
## 🚀 Полная инструкция запуска проекта

### 1. Сборка Docker-образов
```bash
docker-compose build
```
### 2. Запуск контейнеров в фоне
```bash
docker-compose up -d
```
### 3. Создание и активация виртуального окружения + установка зависимостей

Windows:
```powershell
python3 -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```
### 4. Инициализация HDFS
Создание директорий в HDFS и загрузка исходных файлов:
```bash
python init_hdfs.py
```
### 5. Очистка исходного CSV
Очистка исходного метаданных-файла:
```bash
python docker_data/clean_metadata.py
```
### 6. Вход в контейнер Spark
```bash
docker exec -it spark bash
```
### 7. Запуск основного пайплайна PySpark
Внутри контейнера Spark:
```bash
spark-submit /tmp/covid_full_pipeline.py
```
После завершения пайплайна выйти из контейнера:
```bash
exit
```
### 8. Вход в контейнер NameNode
```bash
docker exec -it namenode bash
```
### 9. Выгрузка результатов из HDFS в контейнер
Внутри контейнера namenode выполнить:
```bash
hdfs dfs -get /covid_dataset/processed/covid_only.parquet /data/results/covid_only.parquet
hdfs dfs -get /covid_dataset/processed/kmeans_clusters.parquet /data/results/kmeans_clusters.parquet
hdfs dfs -get /covid_dataset/processed/metadata_with_udf.parquet /data/results/metadata_with_udf.parquet
hdfs dfs -get /covid_dataset/processed/query1_top_patients.parquet /data/results/query1_top_patients.parquet
hdfs dfs -get /covid_dataset/processed/query2_views_join.parquet /data/results/query2_views_join.parquet
hdfs dfs -get /covid_dataset/processed/query3_covid_stats.parquet /data/results/query3_covid_stats.parquet
```
💡 Можно скачать все результаты разом:
```bash
hdfs dfs -get /covid_dataset/processed /data/results
```
После завершения пайплайна выйти из контейнера:
```bash
exit
```
### 10. Выгрузка файлов с контейнера на хост-машину
Для Windows (пример):
```powershell
docker cp namenode:/data/results C:\Project\
```
Для Linux:
```bash
docker cp namenode:/data/results /home/user/Project/
```
## ✅ После выгрузки
Все parquet-файлы будут находиться в папке:
```text
FINAL_PROJECT/results
```
на вашей хост-машине. Эти данные можно использовать для визуализаций и анализа в Jupyter Notebook или других BI-инструментах.

📊 Работа с визуализациями

Запустите Jupyter Notebook:
```text
report.ipynb
```
