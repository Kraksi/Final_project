# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession, Row, functions as F, types as T
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StringIndexer

def main():
    
    if os.access("/spark", os.W_OK):
        warehouse_dir = "/spark/spark_warehouse"
        metastore_dir = "/spark/metastore_db"
    else:
        warehouse_dir = "/tmp/spark_warehouse"
        metastore_dir = "/tmp/metastore_db"

    spark = SparkSession.builder \
        .appName("COVID19 Full Pipeline") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={metastore_dir};create=true") \
        .enableHiveSupport() \
        .getOrCreate()



    # ============================================================
    # Шаг 1 — Чтение CSV
    # ============================================================

    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("hdfs://namenode:9000/covid_dataset/metadata/metadata_cleaned.csv")

    print("✅ CSV загружен:")
    df.show(5)

    # ============================================================
    # Шаг 2 — Запись с партиционированием и бакетированием
    # ============================================================

    # Partition по finding_grouped, age_group
    # Bucket по sex и view
    df.write \
        .mode("overwrite") \
        .partitionBy("finding_grouped", "age_group") \
        .bucketBy(8, "sex", "view") \
        .sortBy("age") \
        .format("parquet") \
        .saveAsTable("covid_metadata_partitioned")

    print("✅ Данные сохранены в parquet-таблицу с партиционированием и бакетированием.")

    # ============================================================
    # Шаг 3 — SQL-аналитика (3 запроса)
    # ============================================================

    # Оконная функция
    query1 = spark.sql("""
        SELECT
            patientid,
            finding_grouped,
            age,
            ROW_NUMBER() OVER (
                PARTITION BY finding_grouped
                ORDER BY age DESC
            ) as rank
        FROM covid_metadata_partitioned
    """)
    query1.show(5)

    query1.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/covid_dataset/processed/query1_top_patients.parquet"
    )
    print("✅ Оконная функция выполнена и результат сохранён.")

    # Справочник проекций снимков
    views = [
        Row(view="PA", view_desc="Posteroanterior view"),
        Row(view="AP", view_desc="Anteroposterior view"),
        Row(view="LATERAL", view_desc="Lateral view")
    ]
    views_df = spark.createDataFrame(views)
    views_df.createOrReplaceTempView("view_lookup")

    # JOIN metadata + view_lookup
    query2 = spark.sql("""
        SELECT m.patientid, m.finding_grouped, m.view, v.view_desc
        FROM covid_metadata_partitioned m
        LEFT JOIN view_lookup v
        ON m.view = v.view
    """)
    query2.show(5)

    query2.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/covid_dataset/processed/query2_views_join.parquet"
    )
    print("JOIN выполнен и сохранён.")

    # Подзапрос для расчёта процента COVID-19
    query3 = spark.sql("""
        SELECT
            'COVID-19' as diagnosis,
            covid_count,
            total_count,
            ROUND(covid_count / total_count * 100, 2) as covid_percent
        FROM (
            SELECT
                SUM(CASE WHEN finding_grouped = 'COVID-19' THEN 1 ELSE 0 END) as covid_count,
                COUNT(*) as total_count
            FROM covid_metadata_partitioned
        )
    """)
    query3.show()

    query3.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/covid_dataset/processed/query3_covid_stats.parquet"
    )
    print("Подзапрос выполнен и сохранён.")

    # ============================================================
    # Шаг 4 — PySpark-обработка
    # ============================================================

    # Фильтрация только COVID-19
    covid_df = df.filter(df["finding_grouped"] == "COVID-19")
    covid_df.show(5)

    covid_df.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/covid_dataset/processed/covid_only.parquet"
    )
    print("Фильтрация по COVID-19 выполнена и данные сохранены.")

    # ------------------------------------------------------------
    # UDF — Унификация диагнозов
    # ------------------------------------------------------------

    @F.udf(returnType=T.StringType())
    def unify_diagnosis(finding):
        if finding and "covid" in finding.lower():
            return "COVID-19"
        elif finding and "pneumonia" in finding.lower():
            return "PNEUMONIA"
        else:
            return "OTHER"

    df_udf = df.withColumn("finding_unified", unify_diagnosis(F.col("finding")))
    df_udf.show(5)

    # ------------------------------------------------------------
    # UDF — Категоризация возраста
    # ------------------------------------------------------------

    @F.udf(returnType=T.StringType())
    def age_category(age):
        if age is None:
            return "unknown"
        elif age < 30:
            return "young"
        elif age <= 60:
            return "middle"
        else:
            return "old"

    df_udf = df_udf.withColumn("age_category", age_category(F.col("age")))
    df_udf.show(5)

    df_udf.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/covid_dataset/processed/metadata_with_udf.parquet"
    )
    print("UDF-обработка выполнена и данные сохранены.")

    # ------------------------------------------------------------
    # ML — кластеризация KMeans
    # ------------------------------------------------------------

    indexer = StringIndexer(inputCol="sex", outputCol="sex_index", handleInvalid="keep")
    df_ml = indexer.fit(df_udf).transform(df_udf)

    assembler = VectorAssembler(
        inputCols=["age", "sex_index"],
        outputCol="features"
    )
    df_ml = assembler.transform(df_ml).na.drop(subset=["features"])

    # Кластеризация
    kmeans = KMeans(k=3, seed=1)
    model = kmeans.fit(df_ml)
    predictions = model.transform(df_ml)

    predictions.select("patientid", "age", "sex", "prediction").show(5)

    predictions.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/covid_dataset/processed/kmeans_clusters.parquet"
    )
    print("Кластеризация выполнена и результаты сохранены.")

    # ------------------------------------------------------------
    # Финальное сохранение metadata.parquet
    # ------------------------------------------------------------

    df_udf.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/covid_dataset/metadata/metadata.parquet"
    )
    print("metadata.parquet сохранён в HDFS.")

    spark.stop()
    print("Все этапы скрипта успешно выполнены.")

if __name__ == "__main__":
    main()
