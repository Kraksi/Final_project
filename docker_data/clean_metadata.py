import pandas as pd
import numpy as np
import subprocess

# Пути внутри проекта
INPUT_FILE = "./docker_data/metadata.csv"
OUTPUT_FILE = "./docker_data/metadata_cleaned.csv"

# Путь в контейнере
CONTAINER_FILE_PATH = "/tmp/metadata_cleaned.csv"

# Путь в HDFS
HDFS_PATH = "/covid_dataset/metadata/metadata_cleaned.csv"

# Функция для классификации возраста в группы
def age_group(age):
    if age < 18:
        return "child"
    elif age < 60:
        return "adult"
    else:
        return "elderly"

# Функция для унификации диагнозов
def unify_finding(finding):
    if "COVID" in finding:
        return "COVID-19"
    elif "PNEUMONIA" in finding:
        return "PNEUMONIA"
    else:
        return finding

def clean_metadata():
    print("[INFO] Чтение исходного CSV...")
    df = pd.read_csv(INPUT_FILE)

    # Приведение к верхнему регистру
    df["finding"] = df["finding"].str.upper()

    # Упрощение вариантов названий диагнозов
    df["finding"] = df["finding"].replace({
        "COVID-19 ARDS": "COVID-19",
        "COVID19": "COVID-19",
        "COVID-19 PNEUMONIA": "COVID-19",
        "COVID19 PNEUMONIA": "COVID-19"
    })

    # Удаление строк с некорректным возрастом
    df = df[(df["age"].isnull()) | ((df["age"] >= 0) & (df["age"] <= 120))]

    # Заполнение пропусков в возрасте случайными значениями 20-80 лет
    random_ages = np.random.randint(20, 81, size=df["age"].isna().sum())
    df.loc[df["age"].isna(), "age"] = random_ages

    # Заполнение пропусков в sex модой
    if "sex" in df.columns:
        mode_sex = df["sex"].mode()[0]
        df["sex"] = df["sex"].fillna(mode_sex)

    # Заполнение пропусков в finding значением "UNKNOWN"
    df["finding"] = df["finding"].fillna("UNKNOWN")

    # Удаление дубликатов
    df.drop_duplicates(inplace=True)

    # Добавляем возрастную группу
    df["age_group"] = df["age"].apply(age_group)

    # Добавляем сгруппированный диагноз
    df["finding_grouped"] = df["finding"].apply(unify_finding)

    print("[INFO] Сохраняю очищенный CSV...")
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"[INFO] Файл сохранен: {OUTPUT_FILE}")

def upload_to_hdfs():
    print("[INFO] Копирую файл в контейнер namenode...")
    subprocess.run([
        "docker", "cp",
        OUTPUT_FILE,
        f"namenode:{CONTAINER_FILE_PATH}"
    ], check=True)

    print("[INFO] Загружаю файл в HDFS...")
    subprocess.run([
        "docker", "exec", "namenode",
        "hdfs", "dfs", "-put", "-f",
        CONTAINER_FILE_PATH,
        HDFS_PATH
    ], check=True)

    print("[INFO] Проверяю содержимое папки в HDFS...")
    result = subprocess.run(
        [
            "docker", "exec", "namenode",
            "hdfs", "dfs", "-ls", "/covid_dataset/metadata"
        ],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    print(f"[INFO] Файл успешно загружен в HDFS: {HDFS_PATH}")

if __name__ == "__main__":
    print("[INFO] Запускаю процесс очистки и загрузки в HDFS...")
    clean_metadata()
    upload_to_hdfs()
    print("[INFO] Всё выполнено успешно!")
