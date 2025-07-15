import subprocess
import os

NAMENODE_CONTAINER = "namenode"

# HDFS директории
HDFS_DIRS = [
    "/covid_dataset/images/",
    "/covid_dataset/metadata",
    "/covid_dataset/processed"
]

# Локальные директории внутри контейнера
LOCAL_IMAGES_DIR = "/data/images"
LOCAL_METADATA_FILE = "/data/metadata.csv"

# Пути в HDFS
HDFS_IMAGES_DIR = "/covid_dataset/images"
HDFS_METADATA_FILE = "/covid_dataset/metadata/metadata.csv"


def run_hdfs_cmd(args):
    cmd = ["docker", "exec", NAMENODE_CONTAINER] + args
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        print(f"Ошибка команды: {' '.join(cmd)}")
        print(result.stderr)
        raise RuntimeError("HDFS command failed")
    else:
        print(result.stdout)


def create_hdfs_dirs():
    for dir_path in HDFS_DIRS:
        print(f"Создаю папку {dir_path} в HDFS...")
        run_hdfs_cmd(["hdfs", "dfs", "-mkdir", "-p", dir_path])


def upload_images():
    print(f"Загружаю изображения из {LOCAL_IMAGES_DIR} в HDFS {HDFS_IMAGES_DIR} ...")

    local_dir = os.path.join(".", "docler_data", "images")
    if not os.path.exists(local_dir):
        print(f"Локальная директория {local_dir} не существует.")
        return
    files = os.listdir(local_dir)
    if not files:
        print(f"Нет файлов в {local_dir}.")
        return

    run_hdfs_cmd([
        "hdfs", "dfs", "-put", "-f", f"{LOCAL_IMAGES_DIR}/*", HDFS_IMAGES_DIR
    ])


def upload_metadata():
    local_path = os.path.join(".", "docler_data", "metadata.csv")
    if not os.path.exists(local_path):
        print("Файл metadata.csv не найден.")
        return

    print(f"Загружаю metadata.csv в HDFS {HDFS_METADATA_FILE} ...")
    run_hdfs_cmd([
        "hdfs", "dfs", "-put", "-f", LOCAL_METADATA_FILE, HDFS_METADATA_FILE
    ])


if __name__ == "__main__":
    create_hdfs_dirs()
    upload_images()
    upload_metadata()
    print("Инициализация HDFS завершена.")
