import subprocess

def run_command(command_list):
    """
    Выполняет команду в shell и выводит stdout/stderr.
    """
    command_str = " ".join(command_list)
    print(f"\nВыполняю команду: {command_str}")
    result = subprocess.run(command_list, capture_output=True, text=True)
    if result.returncode != 0:
        print("❌ Ошибка при выполнении команды:")
        print(result.stderr)
    else:
        print(result.stdout)

if __name__ == "__main__":
    print("🚀 Запуск загрузки данных в HDFS...\n")

    # Создание папок в HDFS
    folders = [
        "/covid_dataset/images",
        "/covid_dataset/metadata",
        "/covid_dataset/processed"
    ]

    for folder in folders:
        print(f"📁 Создаю папку {folder} в HDFS...")
        run_command([
            "docker", "exec", "namenode",
            "hdfs", "dfs", "-mkdir", "-p", folder
        ])

    # Получаем список файлов в /data/images
    print("\n🔍 Получаю список файлов в /data/images ...")
    result = subprocess.run(
        ["docker", "exec", "namenode", "bash", "-c", "ls -1 /data/images"],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        print("❌ Ошибка при получении списка файлов:")
        print(result.stderr)
    else:
        files = result.stdout.strip().split('\n')
        if not files or files == ['']:
            print("⚠️ В папке /data/images нет файлов для загрузки.")
        else:
            print(f"✅ Найдено файлов: {len(files)}\n")

            for i, file_name in enumerate(files, start=1):
                print(f" → [{i}/{len(files)}] Загружаю {file_name} ...")
                run_command([
                    "docker", "exec", "namenode",
                    "hdfs", "dfs", "-put", "-f",
                    f"/data/images/{file_name}",
                    "/covid_dataset/images/"
                ])

    # Загружаем metadata.csv
    print("\n📄 Загружаю metadata.csv в HDFS...")
    run_command([
        "docker", "exec", "namenode",
        "hdfs", "dfs", "-put", "-f",
        "/data/metadata.csv",
        "/covid_dataset/metadata/"
    ])

    print("\n✅ Загрузка в HDFS завершена успешно!")
