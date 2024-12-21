import os
import requests
from pyspark.sql import SparkSession

print(f"Script landing_to_bronze started...")

def download_data(local_file_path, download_dir):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")

    try:
        response = requests.get(downloading_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to download the file. Error: {e}")
        return

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Ensure the download directory exists
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        # Open the local file in write-binary mode and write the content of the response to it
        local_file_path = os.path.join(download_dir, local_file_path + ".csv")
        with open(local_file_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")
        return

# Завантаження даних
download_data("athlete_bio", "dags/vvd")
download_data("athlete_event_results", "dags/vvd")

# Ініціалізація сесії Spark
spark = SparkSession.builder \
    .appName("CSV to Parquet") \
    .getOrCreate()

# Шляхи до CSV-файлів
files = ["athlete_bio", "athlete_event_results"]
download_dir = "dags/vvd"

# Створення директорії bronze, якщо вона не існує
bronze_dir = "dags/bronze"
if not os.path.exists(bronze_dir):
    os.makedirs(bronze_dir)

for file in files:
    # Перевірка існування файлів
    local_file_path = os.path.join(download_dir, file + ".csv")
    if not os.path.exists(local_file_path):
        print(f"File {local_file_path} does not exist.")
        continue

    # Читання CSV-файлу
    df = spark.read.csv(local_file_path, header=True, inferSchema=True)

    # Збереження даних у форматі Parquet
    df.write.parquet(f"{bronze_dir}/{file}")
    
    df.show(30, truncate=False)
    df.printSchema()

# Зупинка сесії Spark
spark.stop()
