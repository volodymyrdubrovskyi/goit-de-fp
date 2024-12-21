import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, TimestampType
from datetime import datetime

# Ініціалізація сесії Spark
spark = SparkSession.builder \
    .appName("Join and Calculate Averages") \
    .getOrCreate()

# Зчитування таблиць
athlete_bio = spark.read.parquet("dags/silver/athlete_bio")
athlete_event_results = spark.read.parquet("dags/silver/athlete_event_results")

# Приведення колонок weight та height до типу Double
athlete_bio = athlete_bio.withColumn("weight", athlete_bio["weight"].cast(DoubleType()))
athlete_bio = athlete_bio.withColumn("height", athlete_bio["height"].cast(DoubleType()))

# Додавання префіксу до колонок у кожній таблиці, щоб уникнути конфліктів
athlete_bio = athlete_bio.withColumnRenamed("country_noc", "bio_country_noc")
athlete_event_results = athlete_event_results.withColumnRenamed("country_noc", "event_country_noc")

# Виконання join за колонкою athlete_id
joined_df = athlete_bio.join(athlete_event_results, athlete_bio.athlete_id == athlete_event_results.athlete_id, "inner")

# Обчислення середніх значень для кожної комбінації sport, medal, sex, bio_country_noc
avg_stats = joined_df.groupBy("sport", "medal", "sex", "bio_country_noc") \
    .agg(F.avg("weight").alias("avg_weight"), F.avg("height").alias("avg_height"))

# Додавання колонки timestamp з часовою міткою виконання програми
timestamp = datetime.now()
avg_stats = avg_stats.withColumn("timestamp", F.lit(timestamp).cast(TimestampType()))

# Запис даних у форматі Parquet в директорію gold/avg_stats
avg_stats.write.parquet("dags/gold/avg_stats")

avg_stats.show(30, truncate=False)
avg_stats.printSchema()

# Зупинка сесії Spark
spark.stop()
