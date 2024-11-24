from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

# Створення SparkSession
spark = SparkSession.builder.appName("CSVStream").getOrCreate()

# Визначення схеми для CSV-файлу
schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("value", StringType(), True),
    ]
)

# Читання потокових даних із CSV-файлу
csvDF = (
    spark.readStream.option("sep", ",")
    .option("header", True)
    .option("maxFilesPerTrigger", 1)
    .schema(schema)
    .csv("csv_directory")
)

# Старт стримінгу й виведення результатів на екран
query = (
    csvDF.writeStream.trigger(availableNow=True)
    # .trigger(processingTime='10 seconds')
    .outputMode("append")
    .format("console")
    .start()
)

# Чекаємо кінця роботи стримінгу
query.awaitTermination()
