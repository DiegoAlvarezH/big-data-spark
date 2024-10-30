from pyspark.sql import SparkSession

# Crear una SparkSession
spark = SparkSession.builder \
    .appName("Streaming de Violencia") \
    .getOrCreate()

# Leer datos desde Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .load()

# Procesar datos en tiempo real
query = df_stream.selectExpr("CAST(value AS STRING)").writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
