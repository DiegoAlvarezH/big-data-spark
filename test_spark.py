from pyspark.sql import SparkSession

# Crear una SparkSession
spark = SparkSession.builder \
    .appName("Prueba de Spark") \
    .getOrCreate()

# Mostrar la versión de Spark
print("Versión de Spark:", spark.version)

# Detener la SparkSession
spark.stop()
