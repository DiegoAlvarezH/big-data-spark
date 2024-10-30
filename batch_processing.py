from pyspark.sql import SparkSession

# Crear una SparkSession
spark = SparkSession.builder \
    .appName("Análisis de Violencia Doméstica") \
    .getOrCreate()

# Cargar el conjunto de datos desde la fuente original
df = spark.read.csv("path/to/Reporte_Delito_Violencia_Intrafamiliar_Polic_a_Nacional.csv", header=True, inferSchema=True)

# Limpieza y transformación
df_cleaned = df.dropDuplicates().na.fill({"Género": "Desconocido"})
df_filtered = df_cleaned.filter(df_cleaned["Tipo_Delito"] == "Violencia Intrafamiliar")

# Mostrar el esquema y las primeras filas
df_filtered.printSchema()
df_filtered.show()

# Almacenar los resultados procesados
df_filtered.write.mode("overwrite").csv("path/to/resultados_procesados.csv")

# Detener la SparkSession
spark.stop()
