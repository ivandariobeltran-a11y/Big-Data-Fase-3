from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, upper
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import desc

# 1. INICIALIZAR SPARK
spark = SparkSession.builder \
    .appName("CensoRealTime_Colombia") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. DEFINIR ESQUEMA COMPLETO (Basado en el CSV de datos.gov.co)
schema = StructType([
    StructField("año", StringType()),
    StructField("nombremunicipio", StringType()),
    StructField("sector", StringType()),
    StructField("zona", StringType()),
    StructField("numero_de_Sedes", IntegerType()),
    StructField("estado", StringType()),
    StructField("timestamp_simulado", IntegerType())
])

# 3. LEER DE KAFKA
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# 4. PARSEAR DATOS
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp_simulado").cast("timestamp")) \
    .withColumn("nombremunicipio", upper(col("nombremunicipio")))

# 5. AGREGACIÓN EN TIEMPO REAL
# Calculamos estadísticas por Municipio cada 1 minuto
analisis_real = parsed_df \
    .groupBy(
        window(col("event_time"), "1 minute"),
        "nombremunicipio"
    ) \
    .agg(
        count("*").alias("Reportes_Recibidos"),
        avg("numero_de_Sedes").alias("Promedio_Infraestructura")
    ) \
    .orderBy("window", desc("Reportes_Recibidos"))

# 6. SALIDA A CONSOLA
print("Esperando flujo de datos desde el dataset de Colombia...")

query = analisis_real.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
