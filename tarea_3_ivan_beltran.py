import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, upper, round, avg

# 1. INICIALIZAR LA SESIÓN DE SPARK
# Se configura la aplicación para el análisis de infraestructura educativa
spark = SparkSession.builder \
    .appName("CensoEdificaciones_Colombia_Batch") \
    .getOrCreate()

# Reducir el nivel de log para evitar mensajes informativos excesivos
spark.sparkContext.setLogLevel("WARN")

# 2. CARGA DEL ARCHIVO DESDE LA FUENTE ORIGINAL
# Requerimiento: Cargar el conjunto de datos desde la fuente original (datos.gov.co)
url = "https://www.datos.gov.co/api/views/upkm-vdjb/rows.csv?accessType=DOWNLOAD"
archivo_local = "censo_edificaciones_colombia.csv"

print("\n" + "="*60)
print("   SISTEMA DE PROCESAMIENTO BATCH - UNAD BIG DATA")
print("="*60)

try:
    print(f"Descargando dataset desde: {url}...")
    # Se realiza la descarga por flujo para manejar archivos grandes
    response = requests.get(url, timeout=60)
    with open(archivo_local, 'wb') as f:
        f.write(response.content)
    print("Estado de la carga: EXITOSA")
except Exception as e:
    print(f"Error crítico en la descarga: {e}")

# Cargar el archivo CSV descargado en un DataFrame
df = spark.read.csv(archivo_local, header=True, inferSchema=True)

# 3. LIMPIEZA Y TRANSFORMACIÓN
# Requerimiento: Operaciones de limpieza y transformación
# Normalizamos los campos de texto y eliminamos duplicados
df_clean = df.dropDuplicates() \
    .filter(col("nombremunicipio").isNotNull()) \
    .withColumn("nombremunicipio", upper(col("nombremunicipio"))) \
    .withColumn("sector", upper(col("sector"))) \
    .withColumn("zona", upper(col("zona")))

# 4. ANÁLISIS EXPLORATORIO DE DATOS (EDA)
# Requerimiento: Análisis exploratorio utilizando DataFrames
print("\n" + "*"*60)
print("      RESULTADOS DEL ANÁLISIS EXPLORATORIO (EDA)")
print("*"*60)

# A. Resumen Estadístico de Variables Numéricas (Formateado para legibilidad)
print("\n[A] Resumen estadístico de infraestructura:")
stats = df_clean.select("año", "numero_de_Sedes").describe()
stats.select(
    col("summary"),
    round(col("año").cast("float"), 0).alias("Anio"),
    round(col("numero_de_Sedes").cast("float"), 2).alias("Prom_Sedes")
).show()

# B. Análisis por Sector (Oficial vs No Oficial)
print("\n[B] Distribución por Sector Económico:")
df_clean.groupBy("sector") \
    .agg(count("*").alias("Total_Establecimientos"),
         round(avg("numero_de_Sedes"), 2).alias("Promedio_Sedes")) \
    .orderBy(desc("Total_Establecimientos")) \
    .show()

# C. Análisis por Zona (Urbana vs Rural)
print("\n[C] Distribución por Zona Geográfica:")
df_clean.groupBy("zona") \
    .agg(count("*").alias("Conteo_Registros")) \
    .orderBy(desc("Conteo_Registros")) \
    .show()

# D. Visualización de los Top 10 Municipios (Requerimiento de Visualización)
print("\n[D] Top 10 Municipios con mayor presencia educativa:")
df_clean.groupBy("nombremunicipio") \
    .agg(count("*").alias("Total_Sedes")) \
    .orderBy(desc("Total_Sedes")) \
    .limit(10) \
    .show()

print("="*60)
print("          FIN DEL PROCESAMIENTO BATCH")
print("="*60)

# Finalizar la sesión de Spark
spark.stop()
