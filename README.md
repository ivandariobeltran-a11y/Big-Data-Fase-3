# Tarea 3 - Procesamiento de Datos con Apache Spark
En este repositorio se presenta la implementación de un procesamiento en Batch desarrollado como parte de la Tarea 3 del curso de Big Data, el uso de Kafka y Spark

## Descripción de la Solución
La solución consiste en una aplicación desarrollada en Python utilizando Apache Spark y Kafka. Su objetivo es realizar un análisis masivo del censo de Establecimientos Educativos en Colombia, extrayendo información directamente desde la fuente oficial de Datos Abiertos.

## 1️⃣ Procesamiento Batch
Archivo: `tarea_3_ivan_beltran.py`
###  Tareas Realizadas:
#### Limpieza de Datos
- Eliminación de registros duplicados  
- Filtrado de valores nulos en campos críticos  
- Normalización de texto (conversión a mayúsculas)  
####  Análisis Exploratorio (EDA)
- A. Resumen estadístico de la infraestructura educativa (año y número de sedes)  
- B. Distribución de establecimientos por sector (Oficial vs. No Oficial)  
- C. Análisis de cobertura por zona (Urbana vs. Rural)  
- D. Identificación de los 10 municipios con mayor presencia educativa  
#### 📈 Visualización
Generación de tablas de resultados formateadas en consola.

### 2️⃣ Productor Kafka
Archivo: `kafka_productor.py`
#### Tareas Realizadas:
- Lee el dataset procesado
- Simula flujo de datos en tiempo real
- Envía datos al tópico Kafka `sensor_data`

### 3️⃣ Procesamiento en Streaming
Archivo: `spark_streaming.py`
####  Tareas Realizadas:
- Consume datos desde Kafka
- Procesa datos en tiempo real
- Calcula:
  - Número de reportes por municipio
  - Promedio de sedes educativas
- Muestra resultados en consola


## ▶️ Ejecución del Proyecto:

### Paso 1: Ejecutar procesamiento batch: 
`python tarea_3_ivan_beltran.py`

### Paso 2: Iniciar Kafka:
Asegúrate de tener Kafka

### Paso 3: Ejecutar productor Kafka:
`python kafka_productor.py`

### Paso 4: Ejecutar streaming con Spark:
`spark-submit spark_streaming.py`
