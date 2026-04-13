# Tarea 3 - Procesamiento de Datos con Apache Spark
En este repositorio se presenta la implementación de un procesamiento en Batch desarrollado como parte de la Tarea 3 del curso de Big Data, el uso de Kafka y Spark

## Descripción de la Solución
La solución consiste en una aplicación desarrollada en Python utilizando Apache Spark y Kafka. Su objetivo es realizar un análisis masivo del censo de Establecimientos Educativos en Colombia, extrayendo información directamente desde la fuente oficial de Datos Abiertos.

##  Tareas Realizadas
###  Carga Automatizada
El script descarga el dataset en formato CSV directamente desde la API de datos.gov.co.

### Limpieza de Datos
- Eliminación de registros duplicados  
- Filtrado de valores nulos en campos críticos  
- Normalización de texto (conversión a mayúsculas)  

###  Análisis Exploratorio (EDA)
- A. Resumen estadístico de la infraestructura educativa (año y número de sedes)  
- B. Distribución de establecimientos por sector (Oficial vs. No Oficial)  
- C. Análisis de cobertura por zona (Urbana vs. Rural)  
- D. Identificación de los 10 municipios con mayor presencia educativa  

### 📈 Visualización
Generación de tablas de resultados formateadas en consola.
