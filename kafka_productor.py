import time
import json
import csv
from kafka import KafkaProducer

# 1. CONFIGURACIÓN DEL PRODUCTOR
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    print("Conexión con Kafka: EXITOSA")
except Exception as e:
    print(f"Error de conexión: {e}")
    exit()

# 2. LECTURA DEL DATASET REAL
archivo_csv = "censo_edificaciones_colombia.csv"

def enviar_datos_reales():
    print(f"Iniciando simulación desde archivo: {archivo_csv}")
    print("Presiona CTRL+C para detener.\n")
    try:
        with open(archivo_csv, mode='r', encoding='utf-8') as file:
            # Usamos DictReader para que cada fila sea un diccionario (JSON-ready)
            reader = csv.DictReader(file)
            for fila in reader:
                # Limpieza básica: asegurar que numero_de_Sedes sea entero
                try:
                    fila['numero_de_Sedes'] = int(fila['numero_de_Sedes'])
                except:
                    fila['numero_de_Sedes'] = 0
                # Agregamos un timestamp actual para el procesamiento en ventana de Spark
                fila['timestamp_simulado'] = int(time.time())
                # Enviar a Kafka
                producer.send('sensor_data', value=fila)
                print(f"[ENVÍO REAL] Municipio: {fila.get('nombremunicipio')} | Sedes: {fila.get('numero_de_Sedes')}")
                # Esperamos 1.5 segundos para simular la llegada pausada de datos
                time.sleep(1.5)
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo {archivo_csv}. Ejecuta primero el script de Batch.")
    except KeyboardInterrupt:
        print("\nSimulación detenida por el usuario.")

if __name__ == "__main__":
    enviar_datos_reales()
    producer.close()
