import os
import boto3
import csv
import json
from loguru import logger
from botocore.exceptions import NoCredentialsError
from datetime import datetime
# Obtener la variable de entorno STAGE
stage = os.getenv('STAGE', 'dev')  # Valor por defecto es 'dev' si no se encuentra

# Configuración del loguru
id = f"ingesta_{stage}_students"  # Identificador único del proceso
log_dir = "/var/log/ciencia_datos"  # Directorio común de logs en la máquina virtual

# Crear el directorio si no existe
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Nombre del archivo de log basado en el contenedor
container_name = os.getenv('HOSTNAME', 'container_name')  # Usando el nombre del contenedor o default
log_filename = f"{log_dir}/{container_name}_log.log"

# Configurar loguru para que los logs se escriban en el archivo y tengan el formato necesario
logger.add(log_filename, 
           format="{time:YYYY-MM-DD HH:mm:ss.SSS} {level} {name} {message}", 
           level="INFO")



# Configuración de boto3
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Definir el nombre de la tabla y el bucket de S3
TABLE_NAME = f'{stage}_t_students'  # Usando la variable de entorno
S3_BUCKET_NAME = 'ciencia_datos_bucket_rockie'
S3_OBJECT_KEY = f'{stage}/t_students/students_data.csv'

# Inicializar la tabla de DynamoDB
table = dynamodb.Table(TABLE_NAME)

# Función para realizar el scan con paginación
def scan_table():
    last_evaluated_key = None
    while True:
        logger.info(f"{id} - Starting DynamoDB scan for {TABLE_NAME}.")
        
        if last_evaluated_key:
            response = table.scan(ExclusiveStartKey=last_evaluated_key)
        else:
            response = table.scan()

        logger.info(f"{id} - Retrieved a batch of items, processing...")

        yield response['Items']  # Devuelve los elementos de cada página
        last_evaluated_key = response.get('LastEvaluatedKey')

        if not last_evaluated_key:
            logger.info(f"{id} - Scan completed for table {TABLE_NAME}.")
            break

# Función para extraer y transformar los datos
def extract_data(items):
    logger.info(f"{id} - Extracting and transforming student data.")
    for item in items:
        try:
            # Asegurarse de que los datos sean del formato correcto
            student_data = item.get('student_data', {})
            student_promos = item.get('student_promos', [])

            # Extraer los datos de interés
            row = {
                'tenant_id': item.get('tenant_id', ''),
                'student_id': item.get('student_id', ''),
                'student_email': item.get('student_email', ''),
                'creation_date': item.get('creation_date', ''),
                'student_name': student_data.get('student_name', ''),
                'password': student_data.get('password', ''),
                'birthday': student_data.get('birthday', ''),
                'gender': student_data.get('gender', ''),
                'telephone': student_data.get('telephone', ''),
                'rockie_coins': student_data.get('rockie_coins', 0),
                'rockie_gems': student_data.get('rockie_gems', 0),
                'student_promos': json.dumps(student_promos)  # Convertir la lista de promos en JSON
            }

            # Escribir los datos extraídos a un archivo CSV temporal
            with open("/tmp/students_data.csv", "a", newline="") as csvfile:
                fieldnames = row.keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(row)

            logger.info(f"{id} - Processed student: {item.get('student_id', 'unknown')}.")
        except Exception as e:
            logger.error(f"{id} - Error processing item: {e}")

# Función para cargar los datos a S3
def upload_to_s3():
    logger.info(f"{id} - Uploading CSV to S3 at {S3_OBJECT_KEY}.")
    try:
        with open("/tmp/students_data.csv", "rb") as data:
            s3.upload_fileobj(data, S3_BUCKET_NAME, S3_OBJECT_KEY)
        logger.info(f"{id} - File uploaded successfully to S3.")
    except Exception as e:
        logger.error(f"{id} - Error uploading file to S3: {e}")

# Función principal que orquesta el proceso
def main():
    logger.info(f"{id} - Process started.")

    # Realizar el scan en la tabla DynamoDB
    for items in scan_table():
        extract_data(items)

    # Subir el archivo a S3
    upload_to_s3()

    logger.success(f"{id} - Data ingestion process completed successfully.")

if __name__ == "__main__":
    main()
