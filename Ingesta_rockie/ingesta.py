import os
import boto3
import csv
import json
from loguru import logger
from botocore.exceptions import NoCredentialsError
from datetime import datetime

# docker run -e STAGE=dev -v C:\Users\LENOVO\.aws:/root/.aws ingesta_students


# Obtener la variable de entorno STAGE
try:
    stage = os.getenv('STAGE')  # Valor por defecto es 'test' si no se encuentra
except Exception as e:
    logger.error(f"Error getting STAGE environment variable: {e}")
    exit()

if stage not in ['dev', 'test', 'prod']:
    logger.error(f"Invalid value for STAGE environment variable: {stage}")
    exit()
entitiy = 'rockie'

# Configuración del loguru
id = f"ingesta_{stage}_{entitiy}"  # Identificador único del proceso
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
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

# Definir el nombre de la tabla y el bucket de S3
TABLE_NAME = f'{stage}_t_rockies'  # Usando la variable de entorno
S3_BUCKET_NAME = f'ciencia-datos-bucket-rockie-{stage}'
S3_OBJECT_KEY = f't_rockies/rockie_data_{stage}.csv'
FILE_NAME = f'/tmp/rockie_data_{stage}.csv'

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
    logger.info(f"{id} - Extracting and transforming {entitiy} data.")
    for item in items:
        try:
            # Asegurarse de que los datos sean del formato correcto
            rockie_data = item.get('rockie_data', {})
            rockie_adorned = rockie_data.get('rockie_adorned', {})
            rockie_all_accessories_ids = rockie_data.get('rockie_all_accessories_ids', [])

            # Extraer los datos de interés
            #{ "rockie_name" : { "S" : "Victoria" }, "evolution" : { "S" : "Stage 1" }, "rockie_all_accessories_ids" : { "L" : [ { "S" : "arms_acc_0629379089" }, { "S" : "bg_acc_0195624796" }, { "S" : "body_acc_0512622772" }, { "S" : "arms_acc_0728137335" }, { "S" : "face_acc_0926785932" } ] }, "rockie_adorned" : { "M" : { "arms_acc" : { "S" : "arms_acc_0629379089" }, "bg_acc" : { "S" : "bg_acc_0195624796" } } } }
            row = {
                'tenant_id': item.get('tenant_id', ''),
                'student_id': item.get('student_id', ''),
                'level': item.get('level', 0),
                'experience': item.get('experience', 0),
                'evolution': item.get('evolution', ''),
                'rockie_name': rockie_data.get('rockie_name', ''),
                'head_accessory': rockie_adorned.get('head_acc', ''),
                'arms_accessory': rockie_adorned.get('arms_acc', ''),
                'body_accessory': rockie_adorned.get('body_acc', ''),
                'face_accessory': rockie_adorned.get('face_acc', ''),
                'background_accessory': rockie_adorned.get('bg_acc', ''),
                'rockie_all_accessories_ids': json.dumps(rockie_all_accessories_ids),  # Convertir la lista de promos en JSON
            }

            # Escribir los datos extraídos a un archivo CSV temporal
            with open(FILE_NAME, "a", newline="") as csvfile:
                fieldnames = row.keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(row)

            logger.info(f"{id} - Processed {entitiy}: {item.get('student_id', 'unknown')}.")
        except Exception as e:
            logger.error(f"{id} - Error processing item: {e}")

# Función para cargar los datos a S3
def upload_to_s3():
    logger.info(f"{id} - Uploading CSV to S3 at {S3_OBJECT_KEY}.")
    try:
        with open(FILE_NAME, "rb") as data:
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
