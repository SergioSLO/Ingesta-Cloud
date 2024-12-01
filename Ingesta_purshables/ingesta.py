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

entitiy = 'purshable'

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
TABLE_NAME = f'{stage}_t_purchasable'  # Usando la variable de entorno
S3_BUCKET_NAME = 'ciencia-datos-bucket-rockie'
S3_OBJECT_KEY_ACCESORY = f'{stage}/t_accesories/accesories_data.csv'
S3_OBJECT_KEY_PROMO = f'{stage}/t_promos/promos_data.csv'
FILE_NAME_ACCESORY = '/tmp/accesories_data.csv'
FILE_NAME_PROMO = '/tmp/promos_data.csv'

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
            product_info = item.get('product_info', {})


            if item.get('store_type', '') == 'Promotion':
                row = {
                'tenant_id': item.get('tenant_id', ''),
                'product_id': item.get('product_id', ''),
                'price': item.get('price', 0),
                'image': product_info.get('image', ''),
                'product_brand': product_info.get('product_brand', ''),
                'category': product_info.get('category', ''),
                'product_name': product_info.get('product_name', '')
                }
                # Escribir los datos extraídos a un archivo CSV temporal
                with open(FILE_NAME_PROMO, "a", newline="") as csvfile:
                    fieldnames = row.keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writerow(row)

            elif item.get('store_type', '') == 'Accessories':
                row = {
                'tenant_id': item.get('tenant_id', ''),
                'product_id': item.get('product_id', ''),
                'price': item.get('price', 0),
                'image': product_info.get('image', ''),
                'category': product_info.get('category', ''),
                'product_name': product_info.get('product_name', '')
                }
                # Escribir los datos extraídos a un archivo CSV temporal
                with open(FILE_NAME_ACCESORY, "a", newline="") as csvfile:
                    fieldnames = row.keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writerow(row)
            else:
                logger.error(f"{id} - Error processing item: {e}")

            logger.info(f"{id} - Processed {entitiy}: {item.get('product_id', 'unknown')}.")
        except Exception as e:
            logger.error(f"{id} - Error processing item: {e}")

# Función para cargar los datos a S3
def upload_to_s3():
    logger.info(f"{id} - Uploading CSV to S3 at {S3_OBJECT_KEY_ACCESORY}.")
    try:
        with open(FILE_NAME_ACCESORY, "rb") as data:
            s3.upload_fileobj(data, S3_BUCKET_NAME, S3_OBJECT_KEY_ACCESORY)
        logger.info(f"{id} - File uploaded successfully to S3.")
    except Exception as e:
        logger.error(f"{id} - Error uploading file to S3: {e}")

    logger.info(f"{id} - Uploading CSV to S3 at {S3_OBJECT_KEY_PROMO}.")
    try:
        with open(FILE_NAME_PROMO, "rb") as data:
            s3.upload_fileobj(data, S3_BUCKET_NAME, S3_OBJECT_KEY_PROMO)
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
