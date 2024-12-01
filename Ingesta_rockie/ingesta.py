import os
import boto3
import csv
from loguru import logger
from botocore.exceptions import NoCredentialsError
from datetime import datetime

# Configuración del loguru
id = "ingesta_rockies"
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

# Obtener la variable de entorno STAGE
stage = os.getenv('STAGE', 'dev')  # Valor por defecto es 'dev' si no se encuentra

# Configuración de boto3
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Definir el nombre de la tabla y el bucket de S3
TABLE_NAME = f'{stage}_t_rockies'  # Usando la variable de entorno
S3_BUCKET_NAME = 'ciencia_datos_bucket_rockie'
S3_OBJECT_KEY = f'{stage}/t_rockies/rockies_data.csv'

# Inicializar la tabla de DynamoDB
table = dynamodb.Table(TABLE_NAME)

# Función para realizar el scan con paginación
def scan_table():
    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            response = table.scan(ExclusiveStartKey=last_evaluated_key)
        else:
            response = table.scan()
        
        yield response['Items']  # Devuelve los elementos de cada página
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break

# Función para extraer y transformar los datos
def extract_data(items):
    for item in items:
        # Asegurarse de que los datos sean del formato correcto
        rockie_data = item.get('rockie_data', {})
        rockie_all_accesories_ids = item.get('rockie_all_accesories_ids', [])

        # Extraer los datos de interés
        row = {
            'tenant_id': item.get('tenant_id', ''),
            'student_id': item.get('student_id', ''),
            'level': item.get('level', ''),
            'experience': item.get('experience', ''),
            'rockie_name': rockie_data.get('rockie_name', ''),
            'rockie_adorned': rockie_data.get('rockie_adorned', ''),
            'head_accesory': rockie_data.get('head_accesory', ''),
            'arms_accesory': rockie_data.get('arms_accesory', ''),
            'body_accesory': rockie_data.get('body_accesory', ''),
            'face_accesory': rockie_data.get('face_accesory', ''),
            'background_accesory': rockie_data.get('background_accesory', ''),
            'rockie_all_accesories_ids': json.dumps(rockie_all_accesories_ids),
            'evolution': item.get('evolution', '')
        }
        
        yield row  # Devuelve los datos transformados

# Función para guardar los datos en un archivo CSV
def save_to_csv(data):
    # Verificar si el archivo ya existe o no para definir el modo de apertura
    file_exists = os.path.isfile('/tmp/rockies_data.csv')
    
    with open('/tmp/rockies_data.csv', mode='a', newline='', encoding='utf-8') as file:
        fieldnames = [
            'tenant_id', 'student_id', 'level', 'experience',
            'rockie_name', 'rockie_adorned', 'head_accesory', 
            'arms_accesory', 'body_accesory', 'face_accesory', 
            'background_accesory', 'rockie_all_accesories_ids', 'evolution'
        ]
        
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        # Escribir los encabezados solo si el archivo no existe
        if not file_exists:
            writer.writeheader()
        
        for row in data:
            writer.writerow(row)
    
    logger.info(f"Datos guardados correctamente en '/tmp/rockies_data.csv'")

# Función para subir el archivo CSV a S3
def upload_to_s3():
    try:
        # Subir el archivo CSV al bucket S3
        s3.upload_file('/tmp/rockies_data.csv', S3_BUCKET_NAME, S3_OBJECT_KEY)
        logger.info(f"Archivo subido correctamente a s3://{S3_BUCKET_NAME}/{S3_OBJECT_KEY}")
    except NoCredentialsError:
        logger.error("Credenciales de AWS no encontradas.")
    except Exception as e:
        logger.error(f"Ocurrió un error al subir el archivo a S3: {str(e)}")

# Función principal para ejecutar la ingesta
def main():
    logger.info(f"Iniciando ingesta de datos de la tabla {TABLE_NAME}")
    
    try:
        # Realizar el scan de la tabla con paginación
        for items in scan_table():
            # Extraer y transformar los datos
            data = extract_data(items)
            # Guardar los datos en el archivo CSV
            save_to_csv(data)
        
        # Subir el archivo CSV a S3
        upload_to_s3()
    
    except Exception as e:
        logger.error(f"Error en el proceso de ingesta: {str(e)}")
    finally:
        logger.info(f"Ingesta completada para la tabla {TABLE_NAME}")

if __name__ == "__main__":
    main()
