import os
import boto3
import csv
import json
from botocore.exceptions import NoCredentialsError

# Obtener la variable de entorno STAGE
stage = os.getenv('STAGE')  # 'dev', 'test', 'prod'

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
            'student_promos': json.dumps(student_promos)  # Convertir la lista a JSON para guardarla como cadena
        }
        yield row


# Función para guardar los datos en un archivo CSV
def save_to_csv(data):
    with open('/tmp/students_data.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

# Función para cargar el archivo CSV en S3
def upload_to_s3(file_path, bucket_name, object_key):
    try:
        s3.upload_file(file_path, bucket_name, object_key)
        print(f"Archivo subido exitosamente a s3://{bucket_name}/{object_key}")
    except NoCredentialsError:
        print("No se encontraron credenciales válidas para AWS.")
    except Exception as e:
        print(f"Error al subir archivo a S3: {e}")

def main():
    all_data = []
    for items in scan_table():
        # Extraer y transformar los datos
        data = list(extract_data(items))
        all_data.extend(data)

    if all_data:
        save_to_csv(all_data)
        upload_to_s3('/tmp/students_data.csv', S3_BUCKET_NAME, S3_OBJECT_KEY)

if __name__ == '__main__':
    main()
