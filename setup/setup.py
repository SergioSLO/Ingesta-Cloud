import boto3
import os
from loguru import logger
# Configuración de AWS Glue
glue_client = boto3.client('glue', region_name='us-east-1')  # Cambia la región si es necesario
# Crear un cliente de S3
s3_client = boto3.client('s3')
# Nombre de la base de datos de Glue
database_name = 'rockie_database'

bucket_name = 'ciencia-datos-bucket-rockie'

# Lista de tablas a crear
tables = ['t_rockies', 't_students', 't_rewards', 't_activities', 't_accesories', 't_promos']

# Esquemas personalizados para algunas tablas
schema_t_students = [
    {"Name": "tenant_id", "Type": "string", "Comment": ""},
    {"Name": "student_id", "Type": "string", "Comment": ""},
    {"Name": "student_email", "Type": "string", "Comment": ""},
    {"Name": "fecha_ingreso", "Type": "string", "Comment": ""},
    {"Name": "nombre", "Type": "string", "Comment": ""},
    {"Name": "contraseña", "Type": "string", "Comment": ""},
    {"Name": "fecha_nacimiento", "Type": "string", "Comment": ""},
    {"Name": "genero", "Type": "string", "Comment": ""},
    {"Name": "telefono", "Type": "string", "Comment": ""},
    {"Name": "rockie_coins", "Type": "int", "Comment": ""},
    {"Name": "rockie_gems", "Type": "int", "Comment": ""},
    {"Name": "student_promos", "Type": "string", "Comment": ""}
]

schema_t_rockies = [
    {"Name": "tenant_id", "Type": "string"},
    {"Name": "student_id", "Type": "string"},
    {"Name": "level", "Type": "int"},
    {"Name": "experience", "Type": "int"},
    {"Name": "evolution", "Type": "string"},
    {"Name": "rockie_name", "Type": "string"},
    {"Name": "arms_acc", "Type": "string"},
    {"Name": "head_acc", "Type": "string"},
    {"Name": "body_acc", "Type": "string"},
    {"Name": "face_acc", "Type": "string"},
    {"Name": "bg_acc", "Type": "string"},
    {"Name": "student_equipment", "Type": "string"}
]

schema_t_rewards = [
    {"Name": "tenant_id", "Type": "string"},
    {"Name": "student_id", "Type": "string"},
    {"Name": "reward_id", "Type": "string"},
    {"Name": "experience", "Type": "int"},
    {"Name": "activity_id", "Type": "string"},
    {"Name": "rockie_coins", "Type": "int"}
]

schema_t_promo = [
    {"Name": "tenant_id", "Type": "string"},
    {"Name": "product_id", "Type": "string"},
    {"Name": "price", "Type": "double"},
    {"Name": "image", "Type": "string"},
    {"Name": "product_brand", "Type": "string"},
    {"Name": "category", "Type": "string"},
    {"Name": "product_name", "Type": "string"}
]

schema_t_accesory = [
    {"Name": "tenant_id", "Type": "string"},
    {"Name": "product_id", "Type": "string"},
    {"Name": "price", "Type": "double"},
    {"Name": "image", "Type": "string"},
    {"Name": "category", "Type": "string"},
    {"Name": "product_name", "Type": "string"}
]

schema_t_activities = [
    {"Name": "tenant_id", "Type": "string"},
    {"Name": "activity_id", "Type": "string"},
    {"Name": "student_id", "Type": "string"},
    {"Name": "activity_type", "Type": "string"},
    {"Name": "creation_date", "Type": "string"},
    {"Name": "time", "Type": "int"}
]


# Función para crear una tabla en AWS Glue
def create_table(stage, table_name):
    table_name_full = f"{stage}_{table_name}"
    
    # Determinar el esquema dependiendo del nombre de la tabla
    if table_name == 't_students':
        schema = schema_t_students
    elif table_name == 't_rockies':
        schema = schema_t_rockies
    elif table_name == 't_rewards':
        schema = schema_t_rewards
    elif table_name == 't_promos':
        schema = schema_t_promo
    elif table_name == 't_accesories':
        schema = schema_t_accesory
    elif table_name == 't_activities':
        schema = schema_t_activities
    else:
        print(f"Esquema no definido para la tabla {table_name_full}.")
    
    # Crear la definición de la tabla
    table_input = {
        'Name': table_name_full,
        'Description': f"Tabla {table_name_full} para el stage {stage}",
        'StorageDescriptor': {
            'Columns': schema,
            'Location': f"s3://ciencia-datos-bucket-rockie/{stage}/{table_name}/",  # Cambia el bucket y la ruta
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'SerdeInfo': {
                'Name': 'OpenCSVSerde',
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                'Parameters': {
                    'separatorChar': ','
                }
            }
        },
        'PartitionKeys': [],
        'TableType': 'EXTERNAL_TABLE'
    }
    
    # Crear la tabla en AWS Glue
    glue_client.create_table(
        DatabaseName=database_name,
        TableInput=table_input
    )
    print(f"Tabla {table_name_full} creada en el stage {stage}.")





# Función para crear las carpetas en el bucket S3
def create_s3_folders(stage):
    for table in tables:
        # Definir la ruta del subfolder
        folder_path = f"{stage}/{table}/"
        # Crear el subfolder (en S3 esto solo se define por el nombre, no es un directorio real)
        s3_client.put_object(Bucket=bucket_name, Key=folder_path)
        print(f"Carpeta creada: {folder_path}")





def main():
    # Obtener la variable de entorno STAGE
    try:
        stage = os.getenv('STAGE') 
    except Exception as e:
        logger.error(f"Error getting STAGE environment variable: {e}")
        exit()

    if stage not in ['dev', 'test', 'prod']:
        logger.error(f"Invalid value for STAGE environment variable: {stage}")
        exit()

    # Ejecutar la función para crear las carpetas
    create_s3_folders(stage)
    # Crear todas las tablas para los diferentes stages
    for table in tables:
        create_table(stage, table)


if __name__ == '__main__':
    main()