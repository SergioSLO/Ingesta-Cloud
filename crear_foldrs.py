import boto3

# Nombre del bucket
bucket_name = 'ciencia-datos-bucket-rockie'

# Crear un cliente de S3
s3_client = boto3.client('s3')

# Definir la estructura de carpetas
folders = ['dev', 'test', 'prod']
subfolders = [
    't_rockies',
    't_students',
    't_rewards',
    't_activities',
    't_accesories',
    't_promos'
]

# Función para crear las carpetas en el bucket S3
def create_s3_folders():
    for folder in folders:
        for subfolder in subfolders:
            # Definir la ruta del subfolder
            folder_path = f"{folder}/{subfolder}/"
            # Crear el subfolder (en S3 esto solo se define por el nombre, no es un directorio real)
            s3_client.put_object(Bucket=bucket_name, Key=folder_path)
            print(f"Carpeta creada: {folder_path}")

# Ejecutar la función para crear las carpetas
create_s3_folders()
