FROM python:3.9-slim

# Definir un argumento para el STAGE
ARG STAGE=dev

# Establecer la variable de entorno STAGE
ENV STAGE=$STAGE

# Copiar los archivos necesarios al contenedor
WORKDIR /app
COPY . /app

# Instalar las dependencias necesarias
RUN pip install boto3 loguru

# Ejecutar ambos scripts secuencialmente
CMD ["sh", "-c", "python crear_folders.py && python glue_aws.py"]
