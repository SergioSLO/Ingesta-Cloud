# Usar una imagen base de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Copiar los archivos necesarios al contenedor
COPY . /app

# Instalar las dependencias necesarias
RUN pip install boto3 loguru

# Crear el directorio de logs si no existe
RUN mkdir -p /var/log/ciencia_datos

# Establecer la variable de entorno para el STAGE
ENV STAGE=dev

# Comando para ejecutar el script
CMD ["python", "ingesta_rockies.py"]
