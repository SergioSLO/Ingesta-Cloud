# Usar una imagen base de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Copiar los archivos necesarios al contenedor
COPY . /app

# Instalar las dependencias necesarias
RUN pip install boto3 loguru

# Establecer la variable de entorno para el STAGE (puede ser sobreescrita al ejecutar el contenedor)
ENV STAGE=dev 

# Crear el directorio de logs si no existe
RUN mkdir -p /var/log/ciencia_datos

# Exponer el puerto 80 (si se requiere acceso web)
EXPOSE 80

# Comando para ejecutar el script
CMD ["python", "ingesta.py"]
