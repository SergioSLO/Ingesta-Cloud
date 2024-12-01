FROM python:3.9-slim


# Copiar los archivos necesarios al contenedor
WORKDIR /app
COPY . /app

# Instalar las dependencias necesarias
RUN pip install boto3 loguru

# Ejecutar ambos scripts secuencialmente
CMD ["python", "setup.py"]
