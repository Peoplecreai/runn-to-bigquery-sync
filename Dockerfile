# Usa una imagen base de Python
FROM python:3.11-slim

# Evita que Python guarde archivos .pyc
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia e instala las librerías necesarias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia tu script al directorio de trabajo
COPY runn_sync.py .

# Define el comando por defecto que se ejecutará cuando el contenedor arranque
CMD [ "python3", "runn_sync.py", "--serve" ]
