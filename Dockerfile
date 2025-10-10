FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY runn_sync.py .
# Cloud Run expone el puerto en la variable de entorno PORT.
# Usamos gunicorn para servir la app Flask y respetar PORT (fallback a 8080).
CMD ["sh", "-c", "exec gunicorn -b :${PORT:-8080} runn_sync:APP"]
