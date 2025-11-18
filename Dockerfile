FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Certificados del sistema para HTTPS
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copia todo el código de la app (incluye runn_sync.py y cualquier config)
COPY . .

# Usuario no root
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Punto de entrada dinámico: por defecto sirve HTTP, pero se puede
# cambiar el modo con RUN_MODE=job/batch para ejecuciones puntuales
ENTRYPOINT ["python", "entrypoint.py"]
