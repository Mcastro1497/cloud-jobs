# Dockerfile
FROM python:3.11-slim

# Evitar prompts al instalar tzdata
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Paquetes del sistema necesarios:
# - tzdata: para ZoneInfo("America/Argentina/Cordoba")
# - ca-certificates: para requests/https
RUN apt-get update && \
    apt-get install -y --no-install-recommends tzdata ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos código
COPY src ./src
COPY Cer_v2.py .
COPY Precios_v4.py .
COPY TIR_v3.py .

# Logs sin buffering
ENV PYTHONUNBUFFERED=1

# Comando por defecto: guarda el WS entre START/STOP
CMD ["python", "-m", "src", "ws-guard"]
