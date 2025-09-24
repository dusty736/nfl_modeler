# services/api/Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 1) Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) Copy code
COPY app ./app

# 3) Optional healthcheck (helps Cloud Run mark the revision healthy)
HEALTHCHECK --interval=30s --timeout=3s \
  CMD python -c "import socket,os; s=socket.socket(); s.settimeout(2); s.connect(('127.0.0.1', int(os.environ.get('PORT','8080')))); s.close()" || exit 1

# 4) Start uvicorn; Cloud Run sets $PORT automatically
#    Use shell form so ${PORT} expands at runtime; keep --factory since you use create_app()
CMD exec uvicorn app.main:create_app --host 0.0.0.0 --port ${PORT:-8080} --factory
