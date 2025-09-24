# services/dashboard/Dockerfile
FROM python:3.11-slim

# Keep Python snappy & clean logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 1) Install Python deps
#    (Make sure requirements.txt includes: dash, and gunicorn if you use it)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) Copy your app code
COPY app.py .
COPY pages ./pages
COPY helpers ./helpers
COPY assets ./assets

# 3) Optional healthcheck (helps Cloud Run mark the revision healthy)
HEALTHCHECK --interval=30s --timeout=3s \
  CMD python -c "import socket,os; s=socket.socket(); s.settimeout(2); s.connect(('127.0.0.1', int(os.environ.get('PORT','8080')))); s.close()" || exit 1

# 4) Start the app
#    Cloud Run injects PORT; app.py should read it like:
#    app.run_server(host="0.0.0.0", port=int(os.getenv('PORT','8080')))
CMD ["python", "app.py"]
