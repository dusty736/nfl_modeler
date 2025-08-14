FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY app.py .
COPY pages ./pages
COPY helpers ./helpers
COPY assets ./assets
EXPOSE 8050
CMD ["python", "app.py"]
