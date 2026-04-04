FROM python:3.11-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generate_data.py .

# Куда писать dataset.csv (в контейнере); с хоста монтируйте каталог сюда
ENV OUTPUT_DIR=/out

VOLUME ["/out"]

ENTRYPOINT ["python", "/app/generate_data.py"]
