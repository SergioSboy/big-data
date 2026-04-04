FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py plot_results.py ./
RUN mkdir -p /app/results

ENV PYTHONUNBUFFERED=1
# С хоста NameNode доступен так (Docker Desktop; на Linux см. README / docker-compose)
ENV HDFS_URL=hdfs://host.docker.internal:9000/dataset.csv

ENTRYPOINT ["python", "app.py"]
