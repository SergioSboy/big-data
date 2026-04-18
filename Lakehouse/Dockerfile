FROM bitnamilegacy/spark:3.5.0

# Один интерпретатор с pip: в Bitnami это обычно /opt/bitnami/python/bin/python3.
# Команда `python` часто указывает на другой бинарник → ModuleNotFoundError: pyspark.
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH="/opt/bitnami/python/bin:/opt/bitnami/common/bin:${PATH}"
ENV PYTHONPATH="/opt/spark:/opt/bitnami/spark/python"

USER root

COPY requirements.txt /tmp/requirements.txt

RUN python3 -m pip install --upgrade pip \
 && python3 -m pip install --no-cache-dir --prefer-binary \
    "pyspark>=3.5.0,<3.6.0" \
    "mlflow>=2.13.0" \
    "numpy>=1.24,<2" \
    "pandas>=2.0,<2.3" \
    "deltalake>=0.15,<0.18" \
    "polars==0.20.9" \
    "delta-spark==3.0.0" \
 && python3 -m pip install --no-cache-dir -r /tmp/requirements.txt
RUN if [ -x /opt/bitnami/python/bin/python3 ]; then \
      ln -sf /opt/bitnami/python/bin/python3 /usr/local/bin/python; \
    fi

USER 1001

RUN python3 -c "import pyspark; print('pyspark ok (app user)')" \
 && (python -c "import pyspark; print('python symlink ok')" || true)

WORKDIR /opt/spark
