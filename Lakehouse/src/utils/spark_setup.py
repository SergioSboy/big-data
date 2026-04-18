"""Spark + Delta: до импорта pyspark подмешиваем каталог из дистрибутива Spark (Bitnami/Docker)."""
import os
import sys


def _ensure_spark_python_on_path():
    """Если PYTHONPATH сброшен в Docker, pyspark лежит в $SPARK_HOME/python."""
    candidates = []
    if os.environ.get("SPARK_HOME"):
        candidates.append(os.environ["SPARK_HOME"])
    candidates.extend(
        [
            "/opt/bitnami/spark",
            "/opt/spark",
            "/usr/local/spark",
        ]
    )
    for home in candidates:
        if not home:
            continue
        py_dir = os.path.join(home, "python")
        init_py = os.path.join(py_dir, "pyspark", "__init__.py")
        if os.path.isfile(init_py) and py_dir not in sys.path:
            sys.path.insert(0, py_dir)
            return


_ensure_spark_python_on_path()

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def create_spark_session(app_name="FlightDelaysPipeline"):
    """Создание Spark сессии с поддержкой Delta Lake"""

    master = os.environ.get("SPARK_MASTER", "local[*]")
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark


def get_delta_path():
    """Получить путь к Delta таблицам"""
    delta_path = os.environ.get("DELTA_LAKE_PATH", "./data/delta")
    os.makedirs(delta_path, exist_ok=True)
    return delta_path
