import sys
import os

_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path[:0] = [_root, "/opt/spark"]

from src.utils.spark_setup import create_spark_session, get_delta_path
from src.silver.transformer import SilverTransformer


def main():
    print("=== Запуск Silver слоя ===")

    spark = create_spark_session("SilverLayer")
    delta_path = get_delta_path()

    transformer = SilverTransformer(spark, delta_path)
    silver_path = transformer.transform_silver()

    print(f"Silver слой сохранен в {silver_path}")
    spark.stop()


if __name__ == "__main__":
    main()