#!/usr/bin/env python

import sys
import os

_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path[:0] = [_root, "/opt/spark"]

from src.utils.spark_setup import create_spark_session, get_delta_path
from src.bronze.loader import BronzeLoader


def main():
    print("=== Запуск Bronze слоя ===")

    spark = create_spark_session("BronzeLayer")
    delta_path = get_delta_path()

    loader = BronzeLoader(spark, delta_path)

    csv = os.environ.get("FLIGHT_DATA_CSV")
    # По годам, append; CSV по умолчанию рядом с delta (см. docker-compose FLIGHT_DATA_CSV)
    loader.load_by_years(2018, 2024, data_path=csv)

    print("Bronze слой успешно создан!")
    spark.stop()


if __name__ == "__main__":
    main()