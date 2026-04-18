import sys
import os

_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path[:0] = [_root, "/opt/spark"]

from src.utils.spark_setup import create_spark_session, get_delta_path
from src.gold.aggregates import GoldAggregates


def main():
    print("=== Запуск Gold слоя ===")

    spark = create_spark_session("GoldLayer")
    delta_path = get_delta_path()

    gold = GoldAggregates(spark, delta_path)

    # Создаем аналитические витрины
    analytics_path = gold.create_analytics_aggregates()
    print(f"Аналитические витрины сохранены в {analytics_path}")

    # Создаем feature table
    feature_path = gold.create_feature_table()
    print(f"Feature table сохранена в {feature_path}")

    spark.stop()


if __name__ == "__main__":
    main()