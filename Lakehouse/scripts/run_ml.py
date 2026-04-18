import sys
import os

_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path[:0] = [_root, "/opt/spark"]

from src.utils.spark_setup import create_spark_session, get_delta_path
from src.ml.trainer import MLTrainer


def main():
    print("=== Запуск ML обучения ===")

    spark = create_spark_session("MLTraining")
    delta_path = get_delta_path()

    mlflow_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    trainer = MLTrainer(spark, delta_path, mlflow_uri=mlflow_uri)
    results = trainer.run_all()

    print("\n=== Результаты обучения ===")
    print(f"Регрессия: {results['regression']}")
    print(f"Классификация: {results['classification']}")

    host_port = os.environ.get("MLFLOW_HOST_PORT", "5050")
    print(f"\nMLflow UI на хосте: http://localhost:{host_port} (внутри Docker tracking: http://mlflow:5000)")
    spark.stop()


if __name__ == "__main__":
    main()