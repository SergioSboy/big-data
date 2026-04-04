import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import psutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, max

try:
    from pyspark import SparkListener
except ImportError:  # pragma: no cover
    SparkListener = None

try:
    from pyspark import StorageLevel
except ImportError:  # pragma: no cover
    StorageLevel = None


def get_memory_usage_mb():
    return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)


def setup_logging(log_file=None):
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
        force=True,
    )


def attach_spark_listener(sc):
    if SparkListener is None:
        logging.info("SparkListener недоступен в этой сборке PySpark — jobs/stages только через ключевые точки логов.")
        return

    class JobStageLogger(SparkListener):
        def onJobStart(self, job_start):
            try:
                jid = job_start.jobId()
                stages = list(job_start.stageIds())
                logging.info("Spark Job START id=%s stages=%s", jid, stages)
            except Exception as e:  # noqa: BLE001
                logging.info("Spark Job START (raw): %s", e)

        def onJobEnd(self, job_end):
            try:
                jid = job_end.jobId()
                res = job_end.jobResult()
                logging.info("Spark Job END id=%s result=%s", jid, res)
            except Exception as e:  # noqa: BLE001
                logging.info("Spark Job END (raw): %s", e)

        def onStageCompleted(self, stage_completed):
            try:
                info = stage_completed.stageInfo()
                sid = info.stageId()
                name = info.name()
                ntasks = info.numTasks()
                logging.info("Spark Stage DONE id=%s tasks=%s name=%s", sid, ntasks, name)
            except Exception as e:  # noqa: BLE001
                logging.info("Spark Stage DONE (raw): %s", e)

    try:
        sc.addSparkListener(JobStageLogger())
    except Exception as e:  # noqa: BLE001
        logging.warning("SparkListener не зарегистрирован: %s", e)


def append_metrics_jsonl(path, record):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)


def main():
    parser = argparse.ArgumentParser(description="Spark job над HDFS для лабораторной (замер времени и RAM).")
    parser.add_argument(
        "--hdfs_url",
        type=str,
        default=os.environ.get("HDFS_URL", "hdfs://localhost:9000/dataset.csv"),
        help="По умолчанию: переменная HDFS_URL или hdfs://localhost:9000/... (в Docker см. Dockerfile)",
    )
    parser.add_argument("--optimize", action="store_true", help="repartition + persist/cache + warmup count")
    parser.add_argument(
        "--experiment",
        type=str,
        default="",
        help="Метка эксперимента для отчёта, напр. 1dn_spark, 1dn_spark_opt, 3dn_spark, 3dn_spark_opt",
    )
    parser.add_argument(
        "--shuffle_partitions",
        type=int,
        default=10,
        help="spark.sql.shuffle.partitions",
    )
    parser.add_argument(
        "--repartition_cols",
        type=int,
        default=6,
        help="Число партиций при --optimize (repartition)",
    )
    parser.add_argument(
        "--results_dir",
        type=str,
        default=os.path.join(os.path.dirname(__file__), "results"),
        help="Каталог для run.log и metrics.jsonl",
    )
    args = parser.parse_args()

    results_dir = os.path.abspath(args.results_dir)
    setup_logging(os.path.join(results_dir, "run.log"))

    spark = (
        SparkSession.builder.appName("HadoopSparkLab")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    attach_spark_listener(spark.sparkContext)

    exp_label = args.experiment or ("opt" if args.optimize else "base")
    logging.info(
        "Старт: experiment=%s optimize=%s hdfs=%s shuffle_partitions=%s",
        exp_label,
        args.optimize,
        args.hdfs_url,
        args.shuffle_partitions,
    )

    t0 = time.perf_counter()
    mem0 = get_memory_usage_mb()

    logging.info("Чтение CSV из HDFS (inferSchema)...")
    try:
        df = spark.read.option("header", True).option("inferSchema", True).csv(args.hdfs_url)
    except Exception as e:  # noqa: BLE001
        err = str(e)
        if "PATH_NOT_FOUND" in err or "Path does not exist" in err:
            logging.error(
                "Файл в HDFS не найден по %s (ожидается объект /dataset.csv в HDFS).\n"
                "Загрузите данные из корня SparkHadoopLab:\n"
                "  docker cp dataset.csv namenode:/dataset.csv\n"
                "  docker exec namenode hdfs dfs -put -f /dataset.csv /dataset.csv\n"
                "Проверка: docker exec namenode hdfs dfs -ls /dataset.csv\n"
                "После «docker compose down -v» содержимое HDFS обнуляется — положите файл снова.",
                args.hdfs_url,
            )
        raise

    if args.optimize:
        logging.info("Оптимизация: repartition(%s), persist, материализация count()", args.repartition_cols)
        df = df.repartition(args.repartition_cols, col("department"))
        if StorageLevel is not None:
            df = df.persist(StorageLevel.MEMORY_AND_DISK)
        else:
            df = df.cache()
        spark.sparkContext.setJobDescription("warmup: cache/persist materialization")
        n = df.count()
        logging.info("После warmup: строк в DataFrame = %s", n)

    logging.info("Группировка и агрегация по department")
    spark.sparkContext.setJobDescription("aggregate: groupBy department")
    agg_df = df.groupBy("department").agg(
        avg("salary").alias("avg_salary"),
        max("performance_score").alias("max_score"),
        avg("age").alias("avg_age"),
    )

    logging.info("Фильтр и сортировка (дополнительный stage shuffle)")
    spark.sparkContext.setJobDescription("filter+orderBy: high earners by max_score")
    filtered_df = agg_df.filter(col("avg_salary") > 70000).orderBy(col("max_score"), ascending=False)

    logging.info("Action: collect()")
    spark.sparkContext.setJobDescription("action: collect results")
    results = filtered_df.collect()

    elapsed = time.perf_counter() - t0
    mem_delta = get_memory_usage_mb() - mem0

    logging.info("Результаты (строк): %d", len(results))
    for row in results:
        logging.info("  %s", row)

    logging.info("Метрики: время=%.4f с, ΔRAM драйвера≈%.2f MB", elapsed, mem_delta)

    record = {
        "experiment": exp_label,
        "optimize": args.optimize,
        "hdfs_url": args.hdfs_url,
        "shuffle_partitions": args.shuffle_partitions,
        "repartition": args.repartition_cols if args.optimize else None,
        "duration_sec": round(elapsed, 4),
        "driver_ram_delta_mb": round(mem_delta, 2),
        "result_rows": len(results),
        "ts_utc": datetime.now(timezone.utc).isoformat(),
    }
    metrics_path = os.path.join(results_dir, "metrics.jsonl")
    append_metrics_jsonl(metrics_path, record)
    logging.info("Метрики дописаны в %s", metrics_path)

    if args.optimize:
        try:
            df.unpersist(blocking=False)
        except Exception:  # noqa: BLE001
            pass

    spark.stop()


if __name__ == "__main__":
    main()
