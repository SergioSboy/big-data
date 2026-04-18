from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import numpy as np
import os


class BronzeLoader:
    """Загрузка сырых данных в Bronze слой (Delta, append по годам)."""

    def __init__(self, spark, delta_path):
        self.spark = spark
        self.delta_path = delta_path
        self.bronze_path = f"{delta_path}/bronze/flights"

    def download_sample_data(self):
        """Синтетические данные с канонической схемой (если нет CSV)."""
        import pandas as pd

        print("Генерация примерных данных за 2024 год...")
        np.random.seed(42)
        dates = pd.date_range("2024-01-01", "2024-12-31", freq="h")

        data = []
        for date in dates[:5000]:
            data.append(
                {
                    "FL_DATE": date.strftime("%Y-%m-%d"),
                    "AIRLINE": np.random.choice(
                        ["DL", "AA", "UA", "WN", "B6"], p=[0.25, 0.25, 0.2, 0.2, 0.1]
                    ),
                    "ORIGIN_AIRPORT": np.random.choice(["JFK", "LAX", "ORD", "DFW", "DEN"]),
                    "DEST_AIRPORT": np.random.choice(["JFK", "LAX", "ORD", "DFW", "DEN"]),
                    "FLIGHT_NUM": int(np.random.randint(100, 9999)),
                    "CRS_DEP_TIME": int(np.random.randint(600, 2200)),
                    "DEP_TIME": None,
                    "DEP_DELAY": None,
                    "CRS_ARR_TIME": 1200,
                    "ARR_TIME": None,
                    "ARR_DELAY": None,
                    "CANCELLED": 0,
                    "DIVERTED": 0,
                    "YEAR": date.year,
                    "MONTH": date.month,
                    "DAY_OF_MONTH": date.day,
                    "DAY_OF_WEEK": int(date.dayofweek) + 1,
                }
            )

        for record in data:
            if np.random.random() < 0.3:
                delay = float(np.random.exponential(30))
                record["DEP_DELAY"] = delay
                record["ARR_DELAY"] = delay
                record["DEP_TIME"] = int(record["CRS_DEP_TIME"] + min(delay, 200))
                record["ARR_TIME"] = int(record["CRS_ARR_TIME"] + min(delay, 200))
            else:
                record["DEP_DELAY"] = 0.0
                record["ARR_DELAY"] = 0.0
                record["DEP_TIME"] = record["CRS_DEP_TIME"]
                record["ARR_TIME"] = record["CRS_ARR_TIME"]

            if np.random.random() < 0.01:
                record["CANCELLED"] = 1
                record["DEP_DELAY"] = None
                record["ARR_DELAY"] = None

        return self.spark.createDataFrame(data)

    def _resolve_col(self, df: DataFrame, *candidates: str):
        """Имя колонки в CSV может отличаться пробелами/регистром."""
        norm = {}
        for c in df.columns:
            if not c:
                continue
            k = c.strip().lower()
            if k and k not in norm:
                norm[k] = c
        for name in candidates:
            key = name.strip().lower()
            if key in norm:
                return F.col(norm[key])
        raise ValueError(f"Не найдена колонка среди {candidates}, есть: {df.columns[:25]}...")

    def _to_canonical(self, df: DataFrame) -> DataFrame:
        """US DOT / BTS (Kaggle 2018–2024) → единая схема для Silver."""
        cols_lower = {c.strip().lower() for c in df.columns}
        if "flightdate" in cols_lower:
            fd = self._resolve_col(df, "FlightDate", "flightdate")
            return df.select(
                self._resolve_col(df, "Year").cast("int").alias("YEAR"),
                self._resolve_col(df, "Month").cast("int").alias("MONTH"),
                self._resolve_col(df, "DayofMonth", "DayOfMonth").cast("int").alias("DAY_OF_MONTH"),
                self._resolve_col(df, "DayOfWeek").cast("int").alias("DAY_OF_WEEK"),
                F.date_format(fd, "yyyy-MM-dd").alias("FL_DATE"),
                F.trim(self._resolve_col(df, "IATA_Code_Marketing_Airline")).alias("AIRLINE"),
                F.trim(self._resolve_col(df, "Origin")).alias("ORIGIN_AIRPORT"),
                F.trim(self._resolve_col(df, "Dest")).alias("DEST_AIRPORT"),
                self._resolve_col(df, "Flight_Number_Marketing_Airline").cast("int").alias("FLIGHT_NUM"),
                self._resolve_col(df, "CRSDepTime").cast("double").cast("int").alias("CRS_DEP_TIME"),
                F.when(self._resolve_col(df, "DepTime").isNotNull(),
                       self._resolve_col(df, "DepTime").cast("double").cast("int")).alias("DEP_TIME"),
                self._resolve_col(df, "DepDelay", "DepDelayMinutes").cast("double").alias("DEP_DELAY"),
                self._resolve_col(df, "CRSArrTime").cast("double").cast("int").alias("CRS_ARR_TIME"),
                F.when(self._resolve_col(df, "ArrTime").isNotNull(),
                       self._resolve_col(df, "ArrTime").cast("double").cast("int")).alias("ARR_TIME"),
                self._resolve_col(df, "ArrDelay", "ArrDelayMinutes").cast("double").alias("ARR_DELAY"),
                self._resolve_col(df, "Cancelled").cast("double").cast("int").alias("CANCELLED"),
                self._resolve_col(df, "Diverted").cast("double").cast("int").alias("DIVERTED"),
            )
        return df

    def load_bronze(self, data_path=None, year=None):
        """Запись батча в Bronze: append (версии Delta), без MERGE — как в ТЗ."""
        if data_path and os.path.exists(data_path):
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(data_path)
            df = self._to_canonical(df)
            if year is not None:
                df = df.filter(F.col("YEAR") == int(year))
        else:
            df = self.download_sample_data()
            if year is not None:
                df = df.filter(F.col("YEAR") == int(year))

        df = (
            df.withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source_file", F.lit(data_path if data_path else "generated"))
        )

        writer = (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
        )
        if DeltaTable.isDeltaTable(self.spark, self.bronze_path):
            writer.save(self.bronze_path)
            print(f"Bronze: append батча (год={year}), mergeSchema=true")
        else:
            writer.save(self.bronze_path)
            print(f"Bronze: создана таблица, записей в батче: {df.count()}")

        delta_table = DeltaTable.forPath(self.spark, self.bronze_path)
        delta_table.optimize().executeCompaction()
        return self.bronze_path

    def load_by_years(self, start_year=2018, end_year=2024, data_path=None):
        """Инкрементальная загрузка по годам: отдельный append на год → история версий."""
        csv_path = data_path
        if csv_path is None:
            parent = os.path.dirname(self.delta_path.rstrip(os.sep))
            default_csv = os.path.join(parent, "flight_data_2018_2024.csv")
            csv_path = os.environ.get("FLIGHT_DATA_CSV", default_csv)
            if not os.path.isabs(csv_path):
                csv_path = os.path.abspath(csv_path)

        use_csv = csv_path and os.path.exists(csv_path)
        if not use_csv:
            print("CSV не найден — одна синтетическая загрузка (демо).")
            self.load_bronze(data_path=None, year=None)
            return self.bronze_path

        prev_version = None
        for year in range(start_year, end_year + 1):
            print(f"Загрузка данных за {year} год (append)...")
            self.load_bronze(data_path=csv_path, year=year)

            dt = DeltaTable.forPath(self.spark, self.bronze_path)
            ver = dt.history(1).select("version").collect()[0]["version"]
            if prev_version is not None:
                snap = (
                    self.spark.read.format("delta")
                    .option("versionAsOf", int(prev_version))
                    .load(self.bronze_path)
                )
                print(
                    f"Time travel: снимок до текущего append — versionAsOf={prev_version}, "
                    f"строк={snap.count()}"
                )
            prev_version = int(ver)

        delta_table = DeltaTable.forPath(self.spark, self.bronze_path)
        delta_table.optimize().executeZOrderBy(["YEAR", "ORIGIN_AIRPORT"])
        return self.bronze_path
