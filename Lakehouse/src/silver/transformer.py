from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType, DoubleType
from delta.tables import DeltaTable
import polars as pl


class SilverTransformer:
    """Трансформация данных в Silver слой"""

    def __init__(self, spark, delta_path):
        self.spark = spark
        self.delta_path = delta_path
        self.silver_path = f"{delta_path}/silver/flights"
        self.bronze_path = f"{delta_path}/bronze/flights"

    def clean_data(self, df):
        """Очистка данных"""

        df = df.filter(F.col("CANCELLED").cast("int") == 0)

        df = df.withColumn("AIRLINE", F.upper(F.trim(F.col("AIRLINE"))))
        df = df.withColumn(
            "FLIGHT_NUM",
            F.coalesce(F.col("FLIGHT_NUM").cast("int"), F.lit(-1)),
        )

        # Удаляем рейсы с выбросами (задержки > 6 часов)
        df = df.filter((F.col("ARR_DELAY").isNull()) | (F.col("ARR_DELAY") <= 360))
        df = df.filter((F.col("DEP_DELAY").isNull()) | (F.col("DEP_DELAY") <= 360))

        # Заполняем NULL задержки нулями
        df = df.fillna({
            "ARR_DELAY": 0,
            "DEP_DELAY": 0,
            "DEP_TIME": 0,
            "ARR_TIME": 0
        })

        return df

    def add_features(self, df):
        """Добавление производных признаков"""

        # Час вылета из CRS_DEP_TIME (формат HHMM)
        df = df.withColumn(
            "hour",
            (F.floor(F.col("CRS_DEP_TIME").cast("double") / F.lit(100))).cast(IntegerType()),
        )

        # Day of week (уже есть, но переименуем)
        df = df.withColumnRenamed("DAY_OF_WEEK", "day_of_week")

        # Season
        df = df.withColumn("season",
                           F.when(F.col("MONTH").between(3, 5), "spring")
                           .when(F.col("MONTH").between(6, 8), "summer")
                           .when(F.col("MONTH").between(9, 11), "fall")
                           .otherwise("winter")
                           )

        # Route
        df = df.withColumn("route", F.concat_ws("_", F.col("ORIGIN_AIRPORT"), F.col("DEST_AIRPORT")))

        # DOT DayOfWeek: 1=Пн … 7=Вс → выходные 6–7
        df = df.withColumn("is_weekend", F.when(F.col("day_of_week").isin(6, 7), 1).otherwise(0))

        # Rush hour flag
        df = df.withColumn("is_rush_hour",
                           F.when((F.col("hour").between(7, 9)) | (F.col("hour").between(17, 19)), 1).otherwise(0))

        return df

    def select_columns(self, df):
        """Выбор подмножества колонок"""

        selected_cols = [
            "FL_DATE", "YEAR", "MONTH", "DAY_OF_MONTH",
            "AIRLINE", "FLIGHT_NUM", "ORIGIN_AIRPORT", "DEST_AIRPORT", "route",
            "CRS_DEP_TIME", "hour", "day_of_week", "season",
            "is_weekend", "is_rush_hour",
            "DEP_DELAY", "ARR_DELAY", "DIVERTED",
        ]

        return df.select(*selected_cols)

    def transform_silver(self):
        """Трансформация Bronze -> Silver"""

        # Читаем из Bronze
        bronze_df = self.spark.read.format("delta").load(self.bronze_path)

        # Применяем трансформации
        silver_df = self.clean_data(bronze_df)
        silver_df = self.add_features(silver_df)
        silver_df = self.select_columns(silver_df)

        # Запись с партиционированием по году и месяцу
        if DeltaTable.isDeltaTable(self.spark, self.silver_path):
            # MERGE для обновления существующих записей
            delta_table = DeltaTable.forPath(self.spark, self.silver_path)

            merge_condition = (
                "target.FL_DATE = source.FL_DATE AND target.CRS_DEP_TIME = source.CRS_DEP_TIME AND "
                "target.ORIGIN_AIRPORT = source.ORIGIN_AIRPORT AND target.DEST_AIRPORT = source.DEST_AIRPORT AND "
                "target.AIRLINE = source.AIRLINE AND target.FLIGHT_NUM = source.FLIGHT_NUM"
            )

            delta_table.alias("target") \
                .merge(silver_df.alias("source"), merge_condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()

            print("Silver таблица обновлена через MERGE")
        else:
            # Первая запись с партиционированием
            silver_df.write.format("delta") \
                .mode("overwrite") \
                .partitionBy("YEAR", "MONTH") \
                .save(self.silver_path)
            print(f"Silver таблица создана с партиционированием по YEAR/MONTH")

        # Применяем Z-ORDER для оптимизации запросов
        delta_table = DeltaTable.forPath(self.spark, self.silver_path)
        delta_table.optimize().executeZOrderBy(["AIRLINE", "ORIGIN_AIRPORT"])

        # VACUUM старых версий (оставляем последние 7 дней)
        delta_table.vacuum(retentionHours=168)

        # Демонстрация Polars lazy query с explain
        self.demonstrate_polars_query()

        return self.silver_path

    def demonstrate_polars_query(self):
        """Демонстрация Polars lazy query с EXPLAIN"""

        # Читаем Delta таблицу через Polars
        try:
            df = pl.scan_delta(self.silver_path)

            # Сложный запрос с фильтрацией и агрегацией
            query = df.filter(
                (pl.col("ARR_DELAY") > 0) &
                (pl.col("YEAR") == 2024)
            ).group_by(
                ["AIRLINE", "season"]
            ).agg([
                pl.col("ARR_DELAY").mean().alias("avg_delay"),
                pl.col("ARR_DELAY").count().alias("flight_count")
            ]).sort(["AIRLINE", "avg_delay"], descending=[False, True])

            # Получение плана запроса
            print("\n" + "=" * 80)
            print("POLARS LAZY QUERY EXPLAIN (с пушдаунами проекций и селекций):")
            print("=" * 80)
            print(query.explain())
            print("\nОбоснование выбора партиций:")
            print("- Партиционирование по YEAR/MONTH позволяет эффективно фильтровать по времени")
            print("- Z-ORDER по AIRLINE и ORIGIN_AIRPORT ускоряет группировки и фильтрации")
            print("- Пушдауны фильтров в Delta слой снижают объем читаемых данных")
            print("=" * 80 + "\n")

        except Exception as e:
            print(f"Polars query демонстрация пропущена: {e}")