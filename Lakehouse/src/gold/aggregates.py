from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline


class GoldAggregates:
    """Витрины Gold: аналитика (overwrite) + feature table для ML."""

    def __init__(self, spark, delta_path):
        self.spark = spark
        self.delta_path = delta_path
        self.silver_path = f"{delta_path}/silver/flights"
        self.gold_path = f"{delta_path}/gold"

    def create_analytics_aggregates(self):
        """Аналитические агрегаты; повторный запуск перезаписывает витрины (без дублей)."""
        silver_df = self.spark.read.format("delta").load(self.silver_path)

        airport_delays = silver_df.groupBy("ORIGIN_AIRPORT", "YEAR").agg(
            F.avg("ARR_DELAY").alias("avg_arrival_delay"),
            F.avg("DEP_DELAY").alias("avg_departure_delay"),
            F.count("*").alias("total_flights"),
            F.sum(F.when(F.col("ARR_DELAY") > 15, 1).otherwise(0)).alias("delayed_flights"),
            F.stddev("ARR_DELAY").alias("delay_stddev"),
        )

        airline_delays = silver_df.groupBy("AIRLINE", "YEAR", "season").agg(
            F.avg("ARR_DELAY").alias("avg_delay"),
            F.percentile_approx("ARR_DELAY", 0.5).alias("median_delay"),
            F.count("*").alias("flight_count"),
        )

        hourly_delays = silver_df.groupBy("hour", "YEAR").agg(
            F.avg("ARR_DELAY").alias("avg_delay"),
            F.count("*").alias("flights_count"),
        )

        seasonal_delays = silver_df.groupBy("season", "AIRLINE", "YEAR").agg(
            F.avg("ARR_DELAY").alias("avg_delay"),
            F.count("*").alias("flights_count"),
        )

        analytics_path = f"{self.gold_path}/analytics"

        for name, df in [
            ("airport_delays", airport_delays),
            ("airline_delays", airline_delays),
            ("hourly_delays", hourly_delays),
            ("seasonal_delays", seasonal_delays),
        ]:
            table_path = f"{analytics_path}/{name}"
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)
            print(f"Сохранена аналитическая витрина (overwrite): {name}")

        return analytics_path

    def create_feature_table(self):
        """Feature table: OHE категорий + вектор features; повторный запуск — overwrite."""
        silver_df = self.spark.read.format("delta").load(self.silver_path)

        feature_df = silver_df.select(
            "FL_DATE",
            "YEAR",
            "MONTH",
            "DAY_OF_MONTH",
            "hour",
            "day_of_week",
            "AIRLINE",
            "ORIGIN_AIRPORT",
            "DEST_AIRPORT",
            "route",
            "is_weekend",
            "is_rush_hour",
            "season",
            "DEP_DELAY",
            "ARR_DELAY",
        )

        feature_df = feature_df.withColumn(
            "is_delayed",
            F.when(F.col("ARR_DELAY") > 15, 1).otherwise(0),
        )

        categorical_cols = ["AIRLINE", "ORIGIN_AIRPORT", "DEST_AIRPORT", "season"]

        stages = []
        for col in categorical_cols:
            stages.append(
                StringIndexer(
                    inputCol=col,
                    outputCol=f"{col}_index",
                    handleInvalid="keep",
                )
            )
            stages.append(
                OneHotEncoder(
                    inputCol=f"{col}_index",
                    outputCol=f"{col}_encoded",
                    handleInvalid="keep",
                )
            )

        feature_cols = (
            ["hour", "day_of_week", "is_weekend", "is_rush_hour", "MONTH", "DEP_DELAY"]
            + [f"{col}_encoded" for col in categorical_cols]
        )

        stages.append(VectorAssembler(inputCols=feature_cols, outputCol="features"))

        model = Pipeline(stages=stages).fit(feature_df)
        feature_df = model.transform(feature_df)

        feature_table_path = f"{self.gold_path}/feature_table"

        feature_df.write.format("delta").mode("overwrite").partitionBy("YEAR").option(
            "overwriteSchema", "true"
        ).save(feature_table_path)

        print(f"Feature table (overwrite) сохранена в {feature_table_path}")

        return feature_table_path
