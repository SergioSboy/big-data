from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import mlflow
import mlflow.spark


class MLTrainer:
    """Регрессия ARR_DELAY и классификация is_delayed; логирование в MLflow."""

    def __init__(self, spark, delta_path, mlflow_uri="http://localhost:5000"):
        self.spark = spark
        self.delta_path = delta_path
        mlflow.set_tracking_uri(mlflow_uri)

    def prepare_data(self):
        """Данные из Gold feature table (колонка features уже собрана в Pipeline)."""
        feature_path = f"{self.delta_path}/gold/feature_table"
        df = self.spark.read.format("delta").load(feature_path)

        base = df.select("features", "ARR_DELAY", "is_delayed", "YEAR").na.drop()
        train_df = base.filter("YEAR < 2024")
        test_df = base.filter("YEAR == 2024")

        if test_df.limit(1).count() == 0 or train_df.limit(1).count() == 0:
            train_df, test_df = base.randomSplit([0.8, 0.2], seed=42)

        return (
            train_df.select("features", "ARR_DELAY", "is_delayed"),
            test_df.select("features", "ARR_DELAY", "is_delayed"),
        )

    def get_gold_table_version(self):
        try:
            from delta.tables import DeltaTable

            feature_path = f"{self.delta_path}/gold/feature_table"
            delta_table = DeltaTable.forPath(self.spark, feature_path)
            return int(delta_table.history(1).select("version").collect()[0]["version"])
        except Exception:
            return 0

    def train_regression_models(self, train_df, test_df):
        models = {
            "LinearRegression": LinearRegression(featuresCol="features", labelCol="ARR_DELAY"),
            "RandomForest": RandomForestRegressor(
                featuresCol="features", labelCol="ARR_DELAY", numTrees=100, seed=42
            ),
            "GBTRegressor": GBTRegressor(featuresCol="features", labelCol="ARR_DELAY", maxIter=50, seed=42),
        }

        rmse_ev = RegressionEvaluator(labelCol="ARR_DELAY", predictionCol="prediction", metricName="rmse")
        r2_ev = RegressionEvaluator(labelCol="ARR_DELAY", predictionCol="prediction", metricName="r2")
        mae_ev = RegressionEvaluator(labelCol="ARR_DELAY", predictionCol="prediction", metricName="mae")

        train_reg = train_df.select("features", "ARR_DELAY")
        test_reg = test_df.select("features", "ARR_DELAY")

        results = {}

        for name, model in models.items():
            with mlflow.start_run(run_name=f"Regression_{name}"):
                mlflow.log_param("model_type", "regression")
                mlflow.log_param("algorithm", name)
                mlflow.log_param("train_size", train_reg.count())
                mlflow.log_param("test_size", test_reg.count())

                trained_model = model.fit(train_reg)
                predictions = trained_model.transform(test_reg)

                rmse = rmse_ev.evaluate(predictions)
                r2 = r2_ev.evaluate(predictions)
                mae = mae_ev.evaluate(predictions)

                mlflow.log_metric("rmse", rmse)
                mlflow.log_metric("r2", r2)
                mlflow.log_metric("mae", mae)

                if name == "RandomForest":
                    arr = trained_model.featureImportances.toArray()
                    imp_dict = {f"idx_{i}": float(v) for i, v in enumerate(arr)}
                    top = sorted(imp_dict.items(), key=lambda x: x[1], reverse=True)[:10]
                    mlflow.log_metrics({k: v for k, v in top})
                    print(f"Regression RF top-5 importance: {top[:5]}")

                mlflow.spark.log_model(trained_model, f"regression_{name}")
                mlflow.log_param("gold_table_version", self.get_gold_table_version())

                results[name] = {"rmse": rmse, "r2": r2, "mae": mae}
                print(f"Regression {name}: RMSE={rmse:.2f}, R2={r2:.3f}, MAE={mae:.2f}")

        return results

    def train_classification_models(self, train_df, test_df):
        models = {
            "LogisticRegression": LogisticRegression(featuresCol="features", labelCol="is_delayed"),
            "RandomForestClassifier": RandomForestClassifier(
                featuresCol="features", labelCol="is_delayed", numTrees=100, seed=42
            ),
        }

        train_cls = train_df.select("features", "is_delayed")
        test_cls = test_df.select("features", "is_delayed")

        acc_ev = MulticlassClassificationEvaluator(
            labelCol="is_delayed", predictionCol="prediction", metricName="accuracy"
        )
        f1_ev = MulticlassClassificationEvaluator(
            labelCol="is_delayed", predictionCol="prediction", metricName="f1"
        )
        auc_ev = BinaryClassificationEvaluator(
            labelCol="is_delayed", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
        )

        results = {}

        for name, model in models.items():
            with mlflow.start_run(run_name=f"Classification_{name}"):
                mlflow.log_param("model_type", "classification")
                mlflow.log_param("algorithm", name)
                mlflow.log_param("train_size", train_cls.count())
                mlflow.log_param("test_size", test_cls.count())

                if name == "LogisticRegression":
                    param_grid = (
                        ParamGridBuilder()
                        .addGrid(model.regParam, [0.01, 0.1])
                        .addGrid(model.elasticNetParam, [0.0, 0.5])
                        .build()
                    )
                    cv = CrossValidator(
                        estimator=model,
                        estimatorParamMaps=param_grid,
                        evaluator=acc_ev,
                        numFolds=3,
                        seed=42,
                    )
                    cv_model = cv.fit(train_cls)
                    best_model = cv_model.bestModel
                    mlflow.log_param("best_regParam", best_model._java_obj.getRegParam())
                    mlflow.log_param("best_elasticNetParam", best_model._java_obj.getElasticNetParam())
                else:
                    best_model = model.fit(train_cls)

                predictions = best_model.transform(test_cls)

                accuracy = acc_ev.evaluate(predictions)
                f1 = f1_ev.evaluate(predictions)
                try:
                    auc = auc_ev.evaluate(predictions)
                except Exception:
                    auc = float("nan")

                mlflow.log_metric("accuracy", accuracy)
                mlflow.log_metric("f1", f1)
                if auc == auc:
                    mlflow.log_metric("auc", auc)

                if name == "RandomForestClassifier":
                    arr = best_model.featureImportances.toArray()
                    imp_dict = {f"idx_{i}": float(v) for i, v in enumerate(arr)}
                    top = sorted(imp_dict.items(), key=lambda x: x[1], reverse=True)[:10]
                    mlflow.log_metrics({k: v for k, v in top})
                    print(f"Classification RF top-5 importance: {top[:5]}")

                mlflow.spark.log_model(best_model, f"classification_{name}")
                mlflow.log_param("gold_table_version", self.get_gold_table_version())

                results[name] = {"accuracy": accuracy, "f1": f1, "auc": auc}
                print(f"Classification {name}: Accuracy={accuracy:.3f}, F1={f1:.3f}, AUC={auc}")

        return results

    def run_all(self):
        print("Подготовка данных...")
        train_df, test_df = self.prepare_data()
        print(f"Train size: {train_df.count()}, Test size: {test_df.count()}")

        print("\n=== Обучение моделей регрессии ===")
        regression_results = self.train_regression_models(train_df, test_df)

        print("\n=== Обучение моделей классификации ===")
        classification_results = self.train_classification_models(train_df, test_df)

        return {"regression": regression_results, "classification": classification_results}
