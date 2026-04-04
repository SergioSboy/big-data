# Лабораторная: Spark поверх HDFS (1 vs 3 DataNode, базовый и оптимизированный Spark)

Проект сравнивает время выполнения и использование памяти драйвером PySpark при работе с CSV в HDFS на кластере Hadoop с **одной** нодой данных и с **тремя**, а также **до и после** программных оптимизаций (`repartition`, `persist`/`cache`, warmup `count()`).

## Соответствие пунктам задания

| Пункт | Реализация |
|--------|------------|
| 1. Датасет ≥100k строк, ≥6 признаков, ≥3 типа, есть категориальный | `data_gen/generate_data.py`: 150k строк; `emp_id`, `age`, `salary`, `department`, `join_date`, `performance_score` (целые, вещественные, строка-категория, дата строкой). |
| 2. Hadoop 1 NN + 1 DN, HDFS, размер блока, лимит памяти | `hadoop_1dn/docker-compose.yml`: `HDFS_CONF_dfs_blocksize=2097152` (2 МиБ), `deploy.resources.limits.memory` для NameNode и DataNode. |
| 3. Spark app, время и RAM, логи этапов, без WARN/FATAL от Spark | `spark_app/app.py`: замер `time.perf_counter()` и RSS драйвера (`psutil`), `logging` в `results/run.log`, `SparkListener` (jobs/stages) и `setJobDescription`, `spark.sparkContext.setLogLevel("ERROR")`. |
| 4. Hadoop 1 NN + 3 DN, повтор | `hadoop_3dn/docker-compose.yml`, те же шаги загрузки и запуска приложения. |
| 5. Сравнение, графики | Метрики пишутся в `spark_app/results/metrics.jsonl`; графики: `python plot_results.py` → `results/charts/comparison.png`. |
| 6. Оптимизация Spark | Флаг `--optimize`: `.repartition(n, "department")`, `.persist(StorageLevel.MEMORY_AND_DISK)` (или `.cache()`), материализующий `count()`, настраиваемые `--shuffle_partitions` / `--repartition_cols`. |

**Четыре эксперимента:** (1 DN, базовый Spark) → (1 DN, opt) → (3 DN, базовый) → (3 DN, opt). Метки для отчёта передаются в `--experiment` (см. ниже).

## Требования

- Docker и Docker Compose (`docker compose` или `docker-compose`)
- Python 3.8+
- JDK 8 или 11 на машине, где крутится PySpark-драйвер

## 1. Генерация данных

```bash
cd data_gen
pip install -r requirements.txt
python generate_data.py
mv dataset.csv ../
```

В корне `SparkHadoopLab` появится `dataset.csv` (~150k строк).

## 2. Кластер 1 NameNode + 1 DataNode

Файл: `hadoop_1dn/docker-compose.yml`.

- **Размер блока HDFS:** `2097152` байт = **2 МиБ** (`HDFS_CONF_dfs_blocksize`).
- **Репликация:** `1` (один DataNode).
- **Память контейнеров:** NameNode и `datanode1` ограничены **1 GiB** каждый (`deploy.resources.limits.memory`).

Запуск и загрузка в HDFS:

```bash
cd hadoop_1dn
docker compose up -d
# дождаться готовности NameNode (порт 9870)

docker cp ../dataset.csv namenode:/dataset.csv
docker exec -it namenode hdfs dfs -put -f /dataset.csv /dataset.csv
```

Проверка: в браузере `http://localhost:9870` → Utilities → Browse the file system → `/dataset.csv`.

## 3. Spark-приложение и четыре прогона

Установка зависимостей (в отдельном venv по желанию):

```bash
cd ../spark_app
pip install -r requirements.txt
```

URL HDFS с хоста: **hdfs://localhost:9000/...** (порт проброшен с NameNode).

Рекомендуемые метки экспериментов (для графиков и таблицы в отчёте):

| № | Кластер | Режим | Команда |
|---|---------|--------|---------|
| 1 | 1 DataNode | базовый | `python app.py --experiment 1dn_spark --hdfs_url hdfs://localhost:9000/dataset.csv` |
| 2 | 1 DataNode | оптимизация | `python app.py --experiment 1dn_spark_opt --hdfs_url hdfs://localhost:9000/dataset.csv --optimize` |
| 3 | 3 DataNode | базовый | после переключения кластера (см. ниже), та же команда с `--experiment 3dn_spark` |
| 4 | 3 DataNode | оптимизация | `--experiment 3dn_spark_opt --optimize` |

Дополнительные аргументы `app.py`:

- `--shuffle_partitions N` — `spark.sql.shuffle.partitions` (по умолчанию 10).
- `--repartition_cols N` — число партиций в `repartition` при `--optimize` (по умолчанию 6).
- `--results_dir ПУТЬ` — каталог для `run.log` и `metrics.jsonl` (по умолчанию `spark_app/results`).

Логи: **консоль** и файл **`spark_app/results/run.log`** (ключевые шаги + при успешной регистрации — старт/конец job и завершение stage). Уровень логов Spark в консоли — **ERROR** (WARN/FATAL от движка не засоряют вывод).

Метрики каждого прогона **дописываются** в `spark_app/results/metrics.jsonl`. Перед «чистой» серией экспериментов файл можно удалить или переименовать.

### Переход на кластер с 3 DataNode

Остановите первый кластер (иначе конфликт имён контейнеров и портов):

```bash
cd ../hadoop_1dn
docker compose down -v
```

Запуск 3 DataNode (`hadoop_3dn/docker-compose.yml`):

- Блок **2 МиБ**, репликация **3**, память NameNode **1 GiB**, каждый DataNode **512 MiB**.

```bash
cd ../hadoop_3dn
docker compose up -d
docker cp ../dataset.csv namenode:/dataset.csv
docker exec -it namenode hdfs dfs -put -f /dataset.csv /dataset.csv
```

Снова из `spark_app` выполните эксперименты 3 и 4 с метками `3dn_spark` и `3dn_spark_opt`.

После лабораторной:

```bash
cd ../hadoop_3dn
docker compose down -v
```

## 4. Графики сравнения

После всех четырёх прогонов (с правильными `--experiment`):

```bash
cd spark_app
python plot_results.py
```

Будет создан файл **`results/charts/comparison.png`**: два графика (время и прирост RAM драйвера) по последним записям для каждой метки эксперимента.

Порядок столбиков по умолчанию: `1dn_spark`, `1dn_spark_opt`, `3dn_spark`, `3dn_spark_opt`. Изменить можно так:

```bash
python plot_results.py --order "1dn_spark,1dn_spark_opt,3dn_spark,3dn_spark_opt" --out results/charts
```

## 5. Дополнительно: Spark UI

Пока работает `app.py`, UI доступен на `http://localhost:4040` — детализация **jobs** и **stages** (удобно для скриншотов к отчёту).

## Структура репозитория

```
SparkHadoopLab/
├── data_gen/           # генерация dataset.csv
├── hadoop_1dn/         # 1 NameNode + 1 DataNode
├── hadoop_3dn/         # 1 NameNode + 3 DataNode
├── spark_app/
│   ├── app.py          # основное приложение + метрики + логи
│   ├── plot_results.py # графики по metrics.jsonl
│   └── results/        # run.log, metrics.jsonl, charts/
└── README.md
```
