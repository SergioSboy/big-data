# Лабораторная: Spark поверх HDFS (1 vs 3 DataNode, базовый и оптимизированный Spark)

Проект сравнивает время выполнения и использование памяти драйвером PySpark при работе с CSV в HDFS на кластере Hadoop с **одной** нодой данных и с **тремя**, а также **до и после** программных оптимизаций (`repartition`, `persist`/`cache`, warmup `count()`).

**Запуск генерации данных и Spark-приложения выполняется в Docker** (отдельные образы и `docker compose`). На хосте достаточно **Docker** и **Docker Compose**; Python и JDK для лабораторной не обязательны.

**Четыре эксперимента:** (1 DN, базовый) → (1 DN, opt) → (3 DN, базовый) → (3 DN, opt). Метки передаются в `--experiment`.

## Требования

- **Docker** и **Docker Compose** (`docker compose`)
- Кластер Hadoop поднимается отдельными compose-файлами в `hadoop_1dn` / `hadoop_3dn` (как раньше)

Все команды ниже выполняйте из корня репозитория **`SparkHadoopLab`** (родительская папка `data_gen` и `spark_app`).

---

## 1. Сборка образов (один раз)

```bash
docker compose -f data_gen/docker-compose.yml build
docker compose -f spark_app/docker-compose.yml build
```

---

## 2. Генерация `dataset.csv` в Docker (при необходимости)

Каталог **`SparkHadoopLab`** монтируется в контейнер как `/out`; файл появится в корне лабораторной: `SparkHadoopLab/dataset.csv`.

```bash
docker compose -f data_gen/docker-compose.yml run --rm generate
```

Переменные (при необходимости): `OUTPUT_DIR`, `OUTPUT_FILENAME` — см. `data_gen/generate_data.py`.

Локальный запуск без Docker (по желанию): `cd data_gen && pip install -r requirements.txt && python generate_data.py` и перенос `dataset.csv` в корень лабораторной.

---

## 3. Кластер Hadoop и загрузка в HDFS

### 3.1. Один DataNode

```bash
cd hadoop_1dn
docker compose up -d

cd ..
docker cp dataset.csv namenode:/dataset.csv
docker exec namenode hdfs dfs -put -f /dataset.csv /dataset.csv
docker exec namenode hdfs dfs -ls /dataset.csv
```

Убедитесь, что в выводе `ls` есть `/dataset.csv`. Если команды Spark пишут `PATH_NOT_FOUND`, чаще всего файл не залит или кластер подняли с **`docker compose down -v`** (тома HDFS пустые) — выполните `docker cp` и `hdfs dfs -put` снова.

### 3.2. Три DataNode (после остановки первого кластера)

```bash
cd hadoop_1dn && docker compose down -v && cd ..
cd hadoop_3dn
docker compose up -d

cd ..
docker cp dataset.csv namenode:/dataset.csv
docker exec namenode hdfs dfs -put -f /dataset.csv /dataset.csv
docker exec namenode hdfs dfs -ls /dataset.csv
```

Параметры кластеров (блок **2 МиБ**, репликация, лимиты памяти) — в соответствующих `docker-compose.yml`.

Hadoop и DataNode подключены к **общей сети Docker `hadoop_lab_net`** (имя фиксировано в compose). Она создаётся при первом `docker compose up` в `hadoop_1dn` или `hadoop_3dn`. После обновления compose пересоздайте кластер: `docker compose down` и снова `up -d`.

---

## 4. Spark-приложение в Docker

Контейнер Spark подключается к **той же сети `hadoop_lab_net`**, что и NameNode/DataNode. URL по умолчанию: **`hdfs://namenode:9000/dataset.csv`**. Так клиент HDFS получает от NameNode адреса DataNode, **доступные из контейнера Spark** (иначе при доступе только к порту 9000 на хосте возникает `BlockMissingException`: внутренние IP DN не маршрутизируются из другой сети).

**Порядок:** сначала поднимите Hadoop (§3), чтобы сеть **`hadoop_lab_net`** существовала; затем запускайте Spark.

Логи и метрики пишутся в **`spark_app/results/`** на хосте (том `./results:/app/results`). Spark UI: **http://localhost:4040**.

Из корня **`SparkHadoopLab`**:

| № | Кластер | Режим | Команда |
|---|---------|--------|---------|
| 1 | 1 DN | базовый | `docker compose -f spark_app/docker-compose.yml run --rm spark --experiment 1dn_spark` |
| 2 | 1 DN | opt | `docker compose -f spark_app/docker-compose.yml run --rm spark --experiment 1dn_spark_opt --optimize` |
| 3 | 3 DN | базовый | после §3.2: `docker compose -f spark_app/docker-compose.yml run --rm spark --experiment 3dn_spark` |
| 4 | 3 DN | opt | `docker compose -f spark_app/docker-compose.yml run --rm spark --experiment 3dn_spark_opt --optimize` |

Дополнительные аргументы `app.py` (после `run --rm spark`): например `--shuffle_partitions 12`, `--repartition_cols 8`, `--results_dir /app/results`.

Явный URL HDFS (редко нужно):

```bash
docker compose -f spark_app/docker-compose.yml run --rm -e HDFS_URL=hdfs://namenode:9000/dataset.csv spark --experiment 1dn_spark
```

## 5. Графики

После четырёх прогонов (с метками `1dn_spark`, `1dn_spark_opt`, `3dn_spark`, `3dn_spark_opt`):

```bash
docker compose -f spark_app/docker-compose.yml run --rm plot
```

Или с параметрами:

```bash
docker compose -f spark_app/docker-compose.yml run --rm plot -- --metrics /app/results/metrics.jsonl --out /app/results/charts
```

Файл: **`spark_app/results/charts/comparison.png`**.

Перед «чистой» серией экспериментов удалите или переименуйте **`spark_app/results/metrics.jsonl`**.

---

После работы необходимо выполнить: `docker compose down -v` в каталоге активного кластера Hadoop.
