# Flight Delays Pipeline — прогнозирование задержек (US Flights 2018–2024)

## Архитектура

**Bronze** → **Silver** → **Gold** → **ML** (Spark + Delta Lake + Polars lazy + MLflow).

- **Bronze**: CSV (схема US DOT / BTS с Kaggle) нормализуется в канонические колонки; загрузка **по годам**, режим **append** + `mergeSchema` (версии Delta + эволюция схемы). После всех батчей — OPTIMIZE (compaction) и Z-ORDER по `YEAR`, `ORIGIN_AIRPORT`.
- **Silver**: очистка (NA, отмены, выбросы задержек), нормализация категорий, признаки `hour`, `day_of_week`, `season`, `route`, флаги; партиции `YEAR`, `MONTH`; идемпотентность через **MERGE** по ключу рейса; OPTIMIZE, Z-ORDER по `AIRLINE`, `ORIGIN_AIRPORT`, **VACUUM**.
- **Gold**: две группы витрин — аналитические агрегаты (overwrite, без дублей при повторном запуске) и **feature table** для ML (overwrite, партиция `YEAR`).
- **ML**: регрессия `ARR_DELAY`, классификация `is_delayed` (порог 15 минут в Gold); сравнение моделей; feature importance (Random Forest); в MLflow — параметры, метрики, модель, **версия Delta gold-таблицы**.

## Технологии

- **Delta Lake** — ACID, time travel, schema evolution, MERGE, OPTIMIZE, Z-ORDER, VACUUM
- **Apache Spark** — Bronze/Silver/Gold и Spark ML
- **Polars LazyFrame** — `scan_delta` → filter → group_by → `explain()`
- **MLflow** — отдельный сервис из готового образа `bitnamilegacy/mlflow:2.13.0` (в `docker-compose` отдельная сборка не используется)
- **Docker Compose** — Spark (master/worker), приложение `app`, MLflow

## Если прервалась загрузка образов (`Interrupted`)

Сообщение обычно означает обрыв сети или отмену загрузки (образы Spark большие, ~сотни МБ и больше).

1. Подтянуть образы, которые указаны в `docker-compose.yml`, и дождаться окончания:
   ```bash
   docker pull bitnamilegacy/spark:3.5.0
   docker pull bitnamilegacy/mlflow:2.13.0
   ```
2. Собрать только приложение (если меняли `Dockerfile`):
   ```bash
   docker compose build app
   ```
3. Запустить снова:
   ```bash
   docker compose up --build
   ```

При нестабильном интернете повторите `docker pull` / `docker compose build` до успеха.

<a id="data-files"></a>

## Данные: откуда взять и куда положить

### Откуда

Нужен **CSV с задержками рейсов США** в духе открытых данных US DOT / BTS (как в разборе на Kaggle: [Flight Delay Analysis 2018–2024](https://www.kaggle.com/code/peymanradmanesh/flight-delay-analysis-2018-2024)). Подойдёт связанный с этим ноутбуком датасет на Kaggle или **аналогичный** CSV с похожими колонками.

Bronze ожидает схему в стиле Kaggle/US DOT, в том числе (имена могут немного отличаться по регистру):

- дата рейса: `FlightDate` (или эквивалент);
- `Year`, `Month`, `DayOfMonth`, `DayOfWeek`;
- перевозчик и рейс: `IATA_Code_Marketing_Airline`, `Flight_Number_Marketing_Airline`;
- маршрут: `Origin`, `Dest`;
- время и задержки: `CRSDepTime`, `DepTime`, `DepDelay`, `CRSArrTime`, `ArrTime`, `ArrDelay`;
- отмена/смена маршрута: `Cancelled`, `Diverted`.

Если каких-то полей нет, пайплайн может не собраться — ориентируйтесь на полный экспорт с Kaggle.

### Куда положить файл

1. Сохраните CSV в каталог **`Lakehouse/data/`** (рядом с репозиторием), имя по умолчанию:
   - **`flight_data_2018_2024.csv`**

2. В Docker каталог `./data` монтируется в контейнер как **`/opt/spark/data`**, поэтому внутри `app` тот же файл доступен как  
   **`/opt/spark/data/flight_data_2018_2024.csv`** — так и задано в `docker-compose` через переменную `FLIGHT_DATA_CSV`.

3. Чтобы указать **другой путь или имя файла**, задайте переменную окружения **`FLIGHT_DATA_CSV`** (абсолютный путь **внутри контейнера**, например `/opt/spark/data/my_flights.csv`, если файл лежит в `./data/` на хосте).

### Что появится после прогона пайплайна

- **`data/delta/`** — Bronze / Silver / Gold (Delta-таблицы); обычно **не коммитят** в Git (см. `.gitignore`).
- Без CSV Bronze может сгенерировать **один синтетический** батч (демо, без полноценной годовой истории).

## Возможности Delta Lake (сверх MERGE)

1. **MERGE** — Silver (обновление без дублирования строк рейса)
2. **OPTIMIZE (compaction)** — Bronze после батчей; Silver после записи
3. **Z-ORDER** — Bronze по `YEAR`, `ORIGIN_AIRPORT`; Silver по `AIRLINE`, `ORIGIN_AIRPORT`
4. **VACUUM** — Silver (retention 168 ч; для демо отключена проверка минимального срока в Spark)
5. **Time travel** — после каждого годового append читается снимок предыдущей **версии** таблицы (`versionAsOf`)
6. **Schema evolution** — `mergeSchema` при append в Bronze; `overwriteSchema` при пересборке Gold

## Запуск

```bash
docker compose up --build
```

(Подойдёт и устаревший вариант `docker-compose up --build`.)

В отдельном терминале:

```bash
docker compose exec app python scripts/run_bronze.py
docker compose exec app python scripts/run_silver.py
docker compose exec app python scripts/run_gold.py
docker compose exec app python scripts/run_ml.py
```

Переменные:

- `DELTA_LAKE_PATH` — корень Delta (по умолчанию `/opt/spark/data/delta` в контейнере)
- `FLIGHT_DATA_CSV` — путь к CSV **внутри контейнера** (в compose по умолчанию `/opt/spark/data/flight_data_2018_2024.csv`; см. [раздел про данные](#data-files))

У контейнера `app` смонтирован каталог `./mlflow_data` в `/mlflow` — тот же путь, что у MLflow server как `--default-artifact-root`, чтобы клиент мог записывать артефакты моделей без ошибки прав.

Если CSV недоступен, Bronze строит **один** синтетический батч (демо).

## MLflow

UI на машине: `http://localhost:5050` (порт задаётся `MLFLOW_HOST_PORT`, по умолчанию **5050**, т.к. **5000** часто занят, в т.ч. на macOS).

Логируются: параметры, RMSE/R²/MAE, accuracy/F1/AUC, важность признаков (индексы вектора `features`), версия Gold (`deltaTable.history`), артефакты Spark ML.

## Polars lazy и пушдауны

Запрос строится в `SilverTransformer.demonstrate_polars_query()` после записи Silver: `pl.scan_delta(silver_path)` → фильтр по `YEAR` / `ARR_DELAY` → `group_by` → `agg` → `sort`.

Типичный вывод `LazyFrame.explain()` (фрагмент; в вашем окружении имена узлов могут слегка отличаться):

```text
1. SELECT [col("ARR_DELAY"), col("AIRLINE"), col("season"), col("YEAR")]
2. FILTER [(col("ARR_DELAY")) > (0)]
   FROM
     FILTER [(col("YEAR")) == (2024)]
     FROM
       SCAN [projected 4 columns]
```

В блоке оптимизации Polars обычно указывается **predicate pushdown** и **projection pushdown** к источнику (Delta/Parquet), что уменьшает читаемые колонки и строки до декодирования.

**Партиции Silver `YEAR`, `MONTH`**: типичные срезы по времени отсекают целые каталоги; вместе с Z-ORDER по перевозчику и аэропорту ускоряются аналитика и группировки в Gold.

## Структура проекта

- `src/` — Bronze, Silver, Gold, ML, утилиты Spark
- `scripts/` — точки входа пайплайна и вспомогательные скрипты
- `notebooks/` — для экспериментов
- `logs/` — смонтирован в контейнер `app` как `/opt/spark/logs` (логи приложения, если пишете их вручную в этот путь)
- `mlflow_data/` — SQLite backend и артефакты MLflow (общий том с `app` и сервисом `mlflow`)
- `data/` — сюда кладёте исходный CSV (`flight_data_2018_2024.csv` по умолчанию); после прогона появляется `data/delta/`
- `REPORT.md` — отчёт по проекту (метрики, архитектура)

## Мониторинг

- Spark UI: `http://localhost:8080`
- MLflow: `http://localhost:5050` (или свой `MLFLOW_HOST_PORT`)