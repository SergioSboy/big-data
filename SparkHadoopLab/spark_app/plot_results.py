"""
Строит сравнительные графики по metrics.jsonl (4 эксперимента).
Запуск из каталога spark_app:
  python plot_results.py
  python plot_results.py --metrics results/metrics.jsonl --out results/charts
"""

import argparse
import json
import os
from collections import OrderedDict

# matplotlib подключаем лениво — чтобы `pip install` был только для отчёта
def _load_metrics(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _pick_latest_per_experiment(rows):
    """Для каждой метки experiment оставляем последнюю запись (последний прогон)."""
    by_exp = OrderedDict()
    for r in rows:
        key = r.get("experiment") or ("opt" if r.get("optimize") else "base")
        by_exp[key] = r
    return by_exp


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--metrics",
        default=os.path.join(os.path.dirname(__file__), "results", "metrics.jsonl"),
        help="Путь к metrics.jsonl",
    )
    parser.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "results", "charts"),
        help="Каталог для PNG",
    )
    parser.add_argument(
        "--order",
        default="1dn_spark,1dn_spark_opt,3dn_spark,3dn_spark_opt",
        help="Порядок столбиков на графике (через запятую), имена experiment из app.py",
    )
    args = parser.parse_args()

    try:
        import matplotlib.pyplot as plt
    except ImportError as e:
        raise SystemExit(
            "Нужен matplotlib: pip install matplotlib\n" + str(e)
        ) from e

    if not os.path.isfile(args.metrics):
        raise SystemExit(f"Файл метрик не найден: {args.metrics}")

    rows = _load_metrics(args.metrics)
    if not rows:
        raise SystemExit("metrics.jsonl пуст — сначала запустите app.py с --experiment для 4 прогонов.")

    latest = _pick_latest_per_experiment(rows)
    order = [x.strip() for x in args.order.split(",") if x.strip()]

    labels = []
    times = []
    rams = []
    for name in order:
        if name not in latest:
            continue
        r = latest[name]
        labels.append(name.replace("_", "\n"))
        times.append(float(r["duration_sec"]))
        rams.append(float(r["driver_ram_delta_mb"]))

    if len(labels) < 2:
        raise SystemExit(
            "Мало данных для сравнения. Ожидаются метки из --order; "
            f"сейчас в файле: {list(latest.keys())}"
        )

    os.makedirs(args.out, exist_ok=True)

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))

    x = range(len(labels))
    axes[0].bar(x, times, color=["#4C72B0", "#55A868", "#C44E52", "#8172B2"][: len(labels)])
    axes[0].set_xticks(list(x))
    axes[0].set_xticklabels(labels, fontsize=9)
    axes[0].set_ylabel("Секунды")
    axes[0].set_title("Время выполнения (меньше — лучше)")
    axes[0].grid(axis="y", linestyle="--", alpha=0.35)

    axes[1].bar(x, rams, color=["#4C72B0", "#55A868", "#C44E52", "#8172B2"][: len(labels)])
    axes[1].set_xticks(list(x))
    axes[1].set_xticklabels(labels, fontsize=9)
    axes[1].set_ylabel("МБ (прирост RSS драйвера)")
    axes[1].set_title("Память драйвера: Δ от старта до конца")
    axes[1].grid(axis="y", linestyle="--", alpha=0.35)

    fig.suptitle("Сравнение экспериментов Spark × Hadoop (метрики из metrics.jsonl)")
    fig.tight_layout()
    out_png = os.path.join(args.out, "comparison.png")
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    print(f"Сохранено: {out_png}")


if __name__ == "__main__":
    main()
