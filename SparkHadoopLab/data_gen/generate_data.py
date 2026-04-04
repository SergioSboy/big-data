import os
import random

import numpy as np
import pandas as pd


def generate_dataset(num_rows=150000, filename=None):
    output_dir = os.environ.get("OUTPUT_DIR", ".")
    if filename is None:
        filename = os.environ.get("OUTPUT_FILENAME", "dataset.csv")
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, filename)

    print(f"Генерация {num_rows} строк в {path}...")

    np.random.seed(42)

    # 6 признаков: 1 id (int), 2 float, 1 categorical (str), 1 date (str), 1 int
    departments = ['Engineering', 'Marketing', 'Sales', 'HR', 'Finance', 'Support']

    data = {
        'emp_id': range(1, num_rows + 1),
        'age': np.random.randint(22, 60, size=num_rows),
        'salary': np.round(np.random.normal(75000, 15000, size=num_rows), 2),
        'department': [random.choice(departments) for _ in range(num_rows)],
        'join_date': pd.date_range(start='2015-01-01', end='2023-12-31', periods=num_rows).strftime('%Y-%m-%d'),
        'performance_score': np.round(np.random.uniform(1.0, 5.0, size=num_rows), 2)
    }

    df = pd.DataFrame(data)
    df.to_csv(path, index=False)
    print(f"Готово! Файл {path} сохранен. Размер: {os.path.getsize(path) / (1024 * 1024):.2f} MB")


if __name__ == "__main__":
    generate_dataset()