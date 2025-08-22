"""
run_etl.py

Скрипт для запуска ETL-процесса:
1. Трансформация старых данных в новый формат.
2. Загрузка преобразованных данных в Elasticsearch.
"""

import subprocess


def main():
    """Запускает ETL-процесс последовательно."""

    # --- Трансформация данных ---
    print("🔄 Start transformation...")
    subprocess.run(
        ["python", "etl/transform_old_to_new_data.py"],
        check=True
    )
    print("✅ Transformation finished.")

    # --- Загрузка данных в Elasticsearch ---
    print("📤 Start loading to Elasticsearch...")
    subprocess.run(
        ["python", "etl/loader.py"],
        check=True
    )
    print("✅ Loading finished.")


if __name__ == "__main__":
    main()
