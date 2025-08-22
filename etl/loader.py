"""
loader.py

Скрипт для создания индекса Elasticsearch и загрузки данных из bulk-файла.

Функции:
- wait_for_es: проверяет доступность Elasticsearch с повторными попытками.
- create_index: создаёт индекс с заданным mapping.
- load_bulk: загружает документы из bulk-файла в Elasticsearch.
"""

import json
import time
from elasticsearch import Elasticsearch, helpers

# --- Конфигурация ---
ES_HOST = "http://fastapi_elastic:9200"  # Хост Elasticsearch
INDEX_NAME = "movies"                     # Имя индекса
BULK_FILE = "data/movies_data_v2.json"   # Путь к bulk-файлу

# --- Подключение к Elasticsearch ---
es = Elasticsearch(ES_HOST)


def wait_for_es(es: Elasticsearch, retries: int = 10, delay: int = 5):
    """
    Ждём пока Elasticsearch будет готов к работе.

    Args:
        es: объект Elasticsearch
        retries: количество попыток
        delay: задержка между попытками в секундах

    Raises:
        RuntimeError: если ES не доступен после всех попыток
    """
    for i in range(retries):
        if es.ping():
            print("✅ Elasticsearch is up!")
            return True
        print(f"⏳ Elasticsearch not ready, retry {i+1}/{retries}...")
        time.sleep(delay)
    raise RuntimeError("Elasticsearch is not available after waiting")


def create_index():
    """
    Создаёт индекс в Elasticsearch с маппингом из файла movies_mapping_v2.json.
    Если индекс уже существует, создаётся ничего не происходит.
    """
    wait_for_es(es)

    # Загружаем mapping
    with open("data/movies_mapping_v2.json", "r", encoding="utf-8") as f:
        mapping = json.load(f)

    if es.indices.exists(index=INDEX_NAME):
        print(f"Index '{INDEX_NAME}' already exists")
        return

    resp = es.indices.create(index=INDEX_NAME, body=mapping)
    print(f"Index '{INDEX_NAME}' created: {resp}")


def load_bulk(file_path: str):
    """
    Загружает документы в Elasticsearch из bulk-файла.

    Args:
        file_path: путь к файлу с bulk-данными
    """
    actions = []
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
        for i in range(0, len(lines), 2):
            action_line = json.loads(lines[i].strip())
            doc_line = json.loads(lines[i+1].strip())
            action = {
                "_op_type": "index",
                "_index": INDEX_NAME,
                "_id": action_line["index"]["_id"],
                "_source": doc_line
            }
            actions.append(action)

            # Отправляем пачками по 500 объектов
            if len(actions) >= 500:
                helpers.bulk(es, actions)
                actions = []

    if actions:
        helpers.bulk(es, actions)


if __name__ == "__main__":
    create_index()
    print('Index created')
    load_bulk(BULK_FILE)
    print("Bulk load finished!")
