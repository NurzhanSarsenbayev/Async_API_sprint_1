import json
from elasticsearch import Elasticsearch, helpers

ES_HOST = "http://localhost:9200"
INDEX_NAME = "movies"
BULK_FILE = "bulk_movies.json"  # путь к твоему файлу в bulk формате

# подключение к Elasticsearch
es = Elasticsearch(ES_HOST)

def load_bulk(file_path: str):
    actions = []
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
        for i in range(0, len(lines), 2):  # каждая пара строк: action + document
            action_line = json.loads(lines[i].strip())
            doc_line = json.loads(lines[i+1].strip())
            action = {
                "_op_type": "index",
                "_index": INDEX_NAME,
                "_id": action_line["index"]["_id"],
                "_source": doc_line
            }
            actions.append(action)

            # отправляем пачками по 500 объектов, чтобы не перегружать память
            if len(actions) >= 500:
                helpers.bulk(es, actions)
                actions = []

    # загружаем оставшиеся объекты
    if actions:
        helpers.bulk(es, actions)

if __name__ == "__main__":
    load_bulk(BULK_FILE)
    print("Bulk load finished!")