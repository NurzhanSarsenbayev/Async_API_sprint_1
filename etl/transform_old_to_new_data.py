"""
transform_old_to_new_data.py

Скрипт для преобразования старого формата данных фильмов в новый формат,
который будет использоваться для загрузки в Elasticsearch.

Функции:
- Генерация уникальных UUID для жанров и персон.
- Трансформация списков актеров, режиссеров и сценаристов.
- Сохранение преобразованных фильмов в новый JSON Lines файл
  (каждая пара строк: действие `index` + документ).
"""

import json
import uuid

# Вспомогательные словари для хранения уникальных UUID
genre_uuid_map = {}
person_uuid_map = {}


def get_genre_uuid(name: str) -> str:

    """Возвращает UUID для жанра, создавая новый, если нужно."""
    if name not in genre_uuid_map:
        genre_uuid_map[name] = str(uuid.uuid4())
    return genre_uuid_map[name]


def get_person_uuid(name: str) -> str:

    """Возвращает UUID для персоны, создавая новый, если нужно."""
    if name not in person_uuid_map:
        person_uuid_map[name] = str(uuid.uuid4())
    return person_uuid_map[name]


def transform_person_list(person_list: list) -> list[dict]:

    """Преобразует список имён персон в список словарей с UUID и полным именем."""
    if not person_list:
        return []
    return [{"uuid": get_person_uuid(p), "full_name": p} for p in person_list]


def transform_movie(old_movie: dict) -> dict:

    """Преобразует старую структуру фильма в новую с UUID для фильма, жанров и персон."""
    return {
        "uuid": str(uuid.uuid4()),
        "title": old_movie.get("title"),
        "imdb_rating": old_movie.get("imdb_rating"),
        "description": old_movie.get("description"),
        "genres": [{"uuid": get_genre_uuid(g), "name": g} for g in old_movie.get("genres", [])],
        "actors": transform_person_list(old_movie.get("actors_names", [])),
        "directors": transform_person_list(old_movie.get("directors_names", [])),
        "writers": transform_person_list(old_movie.get("writers_names", [])),
    }


def main():
    """Чтение старого файла и запись нового с подготовкой для bulk загрузки в Elasticsearch."""
    with open("data/movies_data.json", "r", encoding="utf-8") as f_in, \
         open("data/movies_data_v2.json", "w", encoding="utf-8") as f_out:

        for line in f_in:
            if not line.strip():
                continue
            old_movie = json.loads(line)["_source"]  # если в старом JSON есть "_source"
            new_movie = transform_movie(old_movie)

            # Строка с действием для bulk API
            action = {"index": {"_id": new_movie["uuid"]}}
            f_out.write(json.dumps(action) + "\n")
            f_out.write(json.dumps(new_movie) + "\n")


if __name__ == "__main__":
    main()
