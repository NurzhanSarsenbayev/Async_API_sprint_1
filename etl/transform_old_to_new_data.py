import json
import uuid

# Вспомогательные словари для хранения уникальных UUID
genre_uuid_map = {}
person_uuid_map = {}

def get_genre_uuid(name):
    if name not in genre_uuid_map:
        genre_uuid_map[name] = str(uuid.uuid4())
    return genre_uuid_map[name]

def get_person_uuid(name):
    if name not in person_uuid_map:
        person_uuid_map[name] = str(uuid.uuid4())
    return person_uuid_map[name]

def transform_person_list(person_list):
    if not person_list:
        return []
    return [{"uuid": get_person_uuid(p), "full_name": p} for p in person_list]

def transform_movie(old_movie):
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

# Открываем старый JSON Lines файл
with open("bulk_movies.json", "r") as f_in, open("bulk_movies.json", "w") as f_out:
    for line in f_in:
        if not line.strip():
            continue
        old_movie = json.loads(line)["_source"]  # если в старом JSON есть "_source"
        new_movie = transform_movie(old_movie)
        action = {"index": {"_id": new_movie["uuid"]}}
        f_out.write(json.dumps(action) + "\n")
        f_out.write(json.dumps(new_movie) + "\n")
