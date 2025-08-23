from typing import List, Optional
from uuid import UUID

from elasticsearch import AsyncElasticsearch
from fastapi import Request
from redis.asyncio import Redis
import json

from models.person import Person
from models.film_short import FilmShort


REDIS_URL = "redis://redis:6379"
CACHE_TTL = 300


class PersonService:
    """
    Сервис для работы с персонами
    (актеры, режиссеры, сценаристы) с полным кэшированием в Redis.

    Атрибуты:
        elastic (AsyncElasticsearch): клиент Elasticsearch.
        redis (Redis): клиент Redis для кэширования.
        cache_ttl (int): время жизни кэша в секундах.
    """

    def __init__(self,
                 elastic: AsyncElasticsearch,
                 redis: Redis,
                 cache_ttl: int = CACHE_TTL):
        """
        Инициализация сервиса.

        Args:
            elastic (AsyncElasticsearch): клиент Elasticsearch.
            redis (Redis): клиент Redis.
            cache_ttl (int, optional): TTL кэша в секундах. По умолчанию 300.
        """
        self.elastic = elastic
        self.redis = redis
        self.cache_ttl = cache_ttl

    async def list_persons(self,
                           size: int = 100,
                           page: int = 1) -> List[Person]:
        """
        Получение списка персон с поддержкой кэширования и пагинации.

        Сначала пробуем получить данные из Redis.
        Если кэша нет, выполняем scroll по Elasticsearch.
        После получения всех персон применяем пагинацию и кэшируем результат.

        Args:
            size (int, optional): количество персон на странице.
            По умолчанию 100.
            page (int, optional): номер страницы. По умолчанию 1.

        Returns:
            List[Person]: список персон на запрошенной странице.
        """
        cache_key = f"persons:page={page}:size={size}"
        cached = await self.redis.get(cache_key)
        if cached:
            persons_dict = json.loads(cached)
            return [Person(uuid=uid,
                           full_name=name)
                    for uid, name in persons_dict.items()]

        # fallback через scroll
        persons_dict = {}
        scroll_size = 100
        response = await self.elastic.search(
            index="movies",
            body={"query": {"match_all": {}}, "size": scroll_size},
            scroll="2m"
        )
        scroll_id = response["_scroll_id"]
        hits = response["hits"]["hits"]

        while hits:
            for doc in hits:
                for role in ["actors", "directors", "writers"]:
                    for p in doc["_source"].get(role, []):
                        persons_dict[p["uuid"]] = p["full_name"]
            response = await self.elastic.scroll(scroll_id=scroll_id,
                                                 scroll="2m")
            scroll_id = response["_scroll_id"]
            hits = response["hits"]["hits"]

        # пагинация
        start = (page - 1) * size
        end = start + size
        limited_persons = dict(list(persons_dict.items())[start:end])

        await self.redis.set(cache_key,
                             json.dumps(limited_persons),
                             ex=self.cache_ttl)
        return [Person(uuid=uid,
                       full_name=name) for uid, name in limited_persons.items()]

    async def get_person_by_id(self, person_id: str) -> Optional[Person]:
        """
        Получение одной персоны по UUID с кэшированием.
        Дополнительно возвращает список фильмов, где персона участвовала.
        """
        cache_key = f"person:{person_id}"
        cached = await self.redis.get(cache_key)
        if cached:
            return Person.parse_raw(cached)

        # ищем самого человека в ES
        body = {
            "query": {
                "bool": {
                    "should": [
                        {"nested": {"path": "actors", "query": {"term": {"actors.uuid": person_id}}}},
                        {"nested": {"path": "directors", "query": {"term": {"directors.uuid": person_id}}}},
                        {"nested": {"path": "writers", "query": {"term": {"writers.uuid": person_id}}}}
                    ]
                },
            },
            "size": 1
        }
        result = await self.elastic.search(index="movies", body=body)
        hits = result["hits"]["hits"]
        if not hits:
            return None

        # достаём инфо о человеке
        person = None
        for role in ["actors", "directors", "writers"]:
            for p in hits[0]["_source"].get(role, []):
                if p["uuid"] == person_id:
                    person = Person(uuid=UUID(p["uuid"]), full_name=p["full_name"], role=role[:-1])
                    break
            if person:
                break

        if not person:
            return None

        # ищем все фильмы с этим person_id
        films_query = {
            "_source": ["uuid", "title", "imdb_rating"],
            "query": {
                "bool": {
                    "should": [
                        {"nested": {"path": "actors", "query": {"term": {"actors.uuid": person_id}}}},
                        {"nested": {"path": "writers", "query": {"term": {"writers.uuid": person_id}}}},
                        {"nested": {"path": "directors", "query": {"term": {"directors.uuid": person_id}}}},
                    ]
                }
            },
            "size": 50
        }
        films_result = await self.elastic.search(index="movies", body=films_query)
        person.films = [
            FilmShort(uuid=UUID(f["_source"]["uuid"]),
                      title=f["_source"]["title"],
                      imdb_rating=f["_source"].get("imdb_rating"))
            for f in films_result["hits"]["hits"]
        ]

        # кэшируем
        await self.redis.set(cache_key, person.json(), ex=self.cache_ttl)
        return person

    async def search_persons(self,
                             query_str: str) -> list[Person]:
        """
        Поиск персон строго по полному имени
        (точное совпадение) с кэшированием.

        Сначала пробуем получить данные из Redis.
        Если кэша нет, выполняем поиск по Elasticsearch
        в полях actors, directors и writers.

        Args:
            query_str (str): полное имя персоны для поиска.

        Returns:
            list[Person]: список найденных персон без дубликатов.
        """
        cache_key = f"search_persons:{query_str}"
        cached = await self.redis.get(cache_key)
        if cached:
            persons_dicts = json.loads(cached)
            return [Person(uuid=UUID(p["uuid"]),
                           full_name=p["full_name"],
                           role=p["role"]) for p in persons_dicts]

        roles = ["actors", "directors", "writers"]
        hits = []

        for role in roles:
            query_exact = {
                "_source": [role],
                "query": {
                    "nested": {
                        "path": role,
                        "query": {"match_phrase": {f"{role}.full_name": query_str}}
                    }
                },
                "size": 50
            }
            resp = await self.elastic.search(index="movies", body=query_exact)
            hits.extend(resp["hits"]["hits"])

        # собираем уникальных персонажей
        seen_uuids = set()
        result: list[Person] = []
        for doc in hits:
            src = doc["_source"]
            for role in roles:
                for p in src.get(role, []):
                    if p["uuid"] not in seen_uuids and p["full_name"] == query_str:
                        seen_uuids.add(p["uuid"])
                        person = Person(uuid=UUID(p["uuid"]),
                                             full_name=p["full_name"],
                                             role=role[:-1])
                        # фильмы для конкретного человека
                        films_query = {
                            "_source": ["uuid", "title", "imdb_rating"],
                            "query": {
                                "bool": {
                                    "should": [
                                        {"nested": {"path": "actors", "query": {"term": {"actors.uuid": p["uuid"]}}}},
                                        {"nested": {"path": "writers", "query": {"term": {"writers.uuid": p["uuid"]}}}},
                                        {"nested": {"path": "directors",
                                                    "query": {"term": {"directors.uuid": p["uuid"]}}}},
                                        # если нужно .keyword:
                                        # {"nested": {"path": "actors", "query": {"term": {"actors.uuid.keyword": p["uuid"]}}}},
                                    ],
                                    "minimum_should_match": 1
                                }
                            },
                            "size": 50,
                            "track_total_hits": True
                        }
                        films_result = await self.elastic.search(index="movies", body=films_query)
                        person.films = [
                            FilmShort(uuid=UUID(f["_source"]["uuid"]),
                                      title=f["_source"]["title"],
                                      imdb_rating=f["_source"].get("imdb_rating"))
                            for f in films_result["hits"]["hits"]
                        ]
                        result.append(person)

        # кэшируем результат
        await self.redis.set(
            cache_key,
            json.dumps([{"uuid": str(p.uuid),
                         "full_name": p.full_name,
                         "role": p.role} for p in result]),
            ex=self.cache_ttl
        )

        return result


async def get_person_service(request: Request) -> PersonService:
    """
    Dependency для FastAPI через lifespan.
    Берем redis и elastic из app.state.

    Returns:
        PersonService: готовый сервис для использования в endpoint'ах.
    """
    return PersonService(
        elastic=request.app.state.elastic,
        redis=request.app.state.redis
    )
