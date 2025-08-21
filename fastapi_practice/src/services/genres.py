from typing import List, Optional
from uuid import UUID

from elasticsearch import AsyncElasticsearch
from fastapi import Request
from redis.asyncio import Redis
import json

from models.genre import Genre

REDIS_URL = "redis://redis:6379"
CACHE_TTL = 300  # 5 минут


class GenreService:
    """
    Сервис для работы с жанрами с кэшированием в Redis.

    Логика работы:
    1. Попытка получить данные из Redis.
    2. Если данных нет, делаем scroll по Elasticsearch для получения всех жанров.
    3. Сохраняем результат в Redis для последующего быстрого доступа.
    """

    def __init__(self, elastic: AsyncElasticsearch, redis: Redis, cache_ttl: int = CACHE_TTL):
        """
        Инициализация сервиса.

        :param elastic: экземпляр AsyncElasticsearch для работы с Elasticsearch
        :param redis: экземпляр Redis для кэширования
        :param cache_ttl: время жизни кэша в секундах
        """
        self.elastic = elastic
        self.redis = redis
        self.cache_ttl = cache_ttl

    async def list_genres(self) -> List[Genre]:
        """
        Получение всех жанров с кэшированием.

        :return: список объектов Genre
        :notes:
            - Сначала пробует достать из Redis.
            - Если нет, скроллит все документы в Elasticsearch и собирает уникальные жанры.
            - Сохраняет результат в Redis с TTL.
        """
        genres_raw = await self.redis.get("genres_cache")
        if genres_raw:
            genres_dict = json.loads(genres_raw)
            return [Genre(uuid=uid, name=name) for uid, name in genres_dict.items()]

        # fallback через scroll
        genres_dict = {}
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
                for g in doc["_source"].get("genres", []):
                    genres_dict[g["uuid"]] = g["name"]

            response = await self.elastic.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = response["_scroll_id"]
            hits = response["hits"]["hits"]

        # Сохраняем в Redis
        await self.redis.set("genres_cache", json.dumps(genres_dict), ex=self.cache_ttl)
        return [Genre(uuid=uid, name=name) for uid, name in genres_dict.items()]


    async def search_genres(self, query_str: str) -> List[Genre]:
        """
        Поиск жанров по названию с кэшированием.

        :param query_str: строка поиска
        :return: список уникальных Genre, совпадающих с query_str
        :notes:
            - Сначала ищет в Redis по ключу search_genres:{query_str}.
            - Если нет, делает nested search в Elasticsearch.
            - Формирует уникальные объекты Genre, сериализует UUID в str и сохраняет в Redis.
        """
        cache_key = f"search_genres:{query_str}"

        # Попытка достать из кэша
        cached = await self.redis.get(cache_key)
        if cached:
            genres_dicts = json.loads(cached)
            return [Genre(uuid=UUID(g["uuid"]), name=g["name"]) for g in genres_dicts]

        # Elasticsearch запрос
        query = {
            "_source": ["genres"],
            "query": {
                "nested": {
                    "path": "genres",
                    "query": {
                        "match": {
                            "genres.name": {
                                "query": query_str,
                                "operator": "and"
                            }
                        }
                    },
                    "inner_hits": {}  # опционально, если нужны только совпавшие вложенные объекты
                }
            }
        }

        resp = await self.elastic.search(index="movies", body=query)

        # Формируем уникальные жанры
        unique = {}
        for doc in resp["hits"]["hits"]:
            for g in doc["_source"].get("genres", []):
                name = g.get("name")
                uuid_str = g.get("uuid")
                if name and uuid_str and query_str.lower() in name.lower():
                    unique[uuid_str] = Genre(uuid=UUID(uuid_str), name=name)

        result = list(unique.values())

        # Сохраняем в кэш
        await self.redis.set(
            cache_key,
            json.dumps([{"uuid": str(g.uuid), "name": g.name} for g in result]),
            ex=self.cache_ttl,
        )

        return result

    async def get_genre_by_id(self, genre_id: str) -> Optional[Genre]:
        """
        Получение одного жанра по UUID с кэшированием.

        :param genre_id: UUID жанра
        :return: объект Genre или None, если не найден
        :notes:
            - Сначала ищет в кэше Redis.
            - Если нет, делает запрос в Elasticsearch с term-фильтром по UUID.
        """
        genres_raw = await self.redis.get("genres_cache")
        if genres_raw:
            genres_dict = json.loads(genres_raw)
            if genre_id in genres_dict:
                return Genre(uuid=genre_id, name=genres_dict[genre_id])

        body = {"query": {"term": {"genres.uuid.keyword": genre_id}}, "size": 1}
        result = await self.elastic.search(index="movies", body=body)
        hits = result["hits"]["hits"]
        if not hits:
            return None
        for g in hits[0]["_source"].get("genres", []):
            if g["uuid"] == genre_id:
                return Genre(**g)
        return None

async def get_genre_service(request: Request) -> GenreService:
    """
    Dependency для FastAPI через lifespan.

    Берем redis и elastic из app.state.
    Используется в endpoint'ах FastAPI для внедрения GenreService.

    :param request: объект FastAPI Request
    :return: экземпляр GenreService
    """
    return GenreService(
        elastic=request.app.state.elastic,
        redis=request.app.state.redis
    )
