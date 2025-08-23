import json
from typing import List, Optional
from uuid import UUID
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Request
from redis.asyncio import Redis

from models.film import Film
from models.film_short import FilmShort
from models.person import Person
from models.genre import Genre


FILM_CACHE_EXPIRE_IN_SECONDS = 300  # 5 минут TTL для всех кэшей


class FilmService:
    """
    Сервис для работы с фильмами с кэшированием в Redis.

    Основная логика работы:
    1. Проверка наличия данных в Redis по ключу.
    2. Если данные есть, возвращаем из кэша.
    3. Если данных нет, делаем запрос к Elasticsearch, формируем объект модели,
       сохраняем результат в Redis и возвращаем его.
    4. TTL кэша: 5 минут по умолчанию.
    """

    def __init__(self, redis: Redis,
                 elastic: AsyncElasticsearch,
                 cache_ttl: int = FILM_CACHE_EXPIRE_IN_SECONDS):
        """
        Инициализация сервиса.

        :param redis: экземпляр Redis для кэширования
        :param elastic: экземпляр AsyncElasticsearch для запросов к ES
        :param cache_ttl: время жизни кэша в секундах
        """
        self.redis = redis
        self.elastic = elastic
        self.cache_ttl = cache_ttl

    async def list_films(
        self,
        size: int = 50,
        page: int = 1,
        sort: str = "-imdb_rating",
    ) -> List[FilmShort]:
        """
        Получение списка фильмов с сортировкой с кэшированием.

        :param size: количество фильмов в ответе
        :param page: номер страницы, начиная с 1
        :param sort: поле сортировки, например "-imdb_rating"
        :return: список объектов FilmShort
        :notes:
            - Сначала ищет данные в Redis по уникальному ключу.
            - Если нет, делает запрос в Elasticsearch, формирует FilmShort.
            - Сохраняет результат в Redis и возвращает список.
        """
        cache_key = f"list_films:page={page}:size={size}:sort={sort}"
        cached = await self.redis.get(cache_key)
        if cached:
            films_dicts = json.loads(cached)
            return [FilmShort(**f) for f in films_dicts]

        sort_field = sort.lstrip("-")
        sort_order = "desc" if sort.startswith("-") else "asc"
        from_ = (page - 1) * size  # смещение для пагинации

        query = {
            "from": from_,
            "size": size,
            "sort": [{sort_field: {"order": sort_order}}],
            "_source": ["uuid", "title", "imdb_rating"]
        }

        resp = await self.elastic.search(index="movies", body=query)
        films = [
            FilmShort(
                uuid=UUID(doc["_source"]["uuid"]),
                title=doc["_source"]["title"],
                imdb_rating=doc["_source"].get("imdb_rating")
            ) for doc in resp["hits"]["hits"]
        ]

        await self.redis.set(
            cache_key,
            json.dumps([{"uuid": str(f.uuid),
                         "title": f.title,
                         "imdb_rating": f.imdb_rating} for f in films]),
            ex=self.cache_ttl,
        )
        return films

    async def search_films(self,
                           query_str: str,
                           size: int = 50) -> List[FilmShort]:
        """
        Полнотекстовый поиск фильмов с кэшированием (возвращает FilmShort).

        :param query_str: поисковая строка
        :param size: количество результатов
        :return: список FilmShort
        :notes:
            - Сначала ищет кэш в Redis.
            - Если нет, выполняет multi_match поиск в
              Elasticsearch по полям title и description.
            - Формирует список FilmShort, сохраняет
              в Redis и возвращает.
        """
        cache_key = f"search_films:{query_str}:{size}"
        cached = await self.redis.get(cache_key)
        if cached:
            films_dicts = json.loads(cached)
            return [FilmShort(**f) for f in films_dicts]

        query = {
            "size": size,
            "_source": ["uuid", "title", "imdb_rating"],
            "query": {
                "multi_match": {
                    "query": query_str,
                    "fields": ["title^2", "description"]
                }
            }
        }
        resp = await self.elastic.search(index="movies", body=query)

        films = [FilmShort(**doc["_source"]) for doc in resp["hits"]["hits"]]

        await self.redis.set(
            cache_key,
            json.dumps([f.dict() for f in films], default=str),
            ex=self.cache_ttl
        )
        return films

    async def get_film_by_id(self, film_uuid: UUID) -> Optional[Film]:
        """
        Получение полного фильма по UUID с кэшированием.

        :param film_uuid: UUID фильма
        :return: объект Film или None, если фильм не найден
        :notes:
            - Сначала ищет фильм в Redis.
            - Если нет, делает запрос в Elasticsearch.
            - Формирует Film с жанрами и персоналиями.
            - Сохраняет результат в Redis и возвращает объект.
        """
        cache_key = f"film:{film_uuid}"
        cached = await self.redis.get(cache_key)
        if cached:
            return Film.parse_raw(cached)

        try:
            doc = await self.elastic.get(index="movies", id=str(film_uuid))
        except NotFoundError:
            return None

        src = doc["_source"]
        film = Film(
            uuid=UUID(src["uuid"]),
            title=src.get("title"),
            description=src.get("description"),
            imdb_rating=src.get("imdb_rating"),
            genres=[Genre(uuid=UUID(g["uuid"]),
                          name=g["name"]) for g in src.get("genres", [])],
            actors=[Person(uuid=UUID(a["uuid"]),
                           full_name=a["full_name"],
                           role="actor") for a in src.get("actors", [])],
            writers=[Person(uuid=UUID(w["uuid"]),
                            full_name=w["full_name"],
                            role="writer") for w in src.get("writers", [])],
            directors=[Person(uuid=UUID(d["uuid"]),
                              full_name=d["full_name"],
                              role="director")
                       for d in src.get("directors", [])],
        )

        await self.redis.set(cache_key, film.json(), ex=self.cache_ttl)
        return film


async def get_film_service(request: Request) -> FilmService:
    """
    Dependency для FastAPI через lifespan.

    Берем redis и elastic из app.state.
    Используется в endpoint'ах FastAPI для внедрения FilmService.

    :param request: объект FastAPI Request
    :return: экземпляр FilmService
    """
    return FilmService(
        elastic=request.app.state.elastic,
        redis=request.app.state.redis
    )
