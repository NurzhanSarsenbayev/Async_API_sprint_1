from functools import lru_cache
from typing import Optional, List

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.film import Film, FilmShort

# Время жизни кеша фильма в секундах
FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class FilmService:
    """
    Сервис для работы с фильмами.
    Обеспечивает извлечение данных из Elasticsearch и кеширование в Redis.
    """

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, film_id: str) -> Optional[Film]:
        """
        Получение фильма по ID.
        Сначала пытаемся достать из Redis, если нет — из Elasticsearch.
        """
        film = await self._film_from_cache(film_id)
        if film:
            return film

        film = await self._get_film_from_elastic(film_id)
        if not film:
            return None

        await self._put_film_to_cache(film)
        return film

    async def list_films(
        self,
        size: int = 50,
        sort: str = '-imdb_rating'
    ) -> List[FilmShort]:
        """
        Список фильмов с сортировкой по рейтингу.
        Возвращает сокращённую модель FilmShort.
        """
        sort_field = sort.lstrip('-')
        sort_order = 'desc' if sort.startswith('-') else 'asc'

        query = {
            "sort": [{sort_field: {"order": sort_order}}],
            "size": size,
            "_source": ["id", "title", "imdb_rating"]
        }

        resp = await self.elastic.search(index='movies', body=query)
        hits = resp['hits']['hits']

        result = []
        for doc in hits:
            source = doc['_source']
            # создаем объект FilmShort, сразу конвертируем id в строку
            result.append(FilmShort(
                id=str(source['id']),
                title=source['title'],
                imdb_rating=source.get('imdb_rating')
            ))
        return result

    async def _get_film_from_elastic(self, film_id: str) -> Optional[Film]:
        """
        Получение фильма из Elasticsearch.
        Конвертируем списки dict -> списки строк (имена актёров, режиссёров и т.д.)
        """
        try:
            doc = await self.elastic.get(index='movies', id=film_id)
        except NotFoundError:
            return None

        source = doc['_source']

        # Конвертируем списки словарей в списки строк
        def extract_names(items):
            if not items:
                return []
            return [item['name'] for item in items]

        return Film(
            id=str(source['id']),
            title=source.get('title'),
            description=source.get('description'),
            imdb_rating=source.get('imdb_rating'),
            genre=source.get('genre') or [],
            actors=extract_names(source.get('actors')),
            writers=extract_names(source.get('writers')),
            directors=extract_names(source.get('directors')),
        )

    async def _film_from_cache(self, film_id: str) -> Optional[Film]:
        """
        Получение фильма из Redis.
        """
        data = await self.redis.get(str(film_id))
        if not data:
            return None
        return Film.parse_raw(data)

    async def _put_film_to_cache(self, film: Film):
        """
        Сохраняем фильм в Redis с временем жизни.
        """
        await self.redis.set(
            str(film.id),
            film.json(),
            ex=FILM_CACHE_EXPIRE_IN_SECONDS
        )


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    """
    Dependency для FastAPI.
    Позволяет внедрять FilmService через Depends().
    """
    return FilmService(redis, elastic)