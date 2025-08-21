from fastapi import APIRouter, Depends, Query, HTTPException
from typing import List
from uuid import UUID

from services.films import FilmService, get_film_service
from models.film import FilmShort, Film

router = APIRouter()


@router.get("/", response_model=List[FilmShort])
async def list_films(
    size: int = Query(10, ge=1, description="Количество фильмов на странице"),
    film_service: FilmService = Depends(get_film_service),
):
    """
    Получение списка фильмов с пагинацией.

    Args:
        size (int, optional): количество фильмов на странице. По умолчанию 10.
        film_service (FilmService): сервис для работы с фильмами
        (инжектируется через Depends).

    Returns:
        List[FilmShort]: список фильмов с минимальными данными
        (UUID, название, рейтинг IMDb).

    Примечание:
        Данные кэшируются в Redis для ускорения повторных запросов.
    """
    films = await film_service.list_films(size=size)
    return films


@router.get("/search", response_model=List[FilmShort])
async def search_films(
    query: str = Query(...,
                       description="Строка для полнотекстового поиска по названию фильма"),
    size: int = Query(10, ge=1, description="Количество результатов поиска"),
    film_service: FilmService = Depends(get_film_service),
):
    """
    Поиск фильмов по названию или описанию.

    Args:
        query (str): поисковый запрос.
        size (int, optional): максимальное количество возвращаемых фильмов.
        По умолчанию 10.
        film_service (FilmService): сервис для работы с фильмами.

    Returns:
        List[FilmShort]: список фильмов, соответствующих запросу.

    Примечание:
        Результаты поиска кэшируются в Redis для ускорения повторных запросов.
    """
    return await film_service.search_films(query_str=query, size=size)


@router.get("/{film_id}", response_model=Film)
async def get_film_details(
    film_id: UUID,
    film_service: FilmService = Depends(get_film_service),
):
    """
    Получение полной информации о фильме по его UUID.

    Args:
        film_id (UUID): UUID фильма.
        film_service (FilmService): сервис для работы с фильмами.

    Returns:
        Film: объект с полной информацией о фильме, включая жанры,
        актеров, режиссеров и сценаристов.

    Raises:
        HTTPException: статус 404, если фильм не найден.

    Примечание:
        Данные фильма кэшируются в Redis для ускорения повторных запросов.
    """
    film = await film_service.get_film_by_id(film_id)
    if not film:
        raise HTTPException(status_code=404, detail="Film not found")
    return film
