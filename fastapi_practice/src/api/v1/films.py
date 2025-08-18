from fastapi import APIRouter, Depends, HTTPException
from http import HTTPStatus
from typing import List
from uuid import UUID

from models.film import Film, FilmShort
from services.films import FilmService, get_film_service

router = APIRouter()


@router.get(
    '/',
    response_model=List[FilmShort],
    summary="Список фильмов",
    description="Возвращает список фильмов с краткой информацией (id, title, imdb_rating)"
)
async def films_list(
    page_size: int = 50,
    sort: str = "-imdb_rating",
    film_service: FilmService = Depends(get_film_service)
):
    """
    Эндпоинт для получения списка фильмов.
    Сортировка по умолчанию: -imdb_rating (по убыванию рейтинга).
    """
    return await film_service.list_films(size=page_size, sort=sort)


@router.get(
    '/{film_id}',
    response_model=Film,
    summary="Детали фильма",
    description="Возвращает полную информацию о фильме по UUID"
)
async def film_details(
    film_id: UUID,
    film_service: FilmService = Depends(get_film_service)
):
    """
    Эндпоинт для получения полного описания фильма по его UUID.
    Если фильм не найден — возвращает 404.
    """
    # Преобразуем UUID в строку для поиска в Elasticsearch
    film = await film_service.get_by_id(str(film_id))
    if not film:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='film not found'
        )
    return film
