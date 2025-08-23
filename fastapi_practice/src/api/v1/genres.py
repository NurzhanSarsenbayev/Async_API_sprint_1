from fastapi import APIRouter, Depends, HTTPException, Query
from http import HTTPStatus
from typing import List

from models.genre import Genre
from services.genres import GenreService, get_genre_service

router = APIRouter()


@router.get("/", response_model=List[Genre])
async def genres_list(
    page: int = Query(1, ge=1, description="Номер страницы"),
    size: int = Query(10, ge=1, description="Количество жанров на странице"),
    genre_service: GenreService = Depends(get_genre_service),
):
    """
    Получение списка жанров с пагинацией.

    Args:
        page (int, optional): номер страницы, начиная с 1. По умолчанию 1.
        size (int, optional): количество жанров на странице. По умолчанию 50.
        genre_service (GenreService): сервис для работы с жанрами
            (инжектируется через Depends).

    Returns:
        List[Genre]: список жанров на текущей странице.

    Примечание:
        Данные кэшируются в Redis для ускорения повторных запросов.
    """
    return await genre_service.list_genres(size=size, page=page)


@router.get("/search", response_model=List[Genre])
async def search_genres(
    query: str = Query(..., description="Поисковый запрос для жанров"),
    genre_service: GenreService = Depends(get_genre_service),
):
    """
    Поиск жанров по названию.

    Args:
        query (str): строка для поиска в названиях жанров.
        genre_service (GenreService): сервис для работы с жанрами.

    Returns:
        List[Genre]: список жанров, название которых соответствует запросу.

    Примечание:
        Результаты поиска кэшируются в Redis для ускорения повторных запросов.
    """
    genres = await genre_service.search_genres(query_str=query)
    return genres


@router.get("/{genre_id}", response_model=Genre)
async def genre_details(
    genre_id: str,
    genre_service: GenreService = Depends(get_genre_service)
):
    """
    Получение информации о жанре по его UUID.

    Args:
        genre_id (str): UUID жанра.
        genre_service (GenreService): сервис для работы с жанрами.

    Returns:
        Genre: объект жанра.

    Raises:
        HTTPException: статус 404, если жанр не найден.

    Примечание:
        Данные жанра кэшируются в Redis для ускорения повторных запросов.
    """
    genre = await genre_service.get_genre_by_id(genre_id)
    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail="genre not found")
    return genre
