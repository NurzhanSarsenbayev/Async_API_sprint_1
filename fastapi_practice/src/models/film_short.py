from pydantic import BaseModel
from uuid import UUID
from typing import Optional


class FilmShort(BaseModel):
    """
    Краткая информация о фильме для списков и выдачи API.

    Attributes:
        uuid (UUID): Уникальный идентификатор фильма.
        title (str): Название фильма.
        imdb_rating (Optional[float]): Рейтинг IMDb (может отсутствовать).
    """
    uuid: UUID
    title: str
    imdb_rating: Optional[float] = None