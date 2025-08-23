from pydantic import BaseModel
from uuid import UUID
from typing import List, Optional

from models.person import Person
from models.genre import Genre




class Film(BaseModel):
    """
    Полная информация о фильме, включая описание, рейтинг, жанры и участников.

    Attributes:
        uuid (UUID): Уникальный идентификатор фильма.
        title (str): Название фильма.
        description (Optional[str]): Краткое описание фильма.
        imdb_rating (Optional[float]): Рейтинг IMDb (может отсутствовать).
        genres (List[Genre]): Список жанров фильма.
        actors (List[Person]): Список актёров, участвовавших в фильме.
        writers (List[Person]): Список сценаристов.
        directors (List[Person]): Список режиссёров.
    """
    uuid: UUID
    title: str
    description: Optional[str] = None
    imdb_rating: Optional[float] = None
    genres: List[Genre] = []
    actors: List[Person] = []
    writers: List[Person] = []
    directors: List[Person] = []
