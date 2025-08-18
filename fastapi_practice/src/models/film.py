from uuid import UUID
from pydantic import BaseModel
from typing import List, Optional


class FilmShort(BaseModel):
    """Краткая информация о фильме для списков (например, /films)."""
    id: UUID
    title: str
    imdb_rating: Optional[float] = None


class Film(BaseModel):
    """Полная информация о фильме (например, /films/{id})."""
    id: UUID
    title: str
    description: Optional[str] = None
    imdb_rating: Optional[float] = None
    genre: List[str] = []
    actors: List[str] = []
    writers: List[str] = []
    directors: List[str] = []