from pydantic import BaseModel
from uuid import UUID


class Genre(BaseModel):
    """
    Модель жанра фильма.

    Attributes:
        uuid (UUID): Уникальный идентификатор жанра.
        name (str): Название жанра.
    """
    uuid: UUID
    name: str
