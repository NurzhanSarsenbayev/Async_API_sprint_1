from pydantic import BaseModel
from uuid import UUID
from typing import Optional


class Person(BaseModel):
    """
    Модель персоны (актёра, режиссёра или сценариста).

    Attributes:
        uuid (UUID): Уникальный идентификатор персоны.
        full_name (str): Полное имя персоны.
        role (Optional[str]): Роль персоны
        (например, "actor", "director", "writer"). Может быть None.
    """
    uuid: UUID
    full_name: str
    role: Optional[str] = None
