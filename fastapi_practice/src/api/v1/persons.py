from fastapi import APIRouter, Depends, HTTPException, Query
from http import HTTPStatus
from typing import List

from models.person import Person
from services.persons import PersonService, get_person_service

router = APIRouter()


@router.get("/", response_model=List[Person])
async def persons_list(
        page: int = Query(1, ge=1, description="Номер страницы"),
        size: int = Query(10, ge=1, le=100,
                          description="Количество персон на странице"),
        person_service: PersonService = Depends(get_person_service)
):
    """
    Получение списка всех персон с пагинацией.

    Args:
        page (int): номер страницы (начало с 1).
        size (int): количество персон на странице (макс. 100).
        person_service (PersonService): сервис для работы с персоной.

    Returns:
        List[Person]: список персон с UUID, полным именем и ролью.

    Примечание:
        Данные кэшируются в Redis для ускорения повторных запросов.
    """
    return await person_service.list_persons(size=size, page=page)


@router.get("/search", response_model=List[Person])
async def search_persons(
    query: str = Query(..., description="Поисковая строка для поиска персон"),
    person_service: PersonService = Depends(get_person_service),
):
    """
    Поиск персон (актёров, режиссёров, сценаристов) по полному имени.

    Args:
        query (str): строка поиска.
        person_service (PersonService): сервис для работы с персоной.

    Returns:
        List[Person]: список персон, полностью совпадающих с запросом.

    Примечание:
        Результаты поиска кэшируются в Redis.
    """
    return await person_service.search_persons(query_str=query)


@router.get("/{person_id}", response_model=Person)
async def person_details(
    person_id: str,
    person_service: PersonService = Depends(get_person_service)
):
    """
    Получение информации о конкретной персоне по UUID.

    Args:
        person_id (str): UUID персоны.
        person_service (PersonService): сервис для работы с персоной.

    Returns:
        Person: объект персоны с UUID, полным именем и ролью.

    Raises:
        HTTPException: статус 404, если персона не найдена.

    Примечание:
        Данные кэшируются в Redis для ускорения повторных запросов.
    """
    person = await person_service.get_person_by_id(person_id)
    if not person:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="person not found"
        )
    return person
