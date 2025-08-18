from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from redis.asyncio import Redis

from api.v1 import films
from core import config
from db import elastic, redis

app = FastAPI(
    title="Read-only API для онлайн-кинотеатра",
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    default_response_class=ORJSONResponse,
    description="Информация о фильмах, жанрах и людях, участвовавших в создании произведения",
    version="1.0.0"
)

@app.get("/")
async def root():
    return {"message": "Hello, FastAPI is working!"}


# @app.get("/api/v1/search/",
#          response_model=List[ShortFilm],
#          summary="Поиск кинопроизведений",
#          description="Полнотекстовый поиск по кинопроизведениям",
#          response_description="Название и рейтинг фильма",
#          tags=['Полнотекстовый поиск']
#          )

@app.on_event('startup')
async def startup():
    redis.redis = Redis(host=config.REDIS_HOST, port=config.REDIS_PORT)
    elastic.es = AsyncElasticsearch(
        hosts=[f"http://{config.ELASTIC_HOST}:{config.ELASTIC_PORT}"]
    )


@app.on_event('shutdown')
async def shutdown():
    await redis.redis.close()
    await elastic.es.close()


# Подключаем роутер к серверу, указав префикс /v1/films
# Теги указываем для удобства навигации по документации
app.include_router(films.router, prefix='/api/v1/films', tags=['films'])