import asyncio
from contextlib import asynccontextmanager

from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from redis.asyncio import Redis

from api.v1 import films,genres,persons
from core import config
from services.cache_builder import build_cache_on_startup


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- startup ---
    app.state.redis = Redis(host=config.REDIS_HOST, port=config.REDIS_PORT)
    app.state.elastic = AsyncElasticsearch(
        hosts=[f"http://{config.ELASTIC_HOST}:{config.ELASTIC_PORT}"]
    )

    # запускаем кэширование в фоне
    asyncio.create_task(build_cache_on_startup(app.state.elastic, app.state.redis))

    yield  # здесь приложение доступно

    # --- shutdown ---
    await app.state.redis.close()
    await app.state.elastic.close()


app = FastAPI(
    title="Read-only API для онлайн-кинотеатра",
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    default_response_class=ORJSONResponse,
    description="Информация о фильмах, жанрах и людях, участвовавших в создании произведения",
    version="1.0.0",
    lifespan=lifespan
)

# Подключаем роутер к серверу, указав префикс /v1/films
# Теги указываем для удобства навигации по документации

# Роутер для фильмов
app.include_router(
    films.router,
    prefix="/api/v1/films",
    tags=["films"]
)

# Роутер для жанров
app.include_router(
    genres.router,
    prefix="/api/v1/genres",
    tags=["genres"]
)

# Роутер для персон
app.include_router(
    persons.router,
    prefix="/api/v1/persons",
    tags=["persons"]
)