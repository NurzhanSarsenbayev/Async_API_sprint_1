import json
from elasticsearch import AsyncElasticsearch, exceptions as es_exceptions
from redis.asyncio import Redis


SCROLL_SIZE = 100
SCROLL_TIMEOUT = "2m"
ELASTIC_INDEX = "movies"

async def scroll_all_movies(elastic: AsyncElasticsearch, index: str):
    """Генератор, который скроллит все документы в индексе."""
    try:
        resp = await elastic.search(
            index=index,
            scroll=SCROLL_TIMEOUT,
            body={"query": {"match_all": {}}, "size": SCROLL_SIZE}
        )
    except es_exceptions.NotFoundError:
        return

    scroll_id = resp["_scroll_id"]
    hits = resp["hits"]["hits"]

    while hits:
        for hit in hits:
            yield hit

        resp = await elastic.scroll(scroll_id=scroll_id, scroll=SCROLL_TIMEOUT)
        scroll_id = resp["_scroll_id"]
        hits = resp["hits"]["hits"]

    if scroll_id:
        await elastic.clear_scroll(scroll_id=scroll_id)


async def build_cache_on_startup(elastic: AsyncElasticsearch, redis: Redis):
    """Строим кэш жанров и персон при старте FastAPI."""
    genres_cache_raw = await redis.get("genres_cache") or b"{}"
    persons_cache_raw = await redis.get("persons_cache") or b"{}"
    genres_cache = json.loads(genres_cache_raw)
    persons_cache = json.loads(persons_cache_raw)

    async for movie in scroll_all_movies(elastic, ELASTIC_INDEX):
        for g in movie["_source"].get("genres", []):
            if g["uuid"] not in genres_cache:
                genres_cache[g["uuid"]] = g["name"]

        for role in ["actors", "directors", "writers"]:
            for p in movie["_source"].get(role, []):
                if p["uuid"] not in persons_cache:
                    persons_cache[p["uuid"]] = p["full_name"]

    await redis.set("genres_cache", json.dumps(genres_cache))
    await redis.set("persons_cache", json.dumps(persons_cache))
    print(f"Кэш построен: {len(genres_cache)} жанров и {len(persons_cache)} персон.")