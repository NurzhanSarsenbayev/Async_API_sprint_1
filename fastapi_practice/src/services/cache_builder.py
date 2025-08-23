import json
import asyncio
from elasticsearch import AsyncElasticsearch, exceptions as es_exceptions
from redis.asyncio import Redis

SCROLL_SIZE = 100
SCROLL_TIMEOUT = "2m"
ELASTIC_INDEX = "movies"
CACHE_TTL = 3600  # кеш на 1 час


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


async def build_cache(elastic: AsyncElasticsearch, redis: Redis):
    """
    Построение кэша жанров и персон.
    Повторяет попытку каждые 60 секунд до успешного выполнения,
    затем обновляет каждые 3600 секунд.
    """
    while True:
        try:
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

            await redis.set("genres_cache", json.dumps(genres_cache), ex=CACHE_TTL)
            await redis.set("persons_cache", json.dumps(persons_cache), ex=CACHE_TTL)
            print(f"Кэш построен: {len(genres_cache)} жанров и {len(persons_cache)} персон.")

            # Ждем час до следующего обновления
            await asyncio.sleep(3600)

        except Exception as e:
            print(f"Ошибка при построении кэша: {e}. Повтор через 1 минуту.")
            await asyncio.sleep(5)


async def wait_for_elastic(es: AsyncElasticsearch, timeout: int = 60, initial_delay: int = 30):
    """
    Ждём, пока Elasticsearch не станет доступен.

    :param es: экземпляр AsyncElasticsearch
    :param timeout: общее время ожидания в секундах
    :param initial_delay: время ожидания перед первой попыткой пинга
    """
    if initial_delay > 0:
        print(f"⏳ Waiting {initial_delay} seconds before first ping...")
        await asyncio.sleep(initial_delay)

    for i in range(timeout):
        try:
            if await es.ping():
                print("✅ Elasticsearch is up!")
                return
        except Exception:
            pass
        print(f"⏳ Waiting for Elasticsearch... {i + 1}/{timeout}")
        await asyncio.sleep(1)

    raise RuntimeError("Elasticsearch не доступен после ожидания")

# async def log_exceptions(coro):
#     try:
#         await coro
#     except Exception as e:
#         print(f"Ошибка в background task: {e}")
#
# async def wait_for_index(es: AsyncElasticsearch, index: str, timeout: int = 60):
#     start = asyncio.get_event_loop().time()
#     while True:
#         try:
#             if await es.indices.exists(index=index):
#                 print(f"✅ Index '{index}' exists!")
#                 return
#         except Exception:
#             pass
#         if asyncio.get_event_loop().time() - start > timeout:
#             raise RuntimeError(f"Index '{index}' не доступен после {timeout} секунд")
#         await asyncio.sleep(30)
