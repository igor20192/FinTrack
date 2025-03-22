# app/cache.py
import redis.asyncio as redis
import json
import logging

logger = logging.getLogger(__name__)

REDIS_URL = "redis://localhost:6379/0"


async def get_redis():
    """Создаёт подключение к Redis."""
    return await redis.from_url(REDIS_URL)


async def get_cache(key: str):
    """Получает данные из кэша по ключу."""
    redis = await get_redis()
    cached = await redis.get(key)
    await redis.close()
    if cached:
        logger.info(f"Cache hit for key: {key}")
        return json.loads(cached)
    logger.info(f"Cache miss for key: {key}")
    return None


async def set_cache(key: str, value, expire: int = 3600):
    """Сохраняет данные в кэш с TTL."""
    redis = await get_redis()
    await redis.set(key, json.dumps(value), ex=expire)
    await redis.close()
    logger.info(f"Cache set for key: {key} with TTL: {expire} seconds")


async def clear_cache(pattern: str):
    """Очищает кэш по шаблону (например, 'year_performance:*')."""
    redis = await get_redis()
    cursor = 0
    while True:
        cursor, keys = await redis.scan(cursor, match=pattern)
        if keys:
            await redis.delete(*keys)
            logger.info(f"Cleared cache keys: {keys}")
        if cursor == 0:
            break
    await redis.close()
