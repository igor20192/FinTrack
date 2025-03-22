from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import declarative_base
import os
from decouple import config

user = config("MYSQL_USER")
password = config("MYSQL_PASSWORD")
# Connecting to MySQL in Docker (localhost, since the port is forwarded)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"mysql+aiomysql://{user}:{password}@localhost:3306/track_db?charset=utf8mb4",
)

engine = create_async_engine(DATABASE_URL, echo=True)
Base = declarative_base()

async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db():
    async with async_session() as session:
        yield session
