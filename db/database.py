from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncEngine

from .models import Base


def create_engine(host, port, username, password, db_name) -> tuple[AsyncEngine, async_sessionmaker]:
    database_url = f"postgresql+asyncpg://{username}:{password}@{host}:{port}/{db_name}"
    engine = create_async_engine(database_url)
    AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    return engine, AsyncSessionLocal


async def ensure_tables(engine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)