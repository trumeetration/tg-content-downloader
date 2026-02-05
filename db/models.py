from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class CachedContent(Base):
    __tablename__ = "cached_content"

    id: Mapped[int] = mapped_column(primary_key=True)
    url: Mapped[str] = mapped_column(String(500), unique=True, index=True)
    telegram_file_id: Mapped[str] = mapped_column(String(200))
    name: Mapped[str] = mapped_column(String(200))