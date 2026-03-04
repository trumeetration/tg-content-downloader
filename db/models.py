from typing import List

from sqlalchemy import String, ForeignKey, Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class CachedContent(Base):
    __tablename__ = "cached_content"

    id: Mapped[int] = mapped_column(primary_key=True)
    url: Mapped[str] = mapped_column(String(500), unique=True, index=True)
    files: Mapped[List["ContentFile"]] = relationship()


class ContentFile(Base):
    __tablename__ = "content_files"

    id: Mapped[int] = mapped_column(primary_key=True)
    content_id: Mapped[int] = mapped_column(ForeignKey("cached_content.id"))
    telegram_file_id: Mapped[str] = mapped_column(String(200))
    is_video: Mapped[bool] = mapped_column(Boolean())