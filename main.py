import asyncio
import json
import os
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
import re
from typing import List
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from aiogram import F, Bot, Dispatcher, html
from aiogram.filters import CommandStart
from aiogram.types import Message, LinkPreviewOptions, FSInputFile, InputMediaVideo, InputMediaPhoto, URLInputFile
import aio_pika
import aio_pika.abc
import aiohttp
from sqlalchemy import select, Result
from sqlalchemy.orm import selectinload
import aiofiles

from contracts import LinkResult
from db.database import create_engine, ensure_tables
from db.models import CachedContent, ContentFile
from config import (
    TOKEN, 
    DB_HOST,
    DB_PORT,
    DB_USERNAME,
    DB_PASSWORD,
    DB_NAME,
    RABBITMQ_URL
)


GENERAL_LINK_PATTERN = re.compile(r"https://www\.youtube\.com/(?:watch\?v=[A-Za-z0-9-]+.*|shorts/)|https://www\.instagram\.com/(?:reel|p)/[A-Za-z0-9]+.*", re.IGNORECASE)
YOUTUBE_DEFAULT_LINK_PATTERN = re.compile(r'https://www\.youtube\.com/watch\?v=[A-Za-z0-9-]+.*', re.IGNORECASE)

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler("bot.log", maxBytes=15 * 1024 * 1024, backupCount=5),
        logging.StreamHandler()
    ]
)

logging.getLogger("aio_pika").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

dp = Dispatcher()
bot = Bot(token=TOKEN)

engine, AsyncSessionLocal = create_engine(DB_HOST, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_NAME)


async def get_video_meta_async(path: str | Path) -> tuple[int, int, int]:
    """
    Возвращает (width, height, duration_seconds)
    Асинхронная версия, не блокирует event loop.
    """

    async def _ffprobe_json(path: str | Path, *args: str) -> dict:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            *args,
            str(path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        return json.loads(stdout)

    path = Path(path)

    # параллельно дергаем streams и format
    streams_task = asyncio.create_task(
        _ffprobe_json(path, "-show_streams")
    )
    format_task = asyncio.create_task(
        _ffprobe_json(path, "-show_format")
    )

    streams = await streams_task
    fmt = await format_task

    video_stream = next(
        s for s in streams["streams"] if s.get("codec_type") == "video"
    )
    width = int(video_stream["width"])
    height = int(video_stream["height"])
    duration = int(float(fmt["format"]["duration"]))

    return width, height, duration


def format_url(full_url: str) -> str:
    parsed_url = urlparse(full_url)
    query_params = parse_qs(parsed_url.query)

    new_query_params = {}
    if YOUTUBE_DEFAULT_LINK_PATTERN.search(full_url):
        new_query_params["v"] = query_params["v"][0]

    new_url = urlunparse(parsed_url._replace(query=urlencode(new_query_params))).rstrip('/')
    return new_url


def get_content_id_from_url(url: str) -> str:
    if YOUTUBE_DEFAULT_LINK_PATTERN.search(url):
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        return query_params["v"][0]
    
    else:
        return url.split('/')[-1]


async def add_url_to_queue(url: str, chat_id: int):
    connection: aio_pika.abc.AbstractConnection = await aio_pika.connect(RABBITMQ_URL)

    channel: aio_pika.abc.AbstractChannel = await connection.channel()

    queue_msg = json.dumps({
        "chat_id": chat_id,
        "link": url
    }).encode()

    await channel.default_exchange.publish(
        aio_pika.Message(
            body=queue_msg
        ),
        routing_key="link_requests"
    )

    await connection.close()



@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    await message.answer(f"Hello, {html.bold(message.from_user.full_name)}!")


@dp.message(F.text.startswith('https://'))
async def handle_link(message: Message):
    text = message.text

    if GENERAL_LINK_PATTERN.search(text) is None:
        return await message.answer(text="Only links from Youtube and Inst can be processed")

    url = format_url(text)

    async with AsyncSessionLocal() as session:
        stmt = (
            select(CachedContent)
            .where(CachedContent.url == url)
            .options(selectinload(CachedContent.files))
        )
        result: Result = await session.execute(stmt)
        cached_content = result.scalar_one_or_none()

    if cached_content and cached_content.files:
        logger.info(f"Content with url {url} found in cache with {len(cached_content.files)} files.")
        
        # If only one file, send it directly for a cleaner look
        if len(cached_content.files) == 1:
            file = cached_content.files[0]
            if file.is_video:
                return await message.answer_video(video=file.telegram_file_id)
            else:
                return await message.answer_photo(photo=file.telegram_file_id)

        # If multiple files, send as a media group
        media_group = []
        for file in cached_content.files:
            media = InputMediaVideo(media=file.telegram_file_id) if file.is_video else InputMediaPhoto(media=file.telegram_file_id)
            media_group.append(media)
        
        return await message.answer_media_group(media=media_group)

    # Add task for Download Service
    
    await add_url_to_queue(url, message.chat.id)

    await message.reply(f'You send link {text}', link_preview_options=LinkPreviewOptions(is_disabled=True))


async def download_file(session: aiohttp.ClientSession, url: str, dest: str) -> None:
    logger.info(f'Downloading {url}')
    async with session.get(url) as response:
        async with aiofiles.open(dest, "wb") as f:
            async for chunk in response.content.iter_chunked(1024):
                await f.write(chunk)


async def start_consumer(bot: Bot):
    logger.info("Waiting for completed tasks")

    connection: aio_pika.abc.AbstractConnection = await aio_pika.connect(RABBITMQ_URL)

    channel: aio_pika.abc.AbstractChannel = await connection.channel()

    queue: aio_pika.abc.AbstractQueue = await channel.declare_queue("link_results")

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                message_body = message.body.decode()
                logger.info(f'Consumer received message - {message_body}')
                link_result: LinkResult = json.loads(message_body)

                is_success = link_result["success"]

                if not is_success:
                    try:
                        text = f'Unable to process link {link_result["requested_url"]}\n\nErr: {link_result["error"]}'
                        await bot.send_message(
                            chat_id=link_result["chat_id"],
                            text=text
                        )
                    except Exception as e:
                        logger.error(f'Exception while notifying user that link wasnt processed properply: {e}')
                        await bot.send_message(
                            chat_id=link_result["chat_id"],
                            text="Unable to process link"
                        )
                    
                    continue
                
                downloaded_files = []
                try:
                    # We need to download video files to get their metadata (width, height, duration)
                    # and to reliably attach a custom thumbnail.
                    # For photos, we can pass the URL directly to Telegram.
                    
                    video_items_to_download = []
                    for item in link_result["content_items"]:
                        if item["is_video"]:
                            video_items_to_download.append(item)

                    async with aiohttp.ClientSession() as session:
                        download_tasks = []
                        for item in video_items_to_download:
                            content_url = f'{link_result["bucket_base_url"]}/{item["content_path"]}'
                            thumbnail_url = f'{link_result["bucket_base_url"]}/{item["thumbnail_path"]}'
                            
                            content_filename = item["content_path"].split('/')[-1]
                            thumbnail_filename = item["thumbnail_path"].split('/')[-1]

                            # Add to list for future cleanup
                            downloaded_files.append(content_filename)
                            downloaded_files.append(thumbnail_filename)

                            # Create download tasks
                            download_tasks.append(download_file(session, content_url, content_filename))
                            download_tasks.append(download_file(session, thumbnail_url, thumbnail_filename))
                        
                        if download_tasks:
                            await asyncio.gather(*download_tasks)
                            logger.info(f'Downloaded {len(downloaded_files)} files for videos for request {link_result["requested_url"]}')

                    media_group = []

                    for i, item in enumerate(link_result["content_items"]):
                        content_filename = item["content_path"].split('/')[-1]

                        if item["is_video"]:
                            thumbnail_filename = item["thumbnail_path"].split('/')[-1]
                            w, h, duration = await get_video_meta_async(content_filename)
                            media = InputMediaVideo(
                                media=FSInputFile(path=content_filename),
                                thumbnail=FSInputFile(path=thumbnail_filename),
                                width=w,
                                height=h,
                                duration=duration,
                                supports_streaming=True
                            )
                        else: # It's a photo
                            content_url = f'{link_result["bucket_base_url"]}/{item["content_path"]}'
                            # For photos, we don't need metadata, so we can pass the URL directly.
                            # Using URLInputFile is more explicit than passing a raw string.
                            media = InputMediaPhoto(
                                media=URLInputFile(content_url)
                            )
                        
                        media_group.append(media)
                    
                    logger.info(f'Sending media group to chat {link_result["chat_id"]}')
                    sent_messages = await bot.send_media_group(
                        chat_id=link_result["chat_id"],
                        media=media_group
                    )
                    
                    logger.info(f'Sent media group')

                    async with AsyncSessionLocal() as db_session:
                        url = format_url(link_result["requested_url"])

                        content_files: List[ContentFile] = []
                        for message in sent_messages:
                            is_video = message.video is not None
                            telegram_file_id = message.video.file_id if is_video else message.photo[-1].file_id
                            content_files.append(ContentFile(telegram_file_id=telegram_file_id, is_video=is_video))
                        cached_content = CachedContent(url=url, files=content_files)
                        db_session.add(cached_content)
                        await db_session.commit()
                        logger.info(f'Saved {url} with {len(content_files)} files in database')
                finally:
                    logger.info(f'Deleting temporary files: {downloaded_files}')
                    for f_path in downloaded_files:
                        try:
                            os.remove(f_path)
                        except OSError as e:
                            logger.error(f"Error deleting file {f_path}: {e}")
                    if downloaded_files:
                        logger.info('Temp video files have been deleted')
                    else:
                        logger.info('No temporary files to delete')
                

async def main():
    await ensure_tables(engine)

    asyncio.create_task(start_consumer(bot))

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())