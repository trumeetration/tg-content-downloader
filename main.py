import asyncio
import json
import os
from pathlib import Path
import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from aiogram import F, Bot, Dispatcher, html
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, LinkPreviewOptions, BufferedInputFile, FSInputFile
import aio_pika
import aio_pika.abc
import aiohttp
from sqlalchemy import select
import aiofiles

from contracts import LinkResult
from db.database import create_engine, ensure_tables
from db.models import CachedContent
from config import (
    TOKEN, 
    DB_HOST,
    DB_PORT,
    DB_USERNAME,
    DB_PASSWORD,
    DB_NAME,
    RABBITMQ_URL
)


LINK_PATTERN = r"https://www\.youtube\.com/(?:watch\?v=[A-Za-z0-9-]+.*|shorts/)|https://www.instagram.com/reel/[A-Za-z0-9]+.*"

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
    is_youtube_link = bool(re.search(r'https://www.youtube.com/watch\?v=[A-Za-z0-9-]+.*', full_url))
    if is_youtube_link:
        new_query_params["v"] = query_params["v"][0]

    new_url = urlunparse(parsed_url._replace(query=urlencode(new_query_params))).rstrip('/')
    return new_url


def get_content_id_from_url(url: str) -> str:
    if re.search(r'https://www.youtube.com/watch\?v=[A-Za-z0-9-]+.*', url):
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        return query_params["v"][0]
    
    else:
        return url.split('/')[-1]


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    await message.answer(f"Hello, {html.bold(message.from_user.full_name)}!")


@dp.message(F.text.startswith('https://'))
async def handle_link(message: Message):
    text = message.text

    if re.search(LINK_PATTERN, text) is None:
        return await message.answer(text="Only links from Youtube and Insta can be processed")

    url = format_url(text)
    content_id = get_content_id_from_url(url)
    """
    TODO: надо поменять логику хранения инфы о сохраненных файлах. 
    Пользователь может прислать ссылку со старым post_id но с уникальными query_params. 
    Лучше скорее всего хранить в бд не ссылку а идентификатор контента и ссылку

    TODO: с инатсграма могут скачивать фото, и в этом методе надо проверять какой контент хранится в бд - фото или видео
    """

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(CachedContent).where(CachedContent.url == url)
        )
        cached_content = result.scalar_one_or_none()

    if cached_content:
        return await message.answer_video(video=cached_content.telegram_file_id)

    # Add task for Download Service
    connection: aio_pika.abc.AbstractConnection = await aio_pika.connect(RABBITMQ_URL)

    channel: aio_pika.abc.AbstractChannel = await connection.channel()

    queue_msg = json.dumps({
        "chat_id": message.chat.id,
        "link": text
    }).encode()

    await channel.default_exchange.publish(
        aio_pika.Message(
            body=queue_msg
        ),
        routing_key="link_requests"
    )

    await connection.close()

    await message.reply(f'You send link {text}', link_preview_options=LinkPreviewOptions(is_disabled=True))


async def download_file(session: aiohttp.ClientSession, url: str, dest: str) -> None:
    print(f'Downloading {url}')
    async with session.get(url) as response:
        async with aiofiles.open(dest, "wb") as f:
            async for chunk in response.content.iter_chunked(1024):
                await f.write(chunk)


async def start_consumer(bot: Bot):
    print("Waiting for completed tasks")

    connection: aio_pika.abc.AbstractConnection = await aio_pika.connect(RABBITMQ_URL)

    channel: aio_pika.abc.AbstractChannel = await connection.channel()

    queue: aio_pika.abc.AbstractQueue = await channel.declare_queue("link_results")

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                message_body = message.body.decode()
                print(f'Consumer received message - {message_body}')
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
                        print(f'Exception while notifying user that link wasnt processed properply: {e}')
                        await bot.send_message(
                            chat_id=link_result["chat_id"],
                            text="Unable to process link"
                        )
                    
                    continue
                
                content_url = link_result["result_content_url"]
                thumbnail_url = link_result["result_thumbnail_url"]
                content_filename = content_url.split('/')[-1]
                thumbnail_filename = thumbnail_url.split('/')[-1]

                async with aiohttp.ClientSession() as session:
                    await asyncio.gather(
                        download_file(session, content_url, content_filename),
                        download_file(session, thumbnail_url, thumbnail_filename),
                    )

                    w, h, duration = await get_video_meta_async(content_filename)

                    content_file = FSInputFile(
                        path=content_filename,
                        filename=content_filename
                    )

                    print(f'Sending video to chat {link_result["chat_id"]}')

                    video_send_result = await bot.send_video(
                        chat_id=link_result["chat_id"],
                        video=content_file,
                        supports_streaming=True,
                        thumbnail=FSInputFile(path=thumbnail_filename),
                        width=w,
                        height=h,
                        duration=duration,
                    )
                    video_file_id = video_send_result.video.file_id

                    print(f'Sent video. File id - {video_file_id}')
                    # await bot.send_message(
                    #     chat_id=link_result["chat_id"],
                    #     text="A video could be here"
                    # )

                    print(f'Deleting files {content_filename} and thumbnail {thumbnail_filename}')
                    os.remove(content_filename)
                    os.remove(thumbnail_filename)
                    print('Temp files has been deleted')

                    async with AsyncSessionLocal() as db_session:
                        url = format_url(link_result["requested_url"])

                        cached_content = CachedContent(
                            url=url,
                            telegram_file_id=video_file_id,
                            name=content_filename
                        )
                        db_session.add(cached_content)

                        await db_session.commit()

                        print(f'Saved video {url} with file_id {video_file_id} in database')
                

async def main():
    await ensure_tables(engine)

    asyncio.create_task(start_consumer(bot))

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())