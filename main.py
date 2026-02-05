import asyncio
from os import getenv
import json

from aiogram import F, Bot, Dispatcher, html
from aiogram.filters import CommandStart
from aiogram.types import Message, LinkPreviewOptions, BufferedInputFile
import aio_pika
import aio_pika.abc
from dotenv import load_dotenv
import aiohttp
from sqlalchemy import select

from contracts import LinkResult
from db.database import create_engine, ensure_tables
from db.models import CachedContent

load_dotenv()

TOKEN = getenv("BOT_TOKEN")
DB_HOST = getenv("DATABASE_HOST")
DB_PORT = getenv("DATABASE_PORT")
DB_USERNAME = getenv("DATABASE_USERNAME")
DB_PASSWORD = getenv("DATABASE_PASSWORD")
DB_NAME = getenv("DATABASE_NAME")
RABBITMQ_URL = getenv("RABBITMQ_URL")

LINK_PATTERN = r"https://www.youtube.com/(?:watch\?v=[A-Za-z0-9]+.*|shorts/)|https://www.instagram.com/reel/[A-Za-z0-9]+.*"

dp = Dispatcher()
bot = Bot(token=TOKEN)

engine, AsyncSessionLocal = create_engine(DB_HOST, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_NAME)


def format_url(full_url: str) -> str:
    url = full_url.split('?')[0] if '?' in full_url else full_url # remove query params (like some metrics)
    url = url.removesuffix('/') # remove most right symbol '/' | exmaple https://isnt.com/reel/asudhiuwad/ -> https://isnt.com/reel/asudhiuwad
    return url


def get_content_id_from_url(url: str) -> str:
    return url.split('/')[-1]


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    await message.answer(f"Hello, {html.bold(message.from_user.full_name)}!")


@dp.message(F.text.regexp(LINK_PATTERN))
async def handle_link(message: Message):
    url = format_url(message.text)
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
        await message.answer_video(video=cached_content.telegram_file_id)

        return

    # Add task for Download Service
    connection: aio_pika.abc.AbstractConnection = await aio_pika.connect(RABBITMQ_URL)

    channel: aio_pika.abc.AbstractChannel = await connection.channel()

    queue_msg = json.dumps({
        "chat_id": message.chat.id,
        "link": message.text
    }).encode()

    await channel.default_exchange.publish(
        aio_pika.Message(
            body=queue_msg
        ),
        routing_key="link_requests"
    )

    await connection.close()

    await message.reply(f'You send link {message.text}', link_preview_options=LinkPreviewOptions(is_disabled=True))


@dp.message()
async def echo_handler(message: Message) -> None:
    """
    Handler will forward receive a message back to the sender

    By default, message handler will handle all message types (like a text, photo, sticker etc.)
    """
    try:
        # Send a copy of the received message
        await message.send_copy(chat_id=message.chat.id)
    except TypeError:
        # But not all the types is supported to be copied so need to handle it
        await message.answer("Nice try!")


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
                
                video_url = link_result["result_video_url"]

                async with aiohttp.ClientSession() as session:
                    print(f'Downloading {video_url}')
                    async with session.get(video_url) as response:
                        if response.status == 200:
                            print(f'Video downloaded {video_url}')
                            video_bytes = await response.read()
                            video_name = video_url.split('/')[-1]

                            video_file = BufferedInputFile(
                                video_bytes,
                                video_name
                            )

                            print(f'Sending video to chat {link_result["chat_id"]}')
                            video_send_result = await bot.send_video(
                                chat_id=link_result["chat_id"],
                                video=video_file,
                                supports_streaming=True
                            )
                            video_file_id = video_send_result.video.file_id

                            print(f'Sent video. File id - {video_file_id}')

                            async with AsyncSessionLocal() as db_session:
                                url = format_url(link_result["requested_url"])

                                cached_content = CachedContent(
                                    url=url,
                                    telegram_file_id=video_file_id,
                                    name=video_name
                                )
                                db_session.add(cached_content)

                                await db_session.commit()

                                print(f'Saved video {url} with file_id {video_file_id} in database')
                                
                        else:
                            print(f'error while loading video {video_url} from s3 storage')
                


async def main():
    await ensure_tables(engine)

    asyncio.create_task(start_consumer(bot))

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())