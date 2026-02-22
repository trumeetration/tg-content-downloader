from os import getenv

from dotenv import load_dotenv

load_dotenv()

TOKEN = getenv("BOT_TOKEN")
DB_HOST = getenv("DATABASE_HOST")
DB_PORT = getenv("DATABASE_PORT")
DB_USERNAME = getenv("DATABASE_USERNAME")
DB_PASSWORD = getenv("DATABASE_PASSWORD")
DB_NAME = getenv("DATABASE_NAME")
RABBITMQ_URL = getenv("RABBITMQ_URL")