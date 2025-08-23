import os
from logging import config as logging_config
from pydantic import Field
from pydantic_settings import BaseSettings

from core.logger import LOGGING


class Settings(BaseSettings):
    # Название проекта (идет в Swagger и логах)
    project_name: str = Field(default="movies", env="PROJECT_NAME")

    # Redis
    redis_host: str = Field(default="127.0.0.1", env="REDIS_HOST")
    redis_port: int = Field(default=6379, env="REDIS_PORT")

    # Elasticsearch
    elastic_host: str = Field(default="127.0.0.1", env="ELASTIC_HOST")
    elastic_port: int = Field(default=9200, env="ELASTIC_PORT")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Применяем настройки логирования
logging_config.dictConfig(LOGGING)

# Инициализируем конфиг (один раз)
settings = Settings()

# Корень проекта
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))