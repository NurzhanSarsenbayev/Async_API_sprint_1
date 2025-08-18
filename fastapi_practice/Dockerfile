# Базовый образ Python
FROM python:3.11-slim

# Устанавливаем зависимости системы (для psycopg2, elastic и т.п. могут понадобиться)
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Рабочая директория
WORKDIR /app

# Копируем зависимости
COPY requirements.txt .

# Устанавливаем зависимости Python
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код проекта
COPY src /app/src

# Открываем порт
EXPOSE 8000

# Запуск через uvicorn
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]