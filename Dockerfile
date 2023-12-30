FROM python:3.8-slim

# Установка компилятора C и других зависимостей
RUN apt-get update && apt-get install -y build-essential

WORKDIR /app

# Установка зависимостей Python
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "app.py"]
