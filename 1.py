import os
from google.cloud import storage

# Имя вашего бакета
bucket_name = 'your-bucket-name'

# Путь к файлу на локальном диске, который вы хотите загрузить
source_file_name = 'key.json'

# Имя файла в бакете (может совпадать с локальным именем)
destination_blob_name = 'whatsapp-ix'

# Установка переменной окружения для файла авторизации
# Не рекомендуется для продакшена
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'

# Инициализация клиента хранилища
client = storage.Client()

# Загрузка файла
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Загружает файл в Google Cloud Storage"""
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f'File {source_file_name} uploaded to {destination_blob_name}.')

# Скачивание файла
def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Скачивает файл из Google Cloud Storage"""
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print(f'Blob {source_blob_name} downloaded to {destination_file_name}.')

# Вызов функций для загрузки и скачивания
upload_blob(bucket_name, source_file_name, destination_blob_name)
download_blob(bucket_name, destination_blob_name, source_file_name)