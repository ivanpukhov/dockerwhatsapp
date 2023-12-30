import logging
import os
import uuid
import pandas as pd
import aiohttp
import asyncio
import math
from aiogram import Bot, Dispatcher, executor, types
from google.cloud import storage
import gc
import requests

API_TOKEN = '6827397368:AAEibXop9mLsUQiUO4uR0jax5ZvlS9OEJiw'
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

upload_folder = "uploads"
result_folder = "results"

gcs_bucket_name = 'whatsapp-ix'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'

if not os.path.exists(upload_folder):
    os.makedirs(upload_folder)
if not os.path.exists(result_folder):
    os.makedirs(result_folder)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

validation_url = "https://gate.whapi.cloud/contacts"
whatsapp_notification_url = "https://gate.whapi.cloud/messages/text"

async def download_file(session, url, destination):
    async with session.get(url) as response:
        with open(destination, 'wb') as fd:
            while True:
                chunk = await response.content.read(1024)
                if not chunk:
                    break
                fd.write(chunk)

async def perform_request_with_retries(session, url, payload, headers, max_retries=5, delay=5):
    retries = 0
    while True:
        try:
            async with session.post(url, json=payload, headers=headers) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            retries += 1
            if retries >= max_retries:
                raise
            logging.error(f"Request failed, retrying in {delay} seconds... Error: {e}")
            await asyncio.sleep(delay)

def upload_to_gcs(file_path, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(gcs_bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    logging.info(f"File {file_path} uploaded to {destination_blob_name}.")

def send_notification(message):
    payload = {
        "typing_time": 0,
        "to": "77073670497",
        "body": message
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": "CZWvEC3CzjE0uXqV6PZGGmKyQipLoOke"
    }
    response = requests.post(whatsapp_notification_url, json=payload, headers=headers)
    logging.info(f"Notification sent: {response.text}")

async def process_file_async(file_urls, message):
    async with aiohttp.ClientSession() as session:
        download_tasks = []
        combined_csv_filename = str(uuid.uuid4()) + ".csv"
        combined_csv_path = os.path.join(upload_folder, combined_csv_filename)

        for url in file_urls:
            file_name = url.split('/')[-1]
            temp_csv_path = os.path.join(upload_folder, file_name)
            download_tasks.append(download_file(session, url, temp_csv_path))

        await asyncio.gather(*download_tasks)

        all_data = []
        for url in file_urls:
            file_name = url.split('/')[-1]
            temp_csv_path = os.path.join(upload_folder, file_name)
            df = pd.read_excel(temp_csv_path, na_values=[], keep_default_na=False)
            all_data.append(df)
            os.remove(temp_csv_path)
            gc.collect()

        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.to_csv(combined_csv_path, index=False, encoding='utf-8')

        data = pd.read_csv(combined_csv_path, encoding='utf-8')
        phone_numbers = data.iloc[:, 3].dropna().astype(str).str.replace('\.0', '', regex=True).tolist()
        num_validation_requests = math.ceil(len(phone_numbers) / 100)
        total_steps = len(file_urls) * 2 + num_validation_requests + 1
        current_step = len(file_urls) * 2

        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "authorization": "CZWvEC3CzjE0uXqV6PZGGmKyQipLoOke"
        }
        validation_results = []
        for i in range(0, len(phone_numbers), 100):
            batch = phone_numbers[i:i + 100]
            await message.answer(f"{len(batch)} файлов обрабатывается")

            payload = {
                "blocking": "no_wait",
                "force_check": False,
                "contacts": batch
            }
            response = await perform_request_with_retries(session, validation_url, payload, headers)
            batch_results = response['contacts']
            validation_results.extend(batch_results)
            current_step += 1

        data['Validation'] = [res['status'] if 'status' in res else 'invalid' for res in validation_results]
        data.sort_values(by='Validation', ascending=False, inplace=True)

        valid_data = data[data['Validation'] == 'valid']
        invalid_data = data[data['Validation'] == 'invalid']

        valid_csv_path = os.path.join(result_folder, f"valid_{combined_csv_filename}")
        invalid_csv_path = os.path.join(result_folder, f"invalid_{combined_csv_filename}")

        valid_data.to_csv(valid_csv_path, index=False, encoding='utf-8')
        invalid_data.to_csv(invalid_csv_path, index=False, encoding='utf-8')

        valid_excel_path = valid_csv_path.replace(".csv", ".xlsx")
        invalid_excel_path = invalid_csv_path.replace(".csv", ".xlsx")

        valid_data.to_excel(valid_excel_path, index=False)
        invalid_data.to_excel(invalid_excel_path, index=False)

        upload_to_gcs(valid_excel_path, f"valid_{combined_csv_filename}".replace(".csv", ".xlsx"))
        upload_to_gcs(invalid_excel_path, f"invalid_{combined_csv_filename}".replace(".csv", ".xlsx"))

        valid_file_url = f"https://storage.googleapis.com/{gcs_bucket_name}/valid_{combined_csv_filename}".replace(".csv", ".xlsx")
        invalid_file_url = f"https://storage.googleapis.com/{gcs_bucket_name}/invalid_{combined_csv_filename}".replace(".csv", ".xlsx")
        # Подсчет статистики
        valid_count = len(valid_data)
        invalid_count = len(invalid_data)
        total_count = valid_count + invalid_count
        valid_percentage = (valid_count / total_count) * 100 if total_count > 0 else 0

        # Отправка статистики
        stats_message = f"Обработка завершена. Валидных номеров: {valid_count}, невалидных: {invalid_count}, процент валидных: {valid_percentage:.2f}%"
        await message.answer(stats_message)

        await message.answer(f"Валидные номера - {valid_file_url}, Не валидные номера - {invalid_file_url}")

        os.remove(valid_excel_path)
        os.remove(invalid_excel_path)
        os.remove(combined_csv_path)
        gc.collect()

@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    await message.reply("Привет! Отправь мне ссылку на файл.")

@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def process_link(message: types.Message):
    file_urls = message.text.split()  # Assuming space-separated URLs
    status_message = await message.reply("Файлы приняты. Начинаю обработку...")
    asyncio.create_task(process_file_async(file_urls, status_message))


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
