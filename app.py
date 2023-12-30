import os
import uuid
import pandas as pd
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud import storage
from threading import Thread
import gc  # Для управления сборщиком мусора
import logging

app = Flask(__name__)
CORS(app)
app.config['MAX_CONTENT_LENGTH'] = 5000 * 1024 * 1024

upload_folder = "uploads"
result_folder = "results"
log_folder = "logs"

if not os.path.exists(upload_folder):
    os.makedirs(upload_folder)
if not os.path.exists(result_folder):
    os.makedirs(result_folder)
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

logging.basicConfig(filename=os.path.join(log_folder, "server.log"), level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

validation_url = "https://gate.whapi.cloud/contacts"
whatsapp_notification_url = "https://gate.whapi.cloud/messages/text"
gcs_bucket_name = 'whatsapp-ix'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'


def send_notification(message):
    payload = {
        "typing_time": 0,
        "to": "77073670497@s.whatsapp.net",
        "body": message
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": "Bearer CZWvEC3CzjE0uXqV6PZGGmKyQipLoOke"
    }
    response = requests.post(whatsapp_notification_url, json=payload, headers=headers)
    logging.info(f"Notification sent: {response.text}")


def upload_to_gcs(file_path, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(gcs_bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(file_path)
    logging.info(f"File {file_path} uploaded to {destination_blob_name}.")


def download_from_gcs(source_blob_name, destination_file_name):
    client = storage.Client()
    bucket = client.bucket(gcs_bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)
    logging.info(f"File {source_blob_name} downloaded to {destination_file_name}.")


def process_file_async(file_name):
    temp_csv_path = os.path.join(upload_folder, file_name)
    download_from_gcs(file_name, temp_csv_path)

    data = pd.read_csv(temp_csv_path, encoding='utf-8')
    phone_numbers = data.iloc[:, 3].dropna().astype(str).str.replace('\.0', '', regex=True).tolist()

    # Валидация номеров телефонов
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": "Bearer CZWvEC3CzjE0uXqV6PZGGmKyQipLoOke"
    }
    validation_results = []
    for i in range(0, len(phone_numbers), 100):
        batch = phone_numbers[i:i + 100]
        payload = {
            "blocking": "no_wait",
            "force_check": False,
            "contacts": batch
        }
        response = requests.post(validation_url, json=payload, headers=headers)
        batch_results = response.json()['contacts']
        validation_results.extend(batch_results)

    data['Validation'] = [res['status'] if 'status' in res else 'invalid' for res in validation_results]
    data.sort_values(by='Validation', ascending=False, inplace=True)

    valid_data = data[data['Validation'] == 'valid']
    invalid_data = data[data['Validation'] == 'invalid']

    valid_csv_path = os.path.join(result_folder, f"valid_{file_name}")
    invalid_csv_path = os.path.join(result_folder, f"invalid_{file_name}")

    valid_data.to_csv(valid_csv_path, index=False, encoding='utf-8')
    invalid_data.to_csv(invalid_csv_path, index=False, encoding='utf-8')

    valid_excel_path = valid_csv_path.replace(".csv", ".xlsx")
    invalid_excel_path = invalid_csv_path.replace(".csv", ".xlsx")

    valid_data.to_excel(valid_excel_path, index=False)
    invalid_data.to_excel(invalid_excel_path, index=False)

    os.remove(valid_csv_path)
    os.remove(invalid_csv_path)

    upload_to_gcs(valid_excel_path, f"valid_{file_name}".replace(".csv", ".xlsx"))
    upload_to_gcs(invalid_excel_path, f"invalid_{file_name}".replace(".csv", ".xlsx"))

    valid_file_url = f"https://storage.googleapis.com/{gcs_bucket_name}/valid_{file_name}".replace(".csv", ".xlsx")
    invalid_file_url = f"https://storage.googleapis.com/{gcs_bucket_name}/invalid_{file_name}".replace(".csv", ".xlsx")

    send_notification(
        f"Processed files are available: Valid Numbers - {valid_file_url}, Invalid Numbers - {invalid_file_url}")

    os.remove(valid_excel_path)
    os.remove(invalid_excel_path)

    del data, valid_data, invalid_data  # Очистка памяти
    gc.collect()  # Явный вызов сборщика мусора


@app.route('/process-excel', methods=['POST'])
def process_excel():
    try:
        files = request.files.getlist('files')
        combined_csv_filename = str(uuid.uuid4()) + ".csv"
        combined_csv_path = os.path.join(upload_folder, combined_csv_filename)

        for file in files:
            df = pd.read_excel(file, na_values=[], keep_default_na=False)
            df.to_csv(combined_csv_path, mode='a', header=False, index=False, encoding='utf-8')
            del df
            gc.collect()

        upload_to_gcs(combined_csv_path, combined_csv_filename)
        os.remove(combined_csv_path)

        Thread(target=process_file_async, args=(combined_csv_filename,)).start()

        return jsonify({'message': 'File uploaded and processing started.'})
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=3000, debug=True)
