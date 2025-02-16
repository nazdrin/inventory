import os
import json
from ftplib import FTP

# Конфигурация FTP
FTP_HOST = "164.92.213.254"
FTP_USER = "seller"
FTP_PASS = "your-password"
FTP_DIR = "/upload"  # Папка с файлами на FTP
LOCAL_DIR = os.getcwd()  # Директория, откуда запускается скрипт

def download_file_from_ftp():
    """ Подключение к FTP и скачивание первого найденного файла """
    try:
        ftp = FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)

        files = ftp.nlst()  # Получаем список файлов
        if not files:
            print("Нет файлов для скачивания.")
            return None

        filename = files[0]  # Берем первый файл
        local_path = os.path.join(LOCAL_DIR, filename)

        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {filename}", f.write)
        
        ftp.quit()
        print(f"Файл {filename} успешно скачан в {local_path}")
        return local_path

    except Exception as e:
        print(f"Ошибка FTP: {e}")
        return None

def convert_to_json(file_path):
    """ Преобразует текстовый файл в JSON-формат """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        data = {"lines": [line.strip() for line in lines]}
        json_path = file_path + ".json"

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        print(f"Файл сохранен в JSON-формате: {json_path}")
    except Exception as e:
        print(f"Ошибка при конвертации в JSON: {e}")

if __name__ == "__main__":
    downloaded_file = download_file_from_ftp()
    if downloaded_file:
        convert_to_json(downloaded_file)
