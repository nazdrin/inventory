import requests
from app.database import get_async_db, EnterpriseSettings  # Импорт таблиц из базы данных
from sqlalchemy.future import select
from dotenv import load_dotenv
import os


load_dotenv()
TOKEN = os.getenv("TELEGRAM_DEVELOP")

def send_notification(message: str, enterprise_code: str):
    url = f'https://api.telegram.org/bot{TOKEN}/sendMessage'

    chat_ids = [807661373, 1041598119]

    for chat_id in chat_ids:
        payload = {
            "chat_id": chat_id,  # Отправляем сообщение каждому пользователю
            "text": f"{message} \n\nEnterprise Code: {enterprise_code}"
        }
        try:
            requests.post(url, data=payload)
        except Exception as e:
            pass
