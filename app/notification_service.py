import requests
from app.database import get_async_db, EnterpriseSettings  # Импорт таблиц из базы данных
from sqlalchemy.future import select
# Функция для отправки сообщений в Telegram
def send_notification(message: str, enterprise_code: str):
    token = '5650306279:AAHZHACK7fqnLdHzLBDvY29vs7SXViMGqFs'  # Токен вашего бота
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    payload = {
        'chat_id': 807661373,  # Ваш user_id
        "text": f"{message} \n\nEnterprise Code: {enterprise_code}"
    }
    try:
        response = requests.post(url, data=payload)
        return response.json()
    except Exception as e:
        print(f"Ошибка при отправке сообщения: {e}")
# Функция для отправки сообщений в Telegram администратору предриятия
async def send_notification_to_admin(message: str, enterprise_code: str):
    try:
        # Получаем данные предприятия по кодам
        async with get_async_db() as db:
            enterprise = await db.execute(
                select(EnterpriseSettings).filter(EnterpriseSettings.enterprise_code == enterprise_code)
            )
            enterprise = enterprise.scalars().first()

        # Получаем user_id администратора (email используем как user_id)
        if enterprise:
            user_id = enterprise.email  # Email используется как user_id
            token = '5650306279:AAHZHACK7fqnLdHzLBDvY29vs7SXViMGqFs'  # Токен вашего бота
            url = f'https://api.telegram.org/bot{token}/sendMessage'
            payload = {
                'chat_id': user_id,  # Используем email как user_id
                "text": f"{message} \n\nEnterprise Code: {enterprise.enterprise_code}\nEnterprise Name: {enterprise.enterprise_name}"
            }

            # Отправка сообщения в Telegram
            response = requests.post(url, data=payload)
            return response.json()
        else:
            raise ValueError("Enterprise not found.")
    
    except Exception as e:
        print(f"Ошибка при отправке сообщения: {e}")

# Пример вызова
# await send_notification_to_admin("Test notification: Process failed!", enterprise_code='1')
# Пример использования
# def notify_developer(message: str):
#     user_id = 807661373  # Укажите ваш user_id
#     send_notification(message, user_id)

# Пример вызова
#notify_developer("Test notification: Process failed!")