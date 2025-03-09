import asyncio
import os
from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command
from aiogram.types import Message
from app.database import get_async_db, MappingBranch
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()

dp.include_router(router)

@router.message(Command("start"))
async def start_handler(message: Message):
    await message.answer("Вітаємо! Будь ласка, введіть серійний номер аптеки або магазину для проходження реєстрації.")

@router.message(lambda message: message.text.isdigit())
async def branch_handler(message: Message):
    user_id = str(message.from_user.id)  # Всегда строка!
    branch = message.text
    
    async with get_async_db() as session:
        branch_entry = await session.get(MappingBranch, branch)
        
        if branch_entry:
            # Фильтруем NULL, пустые строки и некорректные значения перед обновлением
            valid_ids = {uid for uid in branch_entry.id_telegram if uid and uid.isdigit()} if branch_entry.id_telegram else set()
            
            if not valid_ids:
                # Если в поле были только NULL или некорректные данные, заменяем его новым ID
                branch_entry.id_telegram = [user_id]
                await message.answer("✅ Ви успішно зареєстровані! Зараз ви єдиний користувач для цього серійного номера.")
            elif user_id in valid_ids:
                await message.answer("ℹ️ Ви вже зареєстровані для цього серійного номера.")
            else:
                # Добавляем нового пользователя в список
                valid_ids.add(user_id)
                branch_entry.id_telegram = list(valid_ids)
                await message.answer("✅ Ваш ID успішно додано до цього серійного номера.")
            
            session.add(branch_entry)
            await session.commit()
        else:
            await message.answer("❌ Помилка! Будь ласка, введіть коректний серійний номер.")

async def notify_user(branch: str, codes: list):
        
    async with get_async_db() as session:
        branch_entry = await session.get(MappingBranch, branch)
        
        if branch_entry and branch_entry.id_telegram:
            user_ids = [uid for uid in branch_entry.id_telegram if uid and uid.isdigit()]  # Фильтруем неверные данные
            orders_list = "\n".join(f"{i+1}. {code}" for i, code in enumerate(codes))
            message_text = f"✅ *Нове(і) замовлення!* \n\n📌 *Номер:* \n\n{orders_list}"
            # Отправляем сообщение всем пользователям
            for user_id in user_ids:
                await bot.send_message(chat_id=int(user_id), text=message_text, parse_mode="Markdown")

async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

