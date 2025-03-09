import asyncio
import os
from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command
from aiogram.types import Message
from app.database import get_async_db, MappingBranch
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")


bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()

dp.include_router(router)

@router.message(Command("start"))
async def start_handler(message: Message):
    await message.answer("Пожалуйста, введите серийный номер аптеки/магазина для регистрации:")

@router.message(lambda message: message.text.isdigit())
async def branch_handler(message: Message):
    user_id = message.from_user.id
    branch = message.text
    
    async with get_async_db() as session:
        branch_entry = await session.get(MappingBranch, branch)
        
        if branch_entry:
            branch_entry.ID_telegram = user_id
            await session.commit()
            await message.answer("Регистрация успешна! Ваш ID привязан к серийному номеру.")
        else:
            await message.answer("Ошибка! Введите корректный серийный номер.")

async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
