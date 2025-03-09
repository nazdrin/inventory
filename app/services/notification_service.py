import asyncio
from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command
from aiogram.types import Message
from app.database import get_async_db, MappingBranch
TOKEN = "7727251903:AAGCcQWTBry4O03gkNYA2HKG-h2xzoPKjbU"


bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()

dp.include_router(router)

@router.message(Command("start"))
async def start_handler(message: Message):
    await message.answer("Пожалуйста, введите серийный номер аптеки/магазина для регистрации:")

@router.message(lambda message: message.text.isdigit())
async def branch_handler(message: Message):
    user_id = str(message.from_user.id)  # Преобразуем ID в строку
    branch = message.text
    
    async with get_async_db() as session:
        branch_entry = await session.get(MappingBranch, branch)
        
        if branch_entry:
            branch_entry.ID_telegram = user_id  # Преобразуем ID в строку
            session.add(branch_entry)
            try:
                await session.commit()
                await message.answer("Регистрация успешна! Ваш ID привязан к серийному номеру.")
            except Exception as e:
                await message.answer(f"Ошибка базы данных: {e}")
        else:
            await message.answer("Ошибка! Введите корректный серийный номер.")

async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
