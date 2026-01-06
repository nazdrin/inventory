import asyncio
import os
import logging
import html
from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.exceptions import TelegramForbiddenError
from app.database import get_async_db, MappingBranch
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CALL_DELAY_SECONDS = float(os.getenv("TELEGRAM_CALL_DELAY_SECONDS", "0"))
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()

dp.include_router(router)

@router.message(Command("start"))
async def start_handler(message: Message):
    await message.answer("Вітаємо! Будь ласка, введіть серійний номер аптеки або магазину для проходження реєстрації.")

@router.message(lambda message: message.text and message.text.isdigit())
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
            safe_orders_list = html.escape(orders_list)

            message_text = (
                "✅ <b>Нове замовлення!</b>\n\n"
                "📌 <b>Номер:</b>\n\n"
                f"{safe_orders_list}"
            )

            for user_id in user_ids:
                try:
                    await bot.send_message(
                        chat_id=int(user_id),
                        text=message_text,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                    )
                except TelegramForbiddenError:
                    # User blocked the bot (or removed chat). Remove from DB so we don't fail next time.
                    logger.warning("Bot was blocked by user_id=%s. Removing from branch=%s", user_id, branch)
                    current_ids = branch_entry.id_telegram or []
                    branch_entry.id_telegram = [uid for uid in current_ids if uid != user_id]
                    session.add(branch_entry)
                    await session.commit()
                except Exception:
                    logger.exception(
                        "Failed to send order notification to user_id=%s branch=%s",
                        user_id,
                        branch,
                    )


# New function: notify_call_request
async def notify_call_request(
    branch: str,
    id: str,
    paymentAmount: float,
    fName: str,
    lName: str,
    phone: str,
    product_name: str,
    order_date: str,
):
    """
    Отправляет уведомление в Telegram о том, что нужно позвонить клиенту.
    Функция может вызываться из стороннего сервиса.

    :param branch: серийный номер аптеки/магазина
    :param id: номер заявки
    :param paymentAmount: сумма заказа
    :param fName: имя клиента
    :param lName: фамилия клиента
    :param phone: номер телефона клиента
    :param product_name: название товара (или список товаров строкой)
    :param order_date: дата заказа
    """
    # Пауза перед отправкой сообщения, длительность берём из .env (TELEGRAM_CALL_DELAY_SECONDS)
    if CALL_DELAY_SECONDS > 0:
        await asyncio.sleep(CALL_DELAY_SECONDS)

    async with get_async_db() as session:
        branch_entry = await session.get(MappingBranch, branch)

        if not branch_entry or not branch_entry.id_telegram:
            return

        # Фильтруем неверные значения ID
        user_ids = [uid for uid in branch_entry.id_telegram if uid and uid.isdigit()]
        if not user_ids:
            return

        safe_fName = html.escape(fName or "")
        safe_lName = html.escape(lName or "")
        safe_phone = html.escape(phone or "")
        safe_id = html.escape(str(id) if id is not None else "")
        safe_product_name = html.escape(product_name or "")
        safe_order_date = html.escape(order_date or "")

        message_text = (
            "📞 <b>Нужно позвонить клиенту</b>\n\n"
            f"👤 <b>Клиент:</b> {safe_fName} {safe_lName}\n"
            f"📱 <b>Телефон:</b> {safe_phone}\n\n"
            f"📝 <b>Номер заявки:</b> {safe_id}\n"
            f"💰 <b>Товар:</b>\n{safe_product_name}\n"
            f"💵 <b>Сумма:</b> {paymentAmount}\n"
            f"📅 <b>Дата заказа:</b> {safe_order_date}"
        )

        for user_id in user_ids:
            try:
                await bot.send_message(
                    chat_id=int(user_id),
                    text=message_text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            except TelegramForbiddenError:
                logger.warning("Bot was blocked by user_id=%s. Removing from branch=%s", user_id, branch)
                current_ids = branch_entry.id_telegram or []
                branch_entry.id_telegram = [uid for uid in current_ids if uid != user_id]
                session.add(branch_entry)
                await session.commit()
            except Exception:
                logger.exception(
                    "Failed to send call request to user_id=%s branch=%s",
                    user_id,
                    branch,
                )

async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
