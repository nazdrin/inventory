# scripts/run_refusal_stub.py
import asyncio
from dotenv import load_dotenv

# подхватим .env (DATABASE_URL и т.п.)
load_dotenv()

from app.business.order_sender import _initiate_refusal_stub

# СЮДА подставь свои тестовые данные
order_obj = {
    "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "statusID": 7,  # можно не ставить — функция выставит сама
    "branchID": "30423",  # ДОЛЖЕН существовать в MappingBranch.branch
    "rows": [
        {
            "goodsCode": "260",
            "goodsName": "Актовегін,амп,2.0,n25",
            "goodsProducer": "Nycomed",
            "qtyShip": 2,
            "priceShip": 168.1,
        }
    ],
}

async def main():
    await _initiate_refusal_stub(order_obj, reason="Недостатня кількість", enterprise_code="223")
            
if __name__ == "__main__":
    asyncio.run(main())