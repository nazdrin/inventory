from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import router as developer_router
from app.database import create_tables

# Инициализация FastAPI приложения
app = FastAPI()

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешает запросы со всех источников
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    await create_tables()

# ❌ Убираем prefix, потому что он уже задан в `routes.py`
app.include_router(developer_router, tags=["Developer Panel"])

# Приветственный эндпоинт
@app.get("/")
def root():
    return {"message": "Welcome to Inventory Service"}

# Логирование всех маршрутов (для отладки)
@app.on_event("startup")
async def log_routes():
    print("🔹 Зарегистрированные маршруты:")
    for route in app.routes:
        print(f"{route.path} - {route.methods}")