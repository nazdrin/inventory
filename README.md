# Inventory Service

## Локальный запуск

### 1. Перейти в проект

```bash
cd /Users/dmitrijnazdrin/inventory_service_1
```

### 2. Активировать виртуальное окружение

```bash
source .venv/bin/activate

### Гит
```
git add .
git commit -m "protein"
git push origin develop

### 3. Установить зависимости Python

```bash
pip install -r requirements.txt
```

### 4. Запустить основные серверы

Backend:

```bash
python3 -m uvicorn app.main:app --reload
```

Frontend:

```bash
npm start
```

## Фоновые сервисы и планировщики

Запускать по необходимости:

```bash
python -m app.services.catalog_scheduler_service
python -m app.services.stock_scheduler_service
python -m app.services.order_scheduler_service
python -m app.services.competitor_price_scheduler
python -m app.services.telegram_bot

python app/services/biotus_check_order_scheduler.py
```

## Работа с PostgreSQL

Подключение к БД:

```bash
psql -U postgres -d inventory_db
```

Полезные команды в `psql`:

```sql
\dt
\q
```

## Полезные SQL-запросы

```sql
SELECT * FROM enterprise_settings;
SELECT * FROM developer_settings;
SELECT * FROM mapping_branch;
SELECT * FROM dropship_enterprises;
SELECT * FROM catalog_mapping;
SELECT * FROM offers;
SELECT * FROM client_notifications;
SELECT * FROM competitor_prices;
```

## Строка подключения к БД

```python
DATABASE_URL = "postgresql+asyncpg://postgres:your_password@localhost/inventory_db"
```

