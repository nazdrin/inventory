# Inventory Service
# опотрные документы для рефакторинга ui
  - docs/refactoring/22_ui_control_plane_refactor_plan.md
  - docs/refactoring/23_settings_control_plane_matrix.md
  - docs/refactoring/11_admin_settings_surface_audit.md



## Быстрые команды

- Выгрузка в `develop`:
 `сделай git develop по правилам из AGENTS.md`
- Мердж в `main`: 
`сделай merge main по правилам из AGENTS.md`

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
git commit -m "zoocompl"
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


# Biotus Pricing — Бизнес-памятка

## 1. Опт (себестоимость)

Формула:

Опт = partner_price_usd × (RSP_UAH / RSP_USD) × (1 - profit_percent)

Описание:
- partner_price_usd — цена партнёра в USD
- RSP_UAH / RSP_USD — курс Biotus
- profit_percent — процент снижения (например 20% → коэффициент 0.8)

---

## 2. Определение сегмента (band)

Сегмент определяется по опту:

- LOW — дешёвые товары
- MID — средние
- HIGH — дорогие

---

## 3. Пороговая цена (минимально допустимая)

Формула:

threshold_price =
    price_opt
    + (price_opt × BASE_THR)
    + THR_MULT_[band]
    + min_markup_threshold

Где:
- BASE_THR — базовая маржа (%)
- THR_MULT — фиксированная надбавка (грн!)
- min_markup_threshold — дополнительная фиксированная надбавка

Важно:
THR_MULT — это НЕ процент, а фиксированная сумма в гривнах.

---

## 4. Цена относительно конкурента

Формула:

under_competitor =
    competitor_price - delta

Где:

delta = ограниченный процент:
    delta = competitor_price × COMP_DISCOUNT_SHARE
    затем ограничивается:
        MIN_DELTA <= delta <= MAX_DELTA

---

## 5. Финальная цена

Главное правило:

final_price = max(threshold_price, under_competitor)

---

## 6. Логика работы

1. Считается опт
2. Формируется пороговая цена (с маржой и надбавками)
3. Считается цена чуть ниже конкурента
4. Выбирается:
   - либо цена под конкурента
   - либо порог (если ниже нельзя)

---

## 7. Примеры

### Кейc 1 — идём под конкурента

Опт = 320  
Порог = 406  
Конкурент = 450  

Результат:
final_price = 430

---

### Кейc 2 — держим порог

Опт = 320  
Порог = 406  
Конкурент = 410  

Результат:
final_price = 406

---

### Кейc 3 — дешёвый товар (LOW)

Опт = 100  
THR_MULT_LOW = 50  

Порог ≈ 168  

Даже при конкуренте 170 →  
final_price ≈ 168

---

### Кейc 4 — дорогой товар (HIGH)

Опт = 600  
THR_MULT_HIGH = 18  

Порог ≈ 676  

Конкурент = 760 →  
final_price ≈ 740

---

## 8. Ключевые правила

1. THR_MULT — фиксированная надбавка (грн), а не %
2. Чем дешевле товар — тем сильнее влияние THR_MULT
3. Цена никогда не падает ниже порога
4. Конкурент учитывается только если это не ломает маржу

---

## 9. Коротко

Цена = max(
    Опт + % маржа + фикс. надбавка,
    Конкурент - скидка
)
```
