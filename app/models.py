from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Date,
    Float,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    Numeric,
    PrimaryKeyConstraint,
    CheckConstraint,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import declarative_base, declarative_mixin
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()

# Функция для получения текущего времени с учётом временной зоны
def now_with_timezone():
    return datetime.now(ZoneInfo("Europe/Kiev"))

# Mixin для временных меток
@declarative_mixin
class TimestampMixin:
    # Значение по умолчанию задаём на стороне БД (server_default=func.now()),
    # чтобы вставки через SQL или миграции не падали. Разрешаем NULL,
    # чтобы не конфликтовать при добавлении колонок в таблицы с данными.
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),   # ← дефолт в БД (NOW() в Postgres)
        nullable=True,               # ← допускаем NULL для совместимости миграций
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),   # ← первичное значение в БД
        onupdate=func.now(),         # ← при UPDATE проставит ORM (когда обновляешь через SQLAlchemy)
        nullable=True,               # ← то же самое по причинам выше
    )
# Таблица номенклатуры
class InventoryData(Base, TimestampMixin):
    __tablename__ = "inventory_data"

    code = Column(String, nullable=False)
    name = Column(String, nullable=False)
    producer = Column(String, nullable=False)
    vat = Column(Float, nullable=False)
    morion = Column(String, nullable=True)
    tabletki = Column(String, nullable=True)
    barcode = Column(String, nullable=True)
    optima = Column(String, nullable=True)
    badm = Column(String, nullable=True)
    venta = Column(String, nullable=True)
    enterprise_code = Column(String, ForeignKey("enterprise_settings.enterprise_code"), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("code", "enterprise_code"),
    )


# Таблица остатков
class InventoryStock(Base, TimestampMixin):
    __tablename__ = "inventory_stock"

    branch = Column(String, primary_key=True)
    code = Column(String, primary_key=True)
    price = Column(Numeric(12, 2), nullable=False)
    qty = Column(Integer, nullable=False)
    price_reserve = Column(Numeric(12, 2), nullable=True)
    enterprise_code = Column(String, ForeignKey("enterprise_settings.enterprise_code"), nullable=False)

    __table_args__ = (
        CheckConstraint(
            "(price_reserve IS NULL) OR (price_reserve <= price)",
            name="ck_inventory_stock_price_reserve_le_price"
        ),
        Index("ix_inventory_stock_ent_code", "enterprise_code"),
        Index("ix_inventory_stock_ent_code_item", "enterprise_code", "code"),
        Index("ix_inventory_stock_available", "enterprise_code",
              postgresql_where=text("qty > 0")),
    )

# Таблица забронированных товаров
class ReservedItems(Base):
    __tablename__ = "reserved_items"
    branch = Column(String, primary_key=True)
    code = Column(String, primary_key=True)
    qty = Column(Integer, nullable=False)

# Таблица форматов данных
class DataFormat(Base):
    __tablename__ = "data_formats"
    id = Column(Integer, primary_key=True, index=True)
    format_name = Column(String, unique=True, nullable=False)

# Таблица настроек предприятий
class EnterpriseSettings(Base, TimestampMixin):
    __tablename__ = "enterprise_settings"
    enterprise_code = Column(String, primary_key=True)
    enterprise_name = Column(String, nullable=False)
    branch_id = Column(String, nullable=False)
    tabletki_login = Column(String, nullable=True)
    tabletki_password = Column(String, nullable=True)
    token = Column(String, nullable=True)
    stock_upload_frequency = Column(Integer, nullable=True)
    catalog_upload_frequency = Column(Integer, nullable=True)
    google_drive_folder_id_ref = Column(String, nullable=True)
    google_drive_folder_id_rest = Column(String, nullable=True)
    data_format = Column(String, nullable=True)
    discount_rate = Column(Float, nullable=True)
    single_store = Column(Boolean, nullable=False, server_default=text("false"))
    order_fetcher = Column(Boolean, nullable=False, server_default=text("false"))
    auto_confirm = Column(Boolean, nullable=False, server_default=text("false"))
    store_serial = Column(String, nullable=True)
    last_stock_upload = Column(DateTime, nullable=True)
    last_catalog_upload = Column(DateTime, nullable=True)
    stock_correction = Column(Boolean, nullable=False, server_default=text("false"))

    __table_args__ = (
        Index("ix_enterprise_settings_data_format", "data_format"),
        Index("ix_enterprise_settings_single_store", "single_store"),
    )

# Сопоставление аптек 
class MappingBranch(Base):
    __tablename__ = "mapping_branch"
    enterprise_code = Column(String, nullable=False)
    branch = Column(String, primary_key=True)
    store_id = Column(String, nullable=False)
    google_folder_id = Column(String, nullable=True)
    id_telegram = Column(ARRAY(String), nullable=True)

# Таблица глобальных настроек разработчика
class DeveloperSettings(Base):
    __tablename__ = "developer_settings"
    developer_login = Column(String, primary_key=True)
    developer_password = Column(String, nullable=False)
    endpoint_catalog = Column(String, nullable=True)
    endpoint_stock = Column(String, nullable=True)
    endpoint_orders = Column(String, nullable=True)
    telegram_token_developer = Column(String, nullable=True)
    message_orders = Column(Boolean, nullable=True)
    morion = Column(String, nullable=True)
    tabletki = Column(String, nullable=True)
    barcode = Column(String, nullable=True)
    optima = Column(String, nullable=True)
    badm = Column(String, nullable=True)
    venta = Column(String, nullable=True)

# Таблица уведомлений
class ClientNotifications(Base, TimestampMixin):
    __tablename__ = "client_notifications"
    id = Column(Integer, primary_key=True, autoincrement=True)
    enterprise_code = Column(String, nullable=False)
    message = Column(String, nullable=False)
    is_read = Column(Boolean, default=False)



class CatalogMapping(Base):
    __tablename__ = "catalog_mapping"
    ID = Column(String, primary_key=True)
    Name = Column(String, nullable=True)
    Producer = Column(String, nullable=True)
    Barcode = Column(String, nullable=True)
    Guid = Column(String, nullable=True)
    Code_Tabletki = Column(String, nullable=True)
    Name_D1  = Column(String, nullable=True, server_default=text("''"))
    Name_D2  = Column(String, nullable=True, server_default=text("''"))
    Name_D3  = Column(String, nullable=True, server_default=text("''"))
    Name_D4  = Column(String, nullable=True, server_default=text("''"))
    Name_D5  = Column(String, nullable=True, server_default=text("''"))
    Name_D6  = Column(String, nullable=True, server_default=text("''"))
    Name_D7  = Column(String, nullable=True, server_default=text("''"))
    Name_D8  = Column(String, nullable=True, server_default=text("''"))
    Name_D9  = Column(String, nullable=True, server_default=text("''"))
    Name_D10 = Column(String, nullable=True, server_default=text("''"))
    Code_D1  = Column(String, nullable=True, server_default=text("''"))
    Code_D2  = Column(String, nullable=True, server_default=text("''"))
    Code_D3  = Column(String, nullable=True, server_default=text("''"))
    Code_D4  = Column(String, nullable=True, server_default=text("''"))
    Code_D5  = Column(String, nullable=True, server_default=text("''"))
    Code_D6  = Column(String, nullable=True, server_default=text("''"))
    Code_D7  = Column(String, nullable=True, server_default=text("''"))
    Code_D8  = Column(String, nullable=True, server_default=text("''"))
    Code_D9  = Column(String, nullable=True, server_default=text("''"))
    Code_D10 = Column(String, nullable=True, server_default=text("''"))

    Name_D11 = Column(String, nullable=True, server_default=text("''"))
    Name_D12 = Column(String, nullable=True, server_default=text("''"))
    Name_D13 = Column(String, nullable=True, server_default=text("''"))
    Name_D14 = Column(String, nullable=True, server_default=text("''"))
    Name_D15 = Column(String, nullable=True, server_default=text("''"))
    Name_D16 = Column(String, nullable=True, server_default=text("''"))
    Name_D17 = Column(String, nullable=True, server_default=text("''"))
    Name_D18 = Column(String, nullable=True, server_default=text("''"))
    Name_D19 = Column(String, nullable=True, server_default=text("''"))
    Name_D20 = Column(String, nullable=True, server_default=text("''"))

    Code_D11 = Column(String, nullable=True, server_default=text("''"))
    Code_D12 = Column(String, nullable=True, server_default=text("''"))
    Code_D13 = Column(String, nullable=True, server_default=text("''"))
    Code_D14 = Column(String, nullable=True, server_default=text("''"))
    Code_D15 = Column(String, nullable=True, server_default=text("''"))
    Code_D16 = Column(String, nullable=True, server_default=text("''"))
    Code_D17 = Column(String, nullable=True, server_default=text("''"))
    Code_D18 = Column(String, nullable=True, server_default=text("''"))
    Code_D19 = Column(String, nullable=True, server_default=text("''"))
    Code_D20 = Column(String, nullable=True, server_default=text("''"))

class DropshipEnterprise(Base, TimestampMixin):
    __tablename__ = "dropship_enterprises"

    code = Column(String, primary_key=True, index=True, doc="Уникальный код предприятия")
    name = Column(String, nullable=False, doc="Название предприятия")

    # Эти поля могут отсутствовать, поэтому разрешаем NULL
    feed_url = Column(String, nullable=True, doc="Ссылка на прайс-фид")
    gdrive_folder = Column(String, nullable=True, doc="Папка на Google Drive")
    city = Column(String, nullable=True, doc="Город")

    # Флаги — NOT NULL + дефолт на уровне БД (устойчиво к «пустым» вставкам)
    is_rrp = Column(Boolean, nullable=False, server_default=text("false"), doc="Флаг — есть ли РРЦ")
    is_wholesale = Column(Boolean, nullable=False, server_default=text("true"), doc="Флаг — опт или розница")
    is_active = Column(Boolean, nullable=False, server_default=text("true"), doc="Флаг активности")
    api_orders_enabled = Column(Boolean, nullable=False, server_default=text("false"), doc="Флаг — заказы через API")
    weekend_work = Column(Boolean, nullable=False, server_default=text("false"), doc="Флаг — работает в выходные")
    use_feed_instead_of_gdrive = Column(Boolean, nullable=False, server_default=text("true"),
                                        doc="Флаг — использовать ФИД (если False — Google Drive)")

    # Числа — пусть будут NULL, если не заданы
    profit_percent = Column(Float, nullable=True, doc="Процент заработка")
    retail_markup = Column(Float, nullable=True, doc="Наценка для розницы")
    min_markup_threshold = Column(Float, nullable=True, doc="Минимальный порог наценки")

    # Приоритет — NOT NULL с дефолтом и ограничением диапазона
    priority = Column(Integer, nullable=False, server_default=text("5"), doc="Приоритет (1–10)")

    __table_args__ = (
        Index("ix_dropship_enterprises_is_active", "is_active"),
        Index("ix_dropship_enterprises_priority", "priority"),
        CheckConstraint("priority BETWEEN 1 AND 10", name="ck_dropship_enterprises_priority_range"),
    )
class CompetitorPrice(Base, TimestampMixin):
    __tablename__ = "competitor_prices"

    # Составной первичный ключ
    code = Column(String, primary_key=True, index=True, doc="Код товара")
    city = Column(String, primary_key=True, doc="Город")
    competitor_price = Column(Numeric(12, 2), nullable=False, doc="Цена конкурента")

    __table_args__ = (
        CheckConstraint("competitor_price >= 0", name="ck_competitor_prices_price_nonneg"),
        Index("ix_competitor_prices_city", "city"),
        Index("ix_competitor_prices_code", "code"),
    )
class Offer(Base):
    __tablename__ = "offers"

    id = Column(BigInteger, primary_key=True)  # службовий PK
    product_code  = Column(String, nullable=False, index=True, doc="Загальний код товару")
    supplier_code = Column(String, nullable=False, index=True, doc="Код/назва постачальника")
    city          = Column(String, nullable=False, index=True, doc="Місто")
    price         = Column(Numeric(12, 2), nullable=False, doc="Ціна у валюті currency")

    # Оптовая цена (опционально для совместимости)
    wholesale_price = Column(
        Numeric(12, 2),
        nullable=True,
        doc="Оптовая цена"
    )

    stock         = Column(Integer, nullable=False, default=0, doc="Доступний залишок ≥0")
    updated_at    = Column(DateTime(timezone=True), nullable=False,
                           server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("product_code", "supplier_code", "city",
                         name="uq_offers_product_supplier_city"),
        Index("ix_offers_city_product_price", "city", "product_code", "price"),
        Index("ix_offers_city_stock_pos", "city",
              postgresql_where=text("stock > 0")),
    )


# Журнал применённых политик балансировщика (что именно было передано в pricing engine на конкретный сегмент).
class BalancerPolicyLog(Base, TimestampMixin):
    """Журнал применённых политик балансировщика (что именно было передано в pricing engine на конкретный сегмент)."""

    __tablename__ = "balancer_policy_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # TEST / LIVE
    mode = Column(String, nullable=False, doc="Режим балансировщика: TEST или LIVE")

    # Версия/снимок конфига на момент применения (чтобы история была воспроизводимой)
    config_version = Column(Integer, nullable=False, doc="Версия конфигурации балансировщика")
    config_snapshot = Column(JSONB, nullable=True, doc="Снимок ключевых настроек конфига на момент применения")

    # Контекст
    city = Column(String, nullable=False, index=True, doc="Город (scope)")
    supplier = Column(String, nullable=False, index=True, doc="Код поставщика (scope)")
    segment_id = Column(String, nullable=False, index=True, doc="ID временного сегмента")
    segment_start = Column(DateTime(timezone=True), nullable=False, doc="Фактическое начало сегмента")
    segment_end = Column(DateTime(timezone=True), nullable=False, doc="Фактическое окончание сегмента")

    # Payload политики
    rules = Column(JSONB, nullable=False, doc="Правила: список объектов {band_id, porog}")
    min_porog_by_band = Column(JSONB, nullable=False, doc="Минимальные пороги по диапазонам на момент применения")

    # Почему применили именно так
    reason = Column(String, nullable=False, doc="Причина: schedule/best_30d/challenger/fallback/cooldown_hold")
    reason_details = Column(JSONB, nullable=True, doc="Детали причины (шаг графика, ожидания/факты и т.п.)")

    # Технические поля
    hash = Column(String, nullable=True, index=True, doc="Хэш для защиты от дублей при повторных запусках")
    is_applied = Column(Boolean, nullable=False, server_default=text("true"), doc="Успешно ли политика была применена")

    __table_args__ = (
        Index("ix_balancer_policy_scope", "city", "supplier", "segment_id", "segment_start"),
        CheckConstraint("mode IN ('TEST','LIVE')", name="ck_balancer_policy_log_mode"),
    )


# Агрегированная статистика по результатам одного временного сегмента
class BalancerSegmentStats(Base, TimestampMixin):
    """Агрегированная статистика по результатам одного временного сегмента.

    Заполняется в конце сегмента (job EndSegment) и обязательно ссылается на policy_log_id,
    чтобы можно было однозначно связать результат с применённой политикой.

    Единица учёта: заказ (order_id). Доставку НЕ учитываем (sale_sum = сумма товаров).
    """

    __tablename__ = "balancer_segment_stats"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Контекст профиля/режима
    profile_name = Column(String, nullable=False, index=True, doc="Имя профиля из конфигурации (например kyiv_test_dsn)")
    mode = Column(String, nullable=False, doc="Режим: TEST или LIVE")

    # Ссылка на применённую политику
    policy_log_id = Column(BigInteger, ForeignKey("balancer_policy_log.id"), nullable=False, index=True)

    # Scope
    city = Column(String, nullable=False, index=True)
    supplier = Column(String, nullable=False, index=True, doc="Код поставщика (например D1/D2)")

    # Временной сегмент
    segment_id = Column(String, nullable=False, index=True)
    segment_start = Column(DateTime(timezone=True), nullable=False)
    segment_end = Column(DateTime(timezone=True), nullable=False)

    # Ценовой диапазон
    band_id = Column(String, nullable=False, index=True)
    band_min_price = Column(Numeric(12, 2), nullable=False, doc="Нижняя граница диапазона")
    band_max_price = Column(Numeric(12, 2), nullable=True, doc="Верхняя граница диапазона (NULL = бесконечность)")

    # Порог и минимальный порог
    porog_used = Column(Numeric(6, 4), nullable=False, doc="Порог, применённый для этого band в сегменте")
    min_porog = Column(Numeric(6, 4), nullable=False, doc="Минимальный порог для band (15/13/11%)")

    # Метрики (доставка НЕ учитывается)
    orders_count = Column(Integer, nullable=False, server_default=text("0"))
    sale_sum = Column(Numeric(14, 2), nullable=False, server_default=text("0"), doc="Σ суммы товаров (без доставки)")
    cost_sum = Column(Numeric(14, 2), nullable=False, server_default=text("0"), doc="Σ себестоимости")

    profit_sum = Column(Numeric(14, 2), nullable=False, server_default=text("0"), doc="Σ (sale_sum - cost_sum)")
    min_profit_sum = Column(Numeric(14, 2), nullable=False, server_default=text("0"), doc="Σ (sale_sum * min_porog)")
    excess_profit_sum = Column(Numeric(14, 2), nullable=False, server_default=text("0"), doc="profit_sum - min_profit_sum")

    excess_profit_per_order = Column(Numeric(14, 4), nullable=True, doc="excess_profit_sum / orders_count")

    # Дневные показатели (для режима лимита/долей)
    day_date = Column(Date, nullable=False, doc="День учёта (для ночных сегментов фиксируем правило: дата старта сегмента)")
    day_total_orders = Column(Integer, nullable=True, doc="Всего заказов в день (по выбранной области учёта)")
    segment_share = Column(Numeric(10, 6), nullable=True, doc="orders_count / day_total_orders")

    # Качество выборки
    orders_sample_ok = Column(Boolean, nullable=False, server_default=text("false"), doc="orders_count >= min_orders_per_segment")
    note = Column(String, nullable=True)

    __table_args__ = (
        Index(
            "ix_balancer_seg_scope",
            "city",
            "supplier",
            "segment_id",
            "band_id",
            "segment_start",
        ),
        CheckConstraint("mode IN ('TEST','LIVE')", name="ck_balancer_segment_stats_mode"),
        CheckConstraint("porog_used >= min_porog", name="ck_balancer_seg_porog_ge_min"),
        CheckConstraint("orders_count >= 0", name="ck_balancer_seg_orders_nonneg"),
        CheckConstraint("sale_sum >= 0", name="ck_balancer_seg_sale_nonneg"),
        CheckConstraint("cost_sum >= 0", name="ck_balancer_seg_cost_nonneg"),
    )


# Факты по каждому заказу, попавшему в расчёт сегмента.
class BalancerOrderFacts(Base, TimestampMixin):
    """Факты по каждому заказу, попавшему в расчёт сегмента.

    Заполняется в конце сегмента (job EndSegment) после выгрузки заказов из SalesDrive.
    Нужна для дебага/аудита: какие именно заказы попали в статистику и какие у них расчётные поля.

    Единица учёта: заказ (order_id). Доставку НЕ учитываем (sale_price = сумма товаров).
    """

    __tablename__ = "balancer_order_facts"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Ссылка на применённую политику
    policy_log_id = Column(BigInteger, ForeignKey("balancer_policy_log.id"), nullable=False, index=True)

    # Контекст профиля/режима
    profile_name = Column(String, nullable=False, index=True, doc="Имя профиля из конфигурации")
    mode = Column(String, nullable=False, doc="Режим: TEST или LIVE")

    # Scope
    city = Column(String, nullable=False, index=True)
    supplier = Column(String, nullable=False, index=True, doc="Код поставщика (например D1/D2)")

    # Временной сегмент
    segment_id = Column(String, nullable=False, index=True)
    segment_start = Column(DateTime(timezone=True), nullable=False)
    segment_end = Column(DateTime(timezone=True), nullable=False)

    # Идентификаторы заказа (SalesDrive)
    order_id = Column(String, nullable=False, index=True, doc="ID заказа в SalesDrive")
    order_number = Column(String, nullable=True, doc="Человекочитаемый номер заказа (если есть)")
    status_id = Column(Integer, nullable=False, doc="Статус заказа, с которым он попал в выборку")
    created_at_source = Column(DateTime(timezone=True), nullable=True, doc="Время создания заказа в источнике")

    # Ценовой диапазон
    band_id = Column(String, nullable=False, index=True)
    band_min_price = Column(Numeric(12, 2), nullable=False)
    band_max_price = Column(Numeric(12, 2), nullable=True)

    # Цены/себестоимость (доставка НЕ учитывается)
    sale_price = Column(Numeric(14, 2), nullable=False, doc="Сумма товаров в заказе (без доставки)")
    cost = Column(Numeric(14, 2), nullable=False, doc="Себестоимость заказа")
    profit = Column(Numeric(14, 2), nullable=False, doc="sale_price - cost")

    # Порог и минимальный порог
    porog_used = Column(Numeric(6, 4), nullable=False, doc="Порог, действовавший в сегменте для данного band")
    min_porog = Column(Numeric(6, 4), nullable=False, doc="Минимальный порог для band")

    # Расчётные поля
    min_profit = Column(Numeric(14, 2), nullable=False, doc="sale_price * min_porog")
    excess_profit = Column(Numeric(14, 2), nullable=False, doc="profit - min_profit")

    # Качество/служебное
    is_in_scope = Column(Boolean, nullable=False, server_default=text("true"), doc="Флаг: учтён ли заказ в расчёте")
    note = Column(String, nullable=True)

    # Сырой payload (часть заказа) для расследований
    raw = Column(JSONB, nullable=True, doc="Сырой JSON заказа/полей из SalesDrive")

    __table_args__ = (
        UniqueConstraint("policy_log_id", "order_id", name="uq_balancer_order_facts_policy_order"),
        Index(
            "ix_balancer_order_facts_scope",
            "city",
            "supplier",
            "segment_id",
            "band_id",
            "segment_start",
        ),
        CheckConstraint("mode IN ('TEST','LIVE')", name="ck_balancer_order_facts_mode"),
        CheckConstraint("sale_price >= 0", name="ck_balancer_order_facts_sale_nonneg"),
        CheckConstraint("cost >= 0", name="ck_balancer_order_facts_cost_nonneg"),
    )


# Состояние LIVE-режима балансировщика (суточные лимиты, итерации, «freeze»)
class BalancerLiveState(Base, TimestampMixin):
    """Состояние LIVE-режима балансировщика на день по поставщику.

    Хранит счётчики/итерации и ссылку на последнюю применённую политику,
    чтобы корректно реализовать дневные лимиты (freeze/soften/stop).

    Ключ: (mode, supplier, day_date)
    """

    __tablename__ = "balancer_live_state"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Режим (сейчас используем только LIVE, поле оставляем для единообразия)
    mode = Column(String, nullable=False, doc="Режим: LIVE")

    # Поставщик (D1/D2/...)
    supplier = Column(String, nullable=False, index=True, doc="Код поставщика")

    # День учёта (локальная логика дня — как в segment_stats: дата старта сегмента)
    day_date = Column(Date, nullable=False, index=True, doc="День учёта")

    # Итерация LIVE-логики (для ограничений типа max 10 итераций)
    live_iter = Column(Integer, nullable=False, server_default=text("0"), doc="Текущая итерация LIVE")

    # Кеш счётчика заказов за день (можно пересчитывать из facts, но хранить удобно)
    day_orders_count = Column(Integer, nullable=False, server_default=text("0"), doc="Заказов за день (кеш)")

    # Последняя применённая политика (нужно для freeze)
    last_policy_log_id = Column(BigInteger, ForeignKey("balancer_policy_log.id"), nullable=True, index=True)

    # Технический флаг: достигнут ли лимит (опционально, удобно для дебага)
    is_limit_reached = Column(Boolean, nullable=False, server_default=text("false"), doc="Достигнут дневной лимит")

    # Метрики LIVE-логики (baseline/best/last) для сравнения и остановок
    baseline_metric = Column(Float, nullable=True, doc="Базовая метрика (эталон) для сравнения в LIVE")
    best_metric = Column(Float, nullable=True, doc="Лучшая метрика за день (best run)")
    best_iter = Column(Integer, nullable=False, server_default=text("0"), doc="Итерация, на которой получен best_metric")
    last_metric = Column(Float, nullable=True, doc="Метрика последнего закрытого сегмента")

    # Снимок лучших правил (best run) — нужен, чтобы откатываться/фиксировать лучший прогон
    best_rules = Column(JSONB, nullable=True, doc="Снимок правил порогов на лучшем прогоне (список {band_id, porog})")

    # Чтобы не пересчитывать один и тот же сегмент повторно
    last_segment_end = Column(DateTime(timezone=True), nullable=True, doc="Конец последнего сегмента, учтённого в LIVE")

    # Идемпотентность: ключ последнего учтённого прогона (segment_id + segment_end)
    last_run_key = Column(String, nullable=True, doc="Ключ последнего учтённого прогона (для защиты от двойного инкремента)")

    # Управляющие флаги остановок/заморозки
    stop_reason = Column(String, nullable=True, doc="Причина остановки LIVE (max_iter/degrade/limit/manual/etc)")
    is_frozen = Column(Boolean, nullable=False, server_default=text("false"), doc="Заморожен ли LIVE (не менять правила)")

    __table_args__ = (
        UniqueConstraint("mode", "supplier", "day_date", name="uq_balancer_live_state_key"),
        Index("ix_balancer_live_state_scope", "supplier", "day_date"),
        CheckConstraint("mode IN ('LIVE')", name="ck_balancer_live_state_mode"),
    )


# Состояние тестового графика балансировщика (чтобы TEST был воспроизводимым между перезапусками)
class BalancerTestState(Base, TimestampMixin):
    """Состояние тестового графика для конкретного дня/сегмента/диапазона.

    Используется в режиме TEST при старте сегмента:
      - берём current_porog как porog_used
      - пересчитываем следующий current_porog по step и direction
      - сохраняем обратно

    Ключ состояния: (profile_name, city, supplier, segment_id, band_id, day_date).
    """

    __tablename__ = "balancer_test_state"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    profile_name = Column(String, nullable=False, index=True)
    mode = Column(String, nullable=False, doc="Режим: TEST (оставляем поле для единообразия)")

    city = Column(String, nullable=False, index=True)
    supplier = Column(String, nullable=False, index=True)
    segment_id = Column(String, nullable=False, index=True)
    band_id = Column(String, nullable=False, index=True)

    # Дата учёта (правило: дата старта сегмента, в т.ч. для ночных сегментов)
    day_date = Column(Date, nullable=False, index=True)

    # Состояние графика
    current_porog = Column(Numeric(6, 4), nullable=False, doc="Текущий порог, который будет применён в сегменте")
    step = Column(Numeric(6, 4), nullable=False, doc="Шаг изменения порога (например 0.01)")
    min_porog = Column(Numeric(6, 4), nullable=False, doc="Нижняя граница (из min_porog_by_band)")
    max_porog = Column(Numeric(6, 4), nullable=False, doc="Верхняя граница теста (например 0.25)")
    direction = Column(SmallInteger, nullable=False, doc="Направление изменения: 1 или -1")

    # Техническое
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "profile_name",
            "city",
            "supplier",
            "segment_id",
            "band_id",
            "day_date",
            name="uq_balancer_test_state_key",
        ),
        Index(
            "ix_balancer_test_state_scope",
            "city",
            "supplier",
            "segment_id",
            "band_id",
            "day_date",
        ),
        CheckConstraint("mode IN ('TEST')", name="ck_balancer_test_state_mode"),
        CheckConstraint("direction IN (1, -1)", name="ck_balancer_test_state_direction"),
        CheckConstraint("current_porog >= min_porog", name="ck_balancer_test_state_porog_ge_min"),
        CheckConstraint("current_porog <= max_porog", name="ck_balancer_test_state_porog_le_max"),
        CheckConstraint("max_porog >= min_porog", name="ck_balancer_test_state_max_ge_min"),
    )