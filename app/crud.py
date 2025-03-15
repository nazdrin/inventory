from sqlalchemy.orm import Session
from app import schemas
from app.database import InventoryData, InventoryStock, ReservedItems, DataFormat, DeveloperSettings, EnterpriseSettings


def create_inventory_data(db: Session, data: schemas.InventoryDataSchema):
    db_data = InventoryData(**data.dict())
    db.add(db_data)
    db.commit()
    db.refresh(db_data)
    return db_data

# Функции для работы с InventoryStock
def create_inventory_stock(db: Session, stock: schemas.InventoryStockSchema):
    db_stock = InventoryStock(**stock.dict())
    db.add(db_stock)
    db.commit()
    db.refresh(db_stock)
    return db_stock

# Функции для работы с ReservedItems
def create_reserved_item(db: Session, item: schemas.ReservedItemsSchema):
    db_item = ReservedItems(**item.dict())
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    return db_item

# Функции для работы с EnterpriseSettings
def create_or_update_enterprise_settings(db: Session, settings: schemas.EnterpriseSettingsSchema):
    """Создать или обновить настройки предприятия."""
    existing_settings = db.query(EnterpriseSettings).filter(
        EnterpriseSettings.enterprise_code == settings.enterprise_code
    ).first()

    if existing_settings:
        # Обновляем существующую запись
        for key, value in settings.dict().items():
            setattr(existing_settings, key, value)
        db.commit()
        db.refresh(existing_settings)
        return existing_settings
    else:
        # Создаем новую запись
        db_settings = EnterpriseSettings(**settings.dict())
        db.add(db_settings)
        db.commit()
        db.refresh(db_settings)
        return db_settings

def get_enterprise_settings(db: Session):
    """Получить все настройки предприятий."""
    return db.query(EnterpriseSettings).all()

def get_enterprise_by_code(db: Session, enterprise_code: str):
    """Получить настройки предприятия по коду."""
    return db.query(EnterpriseSettings).filter(
        EnterpriseSettings.enterprise_code == enterprise_code
    ).first()

def update_enterprise_settings(db: Session, enterprise_code: str, updated_settings: schemas.EnterpriseSettingsSchema):
    """Обновить настройки конкретного предприятия."""
    existing_settings = db.query(EnterpriseSettings).filter(
        EnterpriseSettings.enterprise_code == enterprise_code
    ).first()
    if not existing_settings:
        raise ValueError(f"Enterprise with code {enterprise_code} not found.")

    # Обновить только указанные поля
    for key, value in updated_settings.dict().items():
        if value is not None:
            setattr(existing_settings, key, value)

    db.commit()
    db.refresh(existing_settings)
    return existing_settings

# Функции для работы с DeveloperSettings
def create_or_update_developer_settings(db: Session, settings: schemas.DeveloperSettingsSchema):
    """Создать или обновить настройки разработчика."""
    existing_settings = db.query(DeveloperSettings).filter(
        DeveloperSettings.developer_login == settings.developer_login
    ).first()

    if existing_settings:
        # Обновляем существующую запись
        for key, value in settings.dict().items():
            setattr(existing_settings, key, value)
        db.commit()
        db.refresh(existing_settings)
        return existing_settings
    else:
        # Создаем новую запись
        db_settings = DeveloperSettings(**settings.dict())
        db.add(db_settings)
        db.commit()
        db.refresh(db_settings)
        return db_settings

def get_developer_settings(db: Session):
    """Получить настройки всех разработчиков."""
    return db.query(DeveloperSettings).all()

def update_developer_settings(db: Session, login: str, updated_settings: schemas.DeveloperSettingsSchema):
    """Обновить настройки разработчика."""
    existing_setting = db.query(DeveloperSettings).filter(DeveloperSettings.developer_login == login).first()
    
    if not existing_setting:
        raise ValueError(f"Developer setting with login '{login}' not found.")
    
    # Если логин меняется, проверьте на конфликт
    if updated_settings.developer_login and updated_settings.developer_login != login:
        conflict = db.query(DeveloperSettings).filter(
            DeveloperSettings.developer_login == updated_settings.developer_login
        ).first()
        if conflict:
            raise ValueError(f"Developer login '{updated_settings.developer_login}' already exists.")
    
    for key, value in updated_settings.dict().items():
        setattr(existing_setting, key, value)
    
    db.commit()
    db.refresh(existing_setting)
    return existing_setting

# Функции для работы с DataFormat
def get_data_formats(db: Session):
    """Получить список доступных форматов данных."""
    return db.query(DataFormat).all()

def add_data_format(db: Session, format_name: str):
    """Добавить новый формат данных."""
    existing_format = db.query(DataFormat).filter(
        DataFormat.format_name == format_name
    ).first()

    if existing_format:
        raise ValueError(f"Data format '{format_name}' already exists.")

    data_format = DataFormat(format_name=format_name)
    db.add(data_format)
    db.commit()
    db.refresh(data_format)
    return data_format

def delete_data_format(db: Session, format_name: str):
    """Удалить формат данных."""
    data_format = db.query(DataFormat).filter(
        DataFormat.format_name == format_name
    ).first()

    if not data_format:
        raise ValueError(f"Data format '{format_name}' not found.")

    db.delete(data_format)
    db.commit()
    return {"message": f"Data format '{format_name}' successfully deleted."}