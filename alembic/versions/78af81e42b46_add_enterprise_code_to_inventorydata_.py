from alembic import op
import sqlalchemy as sa

# Имя новой миграции
revision = '78af81e42b46'
down_revision = '8a30f8954e26'
branch_labels = None
depends_on = None

def upgrade():
    # 1. Очищаем таблицы перед миграцией
    op.execute("DELETE FROM inventory_stock;")
    op.execute("DELETE FROM inventory_data;")

    # 2. Добавляем колонку enterprise_code
    op.add_column('inventory_data', sa.Column('enterprise_code', sa.String(), nullable=False))
    op.add_column('inventory_stock', sa.Column('enterprise_code', sa.String(), nullable=False))

    # 3. Добавляем внешние ключи
    op.create_foreign_key("fk_inventory_data_enterprise", "inventory_data", "enterprise_settings", ["enterprise_code"], ["enterprise_code"])
    op.create_foreign_key("fk_inventory_stock_enterprise", "inventory_stock", "enterprise_settings", ["enterprise_code"], ["enterprise_code"])

def downgrade():
    # Удаление внешних ключей
    op.drop_constraint("fk_inventory_data_enterprise", "inventory_data", type_="foreignkey")
    op.drop_constraint("fk_inventory_stock_enterprise", "inventory_stock", type_="foreignkey")

    # Удаление колонок
    op.drop_column('inventory_data', 'enterprise_code')
    op.drop_column('inventory_stock', 'enterprise_code')