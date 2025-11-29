import os
from pathlib import Path

import pandas as pd


# === НАСТРОЙКИ ===
# Корневая папка с городами
ROOT_DIR = Path("/Users/dmitrijnazdrin/Documents/Competitors")

# Имя колонок в исходных файлах
CODE_COL = "Код товара Tabletki.ua"
PRICE_RETAIL_COL = "Цена розн."
FALLBACK_PRICE_COL = "Цена"  # на всякий случай, если "Цена розн." нет


def process_city_folder(city_dir: Path, root_dir: Path):
    """
    Обрабатывает одну папку города:
    - собирает данные из всех .xlsx файлов
    - считает минимальную цену по коду
    - сохраняет итоговый файл в root_dir/<city_name>.xlsx
    - удаляет исходные .xlsx файлы в папке города
    """
    city_name = city_dir.name
    print(f"\n▶ Обработка папки: {city_name}")

    excel_files = list(city_dir.glob("*.xlsx"))
    if not excel_files:
        print("  ⚠ Excel-файлы не найдены, пропускаю.")
        return

    frames = []

    for file_path in excel_files:
        print(f"  - читаю файл: {file_path.name}")
        try:
            df = pd.read_excel(file_path)
        except Exception as e:
            print(f"    ❌ Ошибка чтения {file_path.name}: {e}")
            continue

        # Проверяем наличие колонок
        if CODE_COL not in df.columns:
            print(f"    ⚠ Нет колонки '{CODE_COL}', файл пропускаю.")
            continue

        # Определяем, какую колонку брать как цену
        if PRICE_RETAIL_COL in df.columns:
            price_col = PRICE_RETAIL_COL
        elif FALLBACK_PRICE_COL in df.columns:
            price_col = FALLBACK_PRICE_COL
        else:
            print(
                f"    ⚠ Нет колонок '{PRICE_RETAIL_COL}' или '{FALLBACK_PRICE_COL}', "
                f"файл пропускаю."
            )
            continue

        tmp = df[[CODE_COL, price_col]].copy()
        # Переименуем цену в единый столбец "Цена"
        tmp.rename(columns={price_col: "Цена"}, inplace=True)

        # Убираем строки без кода или цены
        tmp = tmp.dropna(subset=[CODE_COL, "Цена"])

        # Приводим код к int/str, чтобы не было 1080612.0
        # Попробуем сначала к int, если не получится — к str
        try:
            tmp[CODE_COL] = tmp[CODE_COL].astype("Int64")
        except Exception:
            tmp[CODE_COL] = tmp[CODE_COL].astype(str)

        frames.append(tmp)

    if not frames:
        print("  ⚠ Не удалось собрать данные ни из одного файла, пропускаю.")
        return

    all_data = pd.concat(frames, ignore_index=True)

    # Группируем по коду и берём минимальную цену
    result = (
        all_data
        .groupby(CODE_COL, as_index=False)["Цена"]
        .min()
    )

    # Путь к итоговому файлу: в корне Competitors, имя = название папки
    output_path = root_dir / f"{city_name}.xlsx"
    result.to_excel(output_path, index=False)
    print(f"  ✅ Итоговый файл сохранён: {output_path}")

    # === УДАЛЕНИЕ ИСХОДНЫХ ФАЙЛОВ ===
    # Если хотите сначала проверить работу без удаления — закомментируйте блок ниже.
    for file_path in excel_files:
        try:
            file_path.unlink()
            print(f"  🗑 Удалён файл: {file_path.name}")
        except Exception as e:
            print(f"  ❌ Не удалось удалить {file_path.name}: {e}")


def main():
    if not ROOT_DIR.exists():
        print(f"❌ Папка не найдена: {ROOT_DIR}")
        return

    # Проходим по всем подпапкам (Kyiv, Lviv и т.п.)
    for item in ROOT_DIR.iterdir():
        if item.is_dir():
            process_city_folder(item, ROOT_DIR)


if __name__ == "__main__":
    main()