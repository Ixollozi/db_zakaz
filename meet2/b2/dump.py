import pandas as pd
from db import get_db, Store, Vendor, Item, County, DateTime, FactSales
# Параметры
models_to_dump = [Store, Vendor, Item, County, DateTime, FactSales]
output_filename = "dump.csv" # Имя выходного файла

# Получаем сессию БД
session = next(get_db())

# Открываем файл для записи
with open(output_filename, 'w', encoding='utf-8-sig') as f:
    for model in models_to_dump:
        table_name = model.__tablename__   # Имя таблицы
        records = session.query(model).all()  # Получаем все записи

        if not records:
            print(f"Пропущена пустая таблица: {table_name}")
            continue

        # Записываем заголовок таблицы
        f.write(f"### ТАБЛИЦА: {table_name} ###\n")

        # Создаем и записываем DataFrame
        df = pd.DataFrame([{k: v for k, v in row.__dict__.items() if k != '_sa_instance_state'}
                           for row in records])
        df.to_csv(f, index=False)

        # Добавляем пустую строку между таблицами
        f.write("\n")

        print(f"Экспортирована таблица: {table_name} ({len(df)} записей)")

print(f"Данные сохранены в файл '{output_filename}'")