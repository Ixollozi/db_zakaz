import sqlite3
import csv
import os

# Пути к БД и CSV
db_path = '2nf.db'
csv_path = 'iowa_liquor_sales.csv'

# Подключение к БД
connection = sqlite3.connect(db_path)
sql = connection.cursor()

def load_csv_to_temp_table():
    """Создает временную таблицу и загружает данные из CSV без пустых строк."""

    sql.execute("DROP TABLE IF EXISTS TempSales")

    sql.execute('''
        CREATE TEMP TABLE TempSales (
            invoice_and_item_number TEXT,
            date TEXT,
            store_number INTEGER,
            store_name TEXT,
            address TEXT,
            city TEXT,
            zip_code TEXT,
            store_location TEXT,
            county_number INTEGER,
            county TEXT,
            category INTEGER,
            category_name TEXT,
            vendor_number INTEGER,
            vendor_name TEXT,
            item_number INTEGER,
            item_description TEXT,
            pack INTEGER,
            bottle_volume_ml INTEGER,
            state_bottle_cost REAL,
            state_bottle_retail REAL,
            bottles_sold INTEGER,
            sale_dollars REAL,
            volume_sold_liters REAL,
            volume_sold_gallons REAL
        )
    ''')

    with open(csv_path, 'r', encoding='latin-1') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Пропускаем заголовок
        data = [row for row in csv_reader if all(row)]

        if data:
            sql.executemany('''
                INSERT INTO TempSales VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data)
            connection.commit()
            print(f"✔ Загружено {len(data)} строк в TempSales.")
        else:
            print("⚠ В CSV нет валидных данных для загрузки.")


def transfer_data_to_normalized_tables():
    sql.execute('''
        INSERT OR IGNORE INTO counties (county_number, county_name)
        SELECT DISTINCT county_number, county FROM TempSales WHERE county IS NOT NULL;
    ''')

    sql.execute('''
        INSERT OR IGNORE INTO stores (store_number, store_name, address, city, zip_code, store_location, county_id)
        SELECT DISTINCT t.store_number, t.store_name, t.address, t.city, t.zip_code, t.store_location, c.county_id
        FROM TempSales t
        JOIN counties c ON t.county = c.county_name;
    ''')

    sql.execute('''
        INSERT OR IGNORE INTO categories (category, category_name)
        SELECT DISTINCT category, category_name FROM TempSales;
    ''')

    sql.execute('''
        INSERT OR IGNORE INTO vendors (vendor_number, vendor_name)
        SELECT DISTINCT vendor_number, vendor_name FROM TempSales;
    ''')

    sql.execute('''
        INSERT OR IGNORE INTO items (item_number, item_description, pack, bottle_volume_ml, state_bottle_cost, state_bottle_retail, category_id, vendor_id)
        SELECT DISTINCT t.item_number, t.item_description, t.pack, t.bottle_volume_ml, t.state_bottle_cost, t.state_bottle_retail,
        c.category_id, v.vendor_id
        FROM TempSales t
        JOIN categories c ON t.category = c.category
        JOIN vendors v ON t.vendor_number = v.vendor_number;
    ''')

    sql.execute('''
        INSERT OR IGNORE INTO sales (invoice_and_item_number, date, store_id, item_id, bottles_sold, sale_dollars, volume_sold_liters, volume_sold_gallons)
        SELECT DISTINCT t.invoice_and_item_number, t.date, s.store_id, i.item_id, t.bottles_sold, t.sale_dollars, t.volume_sold_liters, t.volume_sold_gallons
        FROM TempSales t
        JOIN stores s ON t.store_number = s.store_number
        JOIN items i ON t.item_number = i.item_number;
    ''')

    connection.commit()
    print("✔ Данные перенесены в нормализованные таблицы.")


def main():
    """Основная логика выполнения."""

    if not os.path.exists(db_path):
        print("❌ Файл базы данных отсутствует:", db_path)
        return

    if not os.path.exists(csv_path):
        print("❌ CSV файл отсутствует:", csv_path)
        return

    load_csv_to_temp_table()  # Загружаем данные во временную таблицу
    transfer_data_to_normalized_tables()  # Переносим в нормализованные таблицы

    print("✅ Загрузка и обработка данных завершены!")


main()
