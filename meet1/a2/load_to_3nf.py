import sqlite3
import csv
import os

# Пути к БД и CSV
db_path = '3nf.db'
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
        INSERT OR IGNORE INTO Counties (county_number, county_name)
        SELECT DISTINCT county_number, county FROM TempSales WHERE county IS NOT NULL;
    ''')
    sql.execute('''
        INSERT OR IGNORE INTO Stores (store_number, store_name, address, city, zip_code, county_number, county)
        SELECT DISTINCT store_number, store_name, address, city, zip_code, county_number, county FROM TempSales;
    ''')
    sql.execute('''
        INSERT OR IGNORE INTO StoreLocations (store_number, store_location)
        SELECT DISTINCT store_number, store_location FROM TempSales;
    ''')
    sql.execute('''
        INSERT OR IGNORE INTO Categories (category, category_name)
        SELECT DISTINCT category, category_name FROM TempSales;
    ''')
    sql.execute('''
        INSERT OR IGNORE INTO Vendors (vendor_number, vendor_name)
        SELECT DISTINCT vendor_number, vendor_name FROM TempSales;
    ''')
    sql.execute('''
        INSERT OR IGNORE INTO Items (item_description, pack, bottle_volume_ml, vendor_number, category)
        SELECT DISTINCT item_description, pack, bottle_volume_ml, vendor_number, category FROM TempSales;
    ''')
    sql.execute('''
        INSERT OR IGNORE INTO ItemVolumes (item_number, bottle_volume_ml)
        SELECT DISTINCT item_number, bottle_volume_ml FROM TempSales;
    ''')
    sql.execute('''
        INSERT OR IGNORE INTO ItemPrices (item_number, state_bottle_cost, state_bottle_retail)
        SELECT DISTINCT item_number, state_bottle_cost, state_bottle_retail FROM TempSales;
    ''')
    sql.execute('''
        INSERT OR IGNORE INTO Invoices (invoice_and_item_number, date, store_number)
        SELECT DISTINCT invoice_and_item_number, date, store_number FROM TempSales;
    ''')
    sql.execute('''
        INSERT OR IGNORE INTO Sales (invoice_and_item_number, bottles_sold, sale_dollars, volume_sold_liters, volume_sold_gallons)
        SELECT DISTINCT invoice_and_item_number, bottles_sold, sale_dollars, volume_sold_liters, volume_sold_gallons FROM TempSales;
    ''')
    connection.commit()
    print("✔ Данные перенесены в нормализованные таблицы.")

def main():
    if not os.path.exists(db_path) or not os.path.exists(csv_path):
        print("❌ Файл отсутствует")
        return
    load_csv_to_temp_table()
    transfer_data_to_normalized_tables()
    print("✅ Завершено!")

main()
