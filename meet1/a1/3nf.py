import sqlite3

# Подключение к базе данных
connection = sqlite3.connect("3nf.db")
sql = connection.cursor()



# 1. Таблица магазинов
sql.execute('''
CREATE TABLE IF NOT EXISTS Stores (
    store_number INTEGER PRIMARY KEY,-- Уникальный идентификатор магазина,
    store_name TEXT ,
    address TEXT ,
    city TEXT ,
    zip_code TEXT ,
    store_location TEXT ,
    county_number INTEGER,
    county TEXT 
)
''')

# 2. Таблица категорий
sql.execute('''
CREATE TABLE IF NOT EXISTS Categories (
    category INTEGER PRIMARY KEY,-- Уникальный идентификатор категории
    category_name TEXT-- Название категории
)
''')

# 3. Таблица поставщиков
sql.execute('''
CREATE TABLE IF NOT EXISTS Vendors (
    vendor_number INTEGER PRIMARY KEY,-- Уникальный идентификатор поставщика
    vendor_name TEXT-- Название поставщика
)
''')

# 4. Таблица товаров
sql.execute('''
CREATE TABLE IF NOT EXISTS Items (
    item_description TEXT,-- Описание товара
    pack INTEGER,-- Количество в упаковке
    bottle_volume_ml INTEGER,-- Объем бутылки в миллилитрах
    vendor_number INTEGER,-- Связь с таблицей поставщиков
    category INTEGER,-- Связь с таблицей категорий
    FOREIGN KEY (vendor_number) REFERENCES Vendors(vendor_number),-- Внешний ключ для поставщиков
    FOREIGN KEY (category) REFERENCES Categories(category)-- Внешний ключ для категорий
)
''')

# 5. Таблица цен на товары
sql.execute('''
CREATE TABLE IF NOT EXISTS ItemPrices (
    item_number INTEGER PRIMARY KEY,-- Уникальный идентификатор товара
    state_bottle_cost REAL,-- Себестоимость бутылки
    state_bottle_retail REAL,-- Розничная цена бутылки
    FOREIGN KEY (item_number) REFERENCES Items(item_number)-- Внешний ключ для товаров
)
''')

# 6. Таблица накладных
sql.execute('''
CREATE TABLE IF NOT EXISTS Invoices (
    invoice_and_item_number TEXT PRIMARY KEY,-- Уникальный номер счета-фактуры
    date TEXT,-- Дата
    store_number INTEGER,-- Связь с таблицей магазинов
    FOREIGN KEY (store_number) REFERENCES Stores(store_number)-- Внешний ключ для магазинов
)
''')

# 7. Таблица продаж по накладным
sql.execute('''
CREATE TABLE IF NOT EXISTS Sales (
    sale_id INTEGER PRIMARY KEY AUTOINCREMENT,-- Уникальный идентификатор продажи
    invoice_and_item_number TEXT,-- Уникальный номер счета-фактуры
    bottles_sold INTEGER,-- Количество проданных бутылок
    sale_dollars REAL,-- Сумма продажи
    volume_sold_liters REAL,-- Объем проданного товара
    volume_sold_gallons REAL,-- Объем проданного товара
    FOREIGN KEY (invoice_and_item_number) REFERENCES Invoices(invoice_and_item_number)-- Внешний ключ для накладных
)
''')

# 8. Таблица округов
sql.execute('''
CREATE TABLE IF NOT EXISTS Counties (
    county_number INTEGER PRIMARY KEY,-- Уникальный идентификатор округа
    county_name TEXT-- Название округа
)
''')

# 9. Таблица локаций магазинов
sql.execute('''
CREATE TABLE IF NOT EXISTS StoreLocations (
    location_id INTEGER PRIMARY KEY AUTOINCREMENT,-- Уникальный идентификатор локации магазина
    store_number INTEGER,-- Связь с таблицей магазинов
    store_location TEXT,-- Координаты или местоположение
    FOREIGN KEY (store_number) REFERENCES Stores(store_number)-- Внешний ключ для магазинов
)
''')

# 10. Таблица объемов товаров
sql.execute('''
CREATE TABLE IF NOT EXISTS ItemVolumes (
    item_number INTEGER PRIMARY KEY,-- Уникальный идентификатор товара
    bottle_volume_ml INTEGER,-- Объем бутылки в миллилитрах
    FOREIGN KEY (item_number) REFERENCES Items(item_number)-- Внешний ключ для товаров
)
''')

# Сохраняем изменения
connection.commit()
