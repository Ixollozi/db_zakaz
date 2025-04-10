import sqlite3

# Подключение к базе данных
connection = sqlite3.connect("2nf.db")
sql = connection.cursor()



# Таблица с информацией о магазинах
sql.execute('''
    CREATE TABLE IF NOT EXISTS stores (
        store_id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Уникальный идентификатор магазина
        store_number INTEGER UNIQUE,  -- Уникальный номер магазина
        store_name TEXT,  -- Название магазина
        address TEXT,  -- Адрес магазина
        city TEXT,  -- Город
        zip_code TEXT,  -- Почтовый индекс
        store_location TEXT,  -- Координаты или местоположение
        county_id INTEGER,  -- Связь с таблицей округов
        FOREIGN KEY (county_id) REFERENCES counties(county_id)  -- Внешний ключ
    )
''')

# Таблица округов
sql.execute('''
    CREATE TABLE IF NOT EXISTS counties (
        county_id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Уникальный идентификатор округа
        county_number INTEGER UNIQUE,  -- Номер округа
        county_name TEXT  -- Название округа
    )
''')

# Таблица категорий товаров
sql.execute('''
    CREATE TABLE IF NOT EXISTS categories (
        category_id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Уникальный идентификатор категории
        category INTEGER UNIQUE,  -- Номер категории
        category_name TEXT  -- Название категории
    )
''')

# Таблица поставщиков
sql.execute('''
    CREATE TABLE IF NOT EXISTS vendors (
        vendor_id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Уникальный идентификатор поставщика
        vendor_number INTEGER UNIQUE,  -- Номер поставщика
        vendor_name TEXT  -- Название поставщика
    )
''')

# Таблица товаров
sql.execute('''
    CREATE TABLE IF NOT EXISTS items (
        item_id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Уникальный идентификатор товара
        item_number INTEGER UNIQUE,  -- Номер товара
        item_description TEXT,  -- Описание товара
        pack INTEGER,  -- Количество в упаковке
        bottle_volume_ml INTEGER,  -- Объем бутылки в миллилитрах
        state_bottle_cost REAL,  -- Себестоимость бутылки
        state_bottle_retail REAL,  -- Розничная цена бутылки
        category_id INTEGER,  -- Связь с таблицей категорий
        vendor_id INTEGER,  -- Связь с таблицей поставщиков
        FOREIGN KEY (category_id) REFERENCES categories(category_id),  -- Внешний ключ для категорий
        FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id)  -- Внешний ключ для поставщиков
    )
''')

# Таблица продаж
sql.execute('''
    CREATE TABLE IF NOT EXISTS sales (
        sale_id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Уникальный идентификатор продажи
        invoice_and_item_number TEXT UNIQUE,  -- Уникальный номер счета-фактуры
        date TEXT,  -- Дата продажи
        store_id INTEGER,  -- Связь с таблицей магазинов
        item_id INTEGER,  -- Связь с таблицей товаров
        bottles_sold INTEGER,  -- Количество проданных бутылок
        sale_dollars REAL,  -- Сумма продажи в долларах
        volume_sold_liters REAL,  -- Объем проданного товара в литрах
        volume_sold_gallons REAL,  -- Объем проданного товара в галлонах
        FOREIGN KEY (store_id) REFERENCES stores(store_id),  -- Внешний ключ для магазинов
        FOREIGN KEY (item_id) REFERENCES items(item_id)  -- Внешний ключ для товаров
    )
''')

# Сохраняем изменения
connection.commit()
