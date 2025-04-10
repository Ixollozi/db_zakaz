# Импортируем необходимые модули
from db import get_db, County, Store, Category, Vendor, Item, DateTime, FactSales
import pandas as pd

# Получаем сессию для взаимодействия с БД
db = next(get_db())

# Чтение CSV файла в DataFrame pandas
df = pd.read_csv("iowa_liquor_sales.csv", header=0)

# Функция для загрузки данных о датах
def load_to_date():
    dates_series = pd.to_datetime(df['date'], errors='coerce')  # Преобразуем столбец 'date' в формат datetime
    valid_dates = dates_series.dropna()  # Убираем строки с некорректными датами

    for date_obj in valid_dates:
        date_entry = db.query(DateTime).filter_by(date=date_obj).first()  # Проверяем, есть ли такая дата в БД
        if not date_entry:  # Если нет - добавляем
            date_entry = DateTime(
                date=date_obj,
                year=date_obj.year,
                month=date_obj.month,
                day=date_obj.day
            )
            try:
                db.add(date_entry)  # Добавляем объект даты
                db.commit()  # Коммитим изменения
                print(f"Добавлена дата: {date_obj}")
            except Exception as e:  # Если ошибка, откатываем изменения
                db.rollback()
                print(f"Ошибка при добавлении даты {date_obj}: {e}")

# Функция для загрузки данных о графствах (counties)
def load_to_county():
    unique_counties = df[['county_number', 'county']].drop_duplicates().dropna()  # Убираем дубликаты и пропуски

    for index, row in unique_counties.iterrows():
        c_number = row['county_number']
        c_name = row['county']

        existing_county = db.query(County).filter_by(county_number=c_number, county=c_name).first()

        if not existing_county:  # Если графство не существует в БД - добавляем
            county_obj = County(county_number=c_number, county=c_name)
            try:
                db.add(county_obj)
                db.commit()
                print(f"Добавлен County: {c_number} - {c_name}")
            except Exception as e:
                db.rollback()
                print(f"Ошибка при добавлении County {c_number}: {e}")

# Функция для загрузки данных о магазинах (stores)
def load_to_store():
    unique_stores = df[['store_number', 'store_name', 'address', 'city', 'zip_code', 'store_location', 'county']].drop_duplicates().dropna()

    for index, row in unique_stores.iterrows():
        s_number = row['store_number']
        s_name = row['store_name']
        s_address = row['address']
        s_city = row['city']
        s_zip = row['zip_code']
        s_location = row['store_location']
        s_county_name = row['county']

        # Найдём соответствующее объект County
        county = db.query(County).filter(County.county == s_county_name).first()

        if not county:  # Если графство не найдено - пропускаем
            print(f"⚠️ Не найден County: {s_county_name}. Магазин {s_number} не будет добавлен.")
            continue

        # Проверяем, существует ли магазин в БД
        existing_store = db.query(Store).filter_by(store_number=s_number).first()

        if not existing_store:  # Если магазина нет, добавляем
            store_obj = Store(
                store_number=s_number,
                store_name=s_name,
                address=s_address,
                city=s_city,
                zip_code=str(s_zip),
                store_location=s_location,
                county=county  # Привязка к графству
            )
            try:
                db.add(store_obj)
                db.commit()
                print(f"Добавлен магазин: {s_number} - {s_name}")
            except Exception as e:
                db.rollback()
                print(f"Ошибка при добавлении магазина {s_number}: {e}")

# Функция для загрузки категорий товаров
def load_to_category():
    unique_categories = df[['category', 'category_name']].drop_duplicates().dropna()

    for index, row in unique_categories.iterrows():
        c_number = row['category']
        c_name = row['category_name']

        existing_category = db.query(Category).filter_by(category=c_number, category_name=c_name).first()

        if not existing_category:  # Если категория не найдена - добавляем
            category_obj = Category(category=c_number, category_name=c_name)
            try:
                db.add(category_obj)
                db.commit()
                print(f"Добавлена категория: {c_number} - {c_name}")
            except Exception:
                db.rollback()
                print(f"Ошибка при добавлении категории {c_number}")

# Функция для загрузки данных о поставщиках (vendors)
def load_to_vendor():
    unique_vendors = df[['vendor_number', 'vendor_name']].drop_duplicates().dropna()

    for index, row in unique_vendors.iterrows():
        v_number = row['vendor_number']
        v_name = row['vendor_name']

        existing_vendor = db.query(Vendor).filter_by(vendor_number=v_number, vendor_name=v_name).first()

        if not existing_vendor:  # Если поставщик не найден - добавляем
            vendor_obj = Vendor(vendor_number=v_number, vendor_name=v_name)
            try:
                db.add(vendor_obj)
                db.commit()
                print(f"Добавлен поставщик: {v_number} - {v_name}")
            except Exception as e:
                db.rollback()
                print(f"Ошибка при добавлении поставщика {v_number}: {e}")

# Функция для загрузки товаров (items)
def load_to_item():
    unique_items = df[['item_number', 'item_description', 'category_name', 'pack', 'bottle_volume_ml']].drop_duplicates().dropna()

    for index, row in unique_items.iterrows():
        i_number = row['item_number']
        i_description = row['item_description']
        i_category_name = row['category_name']
        i_pack = row['pack']
        i_bottle_volume_ml = row['bottle_volume_ml']

        category = db.query(Category).filter(Category.category_name == i_category_name).first()

        if not category:  # Если категория не найдена - пропускаем
            print(f"⚠️ Не найдена категория: {i_category_name}. Предмет {i_number} не будет добавлен.")
            continue

        existing_item = db.query(Item).filter_by(item_number=i_number, item_description=i_description,
                                                 category_id=category.category_id, pack=i_pack, bottle_volume_ml=i_bottle_volume_ml).first()

        if not existing_item:  # Если предмет не найден, добавляем
            item_obj = Item(item_number=i_number, item_description=i_description, category_id=category.category_id,
                            pack=i_pack, bottle_volume_ml=i_bottle_volume_ml)
            try:
                db.add(item_obj)
                db.commit()
                print(f"Добавлен предмет: {i_number} - {i_description}")
            except Exception as e:
                db.rollback()
                print(f"Ошибка при добавлении предмета {i_number}: {e}")

# Функция для загрузки фактов продаж
def load_to_fact_sales():
    for index, row in df.iterrows():
        date_obj = db.query(DateTime).filter_by(date=row['date']).first()
        store_obj = db.query(Store).filter_by(store_number=row['store_number']).first()
        vendor_obj = db.query(Vendor).filter_by(vendor_number=row['vendor_number']).first()
        item_obj = db.query(Item).filter_by(item_number=row['item_number']).first()

        if not all([date_obj, store_obj, vendor_obj, item_obj]):  # Если хотя бы один объект не найден
            print(f"⚠️ Пропущена строка {index} — отсутствует один из объектов.")
            continue

        # Проверка на пустые значения по нужным полям
        if any(pd.isna(row[col]) for col in ['state_bottle_cost', 'state_bottle_retail', 'bottles_sold', 'sale_dollars', 'volume_sold_liters', 'volume_sold_gallons']):
            print(f"⚠️ Пропущена строка {index} — отсутствует одно из значений продаж.")
            continue

        # Создаём объект FactSales и добавляем его в БД
        fact_sales_obj = FactSales(
            invoice_and_item_number=row['invoice_and_item_number'],
            date_id=date_obj.date_id,
            store_id=store_obj.store_id,
            vendor_id=vendor_obj.vendor_id,
            item_id=item_obj.item_id,
            state_bottle_cost=row['state_bottle_cost'],
            state_bottle_retail=row['state_bottle_retail'],
            bottles_sold=row['bottles_sold'],
            sale_dollars=row['sale_dollars'],
            volume_sold_liters=row['volume_sold_liters'],
            volume_sold_gallons=row['volume_sold_gallons']
        )

        try:
            db.add(fact_sales_obj)
            db.commit()
            print(f"Добавлена продажа: {row['invoice_and_item_number']}")
        except Exception as e:
            db.rollback()
            print(f"Ошибка при добавлении продажи {row['invoice_and_item_number']}: {e}")

# Основной блок выполнения
if __name__ == '__main__':
    load_to_date()  # Загружаем данные о датах
    load_to_county()  # Загружаем данные о графствах
    load_to_store()  # Загружаем данные о магазинах
    load_to_category()  # Загружаем категории товаров
    load_to_vendor()  # Загружаем данные о поставщиках
    load_to_item()  # Загружаем товары
    load_to_fact_sales()  # Загружаем данные о продажах
    db.close()  # Закрываем сессию

    # Проверка количества записей после загрузки
    print("📊 Количество записей после загрузки данных:")
    print(f"- DateTime: {db.query(DateTime).count()}")
    print(f"- County: {db.query(County).count()}")
    print(f"- Store: {db.query(Store).count()}")
    print(f"- Category: {db.query(Category).count()}")
    print(f"- Vendor: {db.query(Vendor).count()}")
    print(f"- Item: {db.query(Item).count()}")
    print(f"- FactSales: {db.query(FactSales).count()}")
