from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, Float
from sqlalchemy.orm import sessionmaker, relationship, declarative_base

name = 'postgres'
password = ''
host = 'localhost'
port = 5432
db_name = 'postgres'
# подключение к БД
SQLALCHEMY_DATABASE_URL = f"postgresql://{name}:{password}@{host}:{port}/{db_name}"
# создание движка
engine = create_engine(SQLALCHEMY_DATABASE_URL)
# создание сессии
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# создание базового класса
Base = declarative_base()

# создание контекстного менеджера
def get_db():
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


# Создание модели(таблиц)
class DateTime(Base):
    __tablename__ = 'datetime'
    date_id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)


class County(Base):
    __tablename__ = 'county'
    county_id = Column(Integer, primary_key=True, autoincrement=True)
    county_number = Column(Integer, nullable=False, unique=True)
    county = Column(String, nullable=False)


class Store(Base):
    __tablename__ = 'store'
    store_id = Column(Integer, primary_key=True, autoincrement=True)
    store_number = Column(Integer, nullable=False, unique=True)
    store_name = Column(String, nullable=False)
    address = Column(String, nullable=False)
    city = Column(String, nullable=False)
    zip_code = Column(String, nullable=False)
    store_location = Column(String, nullable=True)
    county_id = Column(Integer, ForeignKey('county.county_id'))
    county = relationship('County')


class Category(Base):
    __tablename__ = 'category'
    category_id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(Integer, nullable=False, unique=True)
    category_name = Column(String, nullable=False)


class Vendor(Base):
    __tablename__ = 'vendor'
    vendor_id = Column(Integer, primary_key=True, autoincrement=True)
    vendor_number = Column(Integer, nullable=False, unique=True)
    vendor_name = Column(String, nullable=False)


class Item(Base):
    __tablename__ = 'item'
    item_id = Column(Integer, primary_key=True, autoincrement=True)
    item_number = Column(Integer, nullable=False, unique=True)
    item_description = Column(String, nullable=False)
    category_id = Column(Integer, ForeignKey('category.category_id'))
    pack = Column(Integer, nullable=False)
    bottle_volume_ml = Column(Integer, nullable=False)
    category = relationship('Category')


class FactSales(Base):
    __tablename__ = 'fact_sales'
    fact_id = Column(Integer, primary_key=True, autoincrement=True)
    invoice_and_item_number = Column(String, nullable=False)
    date_id = Column(Integer, ForeignKey('datetime.date_id'))
    store_id = Column(Integer, ForeignKey('store.store_id'))
    item_id = Column(Integer, ForeignKey('item.item_id'))
    vendor_id = Column(Integer, ForeignKey('vendor.vendor_id'))
    # Добавлены метрики
    state_bottle_cost = Column(Float, nullable=False)
    state_bottle_retail = Column(Float, nullable=False)
    bottles_sold = Column(Integer, nullable=False)
    sale_dollars = Column(Float, nullable=False)
    volume_sold_liters = Column(Float, nullable=False)
    volume_sold_gallons = Column(Float, nullable=False)
    # Добавлены связи
    date = relationship('DateTime')
    store = relationship('Store')
    item = relationship('Item')
    vendor = relationship('Vendor')


# Создать таблицы
Base.metadata.create_all(engine)