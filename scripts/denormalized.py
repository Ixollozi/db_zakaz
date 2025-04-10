import sqlite3

# Подключение к базе данных
conn = sqlite3.connect("../docs/meet1/a2/2nf.db")
cursor = conn.cursor()

# Создание денормализованной таблицы
cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales_denormalized AS
    SELECT
        s.sale_id,
        s.invoice_and_item_number,
        s.date,
        st.store_number,
        st.store_name,
        st.address,
        st.city,
        st.zip_code,
        st.store_location,
        c.county_number,
        c.county_name,
        cat.category,
        cat.category_name,
        v.vendor_number,
        v.vendor_name,
        i.item_number,
        i.item_description,
        i.pack,
        i.bottle_volume_ml,
        i.state_bottle_cost,
        i.state_bottle_retail,
        s.bottles_sold,
        s.sale_dollars,
        s.volume_sold_liters,
        s.volume_sold_gallons
    FROM sales s
    JOIN stores st ON s.store_id = st.store_id
    JOIN counties c ON st.county_id = c.county_id
    JOIN items i ON s.item_id = i.item_id
    JOIN categories cat ON i.category_id = cat.category_id
    JOIN vendors v ON i.vendor_id = v.vendor_id;
""")

# Сохранение изменений и закрытие соединения
conn.commit()
print("Денормализация завершена.")
