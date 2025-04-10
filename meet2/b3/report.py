import os
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import text
from db import engine

# Директория для сохранения графиков
if not os.path.exists('graphs'):
    os.makedirs('graphs')

# Запрос 1: Месячные продажи по категориям
query1 = text("""
SELECT 
    dt.year, 
    dt.month,
    c.category_name,
    SUM(fs.sale_dollars) AS total_sales,
    LAG(SUM(fs.sale_dollars), 1) OVER (
        PARTITION BY c.category_name 
        ORDER BY dt.year, dt.month
    ) AS prev_month_sales
FROM fact_sales fs
JOIN datetime dt ON fs.date_id = dt.date_id
JOIN item i ON fs.item_id = i.item_id
JOIN category c ON i.category_id = c.category_id
GROUP BY dt.year, dt.month, c.category_name
ORDER BY dt.year, dt.month, c.category_name
""")

# Запрос 2: Топ-5 товаров по продажам
query2 = text("""
SELECT 
    i.item_description,
    SUM(fs.bottles_sold) AS bottles_sold,
    SUM(fs.sale_dollars) AS total_sales,
    SUM(SUM(fs.sale_dollars)) OVER () AS grand_total,
    SUM(fs.sale_dollars) / SUM(SUM(fs.sale_dollars)) OVER () * 100 AS percentage
FROM fact_sales fs
JOIN item i ON fs.item_id = i.item_id
GROUP BY i.item_description
ORDER BY total_sales DESC
LIMIT 5
""")

# Запрос 3: Средний чек по магазинам с скользящим средним
query3 = text("""
WITH monthly_store_sales AS (
    SELECT 
        dt.year,
        dt.month,
        s.store_name,
        COUNT(DISTINCT fs.invoice_and_item_number) AS order_count,
        SUM(fs.sale_dollars) AS total_sales,
        SUM(fs.sale_dollars) / COUNT(DISTINCT fs.invoice_and_item_number) AS avg_order
    FROM fact_sales fs
    JOIN datetime dt ON fs.date_id = dt.date_id
    JOIN store s ON fs.store_id = s.store_id
    GROUP BY dt.year, dt.month, s.store_name
)
SELECT 
    year,
    month,
    store_name,
    avg_order,
    AVG(avg_order) OVER (
        PARTITION BY store_name 
        ORDER BY year, month 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3months
FROM monthly_store_sales
ORDER BY store_name, year, month
""")

# Выполнение запросов и получение результатов
df1 = pd.read_sql_query(query1, engine)
df2 = pd.read_sql_query(query2, engine)
df3 = pd.read_sql_query(query3, engine)

# Вывод результатов
print("Результаты запроса 1 (месячные продажи по категориям):")
print(df1.head())

print("\nРезультаты запроса 2 (топ товары):")
print(df2.head())

print("\nРезультаты запроса 3 (средний чек по магазинам):")
print(df3.head())

# Практические выводы

# Запрос 1: Месячные продажи по категориям
if not df1.empty:
    for category in df1['category_name'].unique():
        category_data = df1[df1['category_name'] == category]
        category_sales = category_data['total_sales'].sum()
        prev_month_sales = category_data['prev_month_sales'].iloc[-1] if not category_data['prev_month_sales'].isna().all() else 0
        print(f"\nВывод для категории {category}:")
        print(f"Общие продажи за период: {category_sales} долларов.")
        if prev_month_sales > 0:
            growth = category_sales - prev_month_sales
            print(f"Разница с предыдущим месяцем: {growth} долларов.")
        else:
            print("Продажи за предыдущий месяц отсутствуют для сравнения.")

# Запрос 2: Топ-5 товаров по продажам
if not df2.empty:
    print("\nВывод для топ-5 товаров:")
    for idx, row in df2.iterrows():
        print(f"{row['item_description']} - Продажи: {row['total_sales']} долларов, Доля: {row['percentage']:.2f}%.")

# Запрос 3: Средний чек по магазинам с скользящим средним
if not df3.empty:
    print("\nВывод для среднего чека по магазинам:")
    for store in df3['store_name'].unique():
        store_data = df3[df3['store_name'] == store]
        avg_order = store_data['avg_order'].mean()
        moving_avg = store_data['moving_avg_3months'].iloc[-1]
        print(f"{store} - Средний чек: {avg_order:.2f} долларов, Скользящая средняя за 3 месяца: {moving_avg:.2f} долларов.")

# Создание визуализации для запроса 1 (для первой категории в выборке)
if not df1.empty:
    first_category = df1['category_name'].iloc[0]
    category_data = df1[df1['category_name'] == first_category]

    plt.figure(figsize=(10, 5))
    plt.bar(
        [f"{year}-{month}" for year, month in zip(category_data['year'], category_data['month'])],
        category_data['total_sales']
    )
    plt.title(f'Месячные продажи для категории {first_category}')
    plt.xlabel('Год-Месяц')
    plt.ylabel('Сумма продаж ($)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('graphs/monthly_sales_by_category.png')
    print("\nГрафик сохранен в файл monthly_sales_by_category.png")

# Дополнительная визуализация для запроса 2 (топ товары)
plt.figure(figsize=(12, 6))
plt.bar(df2['item_description'], df2['total_sales'])
plt.title('Топ-5 товаров по продажам')
plt.xlabel('Товар')
plt.ylabel('Сумма продаж ($)')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig('graphs/top_products.png')
print("\nГрафик сохранен в файл top_products.png")

# Бонусный запрос: Продажи по округам
query4 = text("""
SELECT 
    c.county,
    SUM(fs.bottles_sold) AS total_bottles,
    SUM(fs.sale_dollars) AS total_sales,
    SUM(fs.volume_sold_liters) AS total_volume_liters,
    AVG(fs.sale_dollars / fs.bottles_sold) AS avg_bottle_price
FROM fact_sales fs
JOIN store s ON fs.store_id = s.store_id
JOIN county c ON s.county_id = c.county_id
GROUP BY c.county
ORDER BY total_sales DESC
""")

df4 = pd.read_sql_query(query4, engine)

print("\nРезультаты запроса 4 (продажи по округам):")
print(df4.head())

# Визуализация для запроса 4
plt.figure(figsize=(14, 7))
plt.bar(df4['county'], df4['total_sales'])
plt.title('Продажи по округам')
plt.xlabel('Округ')
plt.ylabel('Сумма продаж ($)')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig('graphs/sales_by_county.png')
print("\nГрафик сохранен в файл sales_by_county.png")
