import sqlite3
import csv

# Подключение к базе данных
connection = sqlite3.connect("3nf.db")
sql = connection.cursor()

# Модифицированный SQL запрос - убираем проблемное соединение с Items
sql.execute("""
SELECT 
    i.date AS Дата,
    s.store_name AS Магазин,
    sa.invoice_and_item_number AS Код_товара,
    sa.bottles_sold AS Количество,
    sa.sale_dollars AS Сумма
FROM Sales sa
JOIN Invoices i ON sa.invoice_and_item_number = i.invoice_and_item_number
JOIN Stores s ON i.store_number = s.store_number
ORDER BY i.date
""")

# Получение данных
sales_data = sql.fetchall()

# Получение заголовков столбцов
headers = [description[0] for description in sql.description]

# Создание CSV файла
with open('sales_report.csv', 'w', newline='', encoding='utf-8') as csv_file:
    writer = csv.writer(csv_file)
    # Запись заголовков
    writer.writerow(headers)
    # Запись данных
    writer.writerows(sales_data)

# Добавление итогов
sql.execute("SELECT SUM(bottles_sold), SUM(sale_dollars) FROM Sales")
total_bottles, total_sales = sql.fetchone()

with open('sales_report.csv', 'a', newline='', encoding='utf-8') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow([])
    writer.writerow(['Итого', '', '', total_bottles, total_sales])

# Закрытие соединения
connection.close()

print(f"Отчет по продажам сохранен в файл 'sales_report.csv'")
print(f"Всего записей: {len(sales_data)}")
print(f"Общее количество бутылок: {total_bottles}")
print(f"Общая сумма продаж: {total_sales}")