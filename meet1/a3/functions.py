import sqlite3

def up_data(db_path: str = '3nf.db') -> bool:
    connection = None
    try:
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        sql = connection.cursor()

        # Получаем список таблиц
        tables = [t[0] for t in sql.execute('SELECT name FROM sqlite_master WHERE type = "table";').fetchall()]
        if not tables:
            print("❌ В базе данных нет таблиц.")
            return False

        print("📋 Доступные таблицы:", ", ".join(tables))
        table = input("Введите название таблицы: ").strip()
        if table not in tables:
            print("❌ Ошибка: таблица не найдена!")
            return False

        # Получаем список колонок
        columns = [col[1] for col in sql.execute(f'PRAGMA table_info({table});').fetchall()]
        if not columns:
            print("❌ В выбранной таблице нет колонок.")
            return False

        print("📋 Доступные колонки:", ", ".join(columns))
        column = input("Введите название колонки: ").strip()
        if column not in columns:
            print("❌ Ошибка: колонка не найдена!")
            return False

        # Проверяем, есть ли первичный ключ, иначе используем rowid
        primary_key = sql.execute(f"PRAGMA table_info({table});").fetchone()[1] or "rowid"

        rows = sql.execute(f'SELECT {primary_key}, {column} FROM {table} ORDER BY {primary_key};').fetchall()
        if not rows:
            print("❌ В выбранной таблице нет данных.")
            return False

        print("📋 Данные в колонке:")
        for row in rows:
            print(f"  {primary_key}: {row[primary_key]}, {column}: {row[column]}")

        try:
            row_id = int(input(f"Введите {primary_key} строки для обновления: ").strip())
        except ValueError:
            print("❌ Ошибка: идентификатор должен быть числом!")
            return False

        # Проверяем, существует ли строка
        result = sql.execute(f'SELECT {column} FROM {table} WHERE {primary_key} = ?', (row_id,)).fetchone()
        if not result:
            print("❌ Ошибка: указанного идентификатора не существует!")
            return False

        current_value = result[0]
        print(f"📌 Текущее значение: {current_value}")

        new_value = input("Введите новое значение: ").strip()
        sql.execute(f'UPDATE {table} SET {column} = ? WHERE {primary_key} = ?;', (new_value, row_id))
        connection.commit()
        print("✅ Данные успешно обновлены!")
        return True

    except sqlite3.Error as e:
        print(f"❌ Ошибка базы данных: {e}")
        if connection:
            connection.rollback()
        return False

    except Exception as e:
        print(f"❌ Непредвиденная ошибка: {e}")
        if connection:
            connection.rollback()
        return False

    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    up_data()
