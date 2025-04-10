import sqlite3

db_path = "3nf.db"
dump_file = "dump.sql"

connection = sqlite3.connect(db_path)
with open(dump_file, "w", encoding="utf-8") as f:
    for line in connection.iterdump():
        f.write(f"{line}\n")

connection.close()
print(f"✔ Дамп базы данных сохранён в {dump_file}")