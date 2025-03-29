import sqlite3
import csv

conn = sqlite3.connect(':memory:')
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE shop (
    "ID магазина" TEXT PRIMARY KEY,
    "Район" TEXT,
    "Адрес" TEXT
)
""")

cursor.execute("""
CREATE TABLE product (
    "Артикул" TEXT PRIMARY KEY,
    "Отдел" TEXT,
    "Наименование товара" TEXT,
    "Ед_изм" TEXT,
    "Количество в упаковке" REAL,
    "Цена за упаковку" REAL
)
""")

cursor.execute("""
CREATE TABLE move (
    "ID операции" INTEGER PRIMARY KEY,
    "Дата" TEXT,
    "ID магазина" TEXT,
    "Артикул" TEXT,
    "Количество упаковок, шт" INTEGER,
    "Тип операции" TEXT,
    FOREIGN KEY("ID магазина") REFERENCES shop("ID магазина"),
    FOREIGN KEY("Артикул") REFERENCES product("Артикул")
)
""")

def convert_value(value, column_name):
    if column_name in ["ID операции", "Количество упаковок, шт"]:
        return int(value) if value.isdigit() else 0
    elif column_name in ["Количество в упаковке", "Цена за упаковку"]:
        return float(value.replace(',', '.'))
    return value.strip()

def import_data_from_csv(cursor, filename, table_name):
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        columns = [col.strip() for col in reader.fieldnames if col.strip()]
        
        for row in reader:
            values = [convert_value(row[col], col) for col in columns if col in row]
            placeholders = ', '.join(['?'] * len(columns))
            columns_str = ', '.join([f'"{col}"' for col in columns])
            
            cursor.execute(f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})", values)

import_data_from_csv(cursor, 'shop.txt', 'shop')
import_data_from_csv(cursor, 'product.txt', 'product')
import_data_from_csv(cursor, 'move.txt', 'move')
conn.commit()

query = """
SELECT SUM(m."Количество упаковок, шт" * 
    CASE WHEN p."Ед_изм" = 'грамм' THEN p."Количество в упаковке"/1000 
    ELSE p."Количество в упаковке" END) as total_kg
FROM move m
JOIN product p ON m."Артикул" = p."Артикул"
JOIN shop s ON m."ID магазина" = s."ID магазина"
WHERE p."Наименование товара" LIKE '%Зефир%'
  AND s."Адрес" LIKE '%Революции%'
  AND m."Тип операции" = 'Поступление'
  AND m."Дата" BETWEEN '02.08.2023' AND '10.08.2023'
"""

cursor.execute(query)
result = cursor.fetchone()
total_kg = result[0] if result[0] is not None else 0

print(f"Масса зефира за период 02-10.08.2023: {total_kg:.2f} кг")

conn.close()