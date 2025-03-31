import sqlite3
import csv

import sqlite3

# Подключение к базе данных
connection = sqlite3.connect("baza.db")
cursor = connection.cursor()

# Создание таблиц (остается без изменений)
cursor.execute("""
    CREATE TABLE IF NOT EXISTS `job_titles` (
        `id_job_title` integer primary key NOT NULL UNIQUE,
        `name` TEXT NOT NULL
    );
""")
               
cursor.execute("""
    CREATE TABLE IF NOT EXISTS `employees` (
        `id` integer primary key NOT NULL UNIQUE,
        `surname` TEXT NOT NULL,
        `name` TEXT NOT NULL,
        `id_job_title` INTEGER NOT NULL,
        FOREIGN KEY(`id_job_title`) REFERENCES `job_titles` (`id_job_title`)
    );
""")

# Заполнение таблиц (остается без изменений)
job_titles_data = [
    (1, 'Менеджер'),
    (2, 'Разработчик'),
    (3, 'Аналитик'),
    (4, 'Дизайнер')
]
cursor.executemany("INSERT OR IGNORE INTO `job_titles` (`id_job_title`, `name`) VALUES (?, ?)", job_titles_data)

employees_data = [
    (1, 'Иванов', 'Иван', 2),
    (2, 'Петров', 'Петр', 1),
    (3, 'Сидорова', 'Мария', 3),
    (4, 'Козлов', 'Алексей', 2),
    (5, 'Васильева', 'Ольга', 4)
]
cursor.executemany("INSERT OR IGNORE INTO `employees` (`id`, `surname`, `name`, `id_job_title`) VALUES (?, ?, ?, ?)", employees_data)

connection.commit()

# Исправленные запросы:

# 1. Получить всех сотрудников и их должности (исправлено название таблицы в JOIN)
cursor.execute("""
    SELECT e.surname, e.name, j.name as job_title 
    FROM employees e 
    JOIN job_titles j ON e.id_job_title = j.id_job_title
""")

print("Сотрудники и их должности:")
for employee in cursor.fetchall(): 
    print(f"{employee[0]} {employee[1]} - {employee[2]}")

# 2. Получить всех разработчиков (исправленная версия)
cursor.execute("""
    SELECT e.surname, e.name
    FROM employees e
    JOIN job_titles j ON e.id_job_title = j.id_job_title
    WHERE j.name = 'Разработчик'
""")

print("\nРазработчики:")
for developer in cursor.fetchall():
    print(f"{developer[0]} {developer[1]}")

# Дополнительные запросы из задания:

# 4 простых запросов
print("\nПростые запросы:")
cursor.execute("SELECT COUNT(*) FROM employees")
print("1. Количество сотрудников:", cursor.fetchone()[0])

cursor.execute("SELECT MAX(id) FROM employees")
print("2. Максимальный ID сотрудника:", cursor.fetchone()[0])

cursor.execute("SELECT COUNT(DISTINCT id_job_title) FROM employees")
print("3. Количество занятых должностей:", cursor.fetchone()[0])

cursor.execute("SELECT SUM(id) FROM employees WHERE id_job_title = 2")
print("4. Сумма ID разработчиков:", cursor.fetchone()[0])

# 1. Количество сотрудников по должностям
cursor.execute("""
    SELECT j.name, COUNT(e.id) 
    FROM job_titles j 
    LEFT JOIN employees e ON j.id_job_title = e.id_job_title 
    GROUP BY j.name
""")
print("Количество сотрудников по должностям:")
for row in cursor.fetchall():
    print(f"{row[0]}: {row[1]}")


# 2. Должности с количеством сотрудников > 1
cursor.execute("""
    SELECT j.name, COUNT(e.id) as emp_count
    FROM job_titles j
    JOIN employees e ON j.id_job_title = e.id_job_title
    GROUP BY j.name
    HAVING emp_count > 1
""")
print("\nДолжности с более чем 1 сотрудником:")
for row in cursor.fetchall():
    print(f"{row[0]}: {row[1]}")

# 1. Сотрудники с должностью "Разработчик"
cursor.execute("""
    SELECT e.surname, e.name 
    FROM employees e
    JOIN job_titles j ON e.id_job_title = j.id_job_title
    WHERE j.name = 'Разработчик'
""")
print("\nРазработчики:")
for row in cursor.fetchall():
    print(f"{row[0]} {row[1]}")

# 2. Сотрудники с ID > 2 и их должности
cursor.execute("""
    SELECT e.surname, e.name, j.name as job_title
    FROM employees e
    JOIN job_titles j ON e.id_job_title = j.id_job_title
    WHERE e.id > 2
""")
print("\nСотрудники с ID > 2:")
for row in cursor.fetchall():
    print(f"{row[0]} {row[1]} - {row[2]}")

# 3. Должности, у которых нет сотрудников
cursor.execute("""
    SELECT j.name
    FROM job_titles j
    LEFT JOIN employees e ON j.id_job_title = e.id_job_title
    WHERE e.id IS NULL
""")
print("\nДолжности без сотрудников:")
for row in cursor.fetchall():
    print(row[0])

# Импорт данных из CSV в таблицу employees
with open('employees.csv', 'r', encoding='utf-8') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        cursor.execute(
            "INSERT OR IGNORE INTO employees (id, surname, name, id_job_title) VALUES (?, ?, ?, ?)",
            (row['id'], row['surname'], row['name'], row['id_job_title'])
        )

# Импорт данных из TXT (формат: id|name)
with open('job_titles.txt', 'r', encoding='utf-8') as file:
    for line in file:
        id_job, name = line.strip().split('|')
        cursor.execute(
            "INSERT OR IGNORE INTO job_titles (id_job_title, name) VALUES (?, ?)",
            (int(id_job), name)
        )

connection.commit()

# Закрытие соединения
connection.close()