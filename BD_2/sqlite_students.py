import sqlite3
import csv

def create_database():
    """Создание базы данных и таблиц"""
    conn = sqlite3.connect('university.db')
    cursor = conn.cursor()
    
    # Удаление существующих таблиц (для чистого запуска)
    cursor.execute("DROP TABLE IF EXISTS студенты")
    cursor.execute("DROP TABLE IF EXISTS уровень_обучения")
    cursor.execute("DROP TABLE IF EXISTS направления")
    cursor.execute("DROP TABLE IF EXISTS типы_обучения")
    
    # Создание таблиц
    cursor.execute("""
    CREATE TABLE уровень_обучения (
        id_уровня INTEGER PRIMARY KEY,
        название VARCHAR NOT NULL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE направления (
        id_направления INTEGER PRIMARY KEY,
        название VARCHAR NOT NULL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE типы_обучения (
        id_типа INTEGER PRIMARY KEY,
        название VARCHAR NOT NULL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE студенты (
        id_студента INTEGER PRIMARY KEY,
        id_уровня INTEGER NOT NULL,
        id_направления INTEGER NOT NULL,
        id_типа_обучения INTEGER NOT NULL,
        фамилия VARCHAR NOT NULL,
        имя VARCHAR NOT NULL,
        отчество VARCHAR,
        средний_балл INTEGER NOT NULL,
        FOREIGN KEY (id_уровня) REFERENCES уровень_обучения(id_уровня),
        FOREIGN KEY (id_направления) REFERENCES направления(id_направления),
        FOREIGN KEY (id_типа_обучения) REFERENCES типы_обучения(id_типа)
    )
    """)
    
    conn.commit()
    return conn

def import_data_from_csv(conn):
    """Импорт данных из CSV-файлов"""
    cursor = conn.cursor()
    
    # Импорт уровней обучения
    with open('уровни_обучения.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        cursor.executemany(
            "INSERT INTO уровень_обучения VALUES (:id_уровня, :название)", 
            reader
        )
    
    # Импорт направлений
    with open('направления.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        cursor.executemany(
            "INSERT INTO направления VALUES (:id_направления, :название)", 
            reader
        )
    
    # Импорт типов обучения
    with open('типы_обучения.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        cursor.executemany(
            "INSERT INTO типы_обучения VALUES (:id_типа, :название)", 
            reader
        )
    
    # Импорт студентов
    with open('студенты.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            cursor.execute("""
            INSERT INTO студенты VALUES (
                :id_студента, :id_уровня, :id_направления, :id_типа_обучения,
                :фамилия, :имя, :отчество, :средний_балл
            )
            """, row)
    
    conn.commit()
    print("Данные успешно импортированы")

def execute_queries(conn):
    """Выполнение всех требуемых запросов"""
    cursor = conn.cursor()
    
    print("\n1. Количество всех студентов:")
    cursor.execute("SELECT COUNT(*) FROM студенты")
    print(f"Всего студентов: {cursor.fetchone()[0]}")
    
    print("\n2. Количество студентов по направлениям:")
    cursor.execute("""
    SELECT н.название, COUNT(с.id_студента) 
    FROM студенты с
    JOIN направления н ON с.id_направления = н.id_направления
    GROUP BY н.название
    ORDER BY COUNT(с.id_студента) DESC
    """)
    for row in cursor.fetchall():
        print(f"{row[0]}: {row[1]}")
    
    print("\n3. Количество студентов по формам обучения:")
    cursor.execute("""
    SELECT т.название, COUNT(с.id_студента) 
    FROM студенты с
    JOIN типы_обучения т ON с.id_типа_обучения = т.id_типа
    GROUP BY т.название
    ORDER BY COUNT(с.id_студента) DESC
    """)
    for row in cursor.fetchall():
        print(f"{row[0]}: {row[1]}")
    
    print("\n4. Максимальный, минимальный, средний баллы по направлениям:")
    cursor.execute("""
    SELECT 
        н.название, 
        MAX(с.средний_балл) as max_ball,
        MIN(с.средний_балл) as min_ball,
        ROUND(AVG(с.средний_балл), 1) as avg_ball
    FROM студенты с
    JOIN направления н ON с.id_направления = н.id_направления
    GROUP BY н.название
    ORDER BY н.название
    """)
    for row in cursor.fetchall():
        print(f"{row[0]}: макс. {row[1]}, мин. {row[2]}, ср. {row[3]}")
    
    print("\n5. Средний балл по направлениям, уровням и формам обучения:")
    cursor.execute("""
    SELECT 
        н.название as направление,
        у.название as уровень,
        т.название as форма_обучения,
        ROUND(AVG(с.средний_балл), 1) as средний_балл
    FROM студенты с
    JOIN направления н ON с.id_направления = н.id_направления
    JOIN уровень_обучения у ON с.id_уровня = у.id_уровня
    JOIN типы_обучения т ON с.id_типа_обучения = т.id_типа
    GROUP BY н.название, у.название, т.название
    ORDER BY н.название, у.название, т.название
    """)
    for row in cursor.fetchall():
        print(f"{row[0]}, {row[1]}, {row[2]}: {row[3]}")
    
    print("\n6. Топ-5 студентов 'Прикладная Информатика' (очная форма):")
    cursor.execute("""
    SELECT 
        с.фамилия, с.имя, с.отчество, с.средний_балл
    FROM студенты с
    JOIN направления н ON с.id_направления = н.id_направления
    JOIN типы_обучения т ON с.id_типа_обучения = т.id_типа
    WHERE н.название = 'Прикладная Информатика' AND т.название = 'Очная'
    ORDER BY с.средний_балл DESC
    LIMIT 5
    """)
    for i, row in enumerate(cursor.fetchall(), 1):
        print(f"{i}. {row[0]} {row[1]} {row[2]}: {row[3]} баллов")
    
    print("\n7. Количество однофамильцев:")
    cursor.execute("""
    SELECT фамилия, COUNT(*) as количество
    FROM студенты
    GROUP BY фамилия
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC
    """)
    for row in cursor.fetchall():
        print(f"Фамилия {row[0]}: {row[1]} человек")
    
    print("\n8. Наличие полных тезок:")
    cursor.execute("""
    SELECT фамилия, имя, отчество, COUNT(*) as количество
    FROM студенты
    GROUP BY фамилия, имя, отчество
    HAVING COUNT(*) > 1
    """)
    tezky = cursor.fetchall()
    if tezky:
        print("Найдены полные тезки:")
        for row in tezky:
            print(f"{row[0]} {row[1]} {row[2]}: {row[3]} человека")
    else:
        print("Полных тезок не найдено")

def main():
    """Основная функция"""
    # 1. Создание базы данных
    conn = create_database()
    
    # 2. Импорт данных из CSV
    import_data_from_csv(conn)
    
    # 3. Выполнение запросов
    execute_queries(conn)
        

if __name__ == "__main__":
    main()
