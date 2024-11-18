import sqlite3

def create_database(dbName):
    # Создаем соединение с базой данных 
    conn = sqlite3.connect(dbName)

    # Создаем курсор для выполнения SQL-запросов
    cur = conn.cursor()

    # Создаем таблицу wordList
    cur.execute('''
    CREATE TABLE IF NOT EXISTS wordList (
        rowId INTEGER PRIMARY KEY,
        word TEXT NOT NULL,
        isFiltried INTEGER DEFAULT 0
    );
    ''')

    # Создаем таблицу URLList
    cur.execute('''
    CREATE TABLE IF NOT EXISTS URLList (
        rowId INTEGER PRIMARY KEY,
        URL TEXT NOT NULL
    );
    ''')

    # Создаем таблицу wordLocation
    cur.execute('''
    CREATE TABLE IF NOT EXISTS wordLocation (
        rowId INTEGER PRIMARY KEY,
        fk_wordId INTEGER,
        fk_URLId INTEGER,
        location INTEGER,
        FOREIGN KEY (fk_wordId) REFERENCES wordList(rowId),
        FOREIGN KEY (fk_URLId) REFERENCES URLList(rowId)
    );
    ''')

    # Создаем таблицу linkBetweenURL
    cur.execute('''
    CREATE TABLE IF NOT EXISTS linkBetweenURL (
        rowId INTEGER PRIMARY KEY,
        fk_FromURL_Id INTEGER,
        fk_ToURL_Id INTEGER,
        FOREIGN KEY (fk_FromURL_Id) REFERENCES URLList(rowId),
        FOREIGN KEY (fk_ToURL_Id) REFERENCES URLList(rowId)
    );
    ''')

    # Создаем таблицу linkWord
    cur.execute('''
    CREATE TABLE IF NOT EXISTS linkWord (
        rowId INTEGER PRIMARY KEY,
        fk_wordId INTEGER,
        fk_linkId INTEGER,
        FOREIGN KEY (fk_wordId) REFERENCES wordList(rowId),
        FOREIGN KEY (fk_linkId) REFERENCES linkBetweenURL(rowId)
    );
    ''')

    # Подтверждаем изменения и закрываем соединение
    conn.commit()
    conn.close()


def show_db_structure(dbFileName):
    """Функция для вывода структуры базы данных."""
    conn = sqlite3.connect(dbFileName)
    cur = conn.cursor()
    
    # Получаем список всех таблиц
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cur.fetchall()
    
    print("Структура базы данных:")
    for table in tables:
        table_name = table[0]
        print(f"\nТаблица: {table_name}")
        
        # Получаем структуру таблицы
        cur.execute(f"PRAGMA table_info({table_name});")
        columns = cur.fetchall()
        
        print("  Колонки:")
        for column in columns:
            print(f"    {column[1]} ({column[2]})")
    
    conn.close()

def show_table_contents(dbFileName, table_name):
    """Функция для вывода содержимого таблицы."""
    conn = sqlite3.connect(dbFileName)
    cur = conn.cursor()
    
    try:
        # Выполняем запрос для получения всех данных из таблицы
        cur.execute(f"SELECT * FROM {table_name};")
        rows = cur.fetchall()
        
        if rows:
            print(f"\nСодержимое таблицы '{table_name}':")
            for row in rows:
                print(row)
        else:
            print(f"\nТаблица '{table_name}' пуста.")
    
    except sqlite3.Error as e:
        print(f"Ошибка: {e}")
    
    conn.close()

import sqlite3

def insert_test_data(dbFileName):
    """Функция для заполнения базы данных тестовыми данными."""
    conn = sqlite3.connect(dbFileName)
    cur = conn.cursor()

    try:
        # Вставляем данные в таблицу wordList
        cur.execute("INSERT INTO wordList (word, isFiltried) VALUES ('example', 0);")
        cur.execute("INSERT INTO wordList (word, isFiltried) VALUES ('test', 1);")
        
        # Вставляем данные в таблицу URLList
        cur.execute("INSERT INTO URLList (URL) VALUES ('http://example.com');")
        cur.execute("INSERT INTO URLList (URL) VALUES ('http://test.com');")
        
        # Вставляем данные в таблицу wordLocation
        cur.execute("INSERT INTO wordLocation (fk_wordId, fk_URLId, location) VALUES (1, 1, 15);")
        cur.execute("INSERT INTO wordLocation (fk_wordId, fk_URLId, location) VALUES (2, 2, 30);")
        
        # Вставляем данные в таблицу linkBetweenURL
        cur.execute("INSERT INTO linkBetweenURL (fk_FromURL_Id, fk_ToURL_Id) VALUES (1, 2);")
        cur.execute("INSERT INTO linkBetweenURL (fk_FromURL_Id, fk_ToURL_Id) VALUES (2, 1);")
        
        # Вставляем данные в таблицу linkWord
        cur.execute("INSERT INTO linkWord (fk_wordId, fk_linkId) VALUES (1, 1);")
        cur.execute("INSERT INTO linkWord (fk_wordId, fk_linkId) VALUES (2, 2);")
        
        # Сохраняем изменения
        conn.commit()
        print("База данных успешно заполнена тестовыми данными.")
    
    except sqlite3.Error as e:
        print(f"Ошибка при вставке данных: {e}")
    
    finally:
        conn.close()


def clear_all_tables(dbFileName):
    """Функция для очистки данных во всех таблицах базы данных."""
    conn = sqlite3.connect(dbFileName)
    cur = conn.cursor()
    
    try:
        # Отключаем ограничения внешних ключей, чтобы не было проблем при очистке связанных таблиц
        cur.execute("PRAGMA foreign_keys = OFF;")
        
        # Очищаем таблицы
        cur.execute("DELETE FROM linkWord;")
        cur.execute("DELETE FROM linkBetweenURL;")
        cur.execute("DELETE FROM wordLocation;")
        cur.execute("DELETE FROM URLList;")
        cur.execute("DELETE FROM wordList;")
        
        # Проверяем наличие таблицы sqlite_sequence
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sqlite_sequence';")
        if cur.fetchone():
            # Сбрасываем значения автоинкремента, если таблица существует
            cur.execute("DELETE FROM sqlite_sequence WHERE name='linkWord';")
            cur.execute("DELETE FROM sqlite_sequence WHERE name='linkBetweenURL';")
            cur.execute("DELETE FROM sqlite_sequence WHERE name='wordLocation';")
            cur.execute("DELETE FROM sqlite_sequence WHERE name='URLList';")
            cur.execute("DELETE FROM sqlite_sequence WHERE name='wordList';")
        
        # Включаем обратно ограничения внешних ключей
        cur.execute("PRAGMA foreign_keys = ON;")
        
        # Подтверждаем изменения
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"Ошибка при очистке данных: {e}")
    
    finally:
        conn.close()
