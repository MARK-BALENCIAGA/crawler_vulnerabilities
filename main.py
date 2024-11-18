import requests
from bs4 import BeautifulSoup, Tag
import sqlite3
import re
import logging
from operator import itemgetter
from prettytable import PrettyTable
from urllib.parse import urlparse
from urllib.parse import urljoin
import matplotlib.pyplot as plt
import logging
from collections import deque
import re
import os
import argparse
import database
from statistics import mean

# Функция для получения уровня логирования из аргументов командной строки
def parse_args():
    parser = argparse.ArgumentParser(description='Logging level example.')
    parser.add_argument(
        '--log', 
        default='WARNING', 
        help='Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)'
    )
    args = parser.parse_args()
    return args

class Crawler:
    # Конструктор Инициализация паука с параметрами БД
    def __init__(self, dbFileName):
        self.dbFileName = dbFileName    


    # Очистка всех данных в БД
    def clear_db(self):
        logging.debug(f"Crawler.clear_db: started")
        database.clear_all_tables(self.dbFileName)
        logging.info(f"Crawler.clear_db: Очистка БД прошла успешно")


    # Инициализация таблиц в БД
    def initDB(self):
        database.create_database(self.dbFileName)
        logging.info(f"Crawler.initDB: Инициализация БД прошла успешно")


    # Проиндексирован ли URL (проверка наличия URL в БД)
    def isIndexed(self, url):
        conn = sqlite3.connect(self.dbFileName)
        cur = conn.cursor()
        try:
            # Получаем rowId из таблицы URLList для данного URL
            cur.execute("SELECT rowId FROM URLList WHERE URL = ?;", (url,))
            url_result = cur.fetchone()
            
            if self.isFile(url):
                return True
            elif url_result is not None:
                # Получаем fk_URLId
                fk_URLId = url_result[0]
                
                # Проверяем наличие записи в таблице linkWord для данного fk_URLId
                cur.execute("SELECT rowId FROM wordLocation WHERE fk_URLId = ?;", (fk_URLId,))
                link_word_result = cur.fetchone()
                conn.commit()
                return link_word_result is not None  # True, если запись найдена
            else:
                return False  # URL не найден в таблице URLList
        except sqlite3.Error as e:
            logging.error(f"Ошибка базы данных при проверке URL: {e}")
            return False
        finally:
            cur.close()
            conn.close()


    # Проверка, сущесвтует ли данный URL в базе данных.
    def isInURLList(self, url): 
        conn = sqlite3.connect(self.dbFileName)
        cur = conn.cursor()
        try:
            # Небезопасное формирование SQL-запроса
            query = f"SELECT rowId FROM URLList WHERE URL = '{url}';"
            cur.execute(query)
            result = cur.fetchone()
            conn.commit()
            return result is not None  # True, если запись найдена
        except sqlite3.Error as e:
            logging.error(f"Ошибка базы данных при проверке URL: {e}")
            return False
        finally:
            cur.close()
            conn.close()
        
    
    # Добавление ссылки с одной страницы на другую
    def addLinkRef(self, from_url, to_url, linkText):
        logging.debug(f"Crawler.addLinkRef: started")

        try:
            # Открываем соединение с базой данных
            conn = sqlite3.connect(self.dbFileName)
            cur = conn.cursor()

            # Проверяем, существует ли запись о from_url
            if not self.isInURLList(from_url):
                logging.error(f"URL {from_url} не найден в базе данных URLList. Связь не будет добавлена.")
                return

            # Получаем идентификатор from_url
            cur.execute("SELECT rowId FROM URLList WHERE URL = ?;", (from_url,))
            from_url_id = cur.fetchone()[0]

            # Проверяем, существует ли запись о to_url
            if not self.isInURLList(to_url):
                logging.error(f"URL {to_url} не найден в базе данных URLList. Связь не будет добавлена.")
                return

            # Получаем идентификатор to_url
            cur.execute("SELECT rowId FROM URLList WHERE URL = ?;", (to_url,))
            to_url_id = cur.fetchone()[0]

            # Проверяем, существует ли связь между URL в таблице linkBetweenURL
            cur.execute("SELECT rowId FROM linkBetweenURL WHERE fk_FromURL_Id = ? AND fk_ToURL_Id = ?;", 
                        (from_url_id, to_url_id))
            link_ref_result = cur.fetchone()
            if not link_ref_result:
                # Если связи нет, добавляем её
                cur.execute("INSERT INTO linkBetweenURL (fk_FromURL_Id, fk_ToURL_Id) VALUES (?, ?);", 
                            (from_url_id, to_url_id))
                conn.commit()
                logging.debug(f"Связь между URL {from_url} и {to_url} добавлена в linkBetweenURL.")
                link_ref_id = cur.lastrowid  # Получаем ID новой записи
            else:
                link_ref_id = link_ref_result[0]
                logging.debug(f"Связь между URL {from_url} и {to_url} уже существует в linkBetweenURL.")

            # Сохраняем изменения в базе данных
            conn.commit()

        except sqlite3.Error as e:
            # Откатываем изменения и логируем ошибку
            logging.error(f"Ошибка базы данных при добавлении связи между URL: {e}")
            if conn:
                conn.rollback()

        finally:
            # Закрываем соединение в любом случае
            cur.close()
            conn.close()
            logging.debug("Соединение с базой данных закрыто")
        
        if linkText:
            # Разбиваем текст ссылки на слова
            words = self.separateWords(linkText)

            # Обрабатываем каждое слово
            for word in words:
                # Получаем или создаем идентификатор слова в таблице wordList
                word_id = self.getEntryId("wordList", "word", word, True)
                try: 
                    # Открываем соединение с базой данных
                    conn = sqlite3.connect(self.dbFileName)
                    cur = conn.cursor()
                    # Добавляем запись в таблицу linkWord, связывая слово с linkBetweenURL
                    cur.execute("INSERT INTO linkWord (fk_wordId, fk_linkId) VALUES (?, ?);", (word_id, link_ref_id))
                    # Сохраняем изменения в базе данных
                    conn.commit()
                    
                except sqlite3.Error as e:
                    # Откатываем изменения и логируем ошибку
                    logging.error(f"Ошибка базы данных при добавлении связи между URL: {e}")
                    if conn:
                        conn.rollback()

                finally:
                    # Закрываем соединение в любом случае
                    cur.close()
                    conn.close()
                    logging.debug("Соединение с базой данных закрыто")


    # разделение строки str на отдельные слова 
    def separateWords(self, text):
        wordsList = text.split()
        return wordsList
    

    # Получение текста со страницы
    def getTextOnly(self, soup):
        for script_or_style in soup(["script", "style"]):
            script_or_style.extract()

        # Извлекаем текст
        text = soup.get_text(separator="\n")  # Текст, разделённый абзацами
        
        # Очищаем текст от лишних пробелов и пустых строк
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        
        # Разделяем текст на абзацы и объединяем в одну строку
        text = "\n".join(lines)

        # Приводим весь текст к нижнему регистру
        text = text.lower()

        # Исключаем все символы, не являющиеся буквами и цифрами
        pure_text =  re.sub(r'[^\w\s]', '', text)

        return pure_text


    # Скачивает страницу из интернета   
    def get_soup(self, url):
        logging.debug(f"Crawler.crawl: started")
        try:
            response = requests.get(url, timeout=(5, 10))
            if response.status_code == 200:
                logging.debug(f"Страница {url} успешно загружена.")
                return BeautifulSoup(response.content, 'html.parser')
            else:
                logging.error(f"Ошибка загрузки страницы {url}: код статуса {response.status_code}")
                return None
        except requests.RequestException as e:
            logging.error(f"Ошибка при загрузке страницы {url}: {e}")
        return None


    # Добавляет URL в таблицу URLList перед началом индексации.
    def addUrlToIndex(self, url):
        # Открываем сессию с БД
        conn = sqlite3.connect(self.dbFileName)  # Укажите имя вашей базы данных
        cur = conn.cursor()
        try:
            # Добавляем URL в таблицу URLList
            cur.execute("INSERT INTO URLList (URL) VALUES (?);", (url,))
            logging.debug(f"URL '{url}' добавлен в таблицу URLList.")
            
            # Фиксируем изменения
            conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Ошибка базы данных при добавлении URL в URLList: {e}")
            # Откатываем изменения в случае ошибки
            conn.rollback()
        finally:
            # Закрываем курсор и соединение с базой данных
            cur.close()
            conn.close()

    # Возвращает идентификатор записи в таблице. Если запись не существует, создать новую, если параметр createNew=True.
    def getEntryId(self, table, field, value, createNew=True, is_filtered=0):

        conn = sqlite3.connect(self.dbFileName)
        cur = conn.cursor()
        
        try:
            # Проверяем, есть ли значение в таблице
            cur.execute(f"SELECT rowId FROM {table} WHERE {field} = ?;", (value,))
            result = cur.fetchone()
            
            if result:
                # Если запись найдена, возвращаем её ID
                return result[0]

            # Если запись не найдена и createNew=True, вставляем новую запись с isFiltried
            if createNew:
                cur.execute(f"INSERT INTO {table} ({field}, isFiltried) VALUES (?, ?);", (value, is_filtered))
                conn.commit()
                new_id = cur.lastrowid
                return new_id

            # Если createNew=False и запись не найдена
            return None

        except sqlite3.Error as e:
            logging.error(f"Ошибка базы данных при работе с таблицей {table}: {e}")
            conn.rollback()  # Откатываем транзакцию в случае ошибки
            return None

        finally:
            # Закрываем курсор и соединение с базой данных в любом случае
            cur.close()
            conn.close()


    # Проверяем, что страница не является файлом 
    def isFile(self, url):
        file_extensions = set([
        ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", 
        ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".zip", ".rar", ".7z", ".tar", ".gz",
        ".mp3", ".mp4", ".avi", ".mkv", ".flv", ".mov"
        ])

        # Получаем расширение файла из URL
        file_extension = os.path.splitext(url)[1].lower()

         # Если это файл, пропускаем URL
        if file_extension in file_extensions:
            return  True
        else:
            return False


    def save_url_text(self, url_id, url_text):
        """
        Сохраняет текст страницы в таблице URLText с привязкой к URLId.

        :param url_id: ID URL в таблице URLList.
        :param url_text: Текст страницы.
        """
        conn = sqlite3.connect(self.dbFileName)
        cursor = conn.cursor()

        cursor.execute('''
        INSERT INTO URLText (fk_URLId, url_text)
        VALUES (?, ?)
        ''', (url_id, url_text))

        conn.commit()
        conn.close()

    # Индексация стриниц
    def addToIndex(self, url, soup):

        # если страница уже проиндексирована, то ее не индексируем
        if self.isIndexed(url):
            return
        
        if self.isFile(url):
            return
        
        # Получаем список слов из индексируемой страницы
        text = self.getTextOnly(soup)

        # logging.debug(f"Весь текст со страницы {url}: \n {text} \n")

        

        words = self.separateWords(text)

        # Получаем идентификатор URL
        urlId = self.getEntryId("URLList", "URL", url, True)

        self.save_url_text(urlId, text)

        # Список игнорируемых слов
        ignoreWords = set([
            "в", "на", "с", "по", "к", "из", "у", "о", "от", "до", "об", "за", 
            "над", "под", "перед", "при", "между", "через", "про", "для", "без", 
            "ради", "благодаря", "ввиду", "вместо", "вслед", "из-за", "из-под", 
            "сверх", "среди", "вокруг", "насчёт", "после", "посредством", "вдоль", 
            "помимо", "около", "против", "сквозь", "вне", "вследствие", "согласно", 
            "навстречу", "со", "через", "вопреки", "посредством", "мимо"
        ])

        # Обрабатываем каждое слово на странице
        for i, word in enumerate(words):
            is_filtered = 1 if word in ignoreWords else 0
            
            # Получаем идентификатор слова в таблице wordList, добавляя флаг isFiltered
            wordId = self.getEntryId("wordList", "word", word, True, is_filtered)

            # Пропускаем слова, которые в списке ignoreWords (isFiltered = 1)
            if is_filtered:
                continue
            
            # Добавляем запись в wordLocation для не игнорируемых слов
            try:
                conn = sqlite3.connect(self.dbFileName)
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO wordLocation (fk_wordId, fk_URLId, location) VALUES (?, ?, ?);",
                    (wordId, urlId, i+1)
                )
                conn.commit()
            except sqlite3.Error as e:
                logging.error(f"Ошибка базы данных при добавлении в wordLocation: {e}")
                conn.rollback()
            finally:
                cur.close()
                conn.close()


 # Начиная с заданного списка страниц, выполняет поиск в ширину до заданной глубины, индексируя все встречающиеся по пути страницы
    def crawl(self, urlList, maxDepth=2):

        for currDepth in range(0, maxDepth+1):
            logging.debug(f"Обработка глубины {currDepth}")

            next_url_list = []  # Список для URL следующего уровня глубины
            link_text = ""
            # Обход каждого url на текущей глубине
            for url in urlList:
                logging.debug(f"Обработка URL: {url} на глубине {currDepth}")
                if not self.isInURLList(url):
                    logging.debug(f"Добавляем URL {url} в таблицу UrlList")
                    self.addUrlToIndex(url)
                
                if self.isIndexed(url):
                    logging.debug(f"URL {url} уже индексировался ранее")
                    continue  
                
                # Использовать парсер для работы с тегами
                soup = self.get_soup(url)
                if soup is None:
                    continue

                # Получить список тэгов <a> с текущей страницы
                for tag in soup.find_all('a', href=True):
                    href = tag['href']

                    # Убираем пустые ссылки, якоря и обрабатываем относительные пути
                    href = href.split('#')[0]  # Убираем якоря
                    full_link = urljoin(url, href)  # Приводим к полному URL
                    
                    # Обрезаем все, что после знака "?"
                    full_link = full_link.split('?')[0]

                    if full_link and full_link not in next_url_list and full_link not in urlList:
                        next_url_list.append(full_link)
                        link_text = tag.text.strip()  # Получаем текст ссылки
                        link_text = link_text.lower()
                        logging.debug(f"Добавлена новая ссылка для обхода: {full_link} (Текст: {link_text})")
                        self.addUrlToIndex(full_link)

                    # Добавить в таблицу linkBetweenURL связь между текущим URL и найденной ссылкой
                    self.addLinkRef(url, full_link, link_text)
                    link_text = ""
                # Вызвать функцию для добавления содержимого в индекс
                self.addToIndex(url, soup)
                logging.debug(f"Индексирование страницы {url} завершено")

            # Переход к следующему уровню глубины
            urlList = next_url_list
            logging.info(f"Crawler.clear_db: Обработка уровня {currDepth} завершена, найдено {len(urlList)} новых ссылок для следующего уровня.")
        






class Searcher:
    def __init__(self, dbFileName):
        """Конструктор класса Searcher: инициализация соединения с БД"""
        # Открываем соединение с базой данных
        self.con = sqlite3.connect(dbFileName)
        logging.debug("Соединение с БД установлено.")

    def __del__(self):
        """Деструктор: закрытие соединения с БД"""
        # Закрываем соединение с базой данных при удалении объекта
        self.con.close()
        logging.debug("Соединение с БД закрыто.")

    def dbcommit(self):
        """Зафиксировать изменения в БД"""
        self.con.commit()
        logging.debug("Изменения в БД зафиксированы.")

    def getWordsIds(self, queryString):
        # Привести поисковый запрос к нижнему регистру для соответствия данным в БД
        queryString = queryString.lower()
        logging.debug(f"Приведённый к нижнему регистру запрос: '{queryString}'")

        # Разделить запрос на отдельные слова
        queryWordsList = queryString.split(" ")
        logging.debug(f"Разделённый список слов запроса: {queryWordsList}")

        # Список для хранения идентификаторов rowid
        rowidList = []

        # Для каждого слова из запроса ищем идентификатор в БД
        for word in queryWordsList:
            sql = "SELECT rowId FROM wordList WHERE word = ? LIMIT 1;"

            try:
                result_row = self.con.execute(sql, (word,)).fetchone()
            except sqlite3.Error as e:
                logging.error(f"Ошибка базы данных приполучении данных из таблицы wordList: {e}")
                self.con.rollback()
                continue  # Продолжаем обработку оставшихся слов после ошибки

            # Если идентификатор найден, добавляем его в список
            if result_row:
                word_rowid = result_row[0]
                rowidList.append(word_rowid)
                logging.debug(f"Слово '{word}' найдено с идентификатором {word_rowid}.")
            else:
                # Если слово не найдено, генерируем исключение и прекращаем поиск
                error_msg = f"Слово '{word}' не найдено в БД."
                logging.error(error_msg)
                raise Exception(error_msg)

        logging.debug(f"Список идентификаторов слов: {rowidList}")
        # Возвращаем список идентификаторов
        return rowidList
    

    # Формирует таблицу, содержащую все сочетания позиций всех слов поискового запроса в URL-адресах
    def getMatchRows(self, queryString):

        # Приведение запроса к нижнему регистру и разбиение на слова
        queryString = queryString.lower()
        wordsList = queryString.split(" ")

        # Получаем идентификаторы искомых слов
        wordsidList = self.getWordsIds(queryString)

        # Переменные для хранения частей SQL-запроса
        sqlpart_Name = []       # столбцы для SELECT
        sqlpart_Join = []       # блоки INNER JOIN
        sqlpart_Condition = []  # условия WHERE

        # Конструирование SQL-запроса
        for wordIndex, wordID in enumerate(wordsidList):
            if wordIndex == 0:
                # Базовая часть для первого слова
                sqlpart_Name.append(f"w{wordIndex}.fk_URLId AS fk_URLId")
                sqlpart_Name.append(f"w{wordIndex}.location AS loc_queryWord{wordIndex + 1}")
                sqlpart_Condition.append(f"WHERE w{wordIndex}.fk_wordId={wordID}")
            else:
                # Дополнительные части для последующих слов
                sqlpart_Name.append(f"w{wordIndex}.location AS loc_queryWord{wordIndex + 1}")
                sqlpart_Join.append(
                    f"INNER JOIN wordLocation w{wordIndex} ON w0.fk_URLId = w{wordIndex}.fk_URLId"
                )
                sqlpart_Condition.append(f"AND w{wordIndex}.fk_wordId={wordID}")

        # Объединение частей в окончательный SQL-запрос
        sqlFullQuery = "SELECT " + ", ".join(sqlpart_Name) + "\n"
        sqlFullQuery += "FROM wordLocation w0\n"
        sqlFullQuery += " ".join(sqlpart_Join) + "\n"
        sqlFullQuery += " ".join(sqlpart_Condition)

        logging.debug(f"Сформированный SQL-запрос: {sqlFullQuery}")

        # Выполнение запроса и извлечение результата
        try:
            cur = self.con.execute(sqlFullQuery)
            rows = [row for row in cur]
            logging.debug(f"Получены строки: {rows}")
        except sqlite3.Error as e:
            logging.error(f"Ошибка выполнения SQL-запроса: {e}")
            rows = []

        # Если результаты получены, вывести их в формате таблицы
        if rows:
            # Создание таблицы для вывода с помощью PrettyTable
            table = PrettyTable()
            
            column_names = ["urlid"] + [f"loc_queryWord{index + 1}" for index in range(len(wordsidList))]
            table.field_names = column_names
            
            # Добавление строк в таблицу
            for row in rows:
                table.add_row(row)
            
        else:
            print("Нет данных для отображения.")

        # Возвращаем строки таблицы и идентификаторы слов
        return rows, wordsidList

    # Нормализует ранги в диапазоне от 0.0 до 1.0
    def normalizeScores(self, scores, smallIsBetter=0):
        resultDict = dict()  # словарь для нормализованных значений
        vsmall = 0.00001     # малая величина, чтобы избежать деления на ноль

        minscore = min(scores.values())  # минимальное значение рангов
        maxscore = max(scores.values())  # максимальное значение рангов
        score_range = maxscore - minscore  # диапазон значений

        logging.debug(f"Минимальный ранг: {minscore}, Максимальный ранг: {maxscore}, Диапазон: {score_range}")

        # Нормализация значений в зависимости от параметра smallIsBetter
        for key, val in scores.items():
            if smallIsBetter:
                # Чем меньше значение, тем лучше: нормализуем как отношение минимального значения к текущему
                normalized_value = float(minscore) / max(vsmall, val)
            else:
                # Чем больше значение, тем лучше: нормализуем значение относительно диапазона
                normalized_value = (float(val) - minscore) / max(vsmall, score_range)

            # Округление нормализованного значения до 4 знаков после запятой
            resultDict[key] = round(normalized_value, 4)
            logging.debug(f"Нормализованный ранг для {key}: {resultDict[key]}")

        return resultDict


    #  Подсчитывает частоту комбинаций искомых слов для каждой страницы (urlId) и возвращает нормализованные ранги
    def frequencyScore(self, rowsLoc):
        countsDict = {}  # Словарь для подсчета комбинаций слов на каждой странице
        logging.debug(f"frequencyScore: rowsLoc: {rowsLoc}")
        # Инициализация словаря: добавляем urlId с начальным значением 0
        for row in rowsLoc:
            urlId = row[0]
            countsDict.setdefault(urlId, 0)

        # Увеличиваем счетчик для urlId за каждую найденную комбинацию
        for row in rowsLoc:
            urlId = row[0]
            countsDict[urlId] += 1
            logging.debug(f"Увеличение счетчика для urlId {urlId}: {countsDict[urlId]}")

        logging.debug(f"countsDict: {countsDict}")
        # Нормализация рангов по частоте (чем больше значение, тем лучше)
        normalizedScores = self.normalizeScores(countsDict, smallIsBetter=0)
        logging.debug(f"Нормализованные ранги по частоте: {normalizedScores}")

        return normalizedScores


    # Получает из БД текстовое поле url-адреса по указанному urlid
    def getUrlName(self, id):
        # SQL-запрос для получения url по rowId
        sql = "SELECT URL FROM URLList WHERE rowId = ?"
        try:
            # Выполнение запроса
            cur = self.con.execute(sql, (id,))
            result = cur.fetchone()
            
            # Проверка, найден ли URL
            if result:
                url = result[0]
                logging.debug(f"URL для id {id}: {url}")
                return url
            else:
                logging.warning(f"URL для id {id} не найден в базе данных.")
                return None
        except sqlite3.Error as e:
            logging.error(f"Ошибка при получении URL для id {id}: {e}")
            return None


    # Вычисление PageRank для каждой страницы в базе данных
    def calculatePageRank(self, iterations=5):
        # Удаление текущих данных таблицы PageRank и создание новой таблицы
        self.con.execute('DROP TABLE IF EXISTS pagerank')
        self.con.execute("""CREATE TABLE IF NOT EXISTS pagerank(
                                rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                                urlid INTEGER,
                                score REAL
                            );""")

        # Создание индексов для ускорения запросов
        indexes = [
            "wordidx ON wordlist(word)",
            "urlidx ON urllist(url)",
            "wordurlidx ON wordlocation(fk_wordId)",
            "urltoidx ON linkBetweenURL(fk_ToURL_Id)",
            "urlfromidx ON linkBetweenURL(fk_FromURL_Id)",
            "rankurlididx ON pagerank(urlid)"
        ]
        for index in indexes:
            self.con.execute(f"DROP INDEX IF EXISTS {index.split()[0]};")
            self.con.execute(f"CREATE INDEX IF NOT EXISTS {index};")
            self.con.execute(f"REINDEX {index.split()[0]};")
        
        # Установка начального значения PageRank = 1.0 для каждой страницы
        self.con.execute('INSERT INTO pagerank (urlid, score) SELECT rowid, 1.0 FROM urllist')
        self.dbcommit()

        # Коэффициент затухания
        damping_factor = 0.85

        # Основной цикл итераций PageRank
        for i in range(iterations):
            #logging.debug(f"Итерация {i + 1}")
            
            # Создаем временный словарь для хранения новых значений PageRank
            new_ranks = {}
            
            # Извлекаем все urlid для обхода
            for (urlid,) in self.con.execute("SELECT rowId FROM urllist"):
                # Устанавливаем начальное значение PageRank для текущего URL
                pr = (1 - damping_factor)
                
                # Извлекаем все страницы, ссылающиеся на данный URL
                for (fromid,) in self.con.execute(
                        "SELECT DISTINCT fk_FromURL_Id FROM linkBetweenURL WHERE fk_ToURL_Id=?", (urlid,)):
                    
                    # Получаем PageRank ссылающейся страницы
                    linking_pr = self.con.execute(
                        "SELECT score FROM pagerank WHERE urlid=?", (fromid,)).fetchone()[0]
                    
                    # Считаем общее количество ссылок на ссылающейся странице
                    linking_count = self.con.execute(
                        "SELECT COUNT(*) FROM linkBetweenURL WHERE fk_FromURL_Id=?", (fromid,)).fetchone()[0]
                    
                    # Добавляем вклад ссылающейся страницы
                    pr += damping_factor * (linking_pr / linking_count)
                
                # Сохраняем новое значение PageRank в словаре
                new_ranks[urlid] = pr
            
            # Обновляем значения PageRank в таблице на основе рассчитанных значений
            for urlid, rank in new_ranks.items():
                self.con.execute('UPDATE pagerank SET score=? WHERE urlid=?', (rank, urlid))
            
            self.dbcommit()  # Применение изменений в БД


    # Получает PageRank для каждого URL в списке и нормализует его
    def pagerankScore(self, rows):
        # Извлекаем значения PageRank из таблицы и добавляем в словарь
        self.calculatePageRank()
        scores = {}
        for row in rows:
            urlid = row[0]
            pr = self.con.execute("SELECT score FROM pagerank WHERE urlid=?", (urlid,)).fetchone()[0]
            scores[urlid] = pr
        
        # Нормализуем значения PageRank
        return self.normalizeScores(scores, smallIsBetter=0)


    # Функция для нормализации значений scores в диапазон от 0 до 1.
    def normalize(self, scores):
        min_score = min(scores.values())
        max_score = max(scores.values())
        if max_score == min_score:  # Если все значения одинаковы
            return {k: 0.5 for k in scores}  # Возвращаем среднее значение для всех элементов
        return {k: (v - min_score) / (max_score - min_score) for k, v in scores.items()}

 
    # На основе поискового запроса формирует список URL, вычисляет ранги (M1, M2, M3)
    def getSortedList(self, queryString):   
        # Получить rowsLoc и wordids от getMatchRows(queryString)
        rowsLoc, wordids = self.getMatchRows(queryString)
        logging.debug(f"Результаты getMatchRows для запроса '{queryString}': {rowsLoc}")

        # Получить M1 - ранги по частоте для URL
        m1Scores = self.frequencyScore(rowsLoc)
        logging.debug(f"Ранги по частоте (M1) для URL: {m1Scores}")

        # Получить M2 - ранги по PageRank для URL
        pagerankScores = self.pagerankScore(rowsLoc)
        logging.debug(f"Ранги PageRank (M2) для URL: {pagerankScores}")

        # Вычисляем M3 как среднее значение (M1 + M2) / 2
        m3Scores = {url: (m1Scores.get(url, 0) + pagerankScores.get(url, 0)) / 2 for url in m1Scores}

        # Сортировка по убыванию M3
        rankedScoresList = sorted(m3Scores.items(), key=lambda x: x[1], reverse=True)

        print("Ранжированный результат поисковой выдачи:")
        for index, (urlid, m3) in enumerate(rankedScoresList, start=1):
            url = self.getUrlName(urlid)  # Получаем текстовое значение URL
            print(f"{index}. {url}")

        # Найти URL с максимальным и минимальным значением M1
        max_M1_url = max(m1Scores, key=m1Scores.get)
        min_M1_url = min(m1Scores, key=m1Scores.get)
        
        # Найти URL с максимальным значением M3
        max_M3_url = rankedScoresList[0][0]  # Первый элемент в отсортированном списке имеет наибольшее значение M3

        # Возвращаем rowid для max_M1_url, min_M1_url и max_M3_url
        return max_M1_url, min_M1_url, max_M3_url



    # Извлекает список слов (wordList) для указанного URL на основе таблицы wordLocation
    def getWordListForUrl(self, urlid):
        # Получаем все fk_wordId для данного urlid из таблицы wordLocation, отсортированные по location
        word_ids_with_positions = self.con.execute(
            "SELECT fk_wordId FROM wordLocation WHERE fk_urlId=? ORDER BY location", (urlid,)
        ).fetchall()
        
        # Если word_ids_with_positions пуст, возвращаем пустой список
        if not word_ids_with_positions:
            return []

        # Преобразуем кортежи в список id слов
        word_ids = [word_id[0] for word_id in word_ids_with_positions]

        # Теперь извлекаем фактические слова из таблицы wordlist по fk_wordId
        wordList = []
        for word_id in word_ids:
            word = self.con.execute(
                "SELECT word FROM wordlist WHERE rowid=?", (word_id,)
            ).fetchone()
            
            # Если слово найдено, добавляем его в wordList
            if word:
                wordList.append(word[0])

        return wordList
        


if __name__ == "__main__":
    
    args = parse_args()
   
    # Установка уровня логирования на основе переданного аргумента
    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {args.log}')

    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )

    
  

    dbName = "./database/search_engine.db"

    crawler = Crawler(dbName)

    urlList = ["https://роботека.рф/robot"]

    # urlList = ["'; DROP TABLE linkBetweenURL; --"]
    
    crawler.clear_db()
    crawler.initDB()
    crawler.crawl(urlList, maxDepth=1)

    mySearcher = Searcher(dbName)

    # serach = "example domain"

    serach = "искусственный интеллект"

    max_M1_url, min_M1_url, max_M3_url = mySearcher.getSortedList(serach)

    logging.info("Работа программы успешно завершена!")

