import pickle
from pelicandb import Pelican,DBSession,feed
import os
from pathlib import Path
import os
import time


import queue
import threading


"""
Базовые примеры : CRUD-операции без транзакций, индексов

"""

#Инициализация БД, path= путь к каталогу БД
db = Pelican("samples_db1",path=os.path.dirname(Path(__file__).parent))

#добавление документа без ИД
id = db["goods"].insert({"name":"Банан"})
print("Добавлено:",id,sep=" ")

#добавление документа с ИД
try:
    id = db["goods"].insert({"name":"Банан", "_id":"1"})
except:
    print("Такой документ уже есть")    

#Upsert документа
db["goods"].insert({"name":"Персик", "price":100, "_id":"2"}, upsert=True)
db["goods"].insert({"name":"Персик", "price":99, "_id":"2"}, upsert=True)

#Добавление набора
ids = db["goods"].insert([{"name":"Яблоко", "price":60}, {"name":"Груша", "price":70}], upsert=True)
print("Добавлено:",ids,sep=" ")

#Все документы коллекции
result = db["goods"].all()
print(result)

#Получить по id
result = db["goods"].get("2")
print(result)

#тоже самое через find
result = db["goods"].find({"_id":"2"})
print(result)


#Получить по id конкретную версию документа
result = db["goods"].get_version("2",0)
print(result)

#поиск по условию #1
result = db["goods"].find({"name":"Персик"})
print(result)

#поиск по условию #2
result = db["goods"].find({"price":{"$lte":70}})
print(result)

#поиск по условию #3
result = db["goods"].find({"name":{"$regex":"Пер"}})
print(result)

#Update - поиск, обновление документов коллекциии
#Условие update аналогичны find, a аргумент данных - insert/upsert
db["goods"].update({"name":"Персик"},{"updated":True})
print(db["goods"].find({"name":"Персик"}))

#Delete - поиск, удаление документов коллекциии
#Условие delete аналогичны find, a аргумент данных - insert/upsert
db["goods"].delete({"name":"Персик"})
#Условие в виде списка
db["goods"].delete(["1","2"])

#Сжатие удаленных записей (необязательно)
db['goods'].shrink()

#полное удаление всей коллекции
db["goods"].clear()


"""
Индексы : hash-индекс и текстовый индекс

"""
# hash-индексы
db['goods'].register_hash_index("hash_barcode","barcode", dynamic=False) #хранимый индекс по полю barcode
#после добавления документов, автоматически обновляются индексы
db["goods"].insert([{"name":"Яблоко", "price":60, "barcode":"22000001441" }, {"name":"Груша", "price":70,"barcode":"22000001442"}], upsert=True)
#соответсвенно работает поиск по индексу
r = db['goods'].get_by_index(db["hash_barcode"],"22000001442")
print(r)
db['goods'].register_hash_index("hash_barcode_dynamic","barcode", dynamic=True) #регистрация динамического индекса индекс по полю barcode
#для динамического при запуске имеет смысл реиндексировать
db['goods'].reindex_hash("hash_barcode_dynamic")
r = db['goods'].get_by_index(db["hash_barcode_dynamic"],"22000001442")
print(r)

#текстовые индексы
#регистрация текстового индекса
db['goods'].register_text_index("text_regular","name", dynamic=False) #there are stored indexes
#при необходимости (уже существует база с данными) реиндексируем
db['goods'].reindex_text("text_regular")
db["goods"].insert([{"name":"Яблоко Голден", "price":60, "barcode":"22000001443" }], upsert=True)
t = db['text_regular'].search_text_index("Ябл")
print(t)


"""
Транзакции и хранимые процедуры. Функции поиска. Триггеры.
"""
#Обычная транзакция. Запись в БД происходит поосле того как все операции выполнятся. Если не выполнятся, то транзакция не фиксируется.
#Например, при повторном запуске, первая операция выдаст ошибку
try:
    with DBSession(db) as s:
        
        docs = [{"name":"товар - 1","_id":"12"},{"name":"товар - 2","_id":"121"} ]
        id = db["goods2"].insert(docs, upsert=False, session=s)
        id = db["goods2"].insert(docs, upsert=True, session=s)
except Exception as e:
    print("Транзакция не записана:" + str(e))  

#Использование функции для поиска, вместо условий
def check_name(document, value):
    if document.get("name")== value:
        return True
    else:
        return False
#передаем функцию как параметр, она работает с документом
res2 = db['goods'].find([check_name,"Груша"])


#Использование функции в качестве триггера "перед записью"
def update_document_before_change(type,document):
    
    if document == None:
        raise ValueError("Document is null")
    else:
        if isinstance(document, list):
            for doc in document:
                doc['Checked'] = True
        else:
            document['Checked'] = True

db["goods2"].register_before_change_handler(update_document_before_change)
id = db["goods2"].insert([{"name1":"Банан","_id":"111222"}], upsert=True)


#Использование функции для контроля перед записью

def check_document_before_change(type,document):
    
    if document == None:
        raise ValueError("Document is null")
    else:
        if not 'barcode' in document:
            #raise ValueError("No barcode in document")
            print("No barcode in document")

db["goods2"].register_before_change_handler(check_document_before_change)
id = db["goods2"].insert([{"name1":"Банан","_id":"111222"}], upsert=True)
try:
    with DBSession(db) as s:
        
        docs = [{"name":"товар - 1","_id":"12"},{"name":"товар - 2","_id":"121"} ]
        id = db["goods2"].insert(docs, upsert=True, session=s)
        id = db["goods3"].insert(docs, upsert=True, session=s)
except Exception as e:
    print("Транзакция не записана:" + str(e)) 



"""
Дополнительные приемы повышения производитеьности
"""
#1. Работа с предварительно инициализированным стеком баз данных
#некий словарь с базами данных
db = Pelican("samples_db1",path=os.path.dirname(Path(__file__).parent))
dbmap = {"samples_db1":db}

#2. Начальная инициализация (превентивное чтение таблиц)
dbmap["samples_db1"].initialize()

#3. Использование опции singletone для исключения проверки изменений (если нет параллельной записи)
db = Pelican("samples_db1",path=os.path.dirname(Path(__file__).parent), singleton=True)

#4. Использование опции RAM=True для размещения данных коллекции в памяти (по умолчанию RAM=True)
db = Pelican("samples_db1",path=os.path.dirname(Path(__file__).parent), RAM = True)

#5. Работа с индексами в фоновом потоке (по умолчанию, при изменении документов - индексы записываются синхронно)
q = queue.Queue()
def indexing(q):
    while True:
        task = q.get()
        
        documents = task[0]
        collection_name = task[1]
        db_name = task[2]
        operation = task[3]

        if operation=="add":
            dbmap[db_name][collection_name]._add_values_to_unique_indexes(documents)
            dbmap[db_name][collection_name]._add_values_to_text_indexes(documents)
        elif operation=="delete":
            dbmap[db_name][collection_name]._delete_values_from_unique_indexes(documents)
            dbmap[db_name][collection_name]._delete_values_from_text_indexes(documents)

        q.task_done()


tinput = threading.Thread(target=indexing, args=(q,))
tinput.daemon = True
tinput.start()  

#очередь передается в качестве параметра объекту БД
db2 = Pelican("samples_db2",path=os.path.dirname(Path(__file__).parent),RAM = False, queue=q, singleton=True)

dbmap["samples_db2"] = db2

db2['goods'].register_hash_index("hash_barcode","barcode", dynamic=False)
db2["goods"].insert([{"name":"Яблоко", "price":60, "barcode":"22000001441" }, {"name":"Груша", "price":70,"barcode":"22000001442"}], upsert=True)

#Тут надо понимать, что т.к. индексы записываются асинхронно, то и использование индексов может не поспеть, поэтому ставю sleep
time.sleep(1)
r = db2['goods'].get_by_index(db2["hash_barcode"],"22000001442")
print(r)


"""
Упрощенная работа с готовыми датасетами (синхронизация данных). Создано для того чтобы не заниматься парсингом JSON на стороне БД, а передать сообщение как есть. Для этого оно должно быть в специальном формате.
"""
#в качестве параметра, передается "стек баз данных" из предыдущего примера. Вторым параметром feed идет либо строка, либо python-объект с командами в следующем формате
"""
{
        "<имя БД>": {
            "<имя коллекции>": {
                "uid": "<(необязательно) ИД-операции, для ответа>",
                "<команда:insert/upsert/update/delete/get/find>": <документ(для update-[<условие>,<документ>])> 
            }
        }
    }
"""

#Пример 1 upsert 1 документа
res = feed(dbmap,[
    {
        "samples_db1": {
            "goods_1": {
                "uid": "23",
                "upsert": {
                    "name": "banana"
                }
            }
        }
    }
])

#Пример 2 с транзакцией (транзакция оформляется в квадратные скобки)
res = feed(dbmap,[
    {
        "samples_db1": [ #транзакция
            {
                "goods_1": {
                    "upsert": {
                        
                        "name": "banana"
                    },
                    "uid": "s1"

                }
            },
            {
                "operations_1": {
                    "insert": {
                        
                        "type": "client_operation"
                    },
                    "uid": "s2"
                }
            }
        ]
    }
])

#Пример 3 с поиском
res2 = feed(dbmap,[
    {
        "samples_db1": {
            "goods_1": {
                "uid": "23",
                "find": {
                    "name": "banana"
                }
            }
        }
    }
])

