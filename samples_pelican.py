from pelicandb import Pelican,DBSession,feed
import os
from pathlib import Path
import os
import time


import queue
import threading


"""
Basic Examples: CRUD Operations

"""

#Initializing the database, path= path to the database directory
db = Pelican("samples_db1",path=os.path.dirname(Path(__file__).parent))

#adding a document without an ID
id = db["goods"].insert({"name":"Banana"})
print("Added :",id,sep=" ")

#adding a document with an ID
try:
    id = db["goods"].insert({"name":"Banana", "_id":"1"})
except:
    print("the document already exists")    

#Upsert
db["goods"].insert({"name":"Peach", "price":100, "_id":"2"}, upsert=True)
db["goods"].insert({"name":"Peach", "price":99, "_id":"2"}, upsert=True)

#Insert array of documents
ids = db["goods"].insert([{"name":"Apple", "price":60}, {"name":"Pear", "price":70}], upsert=True)
print("Added:",ids,sep=" ")

#All documents in the collection
result = db["goods"].all()
print(result)

#Get by ID
result = db["goods"].get("2")
print(result)

#...same thing via find
result = db["goods"].find({"_id":"2"})
print(result)


#Get a specific version of a document by id
result = db["goods"].get_version("2",0)
print(result)

#search by condition #1
result = db["goods"].find({"name":"Peach"})
print(result)

#search by condition #2
result = db["goods"].find({"price":{"$lte":70}})
print(result)

#search by condition #3
result = db["goods"].find({"name":{"$regex":"Pea"}})
print(result)

#Update - search, update collection documents
#сondition is similar to find, and the data argument is insert/upsert
db["goods"].update({"name":"Peach"},{"updated":True})
print(db["goods"].find({"name":"Peach"}))

#Delete - search, delete collection documents
#сondition is similar to find, and the data argument is insert/upsert
db["goods"].delete({"name":"Peach"})

#сondition as a list
db["goods"].delete(["1","2"])

#shrink deleted entries (optional)
db['goods'].shrink()

#complete deletion of the entire collection
db["goods"].clear()


"""
Indexes: hash index and text index

"""
# hash indexes
db['goods'].register_hash_index("hash_barcode","barcode", dynamic=False) #stored index on the barcode field
#after adding documents, indexes are automatically updated
db["goods"].insert([{"name":"Apple", "price":60, "barcode":"22000001441" }, {"name":"Pear", "price":70,"barcode":"22000001442"}], upsert=True)
#search by index works accordingly
r = db['goods'].get_by_index(db["hash_barcode"],"22000001442")
print(r)
db['goods'].register_hash_index("hash_barcode_dynamic","barcode", dynamic=True) #dynamic index registration index by barcode field
#for dynamic at startup it makes sense to reindex
db['goods'].reindex_hash("hash_barcode_dynamic")
r = db['goods'].get_by_index(db["hash_barcode_dynamic"],"22000001442")
print(r)

#text indexes
#registering a text index
db['goods'].register_text_index("text_regular","name", dynamic=False) #there are stored indexes
#if necessary (there is already a database with data) we re-index
db['goods'].reindex_text("text_regular")
db["goods"].insert([{"name":"Apple Golden", "price":60, "barcode":"22000001443" }], upsert=True)
t = db['text_regular'].search_text_index("Appl")
print(t)


"""
Transactions and stored procedures. Search functions. Triggers. 
"""
#Regular transaction. Writing to the database occurs after all operations are completed. If they are not executed, the transaction is not committed.
#For example, when this code restarted, the first operation will generate an error
try:
    with DBSession(db) as s:
        
        docs = [{"name":"Item #1","_id":"12"},{"name":"Item #2","_id":"121"} ]
        id = db["goods2"].insert(docs, upsert=False, session=s)
        id = db["goods2"].insert(docs, upsert=True, session=s)
except Exception as e:
    print("Transaction not commited:" + str(e))  

#Using a function to search instead of conditions
def check_name(document, value):
    if document.get("name")== value:
        return True
    else:
        return False
#we pass the function as a parameter, it works with the document
res2 = db['goods'].find([check_name,"Pear"])


#Using a Function as a "Before Write" Trigger
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
id = db["goods2"].insert([{"name1":"Banana","_id":"111222"}], upsert=True)


#Using the function to control before recording

def check_document_before_change(type,document):
    
    if document == None:
        raise ValueError("Document is null")
    else:
        if not 'barcode' in document:
            #raise ValueError("No barcode in document")
            print("No barcode in document")

db["goods2"].register_before_change_handler(check_document_before_change)
id = db["goods2"].insert([{"name1":"Banana","_id":"111222"}], upsert=True)
try:
    with DBSession(db) as s:
        
        docs = [{"name":"Item #1","_id":"12"},{"name":"Item #2","_id":"121"} ]
        id = db["goods2"].insert(docs, upsert=True, session=s)
        id = db["goods3"].insert(docs, upsert=True, session=s)
except Exception as e:
    print("Transaction not commited:" + str(e)) 



"""
Additional tricks to improve productivity
"""
#1. Working with a pre-initialized database stack
#some dictionary with databases
db = Pelican("samples_db1",path=os.path.dirname(Path(__file__).parent))
dbmap = {"samples_db1":db}

#2. Initialization (preemptive reading of tables)
dbmap["samples_db1"].initialize()

#3. Using the singletone option to eliminate change checking (if there is no parallel write)
db = Pelican("samples_db1",path=os.path.dirname(Path(__file__).parent), singleton=True)

#4. Using the RAM=True option to place collection data in memory (default RAM=True)
db = Pelican("samples_db1",path=os.path.dirname(Path(__file__).parent), RAM = True)

#5. Working with indexes in a background thread (by default, when documents change, indexes are written synchronously)
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

#the queue is passed as a parameter to the database object
db2 = Pelican("samples_db2",path=os.path.dirname(Path(__file__).parent),RAM = False, queue=q, singleton=True)

dbmap["samples_db2"] = db2

db2['goods'].register_hash_index("hash_barcode","barcode", dynamic=False)
db2["goods"].insert([{"name":"Apple", "price":60, "barcode":"22000001441" }, {"name":"Pear", "price":70,"barcode":"22000001442"}], upsert=True)

#Here you need to understand that because indexes are written asynchronously, then the use of indexes may not keep up, so I set sleep
time.sleep(1)
r = db2['goods'].get_by_index(db2["hash_barcode"],"22000001442")
print(r)


"""
Simplified work with ready-made datasets (data synchronization). Created so as not to parse JSON on the database side, but to transmit the message as is. To do this, it must be in a special format.
"""
#as a parameter, the "database stack" from the previous example is passed. The second parameter of feed is either a string or a python object with commands in the following format
"""
{
        "<database_name_in_stack>": {
            "<collection>": {
                "uid": "<(optional) Operation ID (for response)>",
                "<command:insert/upsert/update/delete/get/find>": <document(for update-[<condition>,<document>])> 
            }
        }
    }
"""

#Example 1: upsert 1 document
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

#Example 2 with a transaction (the transaction is formatted in square brackets)
res = feed(dbmap,[
    {
        "samples_db1": [ #transaction
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

#Example 3 with search
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

