import copy
import json
from typing import List
import os
import uuid
import pickle
from pathlib import Path
from filelock import Timeout, SoftFileLock
import inspect
from typing import Dict, Iterator, Protocol, Set, Type,Callable,Mapping,Optional
from types import TracebackType
import hashlib
import itertools
#from msgspec.json import decode,encode
import re
import time
import gc



#default timeout value
LOCK_TIMEOUT = 90
#B-tree branches sise for text search
BRANCH_SIZE=10



# Utilities

"""
General encoding and decoding functions. 
If necessary, it can be replaced by msgspec.encode() and msgspec.from msgspec.json import decode,encode

msgspec is faster:

def to_json_str(value):
     return encode(value).decode()

def from_json_str(value):
     return decode(value)

"""

def to_json_str(value):
     return json.dumps(value,ensure_ascii=False)

def from_json_str(value):
     return json.loads(value)



"""
condition check function
Syntax replicates MongoDB query syntas

:condition: query condition
:document: document

:returns: True or False condition is met to the document


"""
def check_condition(condition, document):
    if isinstance(condition,Dict):
        for key,value in condition.items(): #logical expression AND and OR
            if isinstance(value,List):
                if key=="$and":
                    res = None
                    for element in value:
                        if res==None:
                            res = check_condition(element, document)
                        else:    
                            res=res and check_condition(element, document)
                    return res 
                elif key=="$or":
                    res = None
                    for element in value:
                        if res==None:
                            res = check_condition(element, document)
                        else:    
                            res=res or check_condition(element, document)
                    return res  
                
            else:    
                if isinstance(value,Dict): #comparison operators
                    
                    if '$regex' in value: #regular expression matching
                        for key_document,value_document in document.items():
                            if key==key_document and isinstance(value_document,str):
                                pattern = re.compile(value['$regex'])
                            
                                if pattern.search(value_document):
                                    return True
                                else:
                                    return False
                    elif '$ne' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                        
                                if value['$ne']!=value_document:
                                    return True
                                else:
                                    return False
                    elif '$not' in value:
                         return not check_condition({key:value["$not"]}, document)           
                    elif '$in' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                                if value_document in value['$in']:
                                    return True
                                else:
                                    return False 
                    elif '$nin' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                                if not value_document in value['$in']:
                                    return True
                                else:
                                    return False                        
                    elif '$eq' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                        
                                if value['$eq']==value_document:
                                    return True
                                else:
                                    return False 
                    elif '$gt' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                        
                                if value_document>value['$gt']:
                                    return True
                                else:
                                    return False
                    elif '$gte' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                        
                                if value_document>=value['$gte']:
                                    return True
                                else:
                                    return False   
                    elif '$lt' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                        
                                if value_document<value['$lt']:
                                    return True
                                else:
                                    return False
                    elif '$lte' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                        
                                if value_document<=value['$lte']:
                                    return True
                                else:
                                    return False
                                                                                        
                else:   #simple condition (pattern search)
                    for key_document,value_document in document.items():
                        if key==key_document:
                            if value==value_document:
                                return True
                            else:
                                return False
    elif isinstance(condition,List):
        f = condition[0]
        #res = getattr(f,document)(*condition[1:])
        res = f(document,*condition[1:])
        return res

    return False

"""
Quickly writes the data modification prefix to the collection file

:path: filename of collection
:prefix: modification prefix

"""
def write_prefix(path,prefix):
    with open(path,"r+", encoding='utf-8') as f:
            text =f.read(36)
            f.seek(0)
            f.write(prefix)

"""
splits the dictionary into 2 dictionaries
"""
def splitDict(d):
    n = len(d) // 2          
    i = iter(d.items())      

    d1 = dict(itertools.islice(i, n))   
    d2 = dict(i)                        

    return d1, d2

def format_datastr(id,t):
    return '"{id}":[{begin},{end},{version},"{uid}"]'.format(id = id, begin=t[0], end=t[1],version = t[2],uid=t[3])

"""
This function builds a special balanced binary tree.
Each branch concatenates all values of the indexed field that is being searched.
Thus, the search includes branches containing the desired substring
"""
def split_list2(base):
            slist ={}
            if len(base)>BRANCH_SIZE:
                splitted = splitDict(base)
                slist["0"]={}
                slist["0"]["_id"] = "0"
                slist["0"]["base"] = splitted[0]
                slist["0"]["ids"] = list(slist["0"]["base"].keys())
                slist["0"]["text"] = "".join(list(slist["0"]["base"].values()))

                slist["1"]={}
                slist["1"]["_id"] = "1"
                slist["1"]["base"] = splitted[1]
                slist["1"]["ids"] = list(slist["1"]["base"].keys())
                slist["1"]["text"] = "".join(list(slist["1"]["base"].values()))
            else:
                slist["0"]={}
                slist["0"]["_id"] = "0"
                slist["0"]["base"] = base
                slist["0"]["ids"] = list(slist["0"]["base"].keys())
                slist["0"]["text"] = "".join(list(slist["0"]["base"].values()))    
                 
            
            if len(slist["0"]["base"])>BRANCH_SIZE:
                for k in slist.keys():
                    current_part = slist[k]
                    current_part['child'] = split_list2( current_part['base'])
                    

            return slist
"""
search for document IDs, along the branches of the B-tree

:index: text index
:s: search string

:returns: list of document IDs

"""
def get_index_ids_by_string(index,s):
    result = []
    for k,v in index.items():
        if s in v["text"]:
            if 'child' in v:
                internal = get_index_ids_by_string(v['child'],s)
            else:
                internal = v['ids']

            for i in internal:
                 result.append(i)
    return result 

"""
Adding a value to B-tree
Balancing happens automatically

:base: dictionary of B-tree values
:value_id: ID to be insert
:value_id: value to be insert

"""
def insert_in_branchces_dynamic_text_binary(base,value_id,value):

    if not '0' in base:
                    base["0"] = {}
                    base["0"]['_id']="0"
                    base["0"]['ids']=[value_id]
                    base["0"]['text']=value
                    base["0"]['base']={value_id:value}
    elif not '1' in base:
        base["1"] = {}
        base["1"]['_id']="1"
        base["1"]['ids']=[value_id]
        base["1"]['text']=value
        base["1"]['base']={value_id:value}

    else:
            if len(base["0"]['ids'])<len(base["1"]['ids']):
                    
                    current_base = base["0"]
            else:     
    
                    current_base = base["1"]
                 
            current_base['ids'].append(value_id)
            current_base['text']+=value
            current_base['base'][value_id] =value 

            if len(current_base['ids'])>BRANCH_SIZE:
                if not 'child' in current_base:
                    current_base['child'] = split_list2( current_base['base']) 
                else:
                    insert_in_branchces_dynamic_text_binary(current_base['child'],value_id,value)                        

"""
Deleting a value from B-tree

:base: dictionary of B-tree values
:value_id: ID to delete
:value_id: value to be removed

"""
def delete_in_branchces_dynamic_text_binary(base,value_id,value):

    for k in base:
        index = base[k]
        if value_id in index['ids']:
            index['ids'].remove(value_id)
            index['base'].pop(value_id)
            index['text']  = "".join(list(index['base'].values()))

            if 'child' in index:
                delete_in_branchces_dynamic_text_binary(index['child'],value_id,value) 



"""
The main class for connecting to the database and working with collections 
"""    
class Pelican(dict):
    """
    A class that stores data and in which all basic operations with data and database storage take place
    """
    class Collection:
        #********************** External functions *********************

        # CRUD-functions

        """
        Insert document/documants into collection

        :document: document or list of documents to be added
        
        Arguments:
        :no_index: OFF to change indexes
        :upsert: INSERT or UPDATE
        :session: transaction instance


        """
        def insert(self,document,**kwargs):
             if isinstance(document, str):
                try:
                    document = json.loads(document)
                except:
                    raise ValueError(f'Message  string is not a JSON') 
        
             f = self._before_change_handler
             if f!=None:
                f("insert",document)
            
             if isinstance(document,list):
                  if kwargs.get("session")!=None:
                       result = self.insert_many(document,update = kwargs.get("update"),upsert = kwargs.get("upsert"),session = kwargs.get("session"),NoIndex=kwargs.get("no_index"))
                  else:     
                       result = self.insert_many(document,update = kwargs.get("update"),upsert = kwargs.get("upsert"),NoIndex=kwargs.get("no_index"))
             else:   
                  if kwargs.get("session")!=None:
                    result = self.fast_insert(document,update = kwargs.get("update"),upsert = kwargs.get("upsert"),session = kwargs.get("session"),NoIndex=kwargs.get("no_index"))
                  else:  
                    result = self.fast_insert(document,update = kwargs.get("update"),upsert = kwargs.get("upsert"),NoIndex=kwargs.get("no_index"))   

             return result 
       
        """
        Update documents by condition
        
        :condition: document ID/ list of documents IDs/ query condition
        :dataset: values to replace

        """
        def update(self,condition,dataset, **kwargs):
             
             if isinstance(dataset, str):
                try:
                    dataset = json.loads(dataset)
                except:
                    raise ValueError(f'Dataset is not a JSON') 
        
             f = self._before_change_handler
             if f!=None:
                f("update",[condition,dataset])

             if isinstance(condition,str):
                item = self.get(condition) 

                if item == None:
                    raise ValueError(f'Item not found {condition}')     
                  
                for key,value in dataset.items():
                    item[key] = value
                  
                if "session" in kwargs:
                    return self.fast_insert(item, update=True, session = kwargs.get("session"),NoIndex=kwargs.get("no_index"))
                else:
                    return self.fast_insert(item, update=True,NoIndex=kwargs.get("no_index"))
             
             elif isinstance(condition,list) or isinstance(condition,dict):
                  
                  
                  set = []
                  if isinstance(condition,list):
                    
                    for itemkey in condition:
                        item = copy.deepcopy(self.get(itemkey)) 
                        if item!=None:
                            for key,value in dataset.items():
                                item[key] = value
                            set.append(item)    

                  if isinstance(condition,dict): 
                       result = copy.deepcopy(self.find(condition))
                       for item in result:
                        
                        for key,value in dataset.items():
                            item[key] = value
                        set.append(item) 

                  if "session" in kwargs:

                    return self.insert_many(set, update=True, session = kwargs.get("session"))                     
                  
                  else:
                    
                    return self.insert_many(set, update=True)  
        """
        Delete document/list of document/query results

        :condition: document ID/ list of documents IDs/ query condition

        """
        def delete(self,condition, **kwargs):
             
             if isinstance(condition,str):
                item = self.get(condition)  

                if item == None:
                    raise ValueError(f'Item not found {condition}') 

                f = self._before_change_handler
                if f!=None:
                    f("delete",[condition,item])    
                  
                            
                if "session" in kwargs:
                    return self.fast_delete(item, session = kwargs.get("session"),NoIndex=kwargs.get("no_index"))                     
                else:
                    return self.fast_delete(item,NoIndex=kwargs.get("no_index")) 
             
             elif isinstance(condition,list) or isinstance(condition,dict):
                  set = []
                  if isinstance(condition,list):
                    
                    for itemkey in condition:
                        item = copy.deepcopy(self.get(itemkey)) 
                        
                        if item!=None:
                            set.append(item)    

                  if isinstance(condition,dict): 
                       result = copy.deepcopy(self.find(condition))
                       for item in result:
                        
                        if item!=None:
                            set.append(item) 

                  f = self._before_change_handler
                  if f!=None:
                    f("delete",[condition,set])  
                  
                  if "session" in kwargs:

                    return self.delete_many(set, session = kwargs.get("session"))                     
                  
                  else:
                    
                    return self.delete_many(set)                                  

        """
        remove all documents
        """
        def clear(self,  **kwargs) -> str:
            self._recording=True
            self._data = {}
            self._data_idx = {}
            self._maindata_temp = {}

            self._recording=False
            self._modification_uuid=None

            if self._is_index():
                if os.path.exists(self._path):
                    os.remove(self._path) 
            else:     
                if os.path.exists(self._path):
                    os.remove(self._path)
                if os.path.exists(self._path_data):
                    os.remove(self._path_data) 
                if os.path.exists(self._path_id):
                    os.remove(self._path_id)         

    
        #Query functions

        """
        Return document by ID

        :key: document ID   

        """
        def get(self,key):
            if self._db._RAM == True:
                return self._maindata.get(key)        
            else:
                if key in self._maindata_temp:
                     return self._maindata_temp.get(key)  
                else:     
                    last_version = self._data_idx.get(key)
                    if last_version!=None:
                        if os.path.isfile(self._path_data):
                            c = self._data.get(key+"_"+str(last_version))
                            if c!=None:
                                in_file = open(self._path_data, "rb") 
                                in_file.seek(c[0])
                                data = in_file.read(c[1]) 
                                res = pickle.loads(data)
                                return res
            return None    
        
        def get_version(self,key,version):
            
            if version!=None:
                c = self._data.get(key+"_"+str(version))
                if c!=None:
                    in_file = open(self._path_data, "rb") 
                    in_file.seek(c[0])
                    data = in_file.read(c[1]) 
                    res = pickle.loads(data)
                    return res
            return None    
        
        """
        Return document by hash-value

        :index: hash index cokkection
        :value: value of document

        """
        
        def get_by_index(self,index,value):
            key = index._data.get(hashlib.sha1(value.encode()).hexdigest())
            if key==None:
                return None
            else:
                return self.get(key)
        
        """
        Queries documents from the collection by condition

        :condition: query condition
        

        """
        def find(self,condition):    
            if self._db._RAM == True:
                data = self._maindata
                
                result = [element for element in data.values() if check_condition(condition,element)]
            else:         
                data = self.all()

                result = [element for element in data if check_condition(condition,element)]

            return result

            # Write the newly updated data back to the storage
            #self._storage.write(tables)

            # Clear the query cache, as the table contents have changed
            #self.clear_cache()
        
        """
        Returns all documents
        """
        def all(self):
            result = []

            if self._db._RAM == True:
                result = list(self._maindata.values())
            else:         
                for key,value in self._data_idx.items():
                    result.append(self.get_version(key,value))

            return result
        
        
                        
        """
        check collection is index
        """
        def _is_index(self):
            #indexes = self._db._get_unique_indexes()
                 
            return self._name in self._db["db_indexes"].keys()

        # Index/Subscription-functions

        """
        adding text index
        :name: index name
        :key: document field
        """
        def register_text_index(self,name,key,**kwargs):
            admin = {}

            
            path = Path(self._db.__dict__['_basepath']+os.sep+"admin.db")    
            path.parent.mkdir(parents=True, exist_ok=True) 
            
            lock = SoftFileLock(str(path.absolute())+".lock")
            
            try:
                    with  lock.acquire(timeout=self._db['_timeout']):
                        
                        if os.path.isfile(str(path.absolute())):
                            with open(str(path.absolute()),"r", encoding='utf-8') as f:
                                    admin = json.load(f)
                                    f.close()

                        if 'text_indexes' in admin:
                                indexes = admin['text_indexes']
                        else:
                                indexes = {}    
                            
                        indexes[name]={"collection":self._name,"key":key,"dynamic":kwargs.get("dynamic")}

                        admin['text_indexes']=indexes

                        self._db["db_indexes"][name] = {"collection":self._name,"key":key,"dynamic":kwargs.get("dynamic")}
                        self._db["text_indexes"][name] = {"collection":self._name,"key":key,"dynamic":kwargs.get("dynamic")}

                        with open(str(path.absolute()),"w", encoding='utf-8') as f:    
    
                                    json.dump(admin,f)
                                    f.close()    


                                
                                
            except Timeout:
                    raise ValueError(f'Lock collection timeout error: {self._path}')

        """
        adding hash index
        :name: index name
        :key: document field
        """

        def register_hash_index(self,name,key,**kwargs):
            #self._db[self._name+"_"+name] = {}

            self._db._register_unique_index(self._name,name,key,**kwargs)


        """
        reindex hash-index

        :name: index name

        """
        def reindex_hash(self,name):
            indexes = self._db["hash_indexes"]
            index_settings = indexes.get(name)
             
            if  index_settings==None:
                  raise ValueError(f'No index settings found') 
            
            key = index_settings['key']
            self._db[name]._recording=True
            if index_settings.get("dynamic") == True:
                self._db[name]._data = {}
                for value_document in self.all():
                    if key in value_document:
                        id = hashlib.sha1(value_document.get(key).encode()).hexdigest()
                        self._db[name]._data[id] =value_document["_id"]
                 
            else:    
                self._db[name]._data = {}
                
                for value_document in self.all():
                    if key in value_document:
                        id = hashlib.sha1(value_document.get(key).encode()).hexdigest()
                        self._db[name]._data[id] =value_document["_id"]
                    
                
                self._db[name]._write_index()
            self._db[name]._recording=False    

        """
        reindex function for text indexes

        :index_name: name of a index

        """       
        def reindex_text(self,index_name):
             
             #Надо поставить ограничение на маленькие размеры списков
             indexes = self._db["db_indexes"]
             index_settings = indexes.get(index_name)
             
             if  index_settings==None:
                  raise ValueError(f'No index settings found')  

             
             a = {}
             data = self.all()
             #start_time = time.time()
             for document in data:
                  id = document.get("_id")
                  if  id == None:
                    raise ValueError(f'no _id in document: '+str(document))   
                  if index_settings['key'] in document:
                       a[id] = document.get(index_settings['key'])
                  
             #print("preparing array: --- %s seconds ---" % (time.time() - start_time))               
             #start_time = time.time()
             slist =split_list2(a)
             #print("split: --- %s seconds ---" % (time.time() - start_time))               

             if index_settings.get("dynamic") == True:
                 collection = self._db[index_name]
                 collection._recording = True
                 collection._data= slist
                 collection._recording = False
                 

             else: 
                 collection = self._db[index_name]
                 collection._recording = True
                 collection._data= slist

                 l = []
                 string_txt = ""
                 if "0" in slist:
                    l.append(to_json_str(slist["0"]))
                    string_txt+='"0":'+ to_json_str(slist["0"])
                 if "1" in slist:
                    string_txt+="\n" 
                    string_txt+=('"1":'+ json.dumps(slist["1"],ensure_ascii=False))
                    l.append( to_json_str(slist["1"]))
                 
                 collection._write_collection(string_txt)  
                      

                 collection._recording = False       



        """
        Add value to hash-index
        :document: document to be stored to index
        :doc_id: ID of a document
        """

                                              

        def _add_values_to_unique_indexes(self,documents):
            indexes = self._db._get_unique_indexes()
            for index,value in indexes.items():
                if value['collection'] == self._name:
                    d = self._db[index]._data  
                    self._db[index]._recording=True
                    for document in documents:
                        if value['key'] in document:
                            if not(isinstance(document.get(value['key']),dict) or isinstance(document.get(value['key']),list)):
                                self._db[index]._data[hashlib.sha1(document.get(value['key']).encode()).hexdigest()]=document['_id']
                      
                    if not value.get("dynamic") == True:
                        self._db[index]._write_index()
                    self._db[index]._recording=False          

        def _delete_values_from_unique_indexes(self,documents):                        
            indexes = self._db._get_unique_indexes()
            
            for index,value in indexes.items():
                
                if value['collection'] == self._name:
                    d = self._db[index]._data  
                    self._db[index]._recording=True
                    for document in documents:
                        if value['key'] in document:
                            if '_id' in document:
                                key = next((k for k in self._db[index]._data if self._db[index]._data[k] == document['_id']), None)
                                if key!= None:
                                        self._db[index]._data.pop(key,None)
                            
                            
                                
                    if not value.get("dynamic") == True:
                            self._db[index]._write_index()
                    self._db[index]._recording=False  
                           
        def _add_values_to_text_indexes(self,documents):
            indexes = self._db['text_indexes']
            for index,value in indexes.items():
                 if value['collection'] == self._name:
                      for document in documents:
                        if value['key'] in document:
                            self._insert_value_text_index(index,document)                   
                                 

        def _delete_value_text_index(self,index_name,value):
            indexes = self._db['text_indexes']
            index_settings = indexes.get(index_name)
             
            if  index_settings==None:
                  raise ValueError(f'No index settings found')  

            index_key = index_settings['key']

            collection =  self._db[index_name]           
            collection._recording = True
            index =  collection._data 


            if index_key in value:
                if "_id" in value:
                    delete_in_branchces_dynamic_text_binary(index,value["_id"],value[index_key]) 

            if not index_settings.get("dynamic") == True:
                 #l = []
                 #l.append(index["0"])
                 #l.append(index["1"])
                 #self._db[index_name].insert_many(l,upsert=True,NoIndex=True)
                 
                 lock = collection._lock_collection() 
                 l = []
                 l.append('"0":' + to_json_str(index["0"]) )
                 if "1" in index:
                    l.append('"1":' + to_json_str(index["1"]) )
                   
                 collection._write_collection("\n".join(l))  
                 collection._release_collection(lock)       

            collection._recording = False     
            



        def _delete_values_from_text_indexes(self,documents):
            indexes = self._db['text_indexes']
            for index,value in indexes.items():
                 if value['collection'] == self._name:
                      for document in documents:
                        if value['key'] in document:
                            self._delete_value_text_index(index,document)                             

        
        """
        Search substring in a text index
        :s: search string
        """
        def search_text_index(self,s):
             
            indexes = self._db["text_indexes"]
            index_settings = indexes.get(self._name)
            
            if  index_settings==None:
                  raise ValueError(f'No index settings found')  

            key = index_settings['key']

            result = []
            data = self._db[index_settings['collection']]
             #if index_name in self._db[index_name]:
            
            index =  self._data
            #start_time = time.time()
            ids = get_index_ids_by_string(index,s)
            #print("extract indexes: --- %s seconds ---" % (time.time() - start_time))  

            #start_time = time.time()
            for i in ids:
                elem  = data.get(i)
                if key in elem:
                    if s in elem[key]:  
                            result.append(elem)
            #print("lookup in indexes: --- %s seconds ---" % (time.time() - start_time))            
            return result                           
                     

        def _delete_values_from_unique_indexes(self,documents):
            indexes = self._db._get_unique_indexes()
            for index,value in indexes.items():
                 if value['collection'] == self._name:
                      
                      self._db[index]._recording=True
                      for document in documents:
                        if value['key'] in document:
                            if isinstance(document.get(value['key']),str):
                                new_index = {"_id":hashlib.sha1(document.get(value['key']).encode()).hexdigest()}
                                self._db[index]._data.pop(new_index["_id"],None)
                                
                      if not value.get("dynamic") == True:       
                        self._db[index]._write_index()
                      self._db[index]._recording=False          
                                    
        
        """B-tree indexes"""


        """
        Writing the entire collection data to a file.
        Used in some situations
        :content: text string to be stored
        """
        def _write_collection(self,content):
            
            path = Path(self._path)    
            path.parent.mkdir(parents=True, exist_ok=True) 

            prefix = str(uuid.uuid4())

            with open(self._path, 'a', encoding='utf-8') as f:
                f.seek(0)
                f.truncate()
                f.write(prefix)
                f.write("\n")
                f.write(content)
                f.write("\n")

        """
        insert ONE value into the text indexes

        :index_name: name of a index
        :value: value too be inserting

        """  
        def _insert_value_text_index(self,index_name,value):
            indexes = self._db["text_indexes"]
            index_settings = indexes.get(index_name)
             
            if  index_settings==None:
                  raise ValueError(f'No index settings found')  

            index_key = index_settings['key']
            
            collection = self._db[index_name]
            
            index =  collection._data 
            collection._recording = True
            if index_key in value:
                insert_in_branchces_dynamic_text_binary(index,value["_id"],value[index_key])


            if not index_settings.get("dynamic") == True:
                 #self._db[index_name]._recording = True
                 #self._db[index_name]._data = index
                 #l = []
                 #l.append(index["0"])
                 #l.append(index["1"])
                 #self._db[index_name].insert_many(l,upsert=True,NoIndex=True)
                  
                 lock = collection._lock_collection() 
                 l = []
                 if "0" in index:
                    l.append('"0":' + to_json_str(index["0"]) )
                 if "1" in index:
                    l.append('"1":' + to_json_str(index["1"]) )
                   
                 collection._write_collection("\n".join(l))  
                 collection._release_collection(lock)     
                 collection._recording = False

        def _add_value_to_text_indexes(self,document):
            indexes = self._db['text_indexes']
            for index,value in indexes.items():
                 if value['collection'] == self._name:
                      if value['key'] in document:
                           self._insert_value_text_index(index,document)

        """
        initialize data at first call
        """
        def __getattr__(self, item):
            if item=='_data':
                if self._is_modification() or not hasattr(object, '_data') :
                    
                    if not self._is_index():
                        self._data = self._read_collection()
                        self._data_idx=self._read_collection_idx()
                        if self._db._RAM == True:
                           self._maindata = self._read_collection_maindata()     
                    else:       
                        self._data = self._read_index()
                
                
                    return object.__getattribute__(self, '_data')
                
                       
            
            if item=='_data_idx':
                 #self._data_idx=self._read_collection_idx()
                if self._is_modification():
                    #self._data = self._read_collection()
                    if not self._is_index():
                        self._data_idx=self._read_collection_idx()
                return self._data_idx
            
        def __getattribute__(self, item):
            if item=='_data': #return collection dictionary. Read data from file if necessary
                if self._is_index():
                     settings = self._db['db_indexes'].get(self._name)
                     if not settings.get("dynamic")==True:
                        
                        if self._is_modification():#read data only if was a modifiation
                            if self._name in self._db["text_indexes"]:
                                self._data = self._read_collection_txt()     
                            else:    
                                self._data = self._read_index()     
                        
                     
                else:     
                     if self._is_modification():
                        self._data = self._read_collection()
                        self._data_idx = self._read_collection_idx()
                        if self._db._RAM == True:
                           self._maindata = self._read_collection_maindata()     
                
                return object.__getattribute__(self, '_data')
            elif item=='_maindata':
                if self._is_modification():
                    self._data = self._read_collection()
                    self._data_idx = self._read_collection_idx()
                    if self._db._RAM == True:
                        self._maindata = self._read_collection_maindata() 
                return object.__getattribute__(self, '_maindata')
            else:
                return super().__getattribute__(item)   

        """
        generates document ID
        """
        def _get_next_id(self):
            
            next_id = str(uuid.uuid4())

            return next_id
        """
        check data modified by another process
        """
        def _is_modification(self):
            if  self._recording==True:
                 return False
            
            if  self._modification_uuid==None:
                 return True
            


            if not os.path.isfile(self._path):
                return False


            if self._db.singleton == True:
                 return False
            

            uid=None
            with open(self._path,"r", encoding='utf-8') as f:
                text =f.read(36)
                uid = text[:36]

            return uid!=self._modification_uuid

        def _read_collection(self):
            
            #print("read_started")
            
            collection = {}
                
            if os.path.isfile(self._path):
                    path = Path(self._path)    
                    path.parent.mkdir(parents=True, exist_ok=True) 

                    lock = SoftFileLock(self._path+".lock")
                    try:
                        with  lock.acquire(timeout=self._db['_timeout']):
                            #start_time = time.time()
                            collection = {} 
                            with open(self._path,"r", encoding='utf-8') as f:
                                #content = f.readlines()
                                #if len(content)>0:
                                #    uid = content[0].rstrip()
                                #    self._modification_uuid =uid
                                #    collection = json.loads(content[1])
                                #else:
                                #    collection = {}                         
                                content = f.readlines()
                                txt = ",".join(content[1:])
                                #txt = txt.replace("\n","")
                                collection = from_json_str("{"+txt+"}")    
                                uid = content[0].rstrip()
                                self._modification_uuid =uid    

                            #print("decode: --- %s seconds ---" % (time.time() - start_time))     

                    except Timeout:
                        raise ValueError(f'Lock collection timeout error: {self._path}')

            return collection
        
        
        def _read_collection_txt(self):

            #print("read_txt_started")
            
            collection = {}
                
            if os.path.isfile(self._path):
                    path = Path(self._path)    
                    path.parent.mkdir(parents=True, exist_ok=True) 

                    lock = SoftFileLock(self._path+".lock")
                    try:
                        with  lock.acquire(timeout=self._db['_timeout']):
                            with open(self._path,"r", encoding='utf-8') as f:
                                content = f.readlines()
                                txt = ",".join(content[1:])
                                collection = from_json_str("{"+txt+"}")    
                                uid = content[0].rstrip()
                                self._modification_uuid =uid

                    except Timeout:
                        raise ValueError(f'Lock collection timeout error') 

            return collection
        
        def _read_collection_idx(self):

            #print("read_idx_started")
            
            collection = {}
                
            if os.path.isfile(self._path_id):
                    path = Path(self._path_id)    
                    path.parent.mkdir(parents=True, exist_ok=True) 

                    lock = SoftFileLock(self._path_id+".lock")
                    try:
                        with  lock.acquire(timeout=self._db['_timeout']):
                            with open(self._path_id, 'rb') as f:
                                #collection = json.load(f)
                                collection = pickle.load(f)

                    except Timeout:
                        raise ValueError(f'Lock collection timeout error') 

            return collection
        
        def _read_collection_maindata(self):
            
            collection = {}
            if os.path.isfile(self._path_data):
                path = Path(self._path_data)    
                path.parent.mkdir(parents=True, exist_ok=True) 

                lock = SoftFileLock(self._path_data+".lock")
                try:
                    with  lock.acquire(timeout=self._db['_timeout']):
                        for key,value in self._data_idx.items():
                                collection[key] = self.get_version(key,value)
                except Timeout:
                        raise ValueError(f'Lock collection timeout error: {self._path_data}')
            return collection
        
        def _write_index(self,**kwargs):
            path = Path(self._path)    
            path.parent.mkdir(parents=True, exist_ok=True) 
 
            prefix = str(uuid.uuid4())
            
            lock = SoftFileLock(self._path+".lock")
            try:
                    with  lock.acquire(timeout=self._db['_timeout']):
                        with open(self._path,"w", encoding='utf-8') as f: 
                            f.write(prefix)
                            f.write("\n")
                            self._modification_uuid=prefix
                            str_data = json.dumps(self._data)
                            #str_data[1:-1]
                            f.write(str_data) 

            except Timeout:
                raise ValueError(f'Lock collection timeout error: {self._path}')

        def _read_index(self):
            path = Path(self._path)    
            
            path.parent.mkdir(parents=True, exist_ok=True) 
            collection ={}
            
            if os.path.isfile(str(path.absolute())):
                lock = SoftFileLock(str(path.absolute())+".lock")
                try:
                        with  lock.acquire(timeout=self._db['_timeout']):
                            
                            if os.path.isfile(str(path.absolute())):
                                with open(str(path.absolute()),"r", encoding='utf-8') as f:
                                    content = f.readlines()
                                    txt = content[1:]
                                    collection = json.loads(txt[0])   
                                    uid = content[0].rstrip()
                                    self._modification_uuid =uid
                                    
                                return collection    
                except Timeout:
                    raise ValueError(f'Lock collection timeout error: {self._path}') 
            else:
                return collection
            
        """
        Handlers
        """
        def register_before_change_handler(self,handler):
            self._before_change_handler = handler

        """
        Collection initialization
        :name: collection name
        :db_instance: database instance

        """
        def __init__(self,name: str,db_instance):
            
            self._db = db_instance
            self._name = name    
            self._maindata = {}
            self._maindata_temp ={}

            if self._is_index():        
                self._path =db_instance['_basepath'] +os.sep+name+".idx"
            else:    
                 self._path =db_instance['_basepath'] +os.sep+name+".ptr"
            self._path_id =db_instance['_basepath'] +os.sep+name+".id"
            self._path_data =db_instance['_basepath'] +os.sep+name+".dat"
            
            
                   
            
            
            self._next_id = None        
        
        def shrink(self):
            
            def updater():
                new_data = {}

                dbytes = b''
                _begin = 0  
                datastr= ""

                for key,value in self._data.items():
                    
                    if value[3] in self._data_idx:
                        _version =  value[2]
                        in_file = open(self._path_data, "rb") 
                        in_file.seek(value[0])
                        bytes = in_file.read(value[1]) 

                        dbytes+= bytes
                        
                        new_data[key] =[_begin,_begin+len(bytes),_version,value[3]]
                        if datastr=="":
                            datastr = format_datastr(key,new_data[key])
                        else:     
                            datastr +="\n"+ format_datastr(key,new_data[key])

                        _begin+=len(bytes)
                
                return [],dbytes,[],len(dbytes),datastr        
                     
            doc_id = self._update_collection(updater, shrink = True)

            if doc_id == -1:
                        raise ValueError('Write failed')
            
        
        def fast_insert(self,document, **kwargs):
            
            collection = self._data

            self._recording = True

            def updater():
                    
                    
                    if "_id" in document:
                        doc_id = document['_id']
                    else:
                        doc_id = self._get_next_id()     
                        document["_id"]  = doc_id

                    last_version = self._data_idx.get(doc_id)
                    if last_version!=None:    
                        if not (kwargs.get("upsert")==True or kwargs.get("update")==True):
                            raise ValueError(f'Value with ID {str(doc_id)} already exists')
                        else:
                            _version = last_version+1
                            #dbdocument = self.get(doc_id) 
                            

                    else:
                        _version=0     
                    
                    document["_version"]  = _version
                    
                    datastr = ""
                    
                    begin = 0

                    if 'session' in kwargs: #updating with transaction
                        session = kwargs['session']
                        self._maindata_temp[doc_id] = document
                        if self._name in session._operations_add:
                             for line in session._operations_add[self._name]:
                                  begin = line[3]
                                  #_doc_id = line[0]
                                  #last_version = self._data_idx[_doc_id]
                                  #begin = self._data[_doc_id+"_"+str(last_version)][1]


                    if os.path.exists(self._path_data) and begin==0:
                        in_file = open(self._path_data, "rb") 
                        data = in_file.read() 
                        begin = len(data)
                        in_file.close()
                    
                    dbytes = pickle.dumps(document)

                    self._data[doc_id+"_"+str(_version)] =[begin,begin+len(dbytes),_version,doc_id]
                    datastr=format_datastr(doc_id+"_"+str(_version),self._data[doc_id+"_"+str(_version)])
                    self._data_idx[doc_id] = _version
                    
                    if self._db._RAM == True:
                        self._maindata[doc_id] = document   
                    
                    return doc_id,dbytes,_version,begin+len(dbytes),datastr
                    

            if 'session' in kwargs: #updating with transaction
                    session = kwargs['session']
                    if not kwargs.get("NoIndex") == True and (kwargs.get("upsert")==True or kwargs.get("update")==True):  
                        cdocument = [copy.deepcopy(document)]
                        session._related_delete.append((self._name,cdocument))

                    doc_id,bytes,version,end,datastr = self._update_collection_memory(updater,session)

                    if not self._name in session._operations_add:
                             session._operations_add[self._name] = []
                        
                    session._operations_add[self._name].append([doc_id,bytes,version,end,datastr])
                    
                    if not kwargs.get("NoIndex") == True:  
                        session._related_add.append((self._name,[document]))
                        
            else:   #update directly 
                    
                    if not kwargs.get("NoIndex") == True and (kwargs.get("upsert")==True or kwargs.get("update")==True): 
                        if self._db.queue!=None:
                            self._db.queue.put([[document],self._name,self._db._name,"delete"]) 
                        else:   
                            self._delete_values_from_unique_indexes([document]) 
                            self._delete_values_from_text_indexes([document]) 
                    
                    doc_id = self._update_collection(updater)

                    self._recording = False

                    if doc_id == -1:
                        raise ValueError('Write failed')
                    else:
                        #loop = asyncio.get_event_loop()
                        #hr = threading.Thread(target=index_worker, args=(2,))
                        #thr.start()
                        
                        if not kwargs.get("NoIndex") == True:
                             if self._db.queue!=None:
                                self._db.queue.put([[document],self._name,self._db._name,"add"]) 
                             else:       
                                self._add_values_to_unique_indexes([document])
                                self._add_values_to_text_indexes([document])
                                #self._db._add_value_to_subscriptions(self._name,doc_id)

            return doc_id     

        """
        Remove documents from idx-file 

        :document: document or list of documents to be added
        
        Arguments:
        :NoIndex: OFF to change indexes
        :session: transaction instance


        """
        def fast_delete(self,document, **kwargs):
            
            collection = self._data

            self._recording = True

            def updater():
                if "_id" in document:
                    doc_id = document['_id']

                if self.get(doc_id)==None:
                    raise ValueError(f'Value with ID {str(doc_id)} not in collection')
                
                
                self._data_idx.pop(doc_id,None)
                
                return doc_id,None,None,None,None
                    

            if 'session' in kwargs: #updating with transaction
                    session = kwargs['session']
                    
                    doc_id,dbytes,_version,end,datastr = self._update_collection_memory(updater,session)
                    if not self._name in session._operations_replace:
                        session._operations_replace[self._name] = []

                    session._operations_replace[self._name].append([doc_id,dbytes,_version,end,datastr])

                    if not kwargs.get("NoIndex") == True:  
                        session._related_delete.append((self._name,document,doc_id))
                    
                    return doc_id
                    
            else:    #update directly 
                    doc_id = self._update_collection_without_bytes(updater)

                    self._recording = False

                    if doc_id == -1:
                        raise ValueError('Write failed')
                    else:
                        
                        if not kwargs.get("NoIndex") == True: 
                            if self._db.queue!=None:
                                self._db.queue.put([[document],self._name,self._db._name,"delete"]) 
                            else:    
                                self._delete_values_from_unique_indexes([document])
                                self._delete_values_from_text_indexes([document])
                        #    self._add_value_to_text_indexes(document)
                        
                        return doc_id                   
        def delete_many(self, dataset, **kwargs) -> str:
                
            ids = []

            collection = self._data.copy()

            documents = copy.deepcopy(dataset)

            self._recording = True

            def updater():

                no_search_list_file=[]
 
                ids =  []

               
                for document in documents:
                    if "_id" in document:
                        doc_id = document['_id']

                    
                    if self.get(doc_id)==None:
                        raise ValueError(f'Value with ID {str(doc_id)} not in collection')
                    
                    ids.append(doc_id)

                    self._data_idx.pop(doc_id,None)

                    
                return ids,None,None,None,None
                
                #self._data = collection
 
            
           
            if 'session' in kwargs:
                session = kwargs['session']
                
                #res,search,no_search_list_file = self._update_collection_memory(updater,session)
                doc_id,dbytes,_version,end,datastr = self._update_collection_memory(updater,session)

                if not self._name in session._operations_replace:
                    session._operations_replace[self._name] =[]  

                
                session._operations_replace[self._name].append([doc_id,dbytes,_version,end,datastr])

                if not kwargs.get("NoIndex") ==True:
                     pass
                #        for document in documents:
                #            session._related_replace.append((self._name,document,document.get('_id')))
                return doc_id
            else:    

                res = self._update_collection_without_bytes(updater)
                if res == -1:
                    raise ValueError('Write failed')
 
                self._recording = False
                
                if not kwargs.get("NoIndex") ==True:
                    
                    if self._db.queue!=None:
                        self._db.queue.put([documents,self._name,self._db._name,"delete"]) 
                    else:     
                        self._delete_values_from_unique_indexes(documents)
                        self._delete_values_from_text_indexes(documents)

                return res   
                
            return None    

        def insert_many(self,documents, **kwargs):
            #start_time = time.time()
            collection = self._data
            #print("insert_many - read collection: --- %s seconds ---" % (time.time() - start_time))

            self._recording = True

            begin = 0

            if 'session' in kwargs: #updating with transaction
                session = kwargs['session']
                if self._name in session._operations_add:
                        for line in session._operations_add[self._name]:
                            begin = line[3] 
                            #_doc_id = line[0]
                            #last_version = self._data_idx[_doc_id]
                            #begin = self._data[_doc_id+"_"+str(last_version)][1]

            #start_time = time.time()
            
            if os.path.exists(self._path_data) and begin==0:
                in_file = open(self._path_data, "rb") 
                data = in_file.read() 
                begin = len(data)
                in_file.close()
            
            #print("getting begin: --- %s seconds ---" % (time.time() - start_time))
             

            def updater():

                ids = []
                versions = []

                dbytes = b''

                _begin = begin  

                datastr= ""

                #t1 = 0
                #t2 = 0
                #t3 = 0
                #t4 = 0
                #t5 = 0
                    
                liststr=[]    
                for document in documents:    
                    #start_time = time.time()
                    if "_id" in document:
                        doc_id = document['_id']
                    else:
                        doc_id = self._get_next_id()     
                        document["_id"]  = doc_id

                    last_version = self._data_idx.get(doc_id)
                    if last_version!=None:    
                        if not (kwargs.get("upsert")==True or kwargs.get("update")==True):
                            raise ValueError(f'Value with ID {str(doc_id)} already exists')
                        else:
                            _version = last_version+1
                            #dbdocument = self.get(doc_id) 
                            

                    else:
                        _version=0     
                    
                    document["_version"]  = _version 

                    #t1+=(time.time() - start_time)
                    

                    #start_time = time.time()
                    
                    bytes = pickle.dumps(document,pickle.HIGHEST_PROTOCOL)

                    #t2+=(time.time() - start_time)

                    #start_time = time.time()
                    
                    dbytes+= bytes

                    #t3+=(time.time() - start_time)
                    #start_time = time.time()
                    full_id = doc_id+"_"+str(_version)
                    value = [_begin,_begin+len(bytes),_version,doc_id]
                    
                    self._data[full_id]  = value
                    #if datastr=="":
                    #    datastr = format_datastr(doc_id+"_"+str(_version),value)
                    #else:     
                    #    datastr +="\n"+ format_datastr(doc_id+"_"+str(_version),value)
                    liststr.append(format_datastr(doc_id+"_"+str(_version),value))    

                    #t4+=(time.time() - start_time)
                    #start_time = time.time()
                    _begin+=len(bytes)
                    
                    self._data_idx[doc_id] = _version 

                    if self._db._RAM == True:
                        self._maindata[doc_id] = document   

                    ids.append(doc_id)
                    versions.append(_version)
                    #t5+=(time.time() - start_time)

                    if 'session' in kwargs:
                         self._maindata_temp[doc_id] = document

                datastr = "\n".join(liststr)
                #print("updater 1: --- %s seconds ---" % (t1))
                #print("updater 2: --- %s seconds ---" % (t2))
                #print("updater 3: --- %s seconds ---" % (t3))
                #print("updater 3: --- %s seconds ---" % (t4))
                #print("updater 3: --- %s seconds ---" % (t5))
                    
                return ids,dbytes,versions,_begin,datastr
                    

            if 'session' in kwargs: #updating with transaction
                    if not kwargs.get("NoIndex") == True and (kwargs.get("upsert")==True or kwargs.get("update")==True):  
                        
                        cdocuments = copy.deepcopy(documents)
                        session._related_delete.append((self._name,cdocuments))

                    session = kwargs['session']
                    doc_id,dbytes,_version,end,datastr = self._update_collection_memory(updater,session)
                    
                    if not self._name in session._operations_add:
                        session._operations_add[self._name] = []
                    session._operations_add[self._name].append([doc_id,dbytes,_version,end,datastr] )

                    
                    if not kwargs.get("NoIndex") == True:  
                        session._related_add.append((self._name,documents))
            else:   #update directly 
                    if not kwargs.get("NoIndex") == True and (kwargs.get("upsert")==True or kwargs.get("update")==True): 
                        
                        cdocuments = copy.deepcopy(documents)  

                        if self._db.queue!=None:
                            self._db.queue.put([cdocuments,self._name,self._db._name,"delete"]) 
                        else:  
                            self._delete_values_from_unique_indexes(cdocuments) 
                            self._delete_values_from_text_indexes(documents)

                    doc_id = self._update_collection(updater)

                    self._recording = False

                    if doc_id == -1:
                        raise ValueError('Write failed')
                    else:
                        
                        if not kwargs.get("NoIndex") == True:   
                            if self._db.queue!=None:
                                self._db.queue.put([documents,self._name,self._db._name,"add"]) 
                            else: 
                                self._add_values_to_unique_indexes(documents)
                                self._add_values_to_text_indexes(documents)

                        #    self._add_value_to_text_indexes(documents)

            return doc_id     

        """
        Run updater only
        
        :updater: updater function
        
        """
        def _update_collection_memory(self,updater: Callable[[Dict[int, Mapping]], None],session):
            gc.disable()
            
            collection = self._data

            doc_id,dbytes,_version,end,datastr = updater() 
            
            gc.enable() 
            return doc_id,dbytes,_version,end,datastr
        
        def _update_collection(self,updater: Callable[[Dict[int, Mapping]], None],**kwargs):
            
            gc.disable()

            collection = self._data
            
            path = Path(self._path)    
            path.parent.mkdir(parents=True, exist_ok=True) 

            prefix = str(uuid.uuid4())
            
            #starting updater code
            doc_id,dbytes,_version,end,datastr = updater() 
            

            gc.enable()    

            if kwargs.get("shrink") == True:
                os.remove(self._path)
                os.remove(self._path_data)
                self._modification_uuid=None
              
            lock = SoftFileLock(self._path+".lock")

            
            try:
                with  lock.acquire(timeout=self._db['_timeout']):
                        
                    try:    
                        
                        #Writing a pointer file (*.ptr) with a modification prefix
                        prefix_updated=False
                        with open(self._path,"a", encoding='utf-8') as f: 
                                    
                            if self._modification_uuid==None:
                                f.write(prefix)
                                f.write("\n")
                                self._modification_uuid=prefix
                                prefix_updated=True
                                

                            f.write(datastr)
                            f.write("\n") 
                        

                        #update prefix if needed
                        if not prefix_updated:
                            with open(self._path,"r+", encoding='utf-8') as f:
                                text =f.read(36)
                                f.seek(0)
                                f.write(prefix)
                                self._modification_uuid=prefix                                    

                        
                        #write last version dictionary                                
                        with open(self._path_id, 'wb') as f:
                            pickle.dump(self._data_idx, f, pickle.HIGHEST_PROTOCOL)            
                        
                        
                        #write binary data-file
                        out_file = open(self._path_data, "ab")
                        out_file.write(dbytes)
                        out_file.close()   
                        

                    except Exception:
                        raise ValueError(f'Write collections error') 
                        

                        
            except Timeout:
                    raise ValueError(f'Lock collection timeout error: {self._path}')     

            
            return  doc_id               
            
            
        def _update_collection_without_bytes(self,updater: Callable[[Dict[int, Mapping]], None]):
            
            collection = self._data
            
            path = Path(self._path)    
            path.parent.mkdir(parents=True, exist_ok=True) 

            prefix = str(uuid.uuid4())

            doc_id,dbytes,_version,end,datastr = updater() 
        
            lock = SoftFileLock(self._path_id+".lock")
            try:
                with  lock.acquire(timeout=self._db['_timeout']):
                    try:    
                        #with open(self._path,"w", encoding='utf-8') as f: 
                            
                        #    f.write(prefix)
                        #    f.write("\n")
                        #    self._modification_uuid=prefix
                        #    f.write(json.dumps(self._data)) 

                        with open(self._path_id, 'wb') as f:
                            pickle.dump(self._data_idx, f, pickle.HIGHEST_PROTOCOL)     
                        
                        return  doc_id               
                    except:
                        raise ValueError(f'Write collection {self._data_idx} error') 
            except Timeout:
                    raise ValueError(f'Lock collection timeout error')     
            
        """
        Lock collection file
        
        """    
        def _lock_collection(self):
            
            path = Path(self._path)    
            path.parent.mkdir(parents=True, exist_ok=True) 
        
            lock = SoftFileLock(self._path+".lock")
            lock.acquire()

            return lock

        """
        Unlock collection file
        
        """  
        def _release_collection(self,lock):
            
            lock.release()       
    
    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def __getitem__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]
        
        else:
            self.__dict__[key]=self.Collection(key,self)
            return self.__dict__[key]
        
    def _get_unique_indexes(self):
        admin = {}
        indexes = {} 

        path = Path(self.__dict__['_basepath']+os.sep+"admin.db")    
        path.parent.mkdir(parents=True, exist_ok=True) 
        
        lock = SoftFileLock(str(path.absolute())+".lock")
        
        try:
                with  lock.acquire(timeout=self.__dict__['_timeout']):
                      
                    if os.path.isfile(str(path.absolute())):
                        with open(str(path.absolute()),"r", encoding='utf-8') as f:
                                admin = json.load(f)
                                f.close()

                    if 'hash_indexes' in admin:
                            indexes = admin['hash_indexes']
                                 
        except Timeout:
                    raise ValueError(f'Lock admin.db timeout error')
        
        return indexes
    
    def _get_text_indexes(self):
        admin = {}
        indexes = {} 

        path = Path(self.__dict__['_basepath']+os.sep+"admin.db")    
        path.parent.mkdir(parents=True, exist_ok=True) 
        
        lock = SoftFileLock(str(path.absolute())+".lock")
        
        try:
                with  lock.acquire(timeout=self.__dict__['_timeout']):
                      
                    if os.path.isfile(str(path.absolute())):
                        with open(str(path.absolute()),"r", encoding='utf-8') as f:
                                admin = json.load(f)
                                f.close()

                    if 'text_indexes' in admin:
                            indexes = admin['text_indexes']
                                 
        except Timeout:
                    raise ValueError(f'Lock admin.db timeout error')
        
        return indexes
    
                       
    

    def _register_unique_index(self,collection_name,name,key,**kwargs):
        admin = {}

            
        path = Path(self.__dict__['_basepath']+os.sep+"admin.db")    
        path.parent.mkdir(parents=True, exist_ok=True) 
        
        lock = SoftFileLock(str(path.absolute())+".lock")
        
        try:
                with  lock.acquire(timeout=self.__dict__['_timeout']):
                      
                    if os.path.isfile(str(path.absolute())):
                        with open(str(path.absolute()),"r", encoding='utf-8') as f:
                                admin = json.load(f)
                                f.close()

                    if 'hash_indexes' in admin:
                            indexes = admin['hash_indexes']
                    else:
                            indexes = {}    
                        
                    indexes[name]={"collection":collection_name,"key":key,"dynamic":kwargs.get("dynamic",False)}

                    admin['hash_indexes']=indexes

                    self["db_indexes"][name]={"collection":collection_name,"key":key,"dynamic":kwargs.get("dynamic")}
                    self["hash_indexes"][name]={"collection":collection_name,"key":key, "dynamic":kwargs.get("dynamic")}

                    with open(str(path.absolute()),"w", encoding='utf-8') as f:    
 
                                json.dump(admin,f)
                                f.close()    
                                
                                
        except Timeout:
                    raise ValueError(f'Lock admin.db timeout error')

    """
        Pre-reads all the collections
    """
    def initialize(self):
            for r, d, f in os.walk(self.__dict__['_basepath']):
                for file in f:
                    if file.endswith(".ptr"):
                        collection_name = file.replace(".ptr","")
                        self[collection_name].get("")
                    elif file.endswith(".idx"):
                        collection_name = file.replace(".idx","")
                        d =self[collection_name]._data 
            
            #сюда же индексы?        
                        
    def __init__(
        self,
        name: str,
        **kwargs
      
    ):
        

        if 'path' in kwargs:
            basepath = kwargs.get("path")+os.sep+name
        else:
            try:
                abs_path = os.path.abspath((inspect.stack()[0])[1])
                basepath =os.path.dirname(abs_path)+os.sep+name
            except:
                basepath =os.path.dirname(os.path.realpath(__file__))+os.sep+name
        
        if not os.path.exists(basepath):
            os.makedirs(basepath)
        
        for file in os.listdir(basepath):
                if file.endswith(".lock"):
                    duration = time.time()-os.path.getmtime(basepath +os.sep+file)
                    if duration>LOCK_TIMEOUT*2:
                         os.remove(basepath +os.sep+file)
        

        #self._collections: Dict[str, Collection] = {}
        self.__dict__: Dict[str, self.Collection] = {}
        self.__dict__['_basepath']=basepath
        
        if 'timeout' in kwargs:
            self.__dict__['_timeout']=kwargs.get("timeout")
        else:    
            self.__dict__['_timeout']=LOCK_TIMEOUT

        self.__dict__['_RAM']= True
        if 'RAM' in kwargs:
            self.__dict__['_RAM'] = kwargs.get("RAM") 

        self.__dict__['queue']= None       
        if 'queue' in kwargs:
            self.__dict__['queue'] = kwargs.get("queue")

        self.__dict__['singleton']= False       
        if 'singleton' in kwargs:
            self.__dict__['singleton'] = kwargs.get("singleton")    

        self.__dict__['_name'] = name         

        hash_indexes = self._get_unique_indexes()
        text_indexes = self._get_text_indexes()
        common = dict(hash_indexes)
        common.update(text_indexes)
        self.__dict__['db_indexes']  = common
        self.__dict__['text_indexes']  = text_indexes
        self.__dict__['hash_indexes']  = hash_indexes

    def collection(self, name: str, **kwargs) -> Collection:
        

        if name in self.__dict__:
            return self.__dict__[name]

        collection = self.Collection(name, self)
        self.__dict__[name] = collection

        return collection

"""
a class that organizes a transaction within itself
"""
class DBSession:
    def __init__(self, db: Pelican) -> None:
        self._operations = {}
        
        self._operations_add = {}
        self._operations_replace = {}

        self._related_add = []
        self._related_delete = []
        self._db = db
    """
    commit transaction: store collections to files
    """
    def commit(self):

        list_locks = {}
        for collection_name, value in self._operations_add.items():
            lock = self._db[collection_name]._lock_collection()
            list_locks[collection_name]=lock
        
        for collection_name, value in self._operations_replace.items():
             if not collection_name in list_locks:
                lock = self._db[collection_name]._lock_collection()
                list_locks[collection_name]=lock 
        #         if isinstance(value,list):
        #             if len(value) >0:
        #                 lock_required=True
        #         else:
        #            lock_required=True
        #         if lock_required:
        #            lock = self._db[collection_name]._lock_collection()
        #            list_locks[collection_name]=lock    

        prefix = str(uuid.uuid4())
        no_update_uuid=False

        for collection_name, value in self._operations_add.items():
            collection = self._db[collection_name]

            prefix_updated = False
            for line in value:
                dbytes = line[1] 
                datastr = line[4]

                
                with open(collection._path,"a", encoding='utf-8') as f: 
                            
                    if collection._modification_uuid==None:
                        f.write(prefix)
                        f.write("\n")
                        collection._modification_uuid=prefix
                        prefix_updated=True
                        

                    f.write(datastr)
                    f.write("\n") 
            
                
                
                out_file = open(collection._path_data, "ab") 
                out_file.write(dbytes)
                out_file.close()       
            
            if not prefix_updated:
                with open(collection._path,"r+", encoding='utf-8') as f:
                    text =f.read(36)
                    f.seek(0)
                    f.write(prefix)
                    collection._modification_uuid=prefix                                        

            with open(collection._path_id, 'wb') as f:
                pickle.dump(collection._data_idx, f, pickle.HIGHEST_PROTOCOL)            
                          

        for collection_name, lock in list_locks.items():
            #lock = list_locks[collection_name]
            
            self._db[collection_name]._recording=False

            self._db[collection_name]._release_collection(lock)
            
            self._db[collection_name]._maindata_temp = {}

        #for collection_name, value in self._operations_replace.items():
        #     lock = list_locks[collection_name]
            
        #     self._db[collection_name]._recording=False

        #     self._db[collection_name]._release_collection(lock)      

           
            #updating indexes for insert,update operations    
            set = []
            for c_name,documents in self._related_delete:
                if c_name == collection_name:
                    for document in documents:
                        set.append(document)
            
            if len(set)>0:
            
                if self._db.queue!=None:

                    self._db.queue.put([set,self._name,self._db._name,"delete"]) 

                else:    
                    self._db[collection_name]._delete_values_from_unique_indexes(set)
                    self._db[collection_name]._delete_values_from_text_indexes(set)

            set = []
            for c_name,documents in self._related_add:
                if c_name == collection_name:
                    for document in documents:
                        set.append(document)
            
            if len(set)>0: 

                if self._db.queue!=None:

                    self._db.queue.put([set,self._name,self._db._name,"add"]) 

                else:                       
                    self._db[collection_name]._add_values_to_unique_indexes(set)
                    self._db[collection_name]._add_values_to_text_indexes(set)
            

           
                #self._db[collection_name]._delete_values_from_text_indexes(set)
 

        self._operations = {}
        
        self._operations_add = {}
        self._operations_replace = {}

        self._related_add=[]
        self._related_delete=[]

    def __enter__(self) -> "DBSession":
        
        return self        
    """
    left the "with" operator
    """    
    def __exit__(self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType]) -> bool:

        self._operations = {}
        for key in self._operations_add.keys():
             if not key in self._operations:
                 self._operations[key] =""

        if value == None and not traceback:
            self.commit()
            return True
        else:
            for collection_name in self._operations.keys():
                self._db[collection_name]._modification_uuid=None
                c = self._db[collection_name]
            
        self._operations = {}
        
        self._operations_add = {}
        self._operations_replace = {}

        self._related_add=[]
        self._related_delete=[]
        
        return False
    
def feed(dbs,message):
    results = {}
    jmessage  = message 
    if isinstance(message, str):
        try:
            jmessage = json.loads(message)
        except:
            raise ValueError(f'Message  string is not a JSON') 
    if not isinstance(jmessage,list):
        raise ValueError(f'Root element in message should be a list')          
    for db_set in jmessage:
        for db_name,db_value in db_set.items():
            
            if not db_name in dbs:
                raise ValueError(f'Database {db_name} is not initialized')          

            db = dbs[db_name]
            if isinstance(db_value, list):
                session_results = {}
                with DBSession(db) as s:
                    for transaction_item in db_value:
                        
                            for collection_name,collection_set in transaction_item.items():
                                
                                uid,command,parameter =  read_command(collection_set)
                                
                                result = perform_command(command,db,collection_name,parameter,s)
                                
                                if uid!=None:
                                    session_results[uid]=result
                results =  session_results.copy()               

            else:
                for collection_name,collection_set in db_value.items():
                         
                    uid,command,parameter =  read_command(collection_set)   
                    result = perform_command(command,db,collection_name,parameter)
                    
                    if uid!=None:
                        results[uid]=result

                    
    return results                    

def read_command(collection_set):
    uid = None
    command = None
    parameter = None
    for key, value in collection_set.items():
        if key == "uid":
            uid = value
        else:
            command = key
            parameter = value 
    
    return uid,command,parameter  

def perform_command(command,db,collection_name,parameter,session=None):
    kw = {}
    
    if session!=None:
         kw["session"] = session
    
    if command=="insert" or command=="+":         
        result = db[collection_name].insert(parameter,**kw) 
    elif command=="upsert" or command=="++": 
        kw["upsert"]=True        
        result = db[collection_name].insert(parameter,**kw)
    elif command=="update" or command=="->": 
        if isinstance(parameter, list):
            result = db[collection_name].update(parameter[0],parameter[1],**kw) 
        else:
            raise ValueError(f'Parameter {parameter} is not valid')               
    elif command=="delete" or command=="-": 
        result = db[collection_name].delete(parameter,**kw)           
    elif command=="find" or command=="??":         
        result = db[collection_name].find(parameter,**kw) 
    elif command=="get" or command=="?":         
        result = db[collection_name].find(parameter,**kw)     
    elif command=="clear" or command=="--":         
        result = db[collection_name].clear()         

    return result           

          
     