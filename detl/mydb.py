import datetime
import os
import pymongo
import hashlib
from pymongo import MongoClient
from bson.objectid import ObjectId
from detl.db_context import db_context
import json
import logging
from detl.identity import Identity
def db_client(config_path='configs/db.json'):

    with open(config_path) as fd:
        config = json.load(fd)

    host = config['host']
    port = config['port']
    db = config['db']
    collection = config['collection']
    data_folder = config['data_folder']

    return MyDb(host, port, db, collection, data_folder)


class MyDb(object):

    def __init__(self, host, port, db, collection, data_folder):

        self.client = MongoClient(host, port)
        self.db = getattr(self.client, db)
        self.coll = getattr(self.db, collection)
    
        self.data_folder = data_folder

        # TODO : in the future, split the find into two cases : whether the file is available or not (in that case, can download)

    def find(self, identity):

        hash_value = identity.__id_hash__()
        result = self.coll.find_one({'config_hash': hash_value})
        return result
    
    def find_from_hash(self, hash_val):

        return self.coll.find_one({'config_hash': hash_val})


    def find_file(self, identity, unpack_input=False, unpack_len=0):
        if unpack_input:
            all_identities = [Identity('unpack', [identity, i], {}) for i in range(unpack_len)]
            return [self._find_file(all_identities[i]) for i in range(unpack_len)]
        return self._find_file(identity)

    def _find_file(self, identity):

        res = self.find(identity)
        if res is not None:
            if 'file_descriptor' in res:
                return res['file_descriptor']

    
    def insert(self, results, save_func, save_data=True, unpack_input=False):
        if unpack_input:
            for res in results:
                self._insert(res.identity, res, save_func, save_data=save_data)
        else:
            self._insert(results.identity, results, save_func, save_data=save_data)


    def _insert(self, identity, results, save_func, save_data=True):
        
        # TODO : move to computation identity class
        hash_value = identity.__id_hash__()
        identity_dict = identity.to_dict(db=self)
        
        # If save_data
        if save_data:
            # Create a file path
            file_path = self.create_fd(identity)
            # TODO : handle errors
            # Save to file path
            save_func(results, file_path)
            # Add to dict
            identity_dict['file_descriptor'] = file_path

        # Save to collection
        post = self.coll.insert_one(identity_dict)
        return post


    def create_fd(self, identity):
        '''
        Save to a data folder whose name corresponds to the name of the identity
        '''
        # TODO : no global variable
        # TODO : assuming that the data folder is already created
        # TODO : move hash_value to identity init
        hash_value = identity.__id_hash__()
        folder = os.path.join(self.data_folder, identity.name)
        if not os.path.exists(folder):
            os.mkdir(folder)
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fname = timestamp + str(hash_value)
        file_path = os.path.join(folder, fname)
        return file_path
        
    def as_default(self):

        return db_context.get_controller(self)

    def browse(self, fn_name, origin={}):
        '''
        browse('processor_2')
        browse('processor_2', {'source':{}, 'Preprocessor':{'kwargs':{'num_mul': 50}}})

        '''
        # TODO : write the origins as a dictionary with $and in mongo syntax
        results = self.coll.find({'name': fn_name})
        for res in results:
            if res in origin:
                yield res
            else:
                recursive_find(res, origin)
    
    def drop_all(self):
        self.coll.drop()

    def list_results(self, fn_name):
        return self.coll.find({'name': fn_name})

    def has_ancestor(self, obj, query):
        query_results = list(self.coll.find(query))
        print(list(query_results)) 
        print(obj)
        print([res for res in query_results])
        if str(obj['_id']) in [str(res['_id']) for res in query_results]:
            return True

        for arg in obj['args']:
            if type(arg) is ObjectId:
                ancestor_path = self.has_ancestor(self.find_id(arg), query)
                if ancestor_path:
                    return True

        for kw, kwar in obj['kwargs'].items():
            print(kwar)
            if type(kwar) is ObjectId:
                ancestor_path = self.has_ancestor(self.find_id(kwar), query)
                if ancestor_path:
                    return True
    
        return False


    def recursive_get(self, res_metadata):
        
        full_metadata = {'_id': res_metadata['_id'], 'config_hash': res_metadata['config_hash'],
                'args': [], 'kwargs': {}, 'name': res_metadata['name']}

        for arg in res_metadata['args']:
            if type(arg) is ObjectId:
                arg_metadata = self.find_id(arg)
                full_metadata['args'].append(self.recursive_get(arg_metadata))

        for kw, kwar in res_metadata['kwargs'].items():
            if type(kwar) is ObjectId:
                kw_metadata = self.find_id(kwar)
                full_metadata['kwargs'][kw] = self.recursive_get(kw_metadata)
            else:
                full_metadata['kwargs'][kw] = kwar
    
        return full_metadata

    def find_id(self, obj_id):
        return self.coll.find_one({'_id': obj_id})
'''
db.test_pipeline.aggregate(

{"$graphLookup": {"f rom": "test_pipeline",
"startWith": "$args",
"connectFromField": "args",
"connectToField" : "_id",
"as": "ancestors"}},
{"$match": {"name": "processor_2"}},
{ "$addFields": { 
            "ancestors": { 
                "$reverseArray": { 
                    "$map": { 
                        "input": "$ancestors", 
                        "as": "t", 
                        "in": { "name": "$$t.name" }
                    } 
                } 
            }
        }}];

Interface : 

all_res = db.list_results('confusion_matrix')
filt_res = db.filter_results(filt_lambda)
best_res = db.argmax(all_res, lam_func(low, high))

dep_tree = db.recursive_agg(best_res)





'''


