import datetime
import os
import pymongo
import hashlib
from pymongo import MongoClient
from detl.db_context import db_context
import json

def db_client(config_path='configs/db.json'):

    with open(config_path) as fd:
        config = json.load(fd)

    host = config['host']
    port = config['port']
    db = config['db']
    collection = config['collection']
    data_folder = config['data_folder']

    return MyDb(host, port, db, collection, data_folder)


class MyDb():

    def __init__(self, host, port, db, collection, data_folder):
        # TODO : Get mongodb configuration from config folder
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.etl_test_database
        self.coll = self.db.test_pipeline
    
        self.data_folder = 'data'

        # TODO : in the future, split the find into two cases : whether the file is available or not (in that case, can download)

    def find(self, identity):

        hash_value = identity.__id_hash__()
        result = self.coll.find_one({'config_hash': hash_value})
        print('From database : ')
        print(result)
        return result

    def find_file(self, identity):

        res = self.find(identity)
        if res is not None:
            if 'file_descriptor' in res:
                return res['file_descriptor']

    def insert(self, identity, results, save_func, save_data=True):
        
        # TODO : move to computation identity class
        hash_value = identity.__id_hash__()
        identity_dict = identity.to_dict()
        
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

    def browse(fn_name, origin={}):
        '''
        browse('processor_2')
        browse('processor_2', {'source':{}, 'Preprocessor':{'kwargs':{'num_mul': 50}}})

        '''
        # TODO : modify the insert to give an object id instead of hash
        # TODO : write the origins as a dictionary with $and in mongo syntax
        results = self.coll.find({'name': fn_name})
        for res in results:
            if res in origin:
                yield res
            else:
                recursive_find(res, origin)

