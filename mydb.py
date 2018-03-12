import datetime
import os
import pymongo
import hashlib
from pymongo import MongoClient

# TODO : Get mongodb configuration from config folder
client = MongoClient('localhost', 27017)
db = client.etl_test_database
coll = db.test_pipeline

data_folder = 'data'

# TODO : in the future, split the find into two cases : whether the file is available or not (in that case, can download)

def find(identity):

    hash_value = identity.__id_hash__()
    result = coll.find_one({'config_hash': hash_value})
    print('From database : ')
    print(result)
    if result is not None:
        if 'file_descriptor' in result:
            return result['file_descriptor']
    return None

def insert(identity, results, save_func, save_data=True):
    
    # TODO : move to computation identity class
    hash_value = identity.__id_hash__()
    identity_dict = identity.to_dict()
    
    # If save_data
    if save_data:
        # Create a file path
        file_path = create_fd(identity)
        # TODO : handle errors
        # Save to file path
        save_func(results, file_path)
        # Add to dict
        identity_dict['file_descriptor'] = file_path

    # Save to collection
    post = coll.insert_one(identity_dict)
    return post


def create_fd(identity):
    '''
    Save to a data folder whose name corresponds to the name of the identity
    '''
    # TODO : no global variable
    # TODO : assuming that the data folder is already created
    # TODO : move hash_value to identity init
    hash_value = identity.__id_hash__()
    folder = os.path.join(data_folder, identity.name)
    if not os.path.exists(folder):
        os.mkdir(folder)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fname = timestamp + str(hash_value)
    file_path = os.path.join(folder, fname)
    return file_path
    

