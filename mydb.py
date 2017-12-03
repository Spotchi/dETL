import os
import pickle
import pymongo
import hashlib
from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client.etl_test_database
coll = db.test_pipeline

data_folder = 'data'

#def hash_resolution(hash_value):
#    pass
#    # If the value is in the db then fine, otherwise we need to find a hash that 'means' the same
#    '''
#    We can define a function that associates different hashes together. This way we can check that they correspond to the value, and we have loads of tests for free
#    '''

def known_hash(hash_value):
    hval = hash_value.hexdigest()
    if coll.find_one({'config_hash': hval}):
        return True

    return False 
    
def load(hash_value):
    hval = hash_value.hexdigest()
    config = coll.find_one({'config_hash': hval})
    filename = os.path.join(data_folder, hash_value)
    print('Loading ', hval)
    return pickle.load(open(filename, "rb")) 

def add(hash_value, config, obj_value):
    hval = hash_value.hexdigest()
    print(type(hval))
    post = {'config_hash': hval}
   
    post_id = coll.insert_one(post).inserted_id  
    filename = os.path.join(data_folder, hval)
    pickle.dump(obj_value, open(filename, "wb")) 

    print('Value inserted at index {} for value {}', hval, post_id)
