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
    if coll.find_one({'config_hash': hash_value}):
        return True

    return False 
    
def load(hash_value):
   
    config = coll.find_one({'config_hash': hash_value})
    filename = os.path.join(data_folder, hash_value)
    return pickle.load(open(filename, "rb")) 
