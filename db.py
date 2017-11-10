import pymongo
import hashlib
from etl import ETL



def hash_resolution(hash_value):
    pass
    # If the value is in the db then fine, otherwise we need to find a hash that 'means' the same
    '''
    We can define a function that associates different hashes together. This way we can check that they correspond to the value, and we have loads of tests for free
    '''
    
def where_hash(hash_value):
    