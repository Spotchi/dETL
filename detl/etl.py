import numpy as np
import hashlib
import json
import detl.mydb
import inspect
from collections import OrderedDict

class ETL(object):
    
    def __init__(self, fn, *args, **kwargs):
        '''
        A wrapper class around an ETL
        :param fn: The identifier of the function to perform
        :param args: Positional arguments
        :param kwargs: Keyword arguments
        
        When created the ETL stores the values of the function and the arguments
        
        The computation of the actual result is done thanks to the use of forward()

        If the same arguments have already been seen, the forward function will simply get the results from a database
        '''
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.evaluated = False

    def forward(self):
        '''
        Retrieve the result of the underlying ETL
        
        First the hash of the configuration is computed. If the inputs have already been seen then 
        the result is loaded using the database.

        If not all the inputs are evaluated and the function is called with its arguments.
        
        '''
        self.compute_config_hash()

        if mydb.known_hash(self.conf_hash):
            return mydb.load(self.conf_hash)

        eval_args = [evaluate(item) for item in self.args]
        eval_kwargs = {k:evaluate(item) for k,item in self.kwargs.items()}

        result = self.fn(*eval_args, **eval_kwargs)
        self.evaluated = True
        mydb.add(self.conf_hash, self.hash_dict, result)
        return result

    def compute_config_hash(self):
        '''
        Compute the hash of the arguments

        The positional arguments and the keyword arguments are all hashed and put into an ordered dictionary that will be hashed itself. This new hash will be the hash corresponding to the result of the ETL object. 
        '''

        hash_args = [str(hash_ref(item)) for item in self.args]
        hash_kwargs = {k:str(hash_ref(item)) for k, item in self.kwargs.items()}
        ev_dict = [self.fn.__name__, sorted(hash_args), sorted(hash_kwargs.items(), key=lambda t: t[0])]
        self.conf_hash = hashlib.sha256(json.dumps(ev_dict, sort_keys=True).encode())
        self.hash_dict = ev_dict
        return self.conf_hash
        

def evaluate(obj_or_val):
    '''Evaluate an ETL object or a python object
    
    :param obj_or_val: The object to evaluate

    If the object is an ETL object, we return the output of forward()

    Otherwise we return the object itself
    '''
    if isinstance(obj_or_val, ETL):
        return obj_or_val.forward()
    return obj_or_val

def hash_ref(obj_or_val):
    '''
    Hash an ETL object or a python object

    :param obj_or_val:

    If the object is an ETL, return/compute its hash
    Else simply hash the object
    '''
    if isinstance(obj_or_val, ETL):
        if obj_or_val.evaluated:
            return obj_or_val.conf_hash
   
        obj_or_val.forward()
        return obj_or_val.conf_hash

    return hashlib.sha256(str(obj_or_val).encode())

def register_ETL(fn):
    def inner_fn(*args, **kwargs):
        return ETL(fn, *args, **kwargs)
    return inner_fn


@register_ETL
def normalize(x):
    return x-x.mean()

@register_ETL
def divide_etl(x):
    return x/5.0

@register_ETL
def returner(x):
    return x


x = returner(np.array([2.0, 5.0, 42.0]))
x1 = normalize(x)
x2 = divide_etl(x)

print(x2.forward())

