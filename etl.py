import numpy as np
import hashlib
import json
import mydb
import inspect
print(inspect.getmembers(mydb, inspect.ismethod))
class ETL(object):
    
    def __init__(self, fn, *args, **kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.evaluated = False

    def forward(self):
        '''
        Two hashes of the same operation can be different because at some point in the hierarchy, a function has been
        called with a value that has a different hash than previously.
        The config hash is useful because it allows to map the config to the result. The result can then be used for the next step 
        of the pipeline.
        When calling forward, the function checks the hashes of the inputs and then check if they are in the database. If one of them isn't,
        we have to go back there and compute it.
        '''
        self.compute_config_hash()
        if mydb.known_hash(self.conf_hash):
            return mydb.load(self.conf_hash)
        eval_args = [evaluate(item) for item in self.args]
        eval_kwargs = {k:evaluate(item) for k,item in self.kwargs.items()}
        self.evaluated = True
        return self.fn(*eval_args, **eval_kwargs)

    def compute_config_hash(self):
        hash_args = [str(hash_ref(item)) for item in self.args]
        hash_kwargs = {k:str(hash_ref(item)) for k, item in self.kwargs.items()}
        ev_dict = {'fn': self.fn.__name__, 'args': hash_args, 'kwargs': hash_kwargs}
        self.conf_hash = hashlib.sha256(json.dumps(ev_dict, sort_keys=True).encode())
        return self.conf_hash
        

def evaluate(obj_or_val):
    if isinstance(obj_or_val, ETL):
        return obj_or_val.forward()
    return obj_or_val

def hash_ref(obj_or_val):
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

