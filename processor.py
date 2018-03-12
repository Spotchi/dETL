import numpy as np
import json
import abc
from functools import wraps
from identity import Identity
import mydb


# TODO : no need to implement this? Just need to make sure that the state of the object that changes is serialized
def change_state(changed):
    def fn_wrapper(fn):
        @wraps(fn)
        def change_state(*args, **kwargs):
            return fn(*args, **kwargs)

        return change_state
    return fn_wrapper


def load_and_save(load_func, save_func):
    def fn_wrapper(fn):
        @wraps(fn)
        def identified_fn(*args, **kwargs):
            # Get identity of computation using the object's identity and method name and arguments
            compute_identity = Identity(fn.__name__, *args, **kwargs)
            # Verify if hash exists
            # If hash is in db, get filedescriptor and load
            fd = mydb.find(compute_identity)
            
            if fd is not None:
                print('Loading from {}'.format(fd))
                results = load_func(fd)
            # If not, compute and insert
            else:
                print('No result found in the database for identity')
                results = fn(*args, **kwargs)
                mydb.insert(compute_identity, results, save_func)

            return results
        return identified_fn
    return fn_wrapper

def identity_wrapper(fn):
    def ided_fn(*args, **kwargs):
        results = fn(*args, **kwargs)
        fd = mydb.find(results.identity)
        if fd is None:
            mydb.insert(results.identity, None, None, save_data=False)
        return results
    return ided_fn

class Processor(object):
    @identity_wrapper
    def __init__(self, *args, data_server='localhost', **kwargs):
        '''
        A class that processes some computations and includes a save and load function
        This class is merely a different way of implementing the ETL class. Here the functions and arguments are defined by the programmer coding directly in the extending class
        '''
        # self.evaluated = False
        self.data_server = data_server

        # TODO : Should compute the hashes for all the args and the kwargs???
        # YEs, we should still be able to do lazy evaluation
        class_name = self.__class__.__name__
        self.identity = Identity(class_name, *args, **kwargs)

    # Those wrapper will require that all args and kwargs be hashable, which is why my classes are going
    # extend Processor, whose main role will be to keep a hash
    def __id_hash__(self):
        return self.identity.__id_hash__()
    
