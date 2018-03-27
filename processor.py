import numpy as np
import json
from collections import OrderedDict
import abc
from functools import wraps
import string
from random import *

allchar = string.ascii_letters + string.punctuation + string.digits

# TODO : implement
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
            compute_identity = Identity.from_object_and_method(self.identity, fn, args, kwargs)
            # Verify if hash exists
            # If hash is in db, get filedescriptor and load
            fd = mydb.find(compute_identity)
            
            if fd is not None:
                results = load_func(fd)
            # If not, compute and insert
            else:
                results = fn(args, kwargs)
                mydb.insert(compute_identity, results, load_func)

            return results
        return identified_fn
    return fn_wrapper


class Processor(object):
    
    def __init__(self, *args, data_server='localhost', **kwargs):
        '''
        A class that processes some computations and includes a save and load function
        This class is merely a different way of implementing the ETL class. Here the functions and arguments are defined by the programmer coding directly in the extending class
        '''
        # self.evaluated = False
        self.data_server = data_server

        # TODO : Should compute the hashes for all the args and the kwargs???
        # YEs, we should still be able to do lazy evaluation
        self.identity = Identity(class_name, *args, **kwargs)

    # Those wrapper will require that all args and kwargs be hashable, which is why my classes are going
    # extend Processor, whose main role will be to keep a hash
    def __hash__(self):
        return hash(self.identity)
    
    def save(save_func):
        def fn_wrapper(fn):
            @wraps(fn)
            def new_fn(*args, **kwargs):

                # Get hashes for every input
                #                hashes = []... + {}
                # Build hash for the method AND object

                # Save to db using
                #file_description = data_server.get_file_description(self, fn)
                min_char = 8
                max_char = 12
                file_description = "".join(choice(allchar) for x in range(randint(min_char, max_char)))
                results = fn(*args, **kwargs)
                save_func(results, file_description)
                return fn(*args, **kwargs)
            return new_fn
        return fn_wrapper

    @staticmethod
    def load(load_func):
        def fn_wrapper(fn):
            @wraps(fn)
            def new_fn(*args, **kwargs):
                # Get hashes for every input
                # hashes = []... + {}
                # Build hash for the method AND object

                # Save to db using
                # file_description = data_server.get_file_description(self, fn)

                return fn(*args, **kwargs)
            return new_fn
        return fn_wrapper

    # This is needed because the hash of the object is not sufficient, we will need hash of the fn id as well
    @staticmethod
    def identity_wrapper(wrapper_class):
        def fn_wrapper(fn):
            @wraps(fn)
            def new_fn():
                return wrapper_class(fn, *args, **kwargs)
            return new_fn

        return fn_wrapper

    def __hash__(self):
        return 42


    # @abc.abstractmethod
    # def process(self, *args, **kwargs):
    #     '''
    #     Processes using args and kwargs
    #     '''
    #     pass
    #
    # @abc.abstractmethod
    # def save(self, *args, **kwargs):
    #
    #     pass
    #
    # @abc.abstractmethod
    # def load(self, file_descriptor):
    #     pass
    #
    # def equal(self, args1, kwargs1, args2, kwargs2):
    #     pass

    # def get(self):
    #     # TODO : will the data_server be responsible for the loading in the future?
    #     # TODO : check that all the arguments are hashable
    #     self.compute_config_hash()
    #
    #     fd = self.data_server.has(self.id, args, kwargs)
    #     if fd:
    #         return self.load(load_id)
    #
    #     return self.process(args, kwargs)
    #
    #     eval_args = [evaluate(item) for item in self.args]
    #     eval_kwargs = {k:evaluate(item) for k,item in self.kwargs.items()}
    #
    #     result = self.process(*eval_args, **eval_kwargs)
    #     self.evaluated = True
    #     mydb.add(self.conf_hash, self.hash_dict, result)
    #     return result

    # def compute_config_hash(self):
    #     '''
    #     Compute the hash of the arguments
    #
    #     The positional arguments and the keyword arguments are all hashed and put into an ordered dictionary that will be hashed itself. This new hash will be the hash corresponding to the result of the ETL object.
    #     '''
    #
    #     hash_args = [str(hash_ref(item)) for item in self.args]
    #     hash_kwargs = {k:str(hash_ref(item)) for k, item in self.kwargs.items()}
    #     ev_dict = [self.fn.__name__, sorted(hash_args), sorted(hash_kwargs.items(), key=lambda t: t[0])]
    #     self.conf_hash = hashlib.sha256(json.dumps(ev_dict, sort_keys=True).encode())
    #     self.hash_dict = ev_dict
    #     return self.conf_hash
        

def evaluate(obj_or_val):
    '''Evaluate an ETL object or a python object
    
    :param obj_or_val: The object to evaluate

    If the object is an ETL object, we return the output of forward()

    Otherwise we return the object itself
    '''
    if isinstance(obj_or_val, ETL):
        return obj_or_val.forward()
    return obj_or_val



