import numpy as np
import json
import abc
from functools import wraps
from detl.identity import Identity, identify
from detl.db_context import db_context

def load(load_fn, fd, unpack=False):

    if unpack:
        return [load_fn(sfd) for sfd in fd]

    return load_fn(fd)


def load_and_save(load_func, save_func, unpack=False):
    def fn_wrapper(fn):
        @wraps(fn)
        def identified_fn(*args, **kwargs):
            # Get identity of computation using the object's identity and method name and arguments
            compute_identity = Identity(fn.__name__, *args, **kwargs, unpack=unpack)

            # Verify if hash exists
            # If hash is in db, get filedescriptor and load
            db = db_context.get_db()
            
            if db is None:
                return identify(fn(*args, **kwargs), unpack=unpack)
            
            # If unpacking is on we need to be able to trace where the elements come from
            if unpack:
                db._insert(compute_identity, None, None, save_data=False)
            

            fd = db.find_file(compute_identity)
            
            if fd is not None:
                print('Loading from {}'.format(fd))
                results = identify(load(load_func, fd, unpack=unpack), compute_identity, unpack_input=unpack)
            # If not, compute and insert
            else:
                print('No result found in the database for identity')
                results = identify(fn(*args, **kwargs), compute_identity, unpack_input=unpack)
                db.insert(results, save_func, unpack_input=unpack)

            return results
        return identified_fn
    return fn_wrapper


# TODO : some refactoring to allow identification and 
def unpack_save(load_func, save_func, num_outputs):
    def fn_wrapper(fn):
        @wraps(fn)
        def identified_fn(*args, **kwargs):
            # Get identity of computation using the object's identity and method name and arguments
            compute_identity = Identity(fn.__name__, *args, **kwargs)
            all_identities = [Identity('unpack', [compute_identity, i], {}) for i in range(num_outputs)]
            # Verify if hash exists
            # If hash is in db, get filedescriptor and load
            db = db_context.get_db()
            if db is None:
                return fn(*args, **kwargs)

            results = []
            missing = False
            # for idx, identity in enumerate(all_identities):
            #     fd = db.find_file(identity)
            #
            #     if fd is not None:
            #         print('Loading from {}'.format(fd))
            #         results.append(identify(load_func(fd), identity))
            # # If not, compute and insert
            #     else:
            #         missing = True
            #         break
            # TODO : do something about that, 
            # Need to think about how the loading goes for this kind of function. For black box unpacked functions, there is not 
            # much to be done but for map reduce we can check them individually
            # TODO : framework for map reduce jobs
            #if missing:
            results = fn(*args, **kwargs)
            id_res = []
            for idx, res in enumerate(results):
                ided_res = identify(res, all_identities[idx])
                id_res.append(ided_res)
                db.insert(ided_res, save_func)

            return tuple(id_res)
        return identified_fn
    return fn_wrapper


def identity_wrapper(fn):
    def ided_fn(*args, **kwargs):

        compute_identity = Identity(fn.__name__, *args, **kwargs)
        results = identify(fn(*args, **kwargs), compute_identity)
        db = db_context.get_db()
        if db is None:
            return results

        fd = db.find(compute_identity)
        if fd is None:
            db.insert(results, None, save_data=False)
        return results
    return ided_fn

class Processor(object):

    def __init__(self, *args, **kwargs):
        '''
        A class that processes some computations and includes a save and load function
        This class is merely a different way of implementing the ETL class. Here the functions and arguments are defined by the programmer coding directly in the extending class
        '''
        db = db_context.get_db()
        # TODO : Should compute the hashes for all the args and the kwargs???
        # YEs, we should still be able to do lazy evaluation
        class_name = self.__class__.__name__
        self.identity = Identity(class_name, *args, **kwargs)
        
        fd = db.find(self.identity)
        if fd is None:
            db.insert(self, None, save_data=False)


    # Those wrapper will require that all args and kwargs be hashable, which is why my classes are going
    # extend Processor, whose main role will be to keep a hash
    def __id_hash__(self):
        return self.identity.__id_hash__()
 
# TODO : no need to implement this? Just need to make sure that the state of the object that changes is serialized
def change_state(fn):
    @wraps(fn)
    def inner_fn(self, *args, **kwargs):
        assert issubclass(type(self), Processor)
        class_name = self.__class__.__name__
        class_method_name = fn.__name__

        self.identity = Identity(class_name+class_method_name, *args, **kwargs)
        
        fd = db.find(self.identity)
        if fd is None:
            db.insert(self.identity, None, None, save_data=False)
 
        return fn(*args, **kwargs)
    return inner_fn


