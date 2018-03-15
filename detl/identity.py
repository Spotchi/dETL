import types
import json
import hashlib
import copy
from functools import singledispatch, partial
from detl.db_context import db_context
from bson.objectid import ObjectId
from bson.json_util import dumps, loads

def h11(text):
    '''The hash used for the serialized configurations'''
    b = hashlib.md5(text.encode()).hexdigest()[:9]
    return int(b, 16)

class Identity(object):

    def __init__(self, name, *args, **kwargs):
        '''The identity of the output of a function in terms of the name of the function and its 
            arguments
        '''
        self.name = name
        # TODO : should probably copy these
        self.args = args
        self.kwargs = kwargs
        
    def __id_hash__(self):
        '''
        Identity is based on name and hash of arguments
        '''
        id_dict = {'name' : self.name, 'args' : self.args, 'kwargs' : self.kwargs}
        return h11(json.dumps(id_dict, sort_keys=True, default=to_serializable ))

    def to_dict(self, db=None):
        '''Create a serializable version of the configuration'''
        base_dict =  { 'config_hash': self.__id_hash__(), 
                'name': self.name, 
                'args' : self.args, 
                'kwargs' : self.kwargs}
        serialized_dict = json.dumps(base_dict, default=to_serializable)
        reloaded_dict = json.loads(serialized_dict)
        reloaded_dict['args'] = [to_obj_id(el, db=db) for el in reloaded_dict['args']]
        reloaded_dict['kwargs'] = {k:to_obj_id(val, db=db) for k, val in reloaded_dict['kwargs'].items()}

        return reloaded_dict 


@singledispatch
def to_serializable(val, db = None):
    """Used by default."""
    hash_val = val.__id_hash__()
    return hash_val

def to_obj_id(hash_val, db=None):
    print(hash_val)
    if db is not None:
        res = db.find_from_hash(hash_val)
        print(res)
        print('Yodledel')
        if res is not None:
            print(res['_id'])
            return ObjectId(res['_id'])
    return hash_val


    return val
class SourceIdentity(Identity):
    
    def __init__(self, identifier):
        
        name = 'source'
        super(SourceIdentity, self).__init__(name)
        # TODO : check if bsonable
        self.identifier = identifier

    def __id_hash__(self):
        
        return h11(self.identifier)


def identify(obj, identifier):
     
    # Identify base class
    base_cls = obj.__class__
    # Create IDed class name
    new_cls_name = obj.__class__.__name__ + 'IDed'
    
    # Creating hash function
    id_hash = lambda some_obj: some_obj.identity.__id_hash__() 
    obj.identity = SourceIdentity(identifier)

    # EVIL
    # TODO : how to make sure the object isn't changed after this?
    ided_class = type(new_cls_name, (base_cls,), dict(base_cls.__dict__))
    setattr(ided_class, '__id_hash__', id_hash)

    obj.__class__ = ided_class
    
    db = db_context.get_db()
    fd = db.find(obj.identity)
    if fd is None:
        db.insert(obj.identity, None, None, save_data=False)
    return obj   


