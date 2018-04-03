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

    def __init__(self, name, *args, unpack=False, **kwargs):
        '''The identity of the output of a function in terms of the name of the function and its 
            arguments
        '''
        self.name = name
        # TODO : should probably copy these
        self.args = args
        self.kwargs = kwargs
        
    def __id_hash__(self, obj=None):
        '''
        Identity is based on name and hash of arguments
        '''
        # TODO : this is really awful
        if obj is not self:
            obj = self

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
    # TODO : keep this or find a more elegant way that works both for objects and dict
    hash_val = val.__id_hash__()
    return hash_val

def to_obj_id(hash_val, db=None):
    if db is not None:
        res = db.find_from_hash(hash_val)
        if res is not None:
            return ObjectId(res['_id'])
    return hash_val


class SourceIdentity(Identity):
    
    def __init__(self, identifier):
        
        name = 'source'
        super(SourceIdentity, self).__init__(name)
        # TODO : check if bsonable
        self.identifier = identifier

    def __id_hash__(self):
        
        return h11(self.identifier)


def identify_and_insert(obj, identifier, unpack_input=False):

    id_obj = identify(obj, identifier)
    db = db_context.get_db()
    fd = db.find(obj.identity)
    if fd is None:
        db.insert(obj, None, save_data=False, unpack_input=unpack_input)
    return id_obj


def identify(obj, identifier, unpack_input=False):

    if unpack_input:
        return unpack(obj, identifier)

    return _identify(obj, identifier)


def unpack(list_or_tuple, identifier):

    all_identities = [Identity('unpack', [identifier, i], {}) for i in range(len(list_or_tuple))]
    return [_identify(list_or_tuple[i], all_identities[i]) for i in range(len(list_or_tuple))]


def _identify(obj, identifier):
    
    # Identify base class
    base_cls = obj.__class__
    # Create IDed class name
    new_cls_name = obj.__class__.__name__ + 'IDed'
    
    ided_class = type(new_cls_name, (base_cls,), dict(base_cls.__dict__))

    # Creating hash function
    id_hash = lambda some_obj: some_obj.identity.__id_hash__() 

    if type(identifier) is str:
        obj.identity = SourceIdentity(identifier)
    else:
        setattr(obj, 'identity', identifier)
    # Changing the class of the object might not be the right approach
    # EVIL
    # TODO : how to make sure the object isn't changed after this?
    setattr(ided_class, '__id_hash__', id_hash)
    #
    obj.__class__ = ided_class
    # print(ided_class)
    return obj
