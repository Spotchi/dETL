import types
import json
import hashlib
import copy
from functools import singledispatch, partial
from detl.db_context import db_context
from bson.objectid import ObjectId
from bson.json_util import dumps, loads
from inspect import getmodule

def h11(text):
    '''The hash used for the serialized configurations'''
    b = hashlib.md5(text.encode()).hexdigest()[:9]
    return int(b, 16)

class Identity(object):

    def __init__(self, name, *args, unpack=False, save_fn=None, load_fn=None, **kwargs):
        '''The identity of the output of a function in terms of the name of the function and its 
            arguments
        '''
        self.name = name
        # TODO : should probably copy these
        self.args = args
        self.kwargs = kwargs

        self.save_fn = save_fn
        self.save_dict = { 'fn_name': save_fn.__name__,
                'fn_module': getmodule(save_fn).__name__} if save_fn else None
        self.load_dict = {'fn_name': load_fn.__name__,
                'fn_module': getmodule(load_fn).__name__} if load_fn else None
        self.load_fn = load_fn
        
    def __id_hash__(self, obj=None):
        '''
        Identity is based on name and hash of arguments
        '''
        # TODO : this is really awful
        if obj is not self:
            obj = self

        id_dict = {'name' : self.name, 'args' : self.args, 'kwargs' : self.kwargs, 'load_fn':self.load_dict, 'save_fn': self.save_dict}
        return h11(json.dumps(id_dict, sort_keys=True, default=to_serializable ))

    def to_dict(self, db=None):
        '''Create a serializable version of the configuration'''
        base_dict =  { 'config_hash': self.__id_hash__(), 
                'name': self.name, 
                'args' : self.args, 
                'kwargs' : self.kwargs,
                'load_fn' : self.load_dict,
                'save_fn' : self.save_dict}

        serialized_dict = json.dumps(base_dict, default=to_serializable)
        reloaded_dict = json.loads(serialized_dict)
        reloaded_dict['args'] = [to_obj_id(el, db=db) for el in reloaded_dict['args']]
        reloaded_dict['kwargs'] = {k:to_obj_id(val, db=db) for k, val in reloaded_dict['kwargs'].items()}

        return reloaded_dict 

    @classmethod
    def from_dict(cls, conf_dict):
        wrap = Identity(None, [], {})
        wrap.name = conf_dict['name']
        wrap.args = conf_dict['args']
        wrap.kwargs = conf_dict['kwargs']
        wrap.load_dict = conf_dict['load_fn']
        wrap.save_dict = conf_dict['save_fn']

        return wrap

@singledispatch
def to_serializable(val, db = None):
    """Used by default."""
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


