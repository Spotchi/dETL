import types
import json
import hashlib
import copy
from functools import singledispatch
import mydb

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

    def to_dict(self):
        '''Create a serializable version of the configuration'''
        base_dict =  { 'config_hash': self.__id_hash__(), 
                'name': self.name, 
                'args' : self.args, 
                'kwargs' : self.kwargs}
        
        serialized_dict = json.dumps(base_dict, default=to_serializable)

        return json.loads(serialized_dict)

@singledispatch
def to_serializable(val):
    """Used by default."""
    return val.__id_hash__()



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
    
    fd = mydb.find(obj.identity)
    if fd is None:
        mydb.insert(obj.identity, None, None, save_data=False)
    return obj   


