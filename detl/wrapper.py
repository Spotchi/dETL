from detl.identity import Identity
from detl.db_context import db_context
import importlib

def load(load_fn, fd, unpack=False):

    if unpack:
        return [load_fn(sfd) for sfd in fd]

    return load_fn(fd)

class Wrapper(object):

    def __init__(self, fn, args, kwargs, unpack_input=False, save_fn=None, load_fn=None):

        self.fn = fn
        self.args = args
        self.kwargs = kwargs

        self.identity = Identity(fn.__name__, *args, **kwargs, unpack=unpack_input, save_fn=save_fn, load_fn=load_fn)

        self.unpack = unpack_input
        # if self.unpack:
        #    db = db_context.get_db()
        #    if db:
        #        if db.find(self.identity) is None:
        #            db._insert(self.identity, None, None, save_data=False)
        
        self.save_fn = save_fn
        self.load_fn = load_fn
        
        self._data = None

    @classmethod
    def from_hash(cls, config_hash):

        wrap = Wrapper(int, [], {})
        db = db_context.get_db()
        if db is None:
            raise ValueError('No db, cannot build wrapper form hash')

        conf_dict = db.find_from_hash(config_hash)
        if 'file_descriptor' not in conf_dict:
            raise ValueError('No filename')

        fd = conf_dict['file_descriptor']
        
        wrap.identity = Identity.from_dict(conf_dict)
        wrap.load_module = importlib.import_module(conf_dict['load_fn']['fn_module'])
        load_fn = getattr(wrap.load_module, conf_dict['load_fn']['fn_name'])

        wrap._data = load_fn(fd)

        return wrap

    def __id_hash__(self):

        return self.identity.__id_hash__()

    @property
    def data(self, save_data=True):

        if self._data is not None:
            return self._data

        db = db_context.get_db()

        if db is None:
            get_args = [get_data(arg) for arg in self.args]
            get_kwargs = {k:get_data(v) for k,v in self.kwargs.items()}
            results = self.fn(*get_args, **get_kwargs)
            self._data = results
 
        fd = db.find_file(self.identity, unpack_input=self.unpack, unpack_len=4)

        # Either fd is not none for the base case or none of the unpacked fd is None
        # TODO : not clean or understandable
        if (fd is not None and not self.unpack) or \
                (self.unpack and not any(el is None for el in fd)):
            return load(self.load_fn, fd, unpack=self.unpack)

            # If not, compute and insert
        else:
            get_args = [get_data(arg) for arg in self.args]
            get_kwargs = {k:get_data(v) for k,v in self.kwargs.items()}
            results = self.fn(*get_args, **get_kwargs)
            self._data = results
            db._insert(self.identity, results, self.save_fn, save_data=save_data)
        return results

    def get_unpacked_child(self, ind):

        if not self.unpack:
            raise ValueError

        return Wrapper(index_unpackable, [self, ind], {}, unpack_input=False, 
                    save_fn=self.save_fn, load_fn=self.load_fn)


class SourceWrapper(Wrapper):

    def __init__(self, obj, identifier):

        self.identity = SourceIdentity(identifier)
        self.obj = obj
        db = db_context.get_db()
        fd = db.find(self.identity)
        if fd is None:
            db._insert(self.identity, None, save_data=False)
        
    def data(self):
        return obj


def wrap_obj(obj, identifier, unpack_input=False, save_fn=None, load_fn=None):
    
    return Wrapper(returner, [obj], {}, unpack_input=unpack_input, save_fn=save_fn, load_fn=load_fn)


def wrap_results(fn, args, kwargs, unpack_input=False, save_fn=None, load_fn=None):
    
    all_res_wrapped = Wrapper(fn, args, kwargs, unpack_input=unpack_input, save_fn=save_fn, load_fn=load_fn)

    if unpack_input:
        return unpack_results(all_res_wrapped, unpack_input)

    return all_res_wrapped


def unpack_results(wrapper_of_list_or_tp, unpack):

    if type(unpack) is int:
        return [wrapper_of_list_or_tp.get_unpacked_child(i) for i in range(unpack)]


def index_unpackable(unpackable, ind):
    return unpackable[ind]


def get_data(obj_or_wrapped_obj):
    '''Essential to unwrap identified objects'''
    if type(obj_or_wrapped_obj) is Wrapper:
        return obj_or_wrapped_obj.data

    return obj_or_wrapped_obj


