from detl.core.identity import Identity, SourceIdentity
from detl.store.store_context import store_context
from detl.core.result import Result
from detl.core.run import Run
import numbers
from enum import Enum


class WrapperType(Enum):
    FUNCTION = 'fn'
    PRODUCT = 'product'
    INDEX = 'index'


class Wrapper(object):

    def __init__(self):
        pass

    @property
    def identity(self):
        raise NotImplementedError()

    def _run(self):
        raise NotImplementedError()

    def run(self, version_info=None):
        store = store_context.get_db()

        if store is None:
            return self._run()

        identity = store.get_result(self.identity, version_info)
        if identity is None:
            data = self._run()
            store.add_result(self.identity, data)
            return Run(None, data, self.identity)

        # If there is a matching identity (there might not be a value in the result)
        if store.has_value(identity):
            return Run(None, store.get_value(identity), identity)

        data = self._run()
        store.add_result(self.identity, data)
        return Run(None, data, self.identity)

    # def flatMap(self, input_wrapper, f_a_Wrap_b):
    #     return Wrapper(lambda: f_a_Wrap_b(input_wrapper.run()))
    #
    # TODO : this is the way I'll get things done but I need Wrapper to take a function
    # that returns a Run as input
    # def rg_map(self, rg_a_b):
    #     def _run():
    #         return rg_a_b.compute(self.input_wrapper.run())
    #     return Wrapper(_run)

    @classmethod
    def from_hash(cls, config_hash):

        wrap = Wrapper(int, [], {})
        db = store_context.get_db()
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


# TODO : There might not be a need for this class, may just implement a map with ResGroup as fn instead
class FunctionWrapper(Wrapper):

    def __init__(self, res_group, input_wrapper):
        self.res_group = res_group
        self.input_wrapper = input_wrapper
        self._identity = self.res_group.get_result_identity(self.input_wrapper.identity)

    def identity(self):
        return self._identity

    def _run(self):
        return self.res_group.compute(self.input_wrapper.run())


class Product(Wrapper):

    def __init__(self, *args, **kwargs):
        for arg in args:
            assert is_acceptable_type(arg)

        self._args = args
        self._kwargs = kwargs

        self._identity = identity_product(*args, **kwargs)

        self._data = None

    def _run(self):
        computed_args = tuple(conditional_compute(arg) for arg in self._args)
        self._data = computed_args

        return Run(None, self._data, self._identity)


# TODO : More like a Python product
class ArgsProduct(Wrapper):

    def __init__(self, *args, **kwargs):
        for arg in args:
            assert is_acceptable_type(arg)

        self._args = args
        self._kwargs = kwargs

        self._identity = identity_product(*args, **kwargs)

        self._data = None


    def _run(self):
        computed_args = (conditional_compute(arg) for arg in self._args)
        computed_kwargs = {k: conditional_compute(v) for k, v in self._kwargs.items()}
        # TODO : this bit should be in ProductResultGroup's compute
        self._data = (computed_args, computed_kwargs)

        return Run(None, self._data, self._identity)


def unpack_wrapper(input_wrapper, unpack):
    assert unpack > 0
    for idx in range(unpack):
        yield IndexWrapper(input_wrapper, idx)


class IndexWrapper(Wrapper):

    def __init__(self, input_wrapper, index):
        self.input_wrapper = input_wrapper
        self._index = index
        self._identity = identity_index(self.input_wrapper, index)

    def _run(self):
        computed_arg = conditional_compute(self.input_wrapper)
        self._data = computed_arg[self._index]

        return Run(None, self._data, self._identity)


class Returner(Wrapper):

    def __init__(self, data, namespace):
        self.data = data
        self._identity = SourceIdentity(namespace)
        if not is_acceptable_type(data):
            raise ValueError("{data} must be of an acceptable type : {acc}".format(data=data, acc=ACCEPTABLE_TYPES))
        # TODO : check that input is an acceptable type (otherwise need to provide a namespace
        super(Returner, self).__init__()

    def identity(self):
        return self._identity

    def _run(self, version_info=None):
        return Run(None, self.data, self._identity)

    def run(self):
        return self._run()

# I should actually implement this as a variant of Run for trampolining
# class Flatmap(Wrapper):
#
#     def __init__(self, wrapper_in, f):
#         self.wrapper_in = wrapper_in
#         self.f = f



# Result represent one computation whether it happens or not
# ResultGroup is basically a type of computation
# Wrapper is a computation as applied to an input

def conditional_compute(obj):
    if isinstance(obj, Wrapper):
        return obj.run()._data
    if (type(obj) is str) or isinstance(obj, numbers.Numbers):
        return obj

    raise ValueError("Cannot get identiy of object with type : {}".format(type(obj)))


def conditional_identity(obj):
    if isinstance(obj, Wrapper):
        return obj._identity
    if (type(obj) is str) or isinstance(obj, numbers.Numbers):
        return obj

    raise ValueError("Cannot get identiy of object with type : {}".format(type(obj)))


def identity_product(*args, **kwargs):

    id_dict = {'name': 'product', 'args': [conditional_identity(arg) for arg in args],
               'kwargs': {k: conditional_identity(v) for  k, v in kwargs.items()}}

    return id_dict

def identity_index(input_id, index):

    return {'name': 'index{}'.format(index), 'args':input_id}

ACCEPTABLE_TYPES = [Wrapper, numbers.Number, str]

def is_acceptable_type(obj):
    # TODO : test this using Acceptable types
    return isinstance(obj, Wrapper) or \
        type(obj) is str or \
        isinstance(obj, numbers.Number)
