from detl.store.store_context import store_context
from detl.core.result import Result
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
            return Result(data, self.identity)

        # If there is a matching identity (there might not be a value in the result)
        if store.has_value(identity):
            return Result(store.get_value(identity), identity)

        data = self._run()
        store.add_result(self.identity, data)
        return Result(data, self.identity)

    # def flatMap(self, input_wrapper, f_a_Wrap_b):
    #     return Wrapper(lambda: f_a_Wrap_b(input_wrapper.run()))
    #
    # def map(self, input_wrapper, f_a_b):
    #     return Wrapper(lambda: f_a_b)

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
        computed_args = [conditional_compute(arg) for arg in self._args]
        computed_kwargs = {k: conditional_compute(v) for k, v in self._kwargs.items()}
        # TODO : this bit should be in ProductResultGroup's compute
        self._data = (computed_args, computed_kwargs)

        return Result(self._data, self._identity)


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

        return self._data


class FunctionWrapper(Wrapper):

    def _run(self):
        comp_args = conditional_compute(self.input_wrapper)
        self._data = self.res_group.compute(comp_args)
        return self._data



class Returner(Wrapper):

    def __init__(self, data, namespace):
        self.data = data
        self.namespace = namespace
        self._identity = Identity.from_namespace()
        super(Returner, self).__init__(ReturnGroup(namespace))

    def identity(self):
        return self._identity

    def _run(self, version_info=None):
        return Result(self.data, self._identity)

    def run(self):
        return self._run()

# # This is actually the Function wrapper
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
        return obj.run()._value
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

    id_dict = {'name': 'product', 'args': args, 'kwargs': kwargs}

    return ResultIdentity.product(id_dict)

def identity_index(input_id, index):

    return IndexResultIdentity(input_id, index)


def is_acceptable_type(obj):
    return isinstance(obj, Wrapper) or \
        type(obj) is str or \
        isinstance(obj, numbers.Number)
