from detl.wrapper.wrapper import Wrapper, FunctionWrapper, Product, UnpackWrapper
from detl.core.saving import NoSaver
from functools import wraps
from detl.core.identity import Identity
from detl.store.store_context import store_context
from detl.core.result_group import namespace_dict, ResultFunction
from detl.core.identity import IdentityType


def load_and_save(saver, namespace, unpack=0):
    """

    :param saver:
    :param namespace:
    :param unpack: If unpack is 0, no unpacking, otherwise unpacks a tuple of Wrappers of length unpack
    :return:
    """
    assert unpack >= 0

    def fn_wrapper(fn):
        res_group = ResultFunction(fn, saver, IdentityType.FN_ARGS)
        namespace_dict.add_namespace(namespace, res_group)

        @wraps(fn)
        def identified_fn(*args, **kwargs):
            # Check if the database exists and whether or not we need to wrap the results
            db = store_context.get_db()
            if db is None:
                return fn(*args, **kwargs)

            if len(args) == 1 and isinstance(args, Wrapper):
                return FunctionWrapper(res_group, args[0])
            # If the input is not a wrapper, use a product wrapper
            prod = Product(*args, **kwargs)
            res_wrapper = FunctionWrapper(res_group, prod)
            # Unpack results
            if unpack == 0:
                return res_wrapper

            return UnpackWrapper(res_wrapper, unpack)

        return identified_fn
    return fn_wrapper


def identity_wrapper(namespace, unpack=False):
    return load_and_save(NoSaver(), namespace, unpack=unpack)


def unpack_results(wrapper_of_list_or_tp, unpack):

    if type(unpack) is int:
        return [wrapper_of_list_or_tp.get_unpacked_child(i) for i in range(unpack)]


def index_unpackable(unpackable, ind):
    return unpackable[ind]

