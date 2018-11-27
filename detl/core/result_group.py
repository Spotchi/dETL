from detl.core.result import Result
from detl.store.store_context import store_context


class ResultGroup(object):
    """
    A function operation, that corresponds to somewhere in the namespace but without its arguments
    """
    def __init__(self, saver, base_namespace):
        self._saver = saver
        self.namespace = base_namespace

    def namespace(self):
        return self.namespace

    def save(self, res_value):
        store = store_context.get_db()
        self.saver.save(store, self.namespace, res_value)


class ResultFunction(ResultGroup):
    def __init__(self, fn, saver, idType):
        """
        Responsible for storing function, saving and how we identify
        :param fn:
        :param saver:
        :param idType:
        """
        self.fn = fn
        self.saver = saver
        self.idType = idType

    def get_result_identity(self, input_identity):
        return 'lol' + input_identity

    def get_result_value(self, input_value):
        return self.fn(input_value)


    def compute(self, input_result):
        new_id = self.get_result_identity(input_result.identity)
        new_value = self.get_result_value(input_result,value)
        return Result(new_value, new_id)

    def _id_hash(self, input_identity):
        id_str = self.namespace() + input_identity.dump()

        return h11(id_str)


class NamespaceDict(object):

    def __init__(self):
        self._namespace_dict = {}

    def add_namespace(self, ns, res_group):
        # levels = ns.split('/')
        # self._namespace_dict[levels[0]] =
        # for i, lev in levels:
        # TODO : decompose into levels
        self._namespace_dict[ns] = res_group


namespace_dict = NamespaceDict()
