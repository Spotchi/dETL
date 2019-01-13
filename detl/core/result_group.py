from enum import Enum
from detl.core.run import Run
from detl.core.result import Result
from detl.store.store_context import store_context


class GroupType(Enum):
    FN = 'Function'

# TODO : ,ale more explicit
rg_dict = {}

def get_result_group_from_name(name):
    return rg_dict[name]


class ResultGroup(object):
    """
    A function operation, that corresponds to somewhere in the namespace but without its arguments
    """
    def __init__(self, saver, base_namespace):
        self._saver = saver
        self._namespace = base_namespace
        # TODO : where do we fill this?
        rg_dict[base_namespace] = self
        # Should not be here for scalability reasons
        # self.results = []

    def __str__(self):
        return self.namespace

    @property
    def namespace(self):
        return self._namespace

    # TODO : this method shouldn't exist
    def save(self, res_value):
        store = store_context.get_db()
        self.saver.save(store, self.namespace, res_value)

    def to_dict(self):
        raise NotImplementedError()


class ResultFunction(ResultGroup):
    def __init__(self, fn, saver, namespace):
        """
        Responsible for storing function, saving and how we identify
        :param fn:
        :param saver:
        :param namespace:
        """
        self.fn = fn
        super(ResultFunction, self).__init__(saver, namespace)
        # self.saver = saver
        # self.idType = namespace

    def get_result_identity(self, input_identity):
        return self.namespace + str(input_identity)

    def get_result_value(self, input_value):
        return self.fn(input_value)

    def get_result(self, input_result):
        return ResultFunction(self.get_result_identity(), self.get_result_value())

    def compute(self, input_result):
        new_id = self.get_result_identity(input_result.identity)
        new_value = self.get_result_value(input_result.data)
        # TODO : get the id in and figure out where to get the run_info from (maybe argument?)
        return Run(None, new_value, new_id) # ), new_id)

    # Todo : this method belongs in the result class
    def _id_hash(self, input_identity):
        id_str = self.namespace() + input_identity.dump()

        return h11(id_str)

    def to_dict(self):
        return {
            'namespace': self.namespace,
            'type': GroupType.FN.value
        }


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
