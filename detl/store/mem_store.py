from ck.store import Store


class MemStore(Store):

    def __init__(self):
        """
        Initialize a store that remembers runs in memory
        """
        self.root = '/'

        self.namespace = {}
        super(MemStore, self).__init__()

    def list_results(self, namespace, min_date=None, max_date=None, tags=[]):
        """
        """
        return [str(k) + " " + str(it) for k, it in self.namespace.items()]

    def get_results_namespace(self, name, min_date=None, max_date=None, tags=[]):
        if name in self.namespace:
            return [self.namespace[name]]
        return ["Empty namespace"]

    def add_result(self, identity, result):
        """
        The identity contains the result group
        :param identity:
        :param result:
        :return:
        """
        self.namespace[name] = identity

    def get_result(self, result_id, version_info):
        """
        """
        # TODO : the version info will do the selection for the different matching results
        return [it for k, it in self.namespace.items() if it.res_id == result_id][0]

    def delete_result(self, result_id, version_info):
        """
        Delete all versions of the result_id
        :param experiment_id: Integer id for the experiment
        """
        matches = [k for k, it in self.namespace.items() if it.res_id == result_id]
        for k in matches:
            self.namespace.pop(k)
        return matches

    def restore_result(self, result_id, version_info):
        """
        Restore deleted experiment unless it is permanently deleted.
        :param experiment_id: Integer id for the experiment
        """
        pass
