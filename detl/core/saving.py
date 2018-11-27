

class Saver(object):


    def save(self, store, path):
        """
        Saves to a store
        :return:
        """
        pass


    def load(self):
        """
        Returns a result value
        :return:
        """
        pass


class FileSaver(Saver):

    def save(self, result, store, path):
        with open(path, 'w') as fd:
            self._save(result, fd)

    def _save(self, res, fd):
        pass


class PandasSaver(Saver):

    def __init__(self, store, path):
        self.store = store
        self.path = path

    def save(self, result):
        """
        Returns itself but saves to the store under a given path
        :param result:
        :param store:
        :param path:
        :return:
        """
        result.to_csv(self.path)

    def load(self):
        """
        Returns a wrapper
        :param path:
        :param store:
        :return:
        """
        res = pd.read_csv(self.path)
        return Wrapper(res)

class HiveSaver(Saver):
