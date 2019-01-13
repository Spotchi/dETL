class SavingInfo(object):
    """
    Typically just a path
    """
    def __init__(self, path):
        self._path = path

    @property
    def path(self):
        return self._path


class Saver(object):

    def save(self, run, saving_info):
        """
        Saves to a store
        :return:
        """
        pass


    def load(self, saving_info):
        """
        Returns a result value
        :return:
        """
        pass


class FileSaver(Saver):

    def save(self, run, saving_info):
        # result_id = run._res_id
        result_data = run.data
        # Info about user, source code
        # run_info = run.run_info
        # This has to be done in the wrapper code
        # path = self._store.get_result_path(run)
        with open(saving_info.path, 'w') as fd:
            self._save(result_data, fd)
        # Alsohas to be done in the wrapper code
        # self._store.add_run(run)

    def _save(self, data, fd):
        fd.write(str(data))

    def load(self, saving_info):
        with open(saving_info.path, 'r') as fd:
            return self._load(fd)

    def _load(self, fd):
        return fd.readline()


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

# class HiveSaver(Saver):
