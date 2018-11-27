from abc import abstractmethod, ABCMeta


class Store(object):
    """
    Abstract class for Backend Storage
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        """
        """
        pass

    # TODO : instead of experiment, add the notion of result types

    @abstractmethod
    def list_results(self, namespace, min_date=None, max_date=None, tags=[]):
        """
        """
        pass

    @abstractmethod
    def get_results_namespace(self, namespace):
        """
        """
        pass

    @abstractmethod
    def get_result(self, result_id, version_info):
        """
        """
        pass

    @abstractmethod
    def delete_result(self, result_id, version_info):
        """
        Delete all versions of the result_id
        :param experiment_id: Integer id for the experiment
        """
        pass

    @abstractmethod
    def restore_result(self, result_id, version_info):
        """
        Restore deleted experiment unless it is permanently deleted.
        :param experiment_id: Integer id for the experiment
        """
        pass

    # # See my result_id already contains run_name, exp_hash, user_id, time, tags etc...
    #
    # TODO : I might include this kind of things, it might be a good shortcut instead of looking at input data and params the
    # same way
    # def log_metric(self, run_uuid, metric):
    #     """
    #     Logs a metric for the specified run
    #     :param run_uuid: String id for the run
    #     :param metric: Metric instance to log
    #     """
    #     pass
    #
    # def log_param(self, run_uuid, param):
    #     """
    #     Logs a param for the specified run
    #     :param run_uuid: String id for the run
    #     :param param: Param instance to log
    #     """
    #     pass

    def set_tag(self, result_id, tag):
        """
        Sets a tag for the specified run
        :param run_uuid: String id for the run
        :param tag: RunTag instance to set
        """
        pass

    # @abstractmethod
    # def get_metric(self, run_uuid, metric_key):
    #     """
    #     Returns the last logged value for a given metric.
    #     :param run_uuid: Unique identifier for run
    #     :param metric_key: Metric name within the run
    #     :return: A single float value for the given metric if logged, else None
    #     """
    #     pass
    #
    # @abstractmethod
    # def get_param(self, run_uuid, param_name):
    #     """
    #     Returns the value of the specified parameter.
    #     :param run_uuid: Unique identifier for run
    #     :param param_name: Parameter name within the run
    #     :return: Value of the given parameter if logged, else None
    #     """
    #     pass
    #
    # @abstractmethod
    # def get_metric_history(self, run_uuid, metric_key):
    #     """
    #     Returns all logged value for a given metric.
    #     :param run_uuid: Unique identifier for run
    #     :param metric_key: Metric name within the run
    #     :return: A list of float values logged for the give metric if logged, else empty list
    #     """
    #     pass

    # @abstractmethod
    # def search_runs(self, experiment_ids, search_expressions, run_view_type):
    #     """
    #     Returns runs that match the given list of search expressions within the experiments.
    #     Given multiple search expressions, all these expressions are ANDed together for search.
    #     :param experiment_ids: List of experiment ids to scope the search
    #     :param search_expression: list of search expressions
    #     :return: A list of Run objects that satisfy the search expressions
    #     """
    #     pass
    #
    # @abstractmethod
    # def list_run_infos(self, experiment_id, run_view_type):
    #     """
    #     Returns run information for runs which belong to the experiment_id
    #     :param experiment_id: The experiment id which to search.
    #     :return: A list of RunInfo objects that satisfy the search expressions
    #     """
    #     pass