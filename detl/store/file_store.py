
import os

import uuid
import six

# from mlflow.entities import Experiment, Metric, Param, Run, RunData, RunInfo, RunStatus, RunTag, \
#                             ViewType
from detl.core.run_info import RunInfo
from detl.core.view_type import ViewType
from detl.core.run_tag import RunTag
from detl.core.run_status import RunStatus
from detl.core.run import Run
from detl.core.run_info import check_run_is_active, \
    check_run_is_deleted
from detl.store.store import Store
from detl.store.validation import _validate_metric_name, _validate_param_name, _validate_run_id, \
                                    _validate_tag_name
from detl.core.result_group import ResultGroup, get_result_group_from_name
from detl.utils.env import get_env
from detl.utils.file_utils import (is_directory, list_subdirs, mkdir, exists, write_yaml,
                                     read_yaml, find, read_file_lines, read_file, build_path,
                                     write_to, append_to, make_containing_dirs, mv, get_parent_dir,
                                     list_all)
from detl.utils.mlflow_tags import MLFLOW_RUN_NAME, MLFLOW_PARENT_RUN_ID
from detl.store.store_context import store_context
import yaml

from detl.utils.search_utils import does_run_match_clause

_TRACKING_DIR_ENV_VAR = "MLFLOW_TRACKING_DIR"


def _default_root_dir():
    return get_env(_TRACKING_DIR_ENV_VAR) or os.path.abspath("detl_data")

def write_result_group_to_yaml(res_group):
    #todo
    pass

def write_metadata(meta_file, rg_data):
    with open(meta_file, 'w') as fd:
        yaml.safe_dump(rg_data, fd)


def results_to_dictionary(dict_of_results):
    res = dict_of_results.copy()
    for key in dict_of_results:
        res[key]['runs'] = [run for run in dict_of_results[key]['runs']]

    return res

def runs_to_dictionary(dict_of_runs):
    res = dict_of_runs.copy()
    for uuid, run in dict_of_runs.items():
        res[uuid] = run.to_dictionary()

    return res

def _read_persisted_run_info_dict(run_info_dict):
    dict_copy = run_info_dict.copy()
    if 'lifecycle_stage' not in dict_copy:
        dict_copy['lifecycle_stage'] = RunInfo.ACTIVE_LIFECYCLE
    return RunInfo.from_dictionary(dict_copy)


class FileStore(Store):
    TRASH_FOLDER_NAME = ".trash"
    ARTIFACTS_FOLDER_NAME = "artifacts"
    METRICS_FOLDER_NAME = "metrics"
    PARAMS_FOLDER_NAME = "params"
    TAGS_FOLDER_NAME = "tags"
    META_DATA_FILE_NAME = "meta.yaml"
    RESULT_GROUP_NAME = "_res_group"
    META_DATA_MAIN_FILE = "meta_data.yaml"

    def __init__(self, root_directory=None, artifact_root_uri=None):
        """
        Create a new FileStore with the given root directory and a given default artifact root URI.
        """
        super(FileStore, self).__init__()
        self.root_directory = root_directory or _default_root_dir()
        # TODO : artifact_repository could be an object
        # self.artifact_root_uri = artifact_root_uri or self.root_directory
        self.trash_folder = build_path(self.root_directory, FileStore.TRASH_FOLDER_NAME)
        # Create root directory if needed
        if not exists(self.root_directory):
            mkdir(self.root_directory)
        # Create trash folder if needed
        if not exists(self.trash_folder):
            mkdir(self.trash_folder)
        # There is no need for a default experiment in this implementation because every run will create experiments as
        # needed
        # if not self._has_experiment(experiment_id=Experiment.DEFAULT_EXPERIMENT_ID):
        #     self._create_experiment_with_id(name="Default",
        #                                     experiment_id=Experiment.DEFAULT_EXPERIMENT_ID,
        #                                     artifact_uri=None)

        # The main meta data file contains
        # This will be updated on FileStore teardown
        self.meta_data_main_file = build_path(self.root_directory, FileStore.META_DATA_MAIN_FILE)
        self.trash_meta_data_main_file = build_path(self.trash_folder, FileStore.META_DATA_MAIN_FILE)
        self.cached_meta_data = {}
        if exists(self.meta_data_main_file):
            self.cached_meta_data = self.load_meta_data(self.meta_data_main_file)
            self.res_groups_data = self.cached_meta_data['result_groups']
            self.results_data = self.cached_meta_data['results']
            self.runs_data = self.cached_meta_data['runs']
        self.deleted_cached_meta_data = {}
        if exists(self.trash_meta_data_main_file):
            self.deleted_cached_meta_data = self.load_meta_data(self.trash_meta_data_main_file)

    def tearDown(self):
        # TODO : flush to metadata
        pass

    def write_to_meta_data(self):
        meta_yaml_file = os.path.join(self.root_directory, FileStore.META_DATA_MAIN_FILE)
        write_metadata(meta_yaml_file, {'result_groups': self.res_groups_data, 'results': results_to_dictionary(
            self.results_data), 'runs': runs_to_dictionary(self.runs_data)})

    def load_meta_data(self, meta_data_file):
        with open(meta_data_file, 'r') as fd:
            # TODO : create identities and rgs and runs from the dict
            return yaml.load(fd)

    def _check_root_dir(self):
        """
        Run checks before running directory operations.
        """
        if not exists(self.root_directory):
            raise Exception("'%s' does not exist." % self.root_directory)
        if not is_directory(self.root_directory):
            raise Exception("'%s' is not a directory." % self.root_directory)


    def get_result_path(self, res_group, resuld_id, run_uuid):
        return build_path(self._get_res_group(res_group.namespace, assert_exists=True), resuld_id.id_hash, run_uuid)

    def as_default(self):
        return store_context.get_controller(self)


    # # TODO : should this be commit dependent?
    # def create_saver(self, saver):
    #     self.savers.append(saver)
    def create_res_group(self, rg):
        if self.has_res_group(rg):
            # TODO : do some sort of checking here
            pass
        self.res_groups_data.append(rg)

    def has_res_group(self, rg):
        return self._has_res_group(rg.namespace)

    def _has_res_group(self, namespace):
        return self._get_res_group(namespace) is not None

    def get_res_group(self, experiment_id):
        """
        Fetches the experiment. This will search for active as well as deleted experiments.
        :param experiment_id: Integer id for the experiment
        :return: A single Experiment object if it exists, otherwise raises an Exception.
        """
        return self._get_res_group(experiment_id, assert_exists=True)

    def _get_res_group(self, res_group_namespace, view_type=ViewType.ALL, assert_exists=False):
        # TODO : add distinction for deleted result_groups
        if res_group_namespace in self.cached_meta_data['result_groups']:
            return get_result_group_from_name(res_group_namespace)
            # return get_result_group_from_name(self.cached_meta_data['result_groups'][res_group_namespace])
        # TODO : return a construction of a ResGroup from the dictionary

        if assert_exists:
            raise Exception('Result Group {} does not exist.'.format(res_group_namespace))
        return None

    def list_res_groups(self, view_type=ViewType.ACTIVE_ONLY):

        self._check_root_dir()
        rsl = []
        if view_type == ViewType.ACTIVE_ONLY or view_type == ViewType.ALL:
            rsl += self._get_active_experiments(full_path=False)
        if view_type == ViewType.DELETED_ONLY or view_type == ViewType.ALL:
            rsl += self._get_deleted_experiments(full_path=False)
        # return [self.get_res_group(exp_id) for exp_id in set(rsl)]
        return [exp_id for exp_id in set(rsl)]

    # TODO : sequence
    # First the res groups and savers are created,
    # Savers depend on the hash so the jars or whatever is replaced
    # The identities are created when finished with the preliminary stage
    # The run are created at "run time"
    def create_identity(self, some_identity):
        '''
        An identity is an item in the list, raise an error if there is an identity with the same hash
        :return:
        '''
        # TODO : we should have a corresponding Result Group in the db, if not add it
        if self.has_identity(some_identity.__id_hash__()):
            raise ValueError("Trying to create an identity that already exists")
        self.results_data.append(some_identity)

    def create_run(self, run):
        '''
        An identity is an item in the list, raise an error if there is an identity with the same hash
        :return:
        '''
        # TODO : we should have a corresponding REsult_wrapper in the db, if not add it
        if self.has_run(run.__id_hash__()):
            raise ValueError("Trying to create a run that already exists")
        self.runs_data.append(run)


    # TODO : delete, list, search


    def _get_run_dir(self, result_group_name, run_uuid):
        """
        The run_uuid is made of timestamp + uuid
        :param result_group_name:
        :param run_uuid:
        :return:
        """
        _validate_run_id(run_uuid)
        return build_path(self._get_res_group(result_group_name, assert_exists=True), run_uuid)

    # # TODO : unused for now
    # def _get_metric_path(self, result_group_name, run_uuid, metric_key):
    #     _validate_run_id(run_uuid)
    #     _validate_metric_name(metric_key)
    #     return build_path(self._get_run_dir(result_group_name, run_uuid), FileStore.METRICS_FOLDER_NAME,
    #                       metric_key)
    #
    # # TODO : unused for now
    # def _get_param_path(self, result_group_name, run_uuid, param_name):
    #     _validate_run_id(run_uuid)
    #     _validate_param_name(param_name)
    #     return build_path(self._get_run_dir(result_group_name, run_uuid), FileStore.PARAMS_FOLDER_NAME,
    #                       param_name)
    #
    # def _get_tag_path(self, result_group_name, run_uuid, tag_name):
    #     _validate_run_id(run_uuid)
    #     _validate_tag_name(tag_name)
    #     return build_path(self._get_run_dir(result_group_name, run_uuid), FileStore.TAGS_FOLDER_NAME,
    #                       tag_name)

    # # TODO : unused for now
    # def _get_artifact_dir(self, result_group_name, run_uuid):
    #     _validate_run_id(run_uuid)
    #     artifacts_dir = build_path(self.get_res_group(result_group_name).artifact_location,
    #                                run_uuid,
    #                                FileStore.ARTIFACTS_FOLDER_NAME)
    #     return artifacts_dir
    #
    # def _get_active_experiments(self, full_path=False):
    #     return self.res_groups_data.keys()
    #
    # def _get_deleted_experiments(self, full_path=False):
    #     return self.deleted_cached_meta_data['result_groups'].keys()
    #
    # # TODO
    # def get_results_namespace(self):
    #     pass
    #
    # # TODO
    # def list_results_namespace_and_date(self, nm, before=None, after=None, tags=[], view_tyoe=ViewType.ACTIVE_ONLY):
    #     pass


    #
    #
    # # TODO possibly get rid of this one
    # def get_experiment_by_name(self, name):
    #     self._check_root_dir()
    #     for experiment in self.list_res_groups(ViewType.ALL):
    #         if experiment.name == name:
    #             return experiment
    #     return None
    #
    # def get_result_from_identity(self, result_identity):
    #     return self._get_res_group(result_identity.res_group.namespace)
    #
    # def create_minimal_name(self, result_identity):
    #     return build_path(self.tmp_dir, result_identity.get_short_name())
    #
    # def build_wrapper_from_identity(self, identity):
    #     pass
    #     # TODO
    #
    # def delete_res_group(self, res_group_id):
    #     experiment_dir = self._get_res_group(res_group_id, ViewType.ACTIVE_ONLY)
    #     if experiment_dir is None:
    #         raise Exception("Could not find experiment with ID %s" % res_group_id)
    #     mv(experiment_dir, self.trash_folder)
    #
    # def delete_run(self, run_id):
    #     run_info = self._get_run_info(run_id)
    #     check_run_is_active(run_info)
    #     new_info = run_info._copy_with_overrides(lifecycle_stage=RunInfo.DELETED_LIFECYCLE)
    #     self._overwrite_run_info(new_info)
    #
    # def _find_experiment_folder(self, run_path):
    #     """
    #     Given a run path, return the parent directory for its experiment.
    #     """
    #     parent = get_parent_dir(run_path)
    #     if os.path.basename(parent) == FileStore.TRASH_FOLDER_NAME:
    #         return get_parent_dir(parent)
    #     return parent
    #
    # def _find_run_root(self, run_uuid):
    #     _validate_run_id(run_uuid)
    #     self._check_root_dir()
    #     all_experiments = self._get_active_experiments(True) + self._get_deleted_experiments(True)
    #     for experiment_dir in all_experiments:
    #         runs = [res for res in experiment_dir.results if res.uuid == run_uuid]
    #         if len(runs) == 0:
    #             continue
    #         return runs[0]
    #     return None
    #
    # def update_run_info(self, run_uuid, run_status, end_time):
    #     _validate_run_id(run_uuid)
    #     run_info = self.get_run(run_uuid).info
    #     check_run_is_active(run_info)
    #     new_info = run_info._copy_with_overrides(run_status, end_time)
    #     self._overwrite_run_info(new_info)
    #     return new_info
    #
    # def create_run(self, experiment_id, user_id, run_name, source_type,
    #                source_name, entry_point_name, start_time, source_version, tags, parent_run_id):
    #     """
    #     Creates a run with the specified attributes.
    #     """
    #     experiment = self.get_res_group(experiment_id)
    #     if experiment is None:
    #         raise Exception("Could not create run under experiment with ID %s - no such experiment "
    #                         "exists." % experiment_id)
    #     if experiment.lifecycle_stage != ResultGroup.ACTIVE_LIFECYCLE:
    #         raise Exception('Could not create run under non-active experiment with ID '
    #                         '%s.' % experiment_id)
    #     run_uuid = uuid.uuid4().hex
    #     artifact_uri = self._get_artifact_dir(experiment_id, run_uuid)
    #     run_info = RunInfo(run_uuid=run_uuid, experiment_id=experiment_id,
    #                        name="",
    #                        artifact_uri=artifact_uri, source_type=source_type,
    #                        source_name=source_name,
    #                        entry_point_name=entry_point_name, user_id=user_id,
    #                        status=RunStatus.RUNNING, start_time=start_time, end_time=None,
    #                        source_version=source_version, lifecycle_stage=RunInfo.ACTIVE_LIFECYCLE)
    #     # Persist run metadata and create directories for logging metrics, parameters, artifacts
    #     run_dir = self._get_run_dir(run_info.experiment_id, run_info.run_uuid)
    #     mkdir(run_dir)
    #     write_yaml(run_dir, FileStore.META_DATA_FILE_NAME, dict(run_info))
    #     mkdir(run_dir, FileStore.METRICS_FOLDER_NAME)
    #     mkdir(run_dir, FileStore.PARAMS_FOLDER_NAME)
    #     mkdir(run_dir, FileStore.ARTIFACTS_FOLDER_NAME)
    #     for tag in tags:
    #         self.set_tag(run_uuid, tag)
    #     if parent_run_id:
    #         self.set_tag(run_uuid, RunTag(key=MLFLOW_PARENT_RUN_ID, value=parent_run_id))
    #     if run_name:
    #         self.set_tag(run_uuid, RunTag(key=MLFLOW_RUN_NAME, value=run_name))
    #     return Run(run_info=run_info, run_data=None)
    #
    # def _make_experiment_dict(self, experiment):
    #     # Don't persist lifecycle_stage since it's inferred from the ".trash" folder.
    #     experiment_dict = dict(experiment)
    #     del experiment_dict['lifecycle_stage']
    #     return experiment_dict
    #
    # def get_run(self, run_uuid):
    #     """
    #     Will get both active and deleted runs.
    #     """
    #     _validate_run_id(run_uuid)
    #     run_info = self._get_run_info(run_uuid)
    #     # metrics = self.get_all_metrics(run_uuid)
    #     # params = self.get_all_params(run_uuid)
    #     # tags = self.get_all_tags(run_uuid)
    #     return run_info
    #     # Run(run_info, RunData(metrics, params, tags))
    #
    # def _get_run_info(self, run_uuid):
    #     """
    #     Will get both active and deleted runs.
    #     """
    #     run_dir = self._find_run_root(run_uuid)
    #     if run_dir is not None:
    #         return run_dir
    #         # meta = read_yaml(run_dir, FileStore.META_DATA_FILE_NAME)
    #         # return _read_persisted_run_info_dict(meta)
    #     raise Exception("Run '%s' not found" % run_uuid)
    #
    # def _get_run_files(self, run_uuid, resource_type):
    #     _validate_run_id(run_uuid)
    #     if resource_type == "metric":
    #         subfolder_name = FileStore.METRICS_FOLDER_NAME
    #     elif resource_type == "param":
    #         subfolder_name = FileStore.PARAMS_FOLDER_NAME
    #     elif resource_type == "tag":
    #         subfolder_name = FileStore.TAGS_FOLDER_NAME
    #     else:
    #         raise Exception("Looking for unknown resource under run.")
    #     run_dir = self._find_run_root(run_uuid)
    #     return run_dir, []
    #     # if run_dir is None:
    #     #     raise Exception("Run '%s' not found" % run_uuid)
    #     # source_dirs = find(run_dir, subfolder_name, full_path=True)
    #     # if len(source_dirs) == 0:
    #     #     return run_dir, []
    #     # file_names = []
    #     # for root, _, files in os.walk(source_dirs[0]):
    #     #     for name in files:
    #     #         abspath = os.path.join(root, name)
    #     #         file_names.append(os.path.relpath(abspath, source_dirs[0]))
    #     # return source_dirs[0], file_names
    #
    # # @staticmethod
    # # def _get_metric_from_file(parent_path, metric_name):
    # #     _validate_metric_name(metric_name)
    # #     metric_data = read_file_lines(parent_path, metric_name)
    # #     if len(metric_data) == 0:
    # #         raise Exception("Metric '%s' is malformed. No data found." % metric_name)
    # #     last_line = metric_data[-1]
    # #     timestamp, val = last_line.strip().split(" ")
    # #     return Metric(metric_name, float(val), int(timestamp))
    # #
    # # def get_metric(self, run_uuid, metric_key):
    # #     _validate_run_id(run_uuid)
    # #     _validate_metric_name(metric_key)
    # #     parent_path, metric_files = self._get_run_files(run_uuid, "metric")
    # #     if metric_key not in metric_files:
    # #         raise Exception("Metric '%s' not found under run '%s'" % (metric_key, run_uuid))
    # #     return self._get_metric_from_file(parent_path, metric_key)
    # #
    # # def get_all_metrics(self, run_uuid):
    # #     _validate_run_id(run_uuid)
    # #     parent_path, metric_files = self._get_run_files(run_uuid, "metric")
    # #     metrics = []
    # #     for metric_file in metric_files:
    # #         metrics.append(self._get_metric_from_file(parent_path, metric_file))
    # #     return metrics
    # #
    # # def get_metric_history(self, run_uuid, metric_key):
    # #     _validate_run_id(run_uuid)
    # #     _validate_metric_name(metric_key)
    # #     parent_path, metric_files = self._get_run_files(run_uuid, "metric")
    # #     if metric_key not in metric_files:
    # #         raise Exception("Metric '%s' not found under run '%s'" % (metric_key, run_uuid))
    # #     metric_data = read_file_lines(parent_path, metric_key)
    # #     rsl = []
    # #     for pair in metric_data:
    # #         ts, val = pair.strip().split(" ")
    # #         rsl.append(Metric(metric_key, float(val), int(ts)))
    # #     return rsl
    # #
    # # @staticmethod
    # # def _get_param_from_file(parent_path, param_name):
    # #     _validate_param_name(param_name)
    # #     param_data = read_file_lines(parent_path, param_name)
    # #     if len(param_data) == 0:
    # #         raise Exception("Param '%s' is malformed. No data found." % param_name)
    # #     if len(param_data) > 1:
    # #         raise Exception("Unexpected data for param '%s'. Param recorded more than once"
    # #                         % param_name)
    # #     return Param(param_name, str(param_data[0].strip()))
    # #
    # # @staticmethod
    # # def _get_tag_from_file(parent_path, tag_name):
    # #     _validate_tag_name(tag_name)
    # #     tag_data = read_file(parent_path, tag_name)
    # #     return RunTag(tag_name, tag_data)
    # #
    # # def get_param(self, run_uuid, param_name):
    # #     _validate_run_id(run_uuid)
    # #     _validate_param_name(param_name)
    # #     parent_path, param_files = self._get_run_files(run_uuid, "param")
    # #     if param_name not in param_files:
    # #         raise Exception("Param '%s' not found under run '%s'" % (param_name, run_uuid))
    # #     return self._get_param_from_file(parent_path, param_name)
    # #
    # # def get_all_params(self, run_uuid):
    # #     parent_path, param_files = self._get_run_files(run_uuid, "param")
    # #     params = []
    # #     for param_file in param_files:
    # #         params.append(self._get_param_from_file(parent_path, param_file))
    # #     return params
    # #
    # # def get_all_tags(self, run_uuid):
    # #     parent_path, tag_files = self._get_run_files(run_uuid, "tag")
    # #     tags = []
    # #     for tag_file in tag_files:
    # #         tags.append(self._get_tag_from_file(parent_path, tag_file))
    # #     return tags
    #
    # def _list_run_uuids(self, res_group_name, run_view_type):
    #     self._check_root_dir()
    #     res_group = self._get_res_group(res_group_name, assert_exists=True)
    #     run_uuids = res_group.results
    #     if run_view_type == ViewType.ALL:
    #         return run_uuids
    #     elif run_view_type == ViewType.ACTIVE_ONLY:
    #         return [r_id for r_id in run_uuids
    #                 if self._get_run_info(r_id).lifecycle_stage == RunInfo.ACTIVE_LIFECYCLE]
    #     else:
    #         return [r_id for r_id in run_uuids
    #                 if self._get_run_info(r_id).lifecycle_stage == RunInfo.DELETED_LIFECYCLE]
    #
    # def search_runs(self, experiment_ids, search_expressions, run_view_type):
    #     run_uuids = []
    #     if len(search_expressions) == 0:
    #         for experiment_id in experiment_ids:
    #             run_uuids.extend(self._list_run_uuids(experiment_id, run_view_type))
    #     else:
    #         for experiment_id in experiment_ids:
    #             for run_uuid in self._list_run_uuids(experiment_id, run_view_type):
    #                 run = self.get_run(run_uuid)
    #                 if all([does_run_match_clause(run, s) for s in search_expressions]):
    #                     run_uuids.append(run_uuid)
    #     return [self.get_run(run_uuid) for run_uuid in run_uuids]
    #
    # def list_run_infos(self, experiment_id, run_view_type):
    #     run_infos = []
    #     for run_uuid in self._list_run_uuids(experiment_id, run_view_type):
    #         run_infos.append(self._get_run_info(run_uuid))
    #     return run_infos
    #
    # # def log_metric(self, run_uuid, metric):
    # #     _validate_run_id(run_uuid)
    # #     _validate_metric_name(metric.key)
    # #     run = self.get_run(run_uuid)
    # #     check_run_is_active(run.info)
    # #     metric_path = self._get_metric_path(run.info.experiment_id, run_uuid, metric.key)
    # #     make_containing_dirs(metric_path)
    # #     append_to(metric_path, "%s %s\n" % (metric.timestamp, metric.value))
    #
    # # def _writeable_value(self, tag_value):
    # #     if tag_value is None:
    # #         return ""
    # #     elif isinstance(tag_value, six.string_types):
    # #         return tag_value
    # #     else:
    # #         return "%s" % tag_value
    # #
    # # def log_param(self, run_uuid, param):
    # #     _validate_run_id(run_uuid)
    # #     _validate_param_name(param.key)
    # #     run = self.get_run(run_uuid)
    # #     check_run_is_active(run.info)
    # #     param_path = self._get_param_path(run.info.experiment_id, run_uuid, param.key)
    # #     make_containing_dirs(param_path)
    # #     write_to(param_path, self._writeable_value(param.value))
    # #
    # # def set_tag(self, run_uuid, tag):
    # #     _validate_run_id(run_uuid)
    # #     _validate_tag_name(tag.key)
    # #     run = self.get_run(run_uuid)
    # #     check_run_is_active(run.info)
    # #     tag_path = self._get_tag_path(run.info.experiment_id, run_uuid, tag.key)
    # #     make_containing_dirs(tag_path)
    # #     # Don't add trailing newline
    # #     write_to(tag_path, self._writeable_value(tag.value))
    #
    # # def _overwrite_run_info(self, run_info):
    # #     run_dir = self._get_run_dir(run_info.experiment_id, run_info.run_uuid)
    # #     run_info_dict = _make_persisted_run_info_dict(run_info)
    # #     write_yaml(run_dir, FileStore.META_DATA_FILE_NAME, run_info_dict, overwrite=True)
    # #