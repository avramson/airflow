import datetime
import imp
import multiprocessing
import multiprocessing.dummy
import os
import queue
import sys
import traceback

from airflow.exceptions import AirflowException
from airflow.utils.logging import LoggingMixin
from airflow.utils.timeout import timeout



class DagFileParseResult():
    def __init__(self, file_path, dag_ids, exception_info=None):
        self.file_path = file_path
        self.dag_ids = dag_ids
        if exception_info is not None:
            self.exception_trace = "".join(
                traceback.format_exception(*exception_info))
        else:
            self.exception_trace = None

class DagParser(LoggingMixin):
    def __init__(self, file_load_timeout):
        self.file_load_timeout = file_load_timeout
        self.file_last_changed = {}
        self.import_errors = {}

    def get_dags_in_file(self, dag_ids, filepath, only_if_updated=True, safe_mode=True):
        found_dags = []

        if len(dag_ids) == 0:
            return found_dags

        # todo: raise exception?
        if not os.path.isfile(filepath):
            return found_dags

        try:
            # This failed before in what may have been a git sync
            # race condition
            dttm = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))
            if only_if_updated \
                    and filepath in self.file_last_changed \
                    and dttm == self.file_last_changed[filepath]:
                return found_dags

        except Exception as e:
            self.logger.exception(e)
            return found_dags

        mods = []
        if safe_mode and os.path.isfile(filepath):
            with open(filepath, 'rb') as f:
                content = f.read()
                if not all([s in content for s in (b'DAG', b'airflow')]):
                    return found_dags

        self.logger.debug("Importing {}".format(filepath))
        org_mod_name, file_ext = os.path.splitext(os.path.split(filepath)[-1])
        mod_name = 'unusual_prefix_' + org_mod_name

        if mod_name in sys.modules:
            del sys.modules[mod_name]

        with timeout(self.file_load_timeout):
            try:
                # TODO: Do we need this log line?
                # self.logger.info("Importing {} {}".format(mod_name, filepath))
                m = imp.load_source(mod_name, filepath)
                mods.append(m)
            except Exception as e:
                self.logger.exception("Failed to import: " + filepath)
                # TODO: Handle import errors
                self.import_errors[filepath] = str(e)
                self.file_last_changed[filepath] = dttm
                return found_dags

        for m in mods:
            for dag in list(m.__dict__.values()):
                # TODO: Refactor models and put them into separate files so that we can import DAG and do instanceof
                if dag.__class__.__name__ == 'DAG':
                    if not dag.full_filepath:
                        dag.full_filepath = filepath
                    dag.is_subdag = False
                    dag.module_name = m.__name__
                    # self.bag_dag(dag, parent_dag=dag, root_dag=dag)
                    # self._add_dags([dag])
                    # subdags = self._get_all_subdags(dag)
                    # self._add_dags(subdags)
                    if (dag.dag_id in dag_ids):
                        found_dags.append(dag)
                    else:
                        self.logger.error("Skipping DAG ID {} as it was not requested from {}"
                                          .format(dag.dag_id, filepath))
                    # TODO: Why does found dags only include the immediate
                    # subdags, but not the subdags of the subdags?
                    # found_dags += dag.subdags

        self.file_last_changed[filepath] = dttm
        return found_dags

    def get_dags_ids_in_file(self, filepath, only_if_updated=True, safe_mode=True):
        transfer_queue = multiprocessing.Queue()
        process = multiprocessing.Process(target=self.parse_file_for_dag_ids,
                                          args=(transfer_queue, filepath, only_if_updated, safe_mode))
        self.logger.info("Starting process to get DAGs from {}".format(filepath))
        process.start()
        found_dag_ids = []

        try:
            result = transfer_queue.get(block=True, timeout=self.file_load_timeout)
            if not result.exception_trace:
                found_dag_ids += result.dag_ids
            else:
                self.logger.error("Error parsing {} - Got exception:\n{}".format(
                    filepath,
                    result.exception_trace))

        except queue.Empty:
            self.logger.error("Timed out while trying to get DAGs IDs from {}".format(filepath))
        process.join()
        self.logger.info("Finished process to get DAG IDs from {}".format(filepath))

        return found_dag_ids

    def parse_file_for_dag_ids_through_queue(self, queue, file_path, only_if_updated=True, safe_mode=True):
        result = self.parse_file_for_dag_ids(file_path, only_if_updated, safe_mode)
        queue.put(result)

    def parse_file_for_dag_ids(self, file_path, only_if_updated=True, safe_mode=True):
        """Given a path to a python module, this method loads the file and returns
        a list of ID's that are in the file.

        :param queue:
        :param file_path:
        :param only_if_updated:
        :param safe_mode:
        :return:
        """
        # List of DAG ids in the file
        found_dags_ids = []
        empty_result = DagFileParseResult(file_path, [])
        # todo: raise exception?
        if not os.path.isfile(file_path):
            raise AirflowException("{} is not a file".format(file_path))

        # This failed before in what may have been a git sync
        # race condition
        dttm = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        if only_if_updated \
                and file_path in self.file_last_changed \
                and dttm == self.file_last_changed[file_path]:
            return empty_result

        mods = []
        if safe_mode and os.path.isfile(file_path):
            with open(file_path, 'rb') as f:
                content = f.read()
                if not all([s in content for s in (b'DAG', b'airflow')]):
                    return empty_result

        self.logger.debug("Importing {}".format(file_path))
        org_mod_name, file_ext = os.path.splitext(os.path.split(file_path)[-1])
        mod_name = 'unusual_prefix_' + org_mod_name

        if mod_name in sys.modules:
            del sys.modules[mod_name]

        with timeout(self.file_load_timeout):
            self.logger.info("Importing {} {}".format(mod_name, file_path))
            try:
                m = imp.load_source(mod_name, file_path)
                mods.append(m)
            except:
                return DagFileParseResult(file_path, [], exception_info=sys.exc_info())

        for m in mods:
            for dag in list(m.__dict__.values()):
                # TODO: Refactor models and put them into separate files so that we can import DAG and do instanceof
                if dag.__class__.__name__ == 'DAG':
                    if not dag.full_filepath:
                        dag.full_filepath = file_path
                    dag.is_subdag = False
                    dag.module_name = m.__name__
                    # self.bag_dag(dag, parent_dag=dag, root_dag=dag)
                    # self._add_dags([dag])
                    # subdags = self._get_all_subdags(dag)
                    # self._add_dags(subdags)
                    self.logger.info("Adding dag_id {}".format(dag.dag_id))
                    found_dags_ids.append(dag.dag_id)
                    # TODO: Why does found dags only include the immediate
                    # subdags, but not the subdags of the subdags?
                    # found_dags += dag.subdags

        self.file_last_changed[file_path] = dttm
        return DagFileParseResult(file_path, found_dags_ids)

    def _get_dags_in_file_helper(self, queue, filepath, only_if_updated=True, safe_mode=True):
        """Given a path to a python module, this method loads the file and returns
        a list of ID's that are in the file.

        :param queue:
        :param filepath:
        :param only_if_updated:
        :param safe_mode:
        :return:
        """
        found_dags = []

        # todo: raise exception?
        if not os.path.isfile(filepath):
            return found_dags

        try:
            # This failed before in what may have been a git sync
            # race condition
            dttm = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))
            if only_if_updated \
                    and filepath in self.file_last_changed \
                    and dttm == self.file_last_changed[filepath]:
                return found_dags

        except Exception as e:
            self.logger.exception(e)
            return

        mods = []
        if safe_mode and os.path.isfile(filepath):
            with open(filepath, 'rb') as f:
                content = f.read()
                if not all([s in content for s in (b'DAG', b'airflow')]):
                    return

        self.logger.debug("Importing {}".format(filepath))
        org_mod_name, file_ext = os.path.splitext(os.path.split(filepath)[-1])
        mod_name = 'unusual_prefix_' + org_mod_name

        if mod_name in sys.modules:
            del sys.modules[mod_name]

        with timeout(self.file_load_timeout):
            try:
                self.logger.info("Importing {} {}".format(mod_name, filepath))
                m = imp.load_source(mod_name, filepath)
                mods.append(m)
            except Exception as e:
                self.logger.exception("Failed to import: " + filepath)
                # TODO: Handle import errors
                self.import_errors[filepath] = str(e)
                self.file_last_changed[filepath] = dttm
                queue.put(found_dags)
                return

        for m in mods:
            for dag in list(m.__dict__.values()):
                # TODO: Refactor models and put them into separate files so that we can import DAG and do instanceof
                if dag.__class__.__name__ == 'DAG':
                    if not dag.full_filepath:
                        dag.full_filepath = filepath
                    dag.is_subdag = False
                    dag.module_name = m.__name__
                    # self.bag_dag(dag, parent_dag=dag, root_dag=dag)
                    # self._add_dags([dag])
                    # subdags = self._get_all_subdags(dag)
                    # self._add_dags(subdags)
                    found_dags.append(dag)
                    # TODO: Why does found dags only include the immediate
                    # subdags, but not the subdags of the subdags?
                    # found_dags += dag.subdags

        self.file_last_changed[filepath] = dttm

        queue.put(found_dags)

import logging

def parse_file_for_dag_ids(file_path,
                           safe_mode=True,
                           file_load_timeout=120):
    """Given a path to a python module, this method loads the file and returns
    a list of ID's that are in the file.

    :param queue:
    :param file_path:
    :param only_if_updated:
    :param safe_mode:
    :return:
    """
    logging.info("Parsing {}".format(file_path))
    # List of DAG ids in the file
    found_dags_ids = []
    empty_result = DagFileParseResult(file_path, [])
    # todo: raise exception?
    if not os.path.isfile(file_path):
        raise AirflowException("{} is not a file".format(file_path))

    mods = []
    if safe_mode and os.path.isfile(file_path):
        with open(file_path, 'rb') as f:
            content = f.read()
            if not all([s in content for s in (b'DAG', b'airflow')]):
                return empty_result

    org_mod_name, file_ext = os.path.splitext(os.path.split(file_path)[-1])
    mod_name = 'unusual_prefix_' + org_mod_name

    if mod_name in sys.modules:
        del sys.modules[mod_name]

    with timeout(file_load_timeout):
        try:
            m = imp.load_source(mod_name, file_path)
            mods.append(m)
        except:
            return DagFileParseResult(file_path, [], exception_info=sys.exc_info())

    for m in mods:
        for dag in list(m.__dict__.values()):
            # TODO: Refactor models and put them into separate files so that we can import DAG and do instanceof
            if dag.__class__.__name__ == 'DAG':
                if not dag.full_filepath:
                    dag.full_filepath = file_path
                dag.is_subdag = False
                dag.module_name = m.__name__
                # self.bag_dag(dag, parent_dag=dag, root_dag=dag)
                # self._add_dags([dag])
                # subdags = self._get_all_subdags(dag)
                # self._add_dags(subdags)
                found_dags_ids.append(dag.dag_id)
                # TODO: Why does found dags only include the immediate
                # subdags, but not the subdags of the subdags?
                # found_dags += dag.subdags

    return DagFileParseResult(file_path, found_dags_ids)

    return parse_file_for_dag_ids

class ParallelDagParser(LoggingMixin):
    def __init__(self, file_load_timeout, parallelism):
        self.file_load_timeout = file_load_timeout
        self.parallelism = parallelism

    def _parse_one_file(self, file_path):
        dag_parser = DagParser(self.file_load_timeout)
        return dag_parser.parse_file_for_dag_ids()

    def parse_files_for_dag_ids(self, file_paths):

        pool = multiprocessing.Pool(self.parallelism)
        dag_parser = DagParser(self.file_load_timeout)
        parse_results = pool.map(parse_file_for_dag_ids, file_paths)


        # parse_results = []
        # transfer_queue = multiprocessing.Queue()
        # dag_parser = DagParser(self.file_load_timeout)
        #
        # for file_path in file_paths:
        #     process = multiprocessing.Process(target=dag_parser.parse_file_for_dag_ids_through_queue,
        #                                       args=(transfer_queue, file_path))
        #     process.start()
        #     result = transfer_queue.get(block=True, timeout=self.file_load_timeout)
        #     parse_results.append(result)
        #     process.join()


        return parse_results