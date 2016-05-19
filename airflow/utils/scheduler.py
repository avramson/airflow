# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import defaultdict

import datetime
import logging
import os
import re
from airflow.exceptions import AirflowException

class SimpleDag(object):
    def __init__(self,
                 dag_id,
                 task_ids,
                 full_file_path,
                 task_concurrency_limit,
                 is_paused,
                 pickle_id):
        self.dag_id = dag_id
        self.task_ids = task_ids
        self.full_file_path = full_file_path
        self.is_paused = is_paused
        self.task_concurrency_limit = task_concurrency_limit
        self.pickle_id = pickle_id


class SimpleDagBag(object):
    def __init__(self, simple_dags):
        self.simple_dags = simple_dags
        self.dag_id_to_simple_dag = {}

        for simple_dag in simple_dags:
            self.dag_id_to_simple_dag[simple_dag.dag_id] = simple_dag

    def get_dag_ids(self):
        return self.dag_id_to_simple_dag.keys()

    def is_paused(self, dag_id):
        if dag_id not in self.dag_id_to_simple_dag:
            raise AirflowException("Unknown DAG ID {}".format(dag_id))
        return self.dag_id_to_simple_dag[dag_id].is_paused

    def get_task_ids(self, dag_id):
        if dag_id not in self.dag_id_to_simple_dag:
            raise AirflowException("Unknown DAG ID {}".format(dag_id))
        return self.dag_id_to_simple_dag[dag_id].task_ids

    def get_full_file_path(self, dag_id):
        if dag_id not in self.dag_id_to_simple_dag:
            raise AirflowException("Unknown DAG ID {}".format(dag_id))
        return self.dag_id_to_simple_dag[dag_id].full_file_path

    def get_task_concurrency_limit(self, dag_id):
        if dag_id not in self.dag_id_to_simple_dag:
            raise AirflowException("Unknown DAG ID {}".format(dag_id))
        return self.dag_id_to_simple_dag[dag_id].task_concurrency_limit

    def get_pickle_id(self, dag_id):
        if dag_id not in self.dag_id_to_simple_dag:
            raise AirflowException("Unknown DAG ID {}".format(dag_id))
        return self.dag_id_to_simple_dag[dag_id].pickle_id

    def exists(self, dag_id, task_id):
        if dag_id not in self.dag_id_to_simple_dag:
            return False;
        return task_id in self.dag_id_to_simple_dag[dag_id].task_ids

    def dag_count(self):
        return len(self.dag_id_to_simple_dag)


class DagFileProcessingStats(object):

    def __init__(self):
        # Map from the file path to various stats
        self._processed_counts = defaultdict(int)
        self._last_finished_time = {}
        self._processor_pid = {}
        self._processor_start_time = {}
        self._last_runtime= {}

    def processing_started(self, file_path, pid):
        if file_path in self._processor_pid:
            raise AirflowException("{} is already being processed by {}"
                                   .format(file_path, pid))
        self._processor_pid[file_path] = pid
        self._processor_start_time[file_path] = datetime.now()

    def processing_finished(self, file_path):
        if file_path not in self._processor_pid:
            raise AirflowException("{} is not known to be processing"
                                   .format(file_path))
        now = datetime.now()
        self._last_runtime[file_path] = \
            (now - self._processor_start_time[file_path]).total_seconds()
        del self._processor_pid[file_path]
        del self._processor_start_time[file_path]
        self._last_finished_time[file_path] = now
        self._processed_counts[file_path] += 1

    def get_current_pid(self, file_path):
        return self._processor_pid.get(file_path)

    def get_start_time(self, file_path):
        return self._processor_start_time.get(file_path)

    def get_last_finish_time(self, file_path):
        return self._last_finished_time.get(file_path)

    def get_last_runtime(self, file_path):
        return self._last_runtime.get(file_path)

    def get_processed_count(self, file_path):
        return self._processed_counts.get(file_path)


def list_py_file_paths(directory):
    file_paths = []
    if os.path.isfile(directory):
        return [directory]
    elif os.path.isdir(directory):
        patterns = []
        for root, dirs, files in os.walk(directory, followlinks=True):
            ignore_file = [f for f in files if f == '.airflowignore']
            if ignore_file:
                f = open(os.path.join(root, ignore_file[0]), 'r')
                patterns += [p for p in f.read().split('\n') if p]
                f.close()
            for f in files:
                try:
                    file_path = os.path.join(root, f)
                    if not os.path.isfile(file_path):
                        continue
                    mod_name, file_ext = os.path.splitext(
                        os.path.split(file_path)[-1])
                    if file_ext != '.py':
                        continue
                    if not any(
                            [re.findall(p, file_path) for p in patterns]):
                        file_paths.append(file_path)
                except Exception as e:
                    logging.exception("Error while examining {}".format(f))
    return file_paths