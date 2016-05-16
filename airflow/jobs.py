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

from past.builtins import basestring
from collections import defaultdict, Counter
from datetime import datetime
from datetime import timedelta
from itertools import product

import getpass
import logging
import socket
import subprocess
import multiprocessing
import multiprocessing.dummy
import math
import os
import time
from time import sleep

from sqlalchemy import Column, Integer, String, DateTime, func, Index, or_
from sqlalchemy.orm.session import make_transient
from tabulate import tabulate

from airflow import executors, models, settings
from airflow import configuration as conf
from airflow.exceptions import AirflowException
from airflow.executors import DEFAULT_EXECUTOR
from airflow.utils.state import State
from airflow.utils.db import provide_session, pessimistic_connection_handling
from airflow.utils.email import send_email
from airflow.utils.logging import LoggingMixin
from airflow.utils import asciiart
from airflow.utils import dag_utils

Base = models.Base
ID_LEN = models.ID_LEN
Stats = settings.Stats

# Adding signal handler for debugging
import threading, sys, traceback

def dumpstacks(signal, frame):
    print("Dumping stacktrace")
    id2name = dict([(th.ident, th.name) for th in threading.enumerate()])
    code = []
    for threadId, stack in sys._current_frames().items():
        code.append("\n# Thread: %s(%d)" % (id2name.get(threadId,""), threadId))
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
            if line:
                code.append("  %s" % (line.strip()))
    print("\n".join(code))

import signal
signal.signal(signal.SIGQUIT, dumpstacks)
# End adding signal handler

class BaseJob(Base, LoggingMixin):
    """
    Abstract class to be derived for jobs. Jobs are processing items with state
    and duration that aren't task instances. For instance a BackfillJob is
    a collection of task instance runs, but should have it's own state, start
    and end time.
    """

    __tablename__ = "job"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN),)
    state = Column(String(20))
    job_type = Column(String(30))
    start_date = Column(DateTime())
    end_date = Column(DateTime())
    latest_heartbeat = Column(DateTime())
    executor_class = Column(String(500))
    hostname = Column(String(500))
    unixname = Column(String(1000))

    __mapper_args__ = {
        'polymorphic_on': job_type,
        'polymorphic_identity': 'BaseJob'
    }

    __table_args__ = (
        Index('job_type_heart', job_type, latest_heartbeat),
    )

    def __init__(
            self,
            executor=executors.DEFAULT_EXECUTOR,
            heartrate=conf.getfloat('scheduler', 'JOB_HEARTBEAT_SEC'),
            *args, **kwargs):
        self.hostname = socket.getfqdn()
        self.executor = executor
        self.executor_class = executor.__class__.__name__
        self.start_date = datetime.now()
        self.latest_heartbeat = datetime.now()
        self.heartrate = heartrate
        self.unixname = getpass.getuser()
        super(BaseJob, self).__init__(*args, **kwargs)

    def is_alive(self):
        return (
            (datetime.now() - self.latest_heartbeat).seconds <
            (conf.getint('scheduler', 'JOB_HEARTBEAT_SEC') * 2.1)
        )

    def kill(self):
        session = settings.Session()
        job = session.query(BaseJob).filter(BaseJob.id == self.id).first()
        job.end_date = datetime.now()
        try:
            self.on_kill()
        except:
            self.logger.error('on_kill() method failed')
        session.merge(job)
        session.commit()
        session.close()
        raise AirflowException("Job shut down externally.")

    def on_kill(self):
        '''
        Will be called when an external kill command is received
        '''
        pass

    def heartbeat_callback(self):
        pass

    def heartbeat(self):
        '''
        Heartbeats update the job's entry in the database with a timestamp
        for the latest_heartbeat and allows for the job to be killed
        externally. This allows at the system level to monitor what is
        actually active.

        For instance, an old heartbeat for SchedulerJob would mean something
        is wrong.

        This also allows for any job to be killed externally, regardless
        of who is running it or on which machine it is running.

        Note that if your heartbeat is set to 60 seconds and you call this
        method after 10 seconds of processing since the last heartbeat, it
        will sleep 50 seconds to complete the 60 seconds and keep a steady
        heart rate. If you go over 60 seconds before calling it, it won't
        sleep at all.
        '''
        session = settings.Session()
        job = session.query(BaseJob).filter(BaseJob.id == self.id).first()

        if job.state == State.SHUTDOWN:
            self.kill()

        if job.latest_heartbeat:
            sleep_for = self.heartrate - (
                datetime.now() - job.latest_heartbeat).total_seconds()
            if sleep_for > 0:
                sleep(sleep_for)

        job.latest_heartbeat = datetime.now()

        session.merge(job)
        session.commit()
        session.close()

        self.heartbeat_callback()
        self.logger.debug('[heart] Boom.')

    def run(self):
        Stats.incr(self.__class__.__name__.lower()+'_start', 1, 1)
        # Adding an entry in the DB
        session = settings.Session()
        self.state = State.RUNNING
        session.add(self)
        session.commit()
        id_ = self.id
        make_transient(self)
        self.id = id_

        # Run
        self._execute()

        # Marking the success in the DB
        self.end_date = datetime.now()
        self.state = State.SUCCESS
        session.merge(self)
        session.commit()
        session.close()

        Stats.incr(self.__class__.__name__.lower()+'_end', 1, 1)

    def _execute(self):
        raise NotImplementedError("This method needs to be overridden")


class TaskExecutionInfo(object):
    def __init__(self, key, command, priority, queue):
        self.key = key
        self.command = command
        self.priority = priority
        self.queue = queue


class ProcessDagFileResult(object):
    def __init__(self, simple_dag_bag, task_execution_infos):
        self.simple_dag_bag = simple_dag_bag
        self.task_execution_infos = task_execution_infos


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


# def _old_launch_process_to_process_file(thread_name,
#                                    args):
#
#     def helper(h_result_queue, h_file_path, h_pickle_dags):
#         added_to_queue = False
#         try:
#             threading.current_thread().name = thread_name
#             start_time = time.time()
#             scheduler_job = SchedulerJob()
#             reload(settings)
#
#             # TODO: Put here to prevent GC?
#             result = scheduler_job.process_dags_in_one_file(h_file_path,
#                                                        h_pickle_dags)
#             h_result_queue.put(result)
#             added_to_queue = True
#             end_time = time.time()
#             logging.info("Processing {} took {:.3f} seconds"
#                          .format(h_file_path,
#                                  end_time - start_time))
#         finally:
#             # Returning something in the queue is how the parent proecss knows
#             # that this process is done
#             if not added_to_queue:
#                 logging.info("Putting none into queue as no value was returned")
#                 h_result_queue.put(None)
#
#     p = multiprocessing.Process(target=helper, args=args)
#     p.name = "{}-Process".format(thread_name)
#     # Set it to daemon so that we try to kill the process on exit
#     p.daemon = True
#     p.start()
#     return p


def launch_process_to_process_file(result_queue,
                                   file_path,
                                   pickle_dags,
                                   thread_name,
                                   log_file):
    def helper(h_result_queue, h_file_path, h_pickle_dags, h_log_file):
        added_to_queue = False
        try:
            # Re-direct stdout and stderr to the log file for this
            # child process. No buffering to enable responsive file tailing
            parent_dir, file = os.path.split(h_log_file)
            try:
                os.makedirs(parent_dir)
            except os.error as e:
                # Throws error when directly already exists
                pass

            with open(h_log_file, "a", buffering=0) as f:
                sys.stdout = f
                sys.stderr = sys.stdout

                threading.current_thread().name = thread_name
                start_time = time.time()

                reload(settings)

                print("Process(PID={}) working on {}".format(os.getpid(), h_file_path))
                scheduler_job = SchedulerJob()
                result = scheduler_job.process_dags_in_one_file(h_file_path,
                                                                h_pickle_dags)
                h_result_queue.put(result)
                added_to_queue = True
                end_time = time.time()
                logging.info("Processing {} took {:.3f} seconds"
                             .format(h_file_path,
                                     end_time - start_time))
        finally:
            # Returning something in the queue is how the parent process knows
            # that this is done
            if not added_to_queue:
                logging.info("Putting none into queue as no value was returned")
                h_result_queue.put(None)
    p = multiprocessing.Process(target=helper, args=(result_queue,
                                                     file_path,
                                                     pickle_dags,
                                                     log_file))
    p.name = "{}-Process".format(thread_name)
    # Set it to daemon so that we try to kill the process on exit
    p.daemon = True
    p.start()
    return p


class DagFileProcessor(object):
    create_counter = 0

    def __init__(self, file_path, pickle_dags, log_file):
        self.file_path = file_path
        self._result_queue = multiprocessing.Queue()
        self._process = None
        self._pickle_dags = pickle_dags
        self._process_count = 0
        self._result = None
        self._done = False
        self._start_time = None
        self._log_file = log_file
        DagFileProcessor.create_counter += 1

    def start(self):
        self._process = launch_process_to_process_file(
            self._result_queue,
            self.file_path,
            self._pickle_dags,
            "DagFileProcessor{}".format(self.create_counter),
            self._log_file)
        self._start_time = datetime.now()

    def stop(self):
        if self._process is not None:
            raise AirflowException("Tried to call stop before starting!")
        self._process.terminate()

    @property
    def is_done(self):
        if self._process is None:
            raise AirflowException("Tried to see if it's done before starting!")

        if self._done:
            return True

        if not self._result_queue.empty():
            self._result = self._result_queue.get_nowait()
            self._done = True
            return True

        # Potential error case when process dies
        if not self._process.is_alive:
            self._done = True
            if not self._result_queue.empty():
                self._result = self._result_queue.get_nowait()
            return True

        return False

    @property
    def result(self):
        if not self.is_done:
            raise AirflowException("Tried to get the result before it's done!")
        return self._result

    @property
    def start_time(self):
        if self._start_time is None:
            raise AirflowException("Tried to get start time before it started!")
        return self._start_time


class SchedulerJob(BaseJob):
    """
    This SchedulerJob runs indefinitely and constantly schedules the jobs
    that are ready to run. It figures out the latest runs for each
    task and see if the dependencies for the next schedules are met.
    If so it triggers the task instance. It does this for each task
    in each DAG and repeats.

    :param dag_id: to run the scheduler for a single specific DAG
    :type dag_id: string
    :param subdir: to search for DAG under a certain folder only
    :type subdir: string
    :param test_mode: used for unit testing this class only, runs a single
        schedule run
    :type test_mode: bool
    :param refresh_dags_every: force refresh the DAG definition every N
        runs, as specified here
    :type refresh_dags_every: int
    :param do_pickle: to pickle the DAG object and send over to workers
        for non-local executors
    :type do_pickle: bool
    """

    __mapper_args__ = {
        'polymorphic_identity': 'SchedulerJob'
    }

    def __init__(
            self,
            dag_id=None,
            dag_ids=None,
            subdir=None,
            test_mode=False,
            refresh_dags_every=10,
            num_runs=None,
            do_pickle=False,
            *args, **kwargs):
        # for BaseJob compatibility
        self.dag_id = dag_id
        self.dag_ids = [dag_id] if dag_id else []
        if dag_ids:
            self.dag_ids.extend(dag_ids)

        self.subdir = subdir

        if test_mode:
            self.num_runs = 1
        else:
            self.num_runs = num_runs

        self.refresh_dags_every = refresh_dags_every
        self.do_pickle = do_pickle
        super(SchedulerJob, self).__init__(*args, **kwargs)

        self.heartrate = conf.getint('scheduler', 'SCHEDULER_HEARTBEAT_SEC')
        self.max_threads = min(conf.getint('scheduler', 'max_threads'), multiprocessing.cpu_count())
        if 'sqlite' in conf.get('core', 'sql_alchemy_conn'):
            if self.max_threads > 1:
                self.logger.error("Cannot use more than 1 thread when using sqlite. Setting max_threads to 1")
            self.max_threads = 1

    @provide_session
    def manage_slas(self, dag, session=None):
        """
        Finding all tasks that have SLAs defined, and sending alert emails
        where needed. New SLA misses are also recorded in the database.

        Where assuming that the scheduler runs often, so we only check for
        tasks that should have succeeded in the past hour.
        """
        TI = models.TaskInstance
        sq = (
            session
            .query(
                TI.task_id,
                func.max(TI.execution_date).label('max_ti'))
            .filter(TI.dag_id == dag.dag_id)
            .filter(TI.state == State.SUCCESS)
            .filter(TI.task_id.in_(dag.task_ids))
            .group_by(TI.task_id).subquery('sq')
        )

        max_tis = session.query(TI).filter(
            TI.dag_id == dag.dag_id,
            TI.task_id == sq.c.task_id,
            TI.execution_date == sq.c.max_ti,
        ).all()

        ts = datetime.now()
        SlaMiss = models.SlaMiss
        for ti in max_tis:
            task = dag.get_task(ti.task_id)
            dttm = ti.execution_date
            if task.sla:
                dttm = dag.following_schedule(dttm)
                while dttm < datetime.now():
                    following_schedule = dag.following_schedule(dttm)
                    if following_schedule + task.sla < datetime.now():
                        session.merge(models.SlaMiss(
                            task_id=ti.task_id,
                            dag_id=ti.dag_id,
                            execution_date=dttm,
                            timestamp=ts))
                    dttm = dag.following_schedule(dttm)
        session.commit()

        slas = (
            session
            .query(SlaMiss)
            .filter(SlaMiss.email_sent == False or SlaMiss.notification_sent == False)
            .filter(SlaMiss.dag_id == dag.dag_id)
            .all()
        )

        if slas:
            sla_dates = [sla.execution_date for sla in slas]
            qry = (
                session
                .query(TI)
                .filter(TI.state != State.SUCCESS)
                .filter(TI.execution_date.in_(sla_dates))
                .filter(TI.dag_id == dag.dag_id)
                .all()
            )
            blocking_tis = []
            for ti in qry:
                if ti.task_id in dag.task_ids:
                    ti.task = dag.get_task(ti.task_id)
                    blocking_tis.append(ti)
                else:
                    session.delete(ti)
                    session.commit()

            blocking_tis = ([ti for ti in blocking_tis
                            if ti.are_dependencies_met(session=session)])
            task_list = "\n".join([
                sla.task_id + ' on ' + sla.execution_date.isoformat()
                for sla in slas])
            blocking_task_list = "\n".join([
                ti.task_id + ' on ' + ti.execution_date.isoformat()
                for ti in blocking_tis])
            # Track whether email or any alert notification sent
            # We consider email or the alert callback as notifications
            email_sent = False
            notification_sent = False
            if dag.sla_miss_callback:
                # Execute the alert callback
                self.logger.info(' --------------> ABOUT TO CALL SLA MISS CALL BACK ')
                dag.sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis)
                notification_sent = True
            email_content = """\
            Here's a list of tasks thas missed their SLAs:
            <pre><code>{task_list}\n<code></pre>
            Blocking tasks:
            <pre><code>{blocking_task_list}\n{bug}<code></pre>
            """.format(bug=asciiart.bug, **locals())
            emails = []
            for t in dag.tasks:
                if t.email:
                    if isinstance(t.email, basestring):
                        l = [t.email]
                    elif isinstance(t.email, (list, tuple)):
                        l = t.email
                    for email in l:
                        if email not in emails:
                            emails.append(email)
            if emails and len(slas):
                send_email(
                    emails,
                    "[airflow] SLA miss on DAG=" + dag.dag_id,
                    email_content)
                email_sent = True
                notification_sent = True
            # If we sent any notification, update the sla_miss table
            if notification_sent:
                for sla in slas:
                    if email_sent:
                        sla.email_sent = True
                    sla.notification_sent = True
                    session.merge(sla)
            session.commit()
            session.close()

    def import_errors(self, dagbag):
        session = settings.Session()
        session.query(models.ImportError).delete()
        for filename, stacktrace in list(dagbag.import_errors.items()):
            session.add(models.ImportError(
                filename=filename, stacktrace=stacktrace))
        session.commit()

    def schedule_dag(self, dag):
        """
        This method checks whether a new DagRun needs to be created
        for a DAG based on scheduling interval
        Returns DagRun if one is scheduled. Otherwise returns None.
        """
        if dag.schedule_interval:
            DagRun = models.DagRun
            session = settings.Session()
            qry = session.query(DagRun).filter(
                DagRun.dag_id == dag.dag_id,
                DagRun.external_trigger == False,
                DagRun.state == State.RUNNING,
            )
            active_runs = qry.all()
            if len(active_runs) >= dag.max_active_runs:
                return
            for dr in active_runs:
                if (
                        dr.start_date and dag.dagrun_timeout and
                        dr.start_date < datetime.now() - dag.dagrun_timeout):
                    dr.state = State.FAILED
                    dr.end_date = datetime.now()
            session.commit()

            qry = session.query(func.max(DagRun.execution_date)).filter_by(
                    dag_id = dag.dag_id).filter(
                        or_(DagRun.external_trigger == False,
                            # add % as a wildcard for the like query
                            DagRun.run_id.like(DagRun.ID_PREFIX+'%')))
            last_scheduled_run = qry.scalar()
            next_run_date = None
            if dag.schedule_interval == '@once' and not last_scheduled_run:
                next_run_date = datetime.now()
            elif not last_scheduled_run:
                # First run
                TI = models.TaskInstance
                latest_run = (
                    session.query(func.max(TI.execution_date))
                    .filter_by(dag_id=dag.dag_id)
                    .scalar()
                )
                if latest_run:
                    # Migrating from previous version
                    # make the past 5 runs active
                    next_run_date = dag.date_range(latest_run, -5)[0]
                else:
                    task_start_dates = [t.start_date for t in dag.tasks]
                    if task_start_dates:
                        next_run_date = min(task_start_dates)
                    else:
                        next_run_date = None
            elif dag.schedule_interval != '@once':
                next_run_date = dag.following_schedule(last_scheduled_run)

            # don't ever schedule prior to the dag's start_date
            if dag.start_date:
                next_run_date = dag.start_date if not next_run_date else max(next_run_date, dag.start_date)

            # this structure is necessary to avoid a TypeError from concatenating
            # NoneType
            if dag.schedule_interval == '@once':
                schedule_end = next_run_date
            elif next_run_date:
                schedule_end = dag.following_schedule(next_run_date)

            # Don't schedule a dag beyond its end_date (as specified by the dag param)
            if next_run_date and dag.end_date and next_run_date > dag.end_date:
                return

            # Don't schedule a dag beyond its end_date (as specified by the task params)
            # Get the min task end date, which may come from the dag.default_args
            min_task_end_date = []
            task_end_dates = [t.end_date for t in dag.tasks if t.end_date]
            if task_end_dates:
                min_task_end_date = min(task_end_dates)
            if next_run_date and min_task_end_date and next_run_date > min_task_end_date:
                return

            if next_run_date and schedule_end and schedule_end <= datetime.now():
                # Remove me
                self.logger.error("Scheduling dag run {} {}".format(dag.dag_id, next_run_date))
                self.logger.error("Max active runs is {}".format(dag.max_active_runs))
                self.logger.error("Current active runs is {}".format(len(active_runs)))
                next_run = DagRun(
                    dag_id=dag.dag_id,
                    run_id='scheduled__' + next_run_date.isoformat(),
                    execution_date=next_run_date,
                    start_date=datetime.now(),
                    state=State.RUNNING,
                    external_trigger=False
                )
                session.add(next_run)
                session.commit()
                return next_run

    def original_process_dag(self, dag, queue):
        """
        This method schedules a single DAG by looking at the latest
        run for each task and attempting to schedule the following run.

        As multiple schedulers may be running for redundancy, this
        function takes a lock on the DAG and timestamps the last run
        in ``last_scheduler_run``.
        """
        TI = models.TaskInstance
        DagModel = models.DagModel
        session = settings.Session()

        # picklin'
        pickle_id = None
        if self.do_pickle and self.executor.__class__ not in (
                executors.LocalExecutor, executors.SequentialExecutor):
            pickle_id = dag.pickle(session).id

        db_dag = session.query(DagModel).filter_by(dag_id=dag.dag_id).first()
        last_scheduler_run = db_dag.last_scheduler_run or datetime(2000, 1, 1)
        secs_since_last = (
            datetime.now() - last_scheduler_run).total_seconds()
        # if db_dag.scheduler_lock or
        if secs_since_last < self.heartrate:
            session.commit()
            session.close()
            return None
        else:
            # Taking a lock
            db_dag.scheduler_lock = True
            db_dag.last_scheduler_run = datetime.now()
            session.commit()

        active_runs = dag.get_active_runs()

        self.logger.info('Getting list of tasks to skip for active runs.')
        skip_tis = set()
        if active_runs:
            qry = (
                session.query(TI.task_id, TI.execution_date)
                .filter(
                    TI.dag_id == dag.dag_id,
                    TI.execution_date.in_(active_runs),
                    TI.state.in_((State.RUNNING, State.SUCCESS, State.FAILED)),
                )
            )
            skip_tis = {(ti[0], ti[1]) for ti in qry.all()}

        descartes = [obj for obj in product(dag.tasks, active_runs)]
        could_not_run = set()
        self.logger.info('Checking dependencies on {} tasks instances, minus {} '
                     'skippable ones'.format(len(descartes), len(skip_tis)))

        for task, dttm in descartes:
            if task.adhoc or (task.task_id, dttm) in skip_tis:
                continue
            ti = TI(task, dttm)

            ti.refresh_from_db()
            if ti.state in (
                    State.RUNNING, State.QUEUED, State.SUCCESS, State.FAILED):
                # TODO: shouldn't State.UP_FOR_RETRY be here?
                continue
            elif ti.is_runnable(flag_upstream_failed=True):
                self.logger.debug('Queuing task: {}'.format(ti))
                queue.put((ti.key, pickle_id))
            else:
                could_not_run.add(ti)

        # this type of deadlock happens when dagruns can't even start and so
        # the TI's haven't been persisted to the     database.
        if len(could_not_run) == len(descartes) and len(could_not_run) > 0:
            self.logger.error(
                'Dag runs are deadlocked for DAG: {}'.format(dag.dag_id))
            (session
                .query(models.DagRun)
                .filter(
                    models.DagRun.dag_id == dag.dag_id,
                    models.DagRun.state == State.RUNNING,
                    models.DagRun.execution_date.in_(active_runs))
                .update(
                    {models.DagRun.state: State.FAILED},
                    synchronize_session='fetch'))

        # Releasing the lock
        self.logger.debug("Unlocking DAG (scheduler_lock)")
        db_dag = (
            session.query(DagModel)
            .filter(DagModel.dag_id == dag.dag_id)
            .first()
        )
        db_dag.scheduler_lock = False
        session.merge(db_dag)
        session.commit()

        session.close()

    def modified_process_dag(self, dag, queue):
        """
        This method schedules a single DAG by looking at the latest
        run for each task and attempting to schedule the following run.

        As multiple schedulers may be running for redundancy, this
        function takes a lock on the DAG and timestamps the last run
        in ``last_scheduler_run``.
        """
        TI = models.TaskInstance
        DagModel = models.DagModel
        session = settings.Session()

        # db_dag = session.query(DagModel).filter_by(dag_id=dag.dag_id).first()
        # if db_dag and db_dag.last_scheduler_run:
        #     last_scheduler_run = db_dag.last_scheduler_run
        # else:
        #     last_scheduler_run = datetime(2000, 1, 1)
        #
        # secs_since_last = (
        #     datetime.now() - last_scheduler_run).total_seconds()
        # # if db_dag.scheduler_lock or
        # if secs_since_last < self.heartrate:
        #     session.commit()
        #     session.close()
        #     return None
        # else:
        #     # Taking a lock
        #     db_dag.scheduler_lock = True
        #     db_dag.last_scheduler_run = datetime.now()
        #     session.commit()

        active_runs = dag.get_active_runs()

        self.logger.info('Getting list of tasks to skip for active runs.')
        skip_tis = set()
        if active_runs:
            qry = (
                session.query(TI.task_id, TI.execution_date)
                    .filter(
                    TI.dag_id == dag.dag_id,
                    TI.execution_date.in_(active_runs),
                    TI.state.in_((State.RUNNING, State.SUCCESS, State.FAILED)),
                )
            )
            skip_tis = {(ti[0], ti[1]) for ti in qry.all()}

        descartes = [obj for obj in product(dag.tasks, active_runs)]
        could_not_run = set()
        self.logger.info('Checking dependencies on {} tasks instances, minus {} '
                         'skippable ones'.format(len(descartes), len(skip_tis)))

        for task, dttm in descartes:
            if task.adhoc or (task.task_id, dttm) in skip_tis:
                continue
            ti = TI(task, dttm)

            self.logger.info("Examining {}".format(ti))
            ti.refresh_from_db()
            if ti.state in (
                    State.RUNNING, State.QUEUED, State.SUCCESS, State.FAILED):
                # Can state be None? State can be none if the task hasn't been created yet
                # If it's up for retry, ti.is_runnable can return false but it's not really
                #  deadlocked
                # TODO: Remove me
                self.logger.info("Skipping because of state")
                continue
            elif ti.is_runnable(flag_upstream_failed=True):
                self.logger.debug('Queuing task: {}'.format(ti))
                queue.put(ti.key)
            else:
                could_not_run.add(ti)

        # this type of deadlock happens when dagruns can't even start and so
        # the TI's haven't been persisted to the     database.
        if len(could_not_run) == len(descartes) and len(could_not_run) > 0:
            self.logger.error(
                'Dag runs are deadlocked for DAG: {}'.format(dag.dag_id))
            (session
                .query(models.DagRun)
                .filter(
                models.DagRun.dag_id == dag.dag_id,
                models.DagRun.state == State.RUNNING,
                models.DagRun.execution_date.in_(active_runs))
                .update(
                {models.DagRun.state: State.FAILED},
                synchronize_session='fetch'))

        # Releasing the lock
        self.logger.debug("Unlocking DAG (scheduler_lock)")
        db_dag = (
            session.query(DagModel)
                .filter(DagModel.dag_id == dag.dag_id)
                .first()
        )
        db_dag.scheduler_lock = False
        session.merge(db_dag)
        session.commit()

        session.close()

    def original_process_events(self, executor, dagbag):
        """
        Respond to executor events.

        Used to identify queued tasks and schedule them for further processing.
        """
        for key, executor_state in list(executor.get_event_buffer().items()):
            dag_id, task_id, execution_date = key
            if dag_id not in dagbag.dags:
                self.logger.error(
                    'Executor reported a dag_id that was not found in the '
                    'DagBag: {}'.format(dag_id))
                continue
            elif not dagbag.dags[dag_id].has_task(task_id):
                self.logger.error(
                    'Executor reported a task_id that was not found in the '
                    'dag: {} in dag {}'.format(task_id, dag_id))
                continue
            task = dagbag.dags[dag_id].get_task(task_id)
            ti = models.TaskInstance(task, execution_date)
            ti.refresh_from_db()

            if executor_state == State.SUCCESS:
                # collect queued tasks for prioritiztion
                if ti.state == State.QUEUED:
                    self.queued_tis.add(ti)
            else:
                # special instructions for failed executions could go here
                pass


    def modified_process_events(self, executor, dagbag):
        """
        Respond to executor events.
        """
        for key, executor_state in list(executor.get_event_buffer().items()):
            dag_id, task_id, execution_date = key
            self.logger.info("Executor reports {}.{} execution_date={} as {}",
                             dag_id,
                             task_id,
                             execution_date,
                             executor_state)

    @provide_session
    def original_prioritize_queued(self, session, executor, dagbag):
        # Prioritizing queued task instances

        pools = {p.pool: p for p in session.query(models.Pool).all()}
        TI = models.TaskInstance
        queued_tis = (
            session.query(TI)
            .filter(TI.state == State.QUEUED)
            .all()
        )
        self.logger.info(
            "Prioritizing {} queued jobs".format(len(queued_tis)))
        session.expunge_all()
        d = defaultdict(list)
        for ti in queued_tis:
            if ti.dag_id not in dagbag.dags:
                self.logger.info(
                    "DAG no longer in dagbag, deleting {}".format(ti))
                session.delete(ti)
                session.commit()
            elif not dagbag.dags[ti.dag_id].has_task(ti.task_id):
                self.logger.info(
                    "Task no longer exists, deleting {}".format(ti))
                session.delete(ti)
                session.commit()
            else:
                d[ti.pool].append(ti)

        dag_blacklist = set(dagbag.paused_dags())
        for pool, tis in list(d.items()):
            if not pool:
                # Arbitrary:
                # If queued outside of a pool, trigger no more than
                # non_pooled_task_slot_count per run
                open_slots = conf.getint('core', 'non_pooled_task_slot_count')
            else:
                open_slots = pools[pool].open_slots(session=session)

            queue_size = len(tis)
            self.logger.info("Pool {pool} has {open_slots} slots, {queue_size} "
                             "task instances in queue".format(**locals()))
            if open_slots <= 0:
                continue
            tis = sorted(
                tis, key=lambda ti: (-ti.priority_weight, ti.start_date))
            for ti in tis:
                if open_slots <= 0:
                    continue
                task = None
                try:
                    task = dagbag.dags[ti.dag_id].get_task(ti.task_id)
                except:
                    self.logger.error("Queued task {} seems gone".format(ti))
                    session.delete(ti)
                    session.commit()
                    continue

                if not task:
                    continue

                ti.task = task

                # picklin'
                dag = dagbag.dags[ti.dag_id]
                pickle_id = None
                if self.do_pickle and self.executor.__class__ not in (
                        executors.LocalExecutor,
                        executors.SequentialExecutor):
                    self.logger.info("Pickling DAG {}".format(dag))
                    pickle_id = dag.pickle(session).id

                if dag.dag_id in dag_blacklist:
                    continue
                if dag.concurrency_reached:
                    dag_blacklist.add(dag.dag_id)
                    continue
                if ti.are_dependencies_met():
                    executor.queue_task_instance(ti, pickle_id=pickle_id)
                    open_slots -= 1
                else:
                    session.delete(ti)
                    session.commit()
                    continue
                ti.task = task

                session.commit()

    @provide_session
    def delete_outdated_queued_task_instances(self, simple_dagbag, session=None):
        """Queries the ORM for task instances in the queued state and deletes
        ones that don't correspond to a known definition in the SimpleDagBag"""
        TI = models.TaskInstance
        queued_task_instances = (
            session
                .query(TI)
                .filter(TI.state == State.QUEUED)
                .all()
        )
        for task_instance in queued_task_instances:
            if not simple_dagbag.exists(task_instance.dag_id, task_instance.task_id):
                self.logger.error("Deleting {} as it does not have a "
                                  "corresponding task in the dagbag."
                                  .format(task_instance))
                session.delete(task_instance)
        session.commit()

    def get_task_concurrency(self, session, dag_id, task_ids):
        """
        Returns the number of tasks running in the given DAG
        """
        TI = models.TaskInstance
        qry = session.query(func.count(TI.task_id)).filter(
            TI.dag_id == dag_id,
            TI.task_id.in_(task_ids),
            TI.state == State.RUNNING,
        )
        return qry.scalar()

    @provide_session
    def execute_task_instances(self,
                               executor,
                               simple_dag_bag,
                               states,
                               session=None):
        """Fetches task instances from ORM in the specified states, figures
        out pool limits, and sends them to the executor for execution"""
        # Get all the queued task instances
        TI = models.TaskInstance
        queued_task_instances = (
            session
                .query(TI)
                .filter(TI.dag_id.in_(simple_dag_bag.get_dag_ids()))
                .filter(TI.state.in_(states))
                .all()
        )

        # Put one task instance on each line
        if len(queued_task_instances) == 0:
            self.logger.info("No queued tasks to send to the executor")
            return

        task_instance_str = "\n\t".join(
            ["{}".format(x) for x in queued_task_instances])
        self.logger.info("Queued tasks up for execution:\n\t{}".format(task_instance_str))

        # Get the pool settings
        pools = {p.pool: p for p in session.query(models.Pool).all()}

        pool_to_task_instances = defaultdict(list)
        for task_instance in queued_task_instances:
                pool_to_task_instances[task_instance.pool].append(task_instance)

        # Go through each pool, and queue up a task for execution if there are
        # any open slots in the pool.
        for pool, task_instances in pool_to_task_instances.iteritems():
            if not pool:
                # Arbitrary:
                # If queued outside of a pool, trigger no more than
                # non_pooled_task_slot_count per run
                open_slots = conf.getint('core', 'non_pooled_task_slot_count')
            else:
                open_slots = pools[pool].open_slots(session=session)

            num_queued = len(task_instances)
            self.logger.info("Figuring out tasks to run in Pool(name={pool}) "
                             "with {open_slots} open slots and {num_queued} "
                             "task instances in queue".format(**locals()))

            if open_slots <= 0:
                continue

            priority_sorted_task_instances = sorted(
                task_instances, key=lambda ti: (-ti.priority_weight, ti.start_date))

            # DAG IDs with running tasks that equal the concurrency limit of the dag
            dag_ids_at_max_concurrency = []

            for task_instance in priority_sorted_task_instances:
                if open_slots <= 0:
                    # Can't schedule any more since there are no more open slots.
                    break

                if simple_dag_bag.is_paused(task_instance.dag_id):
                    self.logger.info("Not executing queued {} since {} is paused"
                                     .format(task_instance, task_instance.dag_id))
                    continue

                dag_id = task_instance.dag_id
                task_concurrency_limit = simple_dag_bag.get_task_concurrency_limit(dag_id)

                # Checking the task concurrency for the DAG could be expensive?
                if (dag_id in dag_ids_at_max_concurrency or
                        self.get_task_concurrency(session,
                                                  dag_id,
                                                  simple_dag_bag.get_task_ids(dag_id)) >
                        task_concurrency_limit):
                    dag_ids_at_max_concurrency.append(dag_id)
                    self.logger.info("Not executing queued {} since the number "
                                     "of tasks running from DAG {} exceeds the "
                                     "DAG's concurrency limit of {}"
                                     .format(task_instance, dag_id, task_concurrency_limit))
                    continue

                # If the task was queued, dependencies must have been met so no need to do this check.
                #
                #if not task_instance.are_dependencies_met():
                #    # A queued task instance may not have its dependencies met if the DAG definition
                #    # has changed.
                #    self.logger.info("Deleting queued {} as the dependencies are not met")
                #    session.delete(task_instance)
                #    continue

                # Nothing is blocking this task from running, so send it to the executor.
                # executor.queue_task_instance(task_instance, pickle_id=pickle_id)
                # TODO: Should the pickle ID be a part of the task instance?
                # TODO: Should this use TaskInstance.priority_weight_total?
                command = TI.generate_command(
                    task_instance.dag_id,
                    task_instance.task_id,
                    task_instance.execution_date,
                    local=True,
                    mark_success=False,
                    force=False,
                    ignore_dependencies=False,
                    ignore_depends_on_past=False,
                    pool=task_instance.pool,
                    full_file_path=simple_dag_bag.get_full_file_path(task_instance.dag_id),
                    pickle_id=simple_dag_bag.get_pickle_id(task_instance.dag_id))

                priority = task_instance.priority_weight
                queue = task_instance.queue
                self.logger.info("Sending to executor {} with priority {} and queue {}"
                                 .format(task_instance.key, priority, queue ))

                executor.queue_command(
                    task_instance,
                    command,
                    priority=priority,
                    queue=queue)

                open_slots -= 1


    @provide_session
    def prioritize_queued(self, session, queued_tis, executor, dagbag):
        # Prioritizing queued task instances

        pools = {p.pool: p for p in session.query(models.Pool).all()}

        self.logger.info(
            "Prioritizing {} queued jobs".format(len(self.queued_tis)))
        session.expunge_all()
        d = defaultdict(list)
        for ti in queued_tis:
            if ti.dag_id not in dagbag.dags:
                self.logger.info(
                    "DAG no longer in dagbag, deleting {}".format(ti))
                session.delete(ti)
                session.commit()
            elif not dagbag.dags[ti.dag_id].has_task(ti.task_id):
                self.logger.info(
                    "Task no longer exists, deleting {}".format(ti))
                session.delete(ti)
                session.commit()
            else:
                d[ti.pool].append(ti)

        self.queued_tis.clear()

        dag_blacklist = set(dagbag.paused_dags())
        for pool, tis in list(d.items()):
            if not pool:
                # Arbitrary:
                # If queued outside of a pool, trigger no more than
                # non_pooled_task_slot_count per run
                open_slots = conf.getint('core', 'non_pooled_task_slot_count')
            else:
                open_slots = pools[pool].open_slots(session=session)

            queue_size = len(tis)
            self.logger.info("Pool {pool} has {open_slots} slots, {queue_size} "
                             "task instances in queue".format(**locals()))
            if open_slots <= 0:
                continue
            tis = sorted(
                tis, key=lambda ti: (-ti.priority_weight, ti.start_date))
            for ti in tis:
                if open_slots <= 0:
                    continue
                task = None
                try:
                    task = dagbag.dags[ti.dag_id].get_task(ti.task_id)
                except:
                    self.logger.error("Queued task {} seems gone".format(ti))
                    session.delete(ti)
                    session.commit()
                    continue

                if not task:
                    continue

                ti.task = task

                # picklin'
                dag = dagbag.dags[ti.dag_id]
                pickle_id = None
                if self.do_pickle and self.executor.__class__ not in (
                        executors.LocalExecutor,
                        executors.SequentialExecutor):
                    self.logger.info("Pickling DAG {}".format(dag))
                    pickle_id = dag.pickle(session).id

                if dag.dag_id in dag_blacklist:
                    continue
                if dag.concurrency_reached:
                    dag_blacklist.add(dag.dag_id)
                    continue
                if ti.are_dependencies_met():
                    executor.queue_task_instance(ti, pickle_id=pickle_id)
                    open_slots -= 1
                else:
                    session.delete(ti)
                    continue
                ti.task = task

                session.commit()


    def _split_dags(self, dags, size):
        """
        This function splits a list of dags into chunks of int size.
        _split_dags([1,2,3,4,5,6], 3) becomes [[1,2,3],[4,5,6]]
        """
        size = max(1, size)
        return [dags[i:i + size] for i in range(0, len(dags), size)]

    def _do_dags(self, dagbag, dags, tis_out):
        """
        Iterates over the dags and schedules and processes them
        """
        for dag in dags:
            self.logger.debug("Scheduling {}".format(dag.dag_id))
            dag = dagbag.get_dag(dag.dag_id)
            if not dag:
                continue
            try:
                self.schedule_dag(dag)
                self.modified_process_dag(dag, tis_out)
                self.manage_slas(dag)
            except Exception as e:
                self.logger.exception(e)

    def _process_dag_in_separate_process(self, file_path, pickle_dags, queue):
        start_time = time.time()
        result_queue = multiprocessing.Queue()

        def helper():
            result_queue.put(
                self.process_dags_in_one_file(file_path,
                                              pickle_dags))

        p = multiprocessing.Process(target=helper)
        # Set it to daemon so that we try to kill on exit
        p.daemon = True
        p.start()
        p.join()
        end_time = time.time()
        self.logger.info("Processing {} took {:.3f} seconds"
                         .format(file_path,
                                 end_time - start_time))

        if result_queue.empty():
            queue.append(None)
            # return None
        else:
            queue.append(result_queue.get_nowait())
            # return result_queue.get_nowait()

    def _launch_process_to_process_file(self,
                                        thread_name,
                                        file_path,
                                        pickle_dags,
                                        queue):
        def helper(h_result_queue, h_file_path, h_pickle_dags):
            threading.current_thread().name = thread_name
            start_time = time.time()
            h_result_queue.put(
                self.process_dags_in_one_file(h_file_path,
                                              h_pickle_dags))
            end_time = time.time()
            self.logger.info("Processing {} took {:.3f} seconds"
                             .format(h_file_path,
                                     end_time - start_time))

        p = multiprocessing.Process(target=helper, args=(queue,
                                                         file_path,
                                                         pickle_dags))
        # Set it to daemon so that we try to kill on exit
        p.daemon = True
        p.start()
        return p

    # Print out stats about how the DAG's are processed
    def log_file_processing_stats(self, processed_counts, elapsed_time):
        headers = ["File Path", "Processed Cycles", "Avg. Cycle Time"]
        rows = []
        for file_path, count in processed_counts.items():
            rows.append((file_path,
                         count,
                         "{:.2f}s".format(elapsed_time / count if count else 0)))

        # Sort by the average cycle time
        sorted(rows, key=lambda x: x[2])
        log_str = ("\n" +
                   "=" * 80 +
                   "\n" +
                   "DAG File Processing Stats\n\n" +
                   tabulate(rows, headers=headers) +
                   "\n" +
                   "=" * 80)

        self.logger.info(log_str)

    @staticmethod
    def split_path(file_path):
        results = []
        while True:
            head, tail = os.path.split(file_path)
            if len(tail) != 0:
                results.append(tail)
            if file_path == head:
                break;
            file_path = head
        results.reverse()
        return results

    def get_log_file_path(self, dag_file_path):
        # TODO: Make configurable
        log_directory = u"/tmp/airflow/logs/"
        relative_dag_file_path = os.path.relpath(dag_file_path, start=self.subdir)
        path_elements = (SchedulerJob.split_path(relative_dag_file_path))

        # Add a .log suffix for the log file
        path_elements[-1] += ".log"

        return os.path.join(log_directory, *path_elements)

    # This is the modified _execute()
    def _execute(self):
        pessimistic_connection_handling()

        logging.basicConfig(level=logging.DEBUG)
        self.logger.info("Starting the scheduler")

        executor = DEFAULT_EXECUTOR
        executor.start()

        self.runs = 0

        # DAGs can be pickled for easier remote execution by some executors
        pickle_dags = False
        if self.do_pickle and self.executor.__class__ not in (
               executors.LocalExecutor, executors.SequentialExecutor):
            pickle_dags = True

        # How long to run this method for for and when we started
        #loop_time_limit = 15 * 60;
        execute_time_limit = timedelta(minutes=30)
        execute_start_time = datetime.now()

        # How frequently to refresh the known file paths in the DAG directory
        # and when we last refreshed
        dag_dir_refresh_interval = timedelta(minutes=5)
        last_dag_dir_refresh_time = datetime(2000, 1, 1)

        # How frequently to print out DAG processing stats
        # and when we last printed them
        stat_print_interval = timedelta(seconds=10)
        last_stat_print_time = datetime(2000, 1, 1)

        # Deactivate DAGs that haven't been updated by the scheduler
        # for this long
        expiration_age = timedelta(minutes=60)

        # Last time that self.heartbeat() was called.
        last_self_heartbeat_time = datetime.now()

        # Use multiple processes to parse and generate tasks for the
        # DAGs in parallel. By processing them in separate processes,
        # we can get parallelism and isolation from potentially harmful
        # user code.
        parallelism = 64
        self.logger.info("Processing files using up to {} processes at a time"
                         .format(parallelism))

        known_file_paths = []
        file_paths_queue = []
        processors = []
        # Map between a file path, and the DAG IDs in that file
        file_path_to_dag_ids = defaultdict(list)

        # Map from the file path to the number of times it's been processed
        processed_counts = defaultdict(int)

        loop_start_log_str = ("\n" +
                              "=" * 80 +
                              "\nStarting loop at {}:\n" +
                              "=" * 80)
        while datetime.now() - execute_start_time < execute_time_limit:
            loop_start_time = time.time()
            self.logger.info(loop_start_log_str.format(datetime.now().isoformat()))
            # Traverse the DAG directory for Python files containing DAGs
            # on first pass / refresh the list of file paths periodically
            if datetime.now() - last_dag_dir_refresh_time > dag_dir_refresh_interval:
                # Build up a list of Python files that could contain DAGs
                self.logger.info("Searching for files in {}".format(self.subdir))
                known_file_paths = dag_utils.list_py_file_paths(self.subdir)
                self.logger.info("There are {} files in {}"
                                 .format(len(known_file_paths), self.subdir))

                # Since it's possible some files no longer exist, stop
                # the processors from working on them
                processors_to_stop = [x for x in processors
                                      if x.file_path not in known_file_paths]
                processors_to_retain = [x for x in processors
                                        if x.file_path in known_file_paths]

                for processor in processors_to_stop:
                    self.logger.info("Stopping processor for {} since the file no longer exists"
                                     .format(processor.file_path))
                    processor.stop()
                processors = processors_to_retain

                # Also, if a file has been deleted, remove it from the file path ->
                # DAG IDs mapping
                file_path_to_dag_ids = {k: v for k, v in file_path_to_dag_ids.items()
                                        if k in known_file_paths}
                # Set the known DAG IDs to None if they don't exist
                for file_path in known_file_paths:
                    if file_path not in file_path_to_dag_ids:
                        file_path_to_dag_ids[file_path] = None

                last_dag_dir_refresh_time = datetime.now()

            # Remove the processors that have finished
            finished_processors = []
            running_processors = []
            for processor in processors:
                if processor.is_done:
                    self.logger.info("Processor for {} finished".format(processor.file_path))
                    finished_processors.append(processor)
                else:
                    running_processors.append(processor)
            processors = running_processors

            # Count the files that have been processed successfully
            for processor in finished_processors:
                processed_counts[processor.file_path] += 1

            # Create a Simple DAG bag from the DAGs that were parsed so we
            # can tell the executor tasks from which DAGs need to be run
            simple_dags = []
            for processor in finished_processors:
                for simple_dag in processor.result.simple_dag_bag.simple_dags:
                    simple_dags.append(simple_dag)
            simple_dag_bag = SimpleDagBag(simple_dags)

            # Send tasks for execution if available
            if len(simple_dags) > 0:
                self.execute_task_instances(executor,
                                            simple_dag_bag,
                                            (State.QUEUED, State.UP_FOR_RETRY))

            # Generate the list of file paths to process / reprocess the
            # files again if the previous batch is done
            if len(file_paths_queue) == 0:
                # If the file path is already being processed, wait until the next batch
                file_paths_in_progress = [x.file_path for x in processors]
                files_paths_to_queue = list(set(known_file_paths) - set(file_paths_in_progress))

                for processor in processors:
                    self.logger.info("File path {} is still being processed (started: {})"
                                     .format(processor.file_path,
                                             processor.start_time.isoformat()))

                self.logger.info("Queuing the following files for processing:\n\t{}"
                                 .format("\n\t".join(files_paths_to_queue)))
                file_paths_queue.extend(files_paths_to_queue)

            # Start more processors if we can
            while parallelism - len(processors) > 0 and len(file_paths_queue) > 0:
                file_path = file_paths_queue.pop(0)
                log_file_path = self.get_log_file_path(file_path)
                processor = DagFileProcessor(file_path, pickle_dags, log_file_path)
                self.logger.info("Starting a process to generate tasks for {} - logging into {}"
                                 .format(file_path, log_file_path))
                processor.start()
                processors.append(processor)

            # Occasionally print out stats about how fast the files are getting processed
            if datetime.now() - last_stat_print_time > stat_print_interval:
                if len(processed_counts) > 0:
                    self.log_file_processing_stats(processed_counts,
                                                   (datetime.now() - execute_start_time).total_seconds())
                last_stat_print_time = datetime.now()

            # Once the scheduler has been running long enough to ensure all files
            # have been processed, deactivate DAGs that haven't been touched by
            # the scheduler as they were likely deleted
            if datetime.now() - execute_start_time > expiration_age:
                expiration_date = datetime.now() - expiration_age
                models.DAG.deactivate_stale_dags(expiration_date)

            # Call hearbeats
            # TODO: Turned off for the test - re-enable
            # self.logger.info("Heartbeating the executor")
            # executor.heartbeat()

            if (datetime.now() - last_self_heartbeat_time).total_seconds() > self.heartrate:
                self.logger.info("Heartbeating the scheduler")
                self.heartbeat()

            loop_end_time = time.time()
            self.logger.info("Ran scheduling loop in {:.2f}s".format(loop_end_time - loop_start_time))
            self.logger.info("Sleeping")
            time.sleep(1.0)


        # Why is this needed?
        #settings.Session.remove()
        executor.end()

        # while not self.num_runs or self.num_runs > self.runs:
        #     self.runs += 1
        #     loop_start_dttm = datetime.now()
        #     try:
        #         # Read each file for DAGs, figure out the tasks that can be
        #         # run, and create those task instances in the ORM
        #
        #         """
        #         # Test code to process in same process
        #         results = []
        #         for file_path in file_paths:
        #             self.logger.info("Processing DAGs in file {}".format(file_path))
        #             # TODO: Pass pickle_dags to process
        #             result = self.process_dags_in_one_file(file_path)
        #             results.append(result)
        #         """
        #
        #         # Use multiple processes to parse and generate tasks for the
        #         # DAGs in parallel. By processing them in separate processes,
        #         # we can get parallelism and isolation from potentially harmful
        #         # user code.
        #         parallelism = 8
        #         self.logger.info("Processing {} files using up to {} processes at a time"
        #                          .format(len(known_file_paths), parallelism))
        #
        #         # Not using built in thread pool due to unusual behavior when
        #         # receiving signals (e.g. QUIT)
        #         args = list(known_file_paths)
        #         start_count = 0
        #         finished_count = 0
        #         alive_processes = []
        #
        #         results = []
        #         all_forked_process_args = []
        #         # Keep looping until we run out of stuff to parse and all the
        #         # processes are done
        #         while len(args) > 0 or len(alive_processes) > 0:
        #             # If there are more file paths to process and there are
        #             # enough slots free, create parse a files through a new
        #             # process
        #             while len(args) > 0 and len(alive_processes) < parallelism:
        #                 file_path = args.pop()
        #                 # Name of the main thread will be changed in the
        #                 # child process to make log reading easier
        #                 thread_name = "ProcessFile-{}".format(start_count)
        #                 result_queue = multiprocessing.Queue()
        #                 #p = self._launch_process_to_process_file(thread_name,
        #                 #                                         file_path,
        #                 #                                         pickle_dags,
        #                 #
        #                 #                                          result_queue)
        #
        #                 # Prevent args from getting GC'ed?
        #                 forked_process_args = (result_queue, file_path, pickle_dags)
        #                 all_forked_process_args.append(forked_process_args)
        #                 p = launch_process_to_process_file(thread_name,
        #                                                    forked_process_args)
        #                 start_count += 1
        #                 alive_processes.append((p, result_queue))
        #
        #             # Filter out the processes that have finished and add the
        #             # number finished to the total
        #             #initial_count = len(processes)
        #             #processes = [x for x in processes if x.is_alive()]
        #             #filtered_count = len(processes)
        #             #finished_count += initial_count - filtered_count
        #
        #             filtered_processes = []
        #             for process, queue in alive_processes:
        #                 # It's possible for process to be blocked waiting for the queue to empty?
        #                 process_finished = False
        #
        #                 # TODO: There shouldn't be more than one entry
        #                 results_from_queue = []
        #                 while not queue.empty():
        #                     results_from_queue.append(queue.get_nowait())
        #
        #                 if len(results_from_queue) > 1:
        #                     logging.error("Too many results from the queue {} from"
        #                                   .format(results_from_queue, process.name))
        #
        #                 if len(results_from_queue) > 0:
        #                     finished_count += 1
        #                     process_finished = True
        #                     results.extend(results_from_queue)
        #
        #                 # TODO: Remember race condition when a process generates and finishes an entry
        #                 # between calls to queue.empty() and removal from filtered_processes
        #                 if not process_finished:
        #                     filtered_processes.append((process, queue))
        #                 # else:
        #                 #     # TODO: It should never be empty?
        #                 #     if not queue.empty():
        #                 #         results.append(queue.get_nowait())
        #                 #     finished_count += 1
        #             alive_processes = filtered_processes
        #             self.logger.info("DAG task discovery status: {} / {} files processed"
        #                              .format(finished_count,
        #                                      len(known_file_paths)))
        #             time.sleep(1)
        #
        #         # Pick out non-None results from the result queue into a list
        #         #results = []
        #         #while not result_queue.empty():
        #         #    result = result_queue.get_nowait()
        #         #    if result:
        #         #        results.append(result)
        #
        #         results = [x for x in results if x is not None]
        #
        #         # Combine the SimpleDagBags and TaskExecutionInfos from each file
        #         all_simple_dags = []
        #         all_task_execution_infos = []
        #         for result in results:
        #             all_simple_dags.extend(result.simple_dag_bag.simple_dags)
        #             all_task_execution_infos.extend(result.task_execution_infos)
        #         simple_dag_bag = SimpleDagBag(all_simple_dags)
        #
        #         # Now that we know all the active DAG IDs, we can mark the
        #         # ones that we don't know as inactive.
        #         active_dag_ids = simple_dag_bag.get_dag_ids()
        #         models.DAG.deactivate_unknown_dags(active_dag_ids)
        #
        #         # Before scheduling any tasks, delete any queued instances
        #         # that are not a part of any known DAG. They may exist
        #         # because they were created with an earlier version of a DAG,
        #         # or the DAG was deleted after the tasks were created.
        #         self.delete_outdated_queued_task_instances(simple_dag_bag)
        #
        #         # All tasks should exist in the ORM in the queued state at
        #         # this point, so it's possible to prioritize based on the
        #         # pool and execute them.
        #         self.logger.info("Examining queued tasks and sending to the "
        #                          "executor")
        #         self.execute_task_instances(executor,
        #                                     simple_dag_bag,
        #                                     all_task_execution_infos,
        #                                     (State.QUEUED, State.UP_FOR_RETRY))
        #
        #         duration_sec = (datetime.now() - loop_start_dttm).total_seconds()
        #
        #         if len(known_file_paths) > 0:
        #             self.logger.info("Scheduling task(s) from {} DAG(s) in {} "
        #                              "file(s) took {:.3f} seconds. Average: "
        #                              "{:.3f} seconds/file"
        #                              .format(simple_dag_bag.dag_count(),
        #                                      len(known_file_paths),
        #                                      duration_sec,
        #                                      duration_sec/len(known_file_paths)))
        #         import sys
        #         self.logger.info("Exiting for test!")
        #         sys.exit(-1)
        #         try:
        #             # We really just want the scheduler to never ever stop.
        #             self.logger.info("Heartbeating the executor")
        #             executor.heartbeat()
        #             self.logger.info("Heartbeating self")
        #             self.heartbeat()
        #         except Exception as e:
        #             self.logger.exception(e)
        #             self.logger.error("Tachycardia!")
        #
        #
        #     except Exception as deep_e:
        #         self.logger.exception(deep_e)
        #         raise
        #     finally:
        #         # Why is this needed?
        #         settings.Session.remove()
        # executor.end()

    @provide_session
    def process_dags_in_one_file(self, file_path, pickle_dags=False, session=None):
        """Loads the specified file and for each dag in the file, pickle
        the dag, figure out what tasks can be run, and create respective task
        instances in the ORM. Return a ProcessDagFileResult."""
        self.logger.info("Processing file {} for tasks to queue".format(file_path))
        # As DAGs are parsed from this file, they will be converted into SimpleDags
        simple_dags = []
        task_execution_infos = []

        TI = models.TaskInstance

        try:
            dagbag = models.DagBag(file_path)
        except Exception as e:
            self.logger.exception("Failed at reloading the DAG file {}".format(file_path))
            Stats.incr('dag_file_refresh_error', 1, 1)
            return ProcessDagFileResult(SimpleDagBag([]), [])

        if len(dagbag.dags) > 0:
            self.logger.info("DAG(s) {} retrieved from {}"
                             .format(dagbag.dags.keys(),
                                     file_path))
        else:
            self.logger.warn("No viable dags retrieved from {}".format(file_path))
            return ProcessDagFileResult(SimpleDagBag([]), [])

        # Save individual DAGs in the ORM
        sync_time = datetime.now()
        for dag in dagbag.dags.values():
            models.DAG.sync_to_db(dag, sync_time)

        # Pickle the DAGs (if necessary) and put them into a SimpleDag
        for dag_id in dagbag.dags:
            dag = dagbag.get_dag(dag_id)
            pickle_id = None
            if pickle_dags:
                pickle_id = dag.pickle(session).id

            task_ids = [task.task_id for task in dag.tasks]
            simple_dags.append(SimpleDag(dag.dag_id,
                                         task_ids,
                                         dag.full_filepath,
                                         dag.concurrency,
                                         dag.is_paused,
                                         pickle_id))
        simple_dag_bag = SimpleDagBag(simple_dags)

        if len(self.dag_ids) > 0:
            dags = [dag for dag in dagbag.dags.values() if dag.dag_id in self.dag_ids]
        else:
            dags = [
                dag for dag in dagbag.dags.values()
                if not dag.parent_dag]

        tis_q = multiprocessing.Queue()

        for dag in dags:
            logging.info("Generating new task instances for DAG ID {}".format(dag.dag_id))
            self._do_dags(dagbag, dags, tis_q)

        #pickle_id = None
        #if self.do_pickle and self.executor.__class__ not in (
        #        executors.LocalExecutor, executors.SequentialExecutor):
        #    pickle_id = dag.pickle(session).id

        while not tis_q.empty():
            ti_key = tis_q.get()
            dag = dagbag.dags[ti_key[0]]
            task = dag.get_task(ti_key[1])
            ti = TI(task, ti_key[2])
            # Task starts out in the queued state. All tasks in the queued
            # state will be scheduled later in the execution loop.
            ti.state = State.QUEUED
            pool = ti.pool
            pickle_id = simple_dag_bag.get_pickle_id(dag.dag_id)

            command = ti.command(
                local=True,
                mark_success=False,
                force=False,
                ignore_dependencies=False,
                ignore_depends_on_past=False,
                pool=pool,
                pickle_id=pickle_id)

            task_execution_infos.append(TaskExecutionInfo(
                ti.key,
                command,
                ti.task.priority_weight_total,
                ti.task.queue))

            # Also save this task instance to the DB.
            self.logger.info("Creating {} in ORM".format(ti))
            session.add(ti)
            session.commit()

        # Record import errors into the ORM
        try:
            self.import_errors(dagbag)
        except Exception as e:
            self.logger.exception("Error logging import errors!")
        try:
            dagbag.kill_zombies()
        except Exception as e:
            self.logger.exception("Error killing zombies!")

        return ProcessDagFileResult(simple_dag_bag, task_execution_infos)


    def _execute_original(self):
        TI = models.TaskInstance

        pessimistic_connection_handling()

        logging.basicConfig(level=logging.DEBUG)
        self.logger.info("Starting the scheduler")

        dagbag = models.DagBag(self.subdir, sync_to_db=True)
        executor = self.executor = dagbag.executor
        executor.start()
        self.runs = 0
        while not self.num_runs or self.num_runs > self.runs:
            try:
                loop_start_dttm = datetime.now()
                try:
                    self.prioritize_queued(executor=executor, dagbag=dagbag)
                except Exception as e:
                    self.logger.exception(e)

                self.runs += 1
                try:
                    if self.runs % self.refresh_dags_every == 0:
                        dagbag = models.DagBag(self.subdir, sync_to_db=True)
                    else:
                        dagbag.collect_dags(only_if_updated=True)
                except Exception as e:
                    self.logger.error("Failed at reloading the dagbag. {}".format(e))
                    Stats.incr('dag_refresh_error', 1, 1)
                    sleep(5)

                if len(self.dag_ids) > 0:
                    dags = [dag for dag in dagbag.dags.values() if dag.dag_id in self.dag_ids]
                else:
                    dags = [
                        dag for dag in dagbag.dags.values()
                        if not dag.parent_dag]

                paused_dag_ids = dagbag.paused_dags()
                dags = [x for x in dags if x.dag_id not in paused_dag_ids]
                # dags = filter(lambda x: x.dag_id not in paused_dag_ids, dags)

                self.logger.debug("Total Cores: {} Max Threads: {} DAGs:{}".
                                  format(multiprocessing.cpu_count(),
                                         self.max_threads,
                                         len(dags)))
                dags = self._split_dags(dags, math.ceil(len(dags) / self.max_threads))
                tis_q = multiprocessing.Queue()
                jobs = [multiprocessing.Process(target=self._do_dags,
                                                args=(dagbag, dags[i], tis_q))
                        for i in range(len(dags))]

                self.logger.info("Starting {} scheduler jobs".format(len(jobs)))
                for j in jobs:
                    j.start()

                while any(j.is_alive() for j in jobs):
                    while not tis_q.empty():
                        ti_key, pickle_id = tis_q.get()
                        dag = dagbag.dags[ti_key[0]]
                        task = dag.get_task(ti_key[1])
                        ti = TI(task, ti_key[2])
                        self.executor.queue_task_instance(ti, pickle_id=pickle_id)

                for j in jobs:
                    j.join()

                self.logger.info("Done queuing tasks, calling the executor's "
                                 "heartbeat")
                duration_sec = (datetime.now() - loop_start_dttm).total_seconds()
                self.logger.info("Loop took: {} seconds".format(duration_sec))
                try:
                    self.import_errors(dagbag)
                except Exception as e:
                    self.logger.exception(e)
                try:
                    dagbag.kill_zombies()
                except Exception as e:
                    self.logger.exception(e)
                try:
                    # We really just want the scheduler to never ever stop.
                    executor.heartbeat()
                    self.heartbeat()
                except Exception as e:
                    self.logger.exception(e)
                    self.logger.error("Tachycardia!")
            except Exception as deep_e:
                self.logger.exception(deep_e)
                raise
            finally:
                settings.Session.remove()
        executor.end()

    def heartbeat_callback(self):
        Stats.gauge('scheduler_heartbeat', 1, 1)


class BackfillJob(BaseJob):
    """
    A backfill job consists of a dag or subdag for a specific time range. It
    triggers a set of task instance runs, in the right order and lasts for
    as long as it takes for the set of task instance to be completed.
    """

    __mapper_args__ = {
        'polymorphic_identity': 'BackfillJob'
    }

    def __init__(
            self,
            dag, start_date=None, end_date=None, mark_success=False,
            include_adhoc=False,
            donot_pickle=False,
            ignore_dependencies=False,
            ignore_first_depends_on_past=False,
            pool=None,
            *args, **kwargs):
        self.dag = dag
        self.dag_id = dag.dag_id
        self.bf_start_date = start_date
        self.bf_end_date = end_date
        self.mark_success = mark_success
        self.include_adhoc = include_adhoc
        self.donot_pickle = donot_pickle
        self.ignore_dependencies = ignore_dependencies
        self.ignore_first_depends_on_past = ignore_first_depends_on_past
        self.pool = pool
        super(BackfillJob, self).__init__(*args, **kwargs)

    def _execute(self):
        """
        Runs a dag for a specified date range.
        """
        session = settings.Session()

        start_date = self.bf_start_date
        end_date = self.bf_end_date

        # picklin'
        pickle_id = None
        if not self.donot_pickle and self.executor.__class__ not in (
                executors.LocalExecutor, executors.SequentialExecutor):
            pickle = models.DagPickle(self.dag)
            session.add(pickle)
            session.commit()
            pickle_id = pickle.id

        executor = self.executor
        executor.start()
        executor_fails = Counter()

        # Build a list of all instances to run
        tasks_to_run = {}
        failed = set()
        succeeded = set()
        started = set()
        skipped = set()
        not_ready = set()
        deadlocked = set()

        for task in self.dag.tasks:
            if (not self.include_adhoc) and task.adhoc:
                continue

            start_date = start_date or task.start_date
            end_date = end_date or task.end_date or datetime.now()
            for dttm in self.dag.date_range(start_date, end_date=end_date):
                ti = models.TaskInstance(task, dttm)
                tasks_to_run[ti.key] = ti
                session.merge(ti)
        session.commit()

        # Triggering what is ready to get triggered
        while tasks_to_run and not deadlocked:
            not_ready.clear()
            for key, ti in list(tasks_to_run.items()):

                ti.refresh_from_db()
                ignore_depends_on_past = (
                    self.ignore_first_depends_on_past and
                    ti.execution_date == (start_date or ti.start_date))

                # The task was already marked successful or skipped by a
                # different Job. Don't rerun it.
                if key not in started:
                    if ti.state == State.SUCCESS:
                        succeeded.add(key)
                        tasks_to_run.pop(key)
                        continue
                    elif ti.state == State.SKIPPED:
                        skipped.add(key)
                        tasks_to_run.pop(key)
                        continue

                # Is the task runnable? -- then run it
                if ti.is_queueable(
                        include_queued=True,
                        ignore_depends_on_past=ignore_depends_on_past,
                        flag_upstream_failed=True):
                    self.logger.debug('Sending {} to executor'.format(ti))
                    executor.queue_task_instance(
                        ti,
                        mark_success=self.mark_success,
                        pickle_id=pickle_id,
                        ignore_dependencies=self.ignore_dependencies,
                        ignore_depends_on_past=ignore_depends_on_past,
                        pool=self.pool)
                    started.add(key)

                # Mark the task as not ready to run
                elif ti.state in (State.NONE, State.UPSTREAM_FAILED):
                    not_ready.add(key)

            self.heartbeat()
            executor.heartbeat()

            # If the set of tasks that aren't ready ever equals the set of
            # tasks to run, then the backfill is deadlocked
            if not_ready and not_ready == set(tasks_to_run):
                deadlocked.update(tasks_to_run.values())
                tasks_to_run.clear()

            # Reacting to events
            for key, state in list(executor.get_event_buffer().items()):
                dag_id, task_id, execution_date = key
                if key not in tasks_to_run:
                    continue
                ti = tasks_to_run[key]
                ti.refresh_from_db()

                # executor reports failure
                if state == State.FAILED:

                    # task reports running
                    if ti.state == State.RUNNING:
                        msg = (
                            'Executor reports that task instance {} failed '
                            'although the task says it is running.'.format(key))
                        self.logger.error(msg)
                        ti.handle_failure(msg)
                        tasks_to_run.pop(key)

                    # task reports skipped
                    elif ti.state == State.SKIPPED:
                        self.logger.error("Skipping {} ".format(key))
                        skipped.add(key)
                        tasks_to_run.pop(key)

                    # anything else is a failure
                    else:
                        self.logger.error("Task instance {} failed".format(key))
                        failed.add(key)
                        tasks_to_run.pop(key)

                # executor reports success
                elif state == State.SUCCESS:

                    # task reports success
                    if ti.state == State.SUCCESS:
                        self.logger.info(
                            'Task instance {} succeeded'.format(key))
                        succeeded.add(key)
                        tasks_to_run.pop(key)

                    # task reports failure
                    elif ti.state == State.FAILED:
                        self.logger.error("Task instance {} failed".format(key))
                        failed.add(key)
                        tasks_to_run.pop(key)

                    # task reports skipped
                    elif ti.state == State.SKIPPED:
                        self.logger.info("Task instance {} skipped".format(key))
                        skipped.add(key)
                        tasks_to_run.pop(key)

                    # this probably won't ever be triggered
                    elif ti in not_ready:
                        self.logger.info(
                            "{} wasn't expected to run, but it did".format(ti))

                    # executor reports success but task does not - this is weird
                    elif ti.state not in (
                            State.SUCCESS,
                            State.QUEUED,
                            State.UP_FOR_RETRY):
                        self.logger.error(
                            "The airflow run command failed "
                            "at reporting an error. This should not occur "
                            "in normal circumstances. Task state is '{}',"
                            "reported state is '{}'. TI is {}"
                            "".format(ti.state, state, ti))

                        # if the executor fails 3 or more times, stop trying to
                        # run the task
                        executor_fails[key] += 1
                        if executor_fails[key] >= 3:
                            msg = (
                                'The airflow run command failed to report an '
                                'error for task {} three or more times. The '
                                'task is being marked as failed. This is very '
                                'unusual and probably means that an error is '
                                'taking place before the task even '
                                'starts.'.format(key))
                            self.logger.error(msg)
                            ti.handle_failure(msg)
                            tasks_to_run.pop(key)

            msg = ' | '.join([
                "[backfill progress]",
                "waiting: {0}",
                "succeeded: {1}",
                "kicked_off: {2}",
                "failed: {3}",
                "skipped: {4}",
                "deadlocked: {5}"
            ]).format(
                len(tasks_to_run),
                len(succeeded),
                len(started),
                len(failed),
                len(skipped),
                len(deadlocked))
            self.logger.info(msg)

        executor.end()
        session.close()

        err = ''
        if failed:
            err += (
                "---------------------------------------------------\n"
                "Some task instances failed:\n{}\n".format(failed))
        if deadlocked:
            err += (
                '---------------------------------------------------\n'
                'BackfillJob is deadlocked.')
            deadlocked_depends_on_past = any(
                t.are_dependencies_met() != t.are_dependencies_met(
                    ignore_depends_on_past=True)
                for t in deadlocked)
            if deadlocked_depends_on_past:
                err += (
                    'Some of the deadlocked tasks were unable to run because '
                    'of "depends_on_past" relationships. Try running the '
                    'backfill with the option '
                    '"ignore_first_depends_on_past=True" or passing "-I" at '
                    'the command line.')
            err += ' These tasks were unable to run:\n{}\n'.format(deadlocked)
        if err:
            raise AirflowException(err)

        self.logger.info("Backfill done. Exiting.")


class LocalTaskJob(BaseJob):

    __mapper_args__ = {
        'polymorphic_identity': 'LocalTaskJob'
    }

    def __init__(
            self,
            task_instance,
            ignore_dependencies=False,
            ignore_depends_on_past=False,
            force=False,
            mark_success=False,
            pickle_id=None,
            pool=None,
            *args, **kwargs):
        self.task_instance = task_instance
        self.ignore_dependencies = ignore_dependencies
        self.ignore_depends_on_past = ignore_depends_on_past
        self.force = force
        self.pool = pool
        self.pickle_id = pickle_id
        self.mark_success = mark_success
        super(LocalTaskJob, self).__init__(*args, **kwargs)

    def _execute(self):
        command = self.task_instance.command(
            raw=True,
            ignore_dependencies=self.ignore_dependencies,
            ignore_depends_on_past=self.ignore_depends_on_past,
            force=self.force,
            pickle_id=self.pickle_id,
            mark_success=self.mark_success,
            job_id=self.id,
            pool=self.pool,
        )
        self.process = subprocess.Popen(['bash', '-c', command])
        return_code = None
        while return_code is None:
            self.heartbeat()
            return_code = self.process.poll()

    def on_kill(self):
        self.process.terminate()

    """
    def heartbeat_callback(self):
        if datetime.now() - self.start_date < timedelta(seconds=300):
            return
        # Suicide pill
        TI = models.TaskInstance
        ti = self.task_instance
        session = settings.Session()
        state = session.query(TI.state).filter(
            TI.dag_id==ti.dag_id, TI.task_id==ti.task_id,
            TI.execution_date==ti.execution_date).scalar()
        session.commit()
        session.close()
        if state != State.RUNNING:
            logging.warning(
                "State of this instance has been externally set to "
                "{self.task_instance.state}. "
                "Taking the poison pill. So long.".format(**locals()))
            self.process.terminate()
    """
