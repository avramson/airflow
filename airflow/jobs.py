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
from itertools import product
import getpass
import logging
import socket
import subprocess
import multiprocessing
import math
from time import sleep

from sqlalchemy import Column, Integer, String, DateTime, func, Index, or_
from sqlalchemy.orm.session import make_transient

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
        self.hostname = socket.gethostname()
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
        return self.dag_id_to_simple_dag[dag_id].task_concurrency

    def get_pickle_id(self, dag_id):
        if dag_id not in self.dag_id_to_simple_dag:
            raise AirflowException("Unknown DAG ID {}".format(dag_id))
        return self.dag_id_to_simple_dag[dag_id].pickle_id


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
        self.queued_tis = set()
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

            if next_run_date and dag.end_date and next_run_date > dag.end_date:
                return

            if next_run_date and schedule_end and schedule_end <= datetime.now():
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

        self.logger.info(
            "Prioritizing {} queued jobs".format(len(self.queued_tis)))
        session.expunge_all()
        d = defaultdict(list)
        for ti in self.queued_tis:
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

    @provide_session
    def delete_defunct_queued_task_instances(self, session, simple_dagbag):
        """Queries the ORM for task instances in the queued state and deletes
        ones that correspond to a known definition in the SimpleDagBag"""
        TI = models.TaskInstance
        queued_task_instances = (
            session
                .query(TI)
                .filter(TI.state == State.QUEUED)
                .all()
        )
        for task_instance in queued_task_instances:
            if (simple_dagbag.exists(task_instance.dag_id, task_instance.task_id)):
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
    def execute_queued_task_instances(self,
                                      session,
                                      executor,
                                      simple_dag_bag,
                                      task_execution_infos=TaskExecutionInfo()):
        """Fetches task instances from ORM, figures out pool limits, and
        sends them to the executor for execution"""
        # Get all the queued task instances
        TI = models.TaskInstance
        queued_task_instances = (
            session
                .query(TI)
                .filter(TI.state == State.QUEUED)
                .all()
        )

        # Get the pool settings
        pools = {p.pool: p for p in session.query(models.Pool).all()}

        pool_to_task_instances = defaultdict(list)
        for task_instance in queued_task_instances:
                pool_to_task_instances[task_instance.pool].append(task_instance)

        # Go through each pool, and queue up a task for execution if there are
        # slots open in the pool.
        for pool, task_instances in list(pool_to_task_instances):
            self.logger.info("Processing tasks for pool={}".format(pool))
            if not pool:
                # Arbitrary:
                # If queued outside of a pool, trigger no more than
                # non_pooled_task_slot_count per run
                open_slots = conf.getint('core', 'non_pooled_task_slot_count')
            else:
                open_slots = pools[pool].open_slots(session=session)

            num_queued = len(task_instances)
            self.logger.info("Pool {pool} has {open_slots} slots with {num_queued} "
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
                                                  simple_dag_bag.get_tasks_ids(dag_id)) >
                        task_concurrency_limit):
                    dag_ids_at_max_concurrency.append(dag_id)
                    self.logger.info("Not executing queued {} since the number "
                                     "of tasks running from DAG {} exceeds the "
                                     "DAG's concurrency limit of {}"
                                     .format(task_instance, dag_id, task_concurrency_limit))
                    continue

                if not task_instance.are_dependencies_met():
                    # A queued task instance may not have its dependencies met if the DAG definition
                    # has changed.
                    self.logger.info("Deleting queued {} as the dependencies are not met")
                    session.delete(task_instance)
                    continue

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
                    pickle_id=simple_dag_bag.get_pickle_id(task_instance.dag_id, task_instance.task_id))

                priority = task_instance.priority_weight
                queue = task_instance.queue
                self.logger.info("Sending to executor {} with priority {} and queue {}"
                                 .format(task_instance.key, priority, queue ))
                executor.queue_command(
                    task_instance.key,
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

    def _execute_modified(self):
        pessimistic_connection_handling()

        logging.basicConfig(level=logging.DEBUG)
        self.logger.info("Starting the scheduler")

        # Build up a list of Python files that could contain DAGs
        file_paths = dag_utils.list_py_file_paths(self.subdir)
        executor = DEFAULT_EXECUTOR
        executor.start()

        self.runs = 0

        # DAGs can be pickled for easier remote execution by some executors
        pickle_dags = False
        if self.do_pickle and self.executor.__class__ not in (
               executors.LocalExecutor, executors.SequentialExecutor):
            pickle_dags = True

        while not self.num_runs or self.num_runs > self.runs:
            self.runs += 1
            loop_start_dttm = datetime.now()
            try:
                results = []

                # Read each file for DAGs, figure out the tasks that can be
                # run, and create those task instances in the ORM
                for file_path in file_paths:
                    self.logger.info("Processing DAGs in file {}".format(file_path))
                    result = self._process_dags_in_one_file(file_path, executor, pickle_dags)
                    results.append(result)

                # Combine the SimpleDagBags and TaskExecutionInfos from each file
                all_simple_dags = []
                all_task_execution_infos = []
                for result in results:
                    all_simple_dags.extend(result.simple_dag_bag.simple_dags)
                    all_task_execution_infos.extend(result.task_execution_infos)
                simple_dag_bag = SimpleDagBag(all_simple_dags)


                # Before scheduling any tasks, delete any queued instances
                # that are not a part of any known DAG. They may be defunct
                # because they were created with an earlier version of a DAG,
                # or the DAG was deleted after the tasks were created.
                self.delete_defunct_queued_task_instances(simple_dag_bag)

                # All tasks should exist in the ORM in the queued state at
                # this point, so it's possible to prioritize based on the
                # pool and execute them.
                self.logger.info("Examining queued tasks and sending to the "
                                 "executor")
                self.execute_queued_task_instances(executor, simple_dag_bag)

                self.logger.info("Heartbeating the executor")
                duration_sec = (datetime.now() - loop_start_dttm).total_seconds()
                self.logger.info("Loop took: {} seconds".format(duration_sec))

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

    @provide_session
    def _process_dags_in_one_file(self, session, file_path, pickle_dags=True):
        """Loads the specified file and for each dag in the file, pickle
        the dag, figure out what tasks can be run, and create respective task
        instances in the ORM"""

        # As DAGs are parsed from this file, they will be converted into SimpleDags
        simple_dags = []
        task_execution_infos = []
        try:
            TI = models.TaskInstance

            try:
                dagbag = models.DagBag(file_path, sync_to_db=True)
            except Exception as e:
                self.logger.error("Failed at reloading the DAG file {}".format(file_path))
                Stats.incr('dag_file_refresh_error', 1, 1)
                return ProcessDagFileResult([], [])

            # Pickle the DAGs (if necessary) and put them into a SimpleDag
            for dag in dagbag.dags:
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
        finally:
            # TODO: Figure out if this works in multiprocessing mode
            # TODO: Should this be moved to _execute()?
            settings.Session.remove()

    # This is the modified _execute()
    def _execute(self):
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
                    self.process_events(executor=executor, dagbag=dagbag)
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
                for j in jobs:
                    j.join()

                while not tis_q.empty():
                    ti_key, pickle_id = tis_q.get()
                    dag = dagbag.dags[ti_key[0]]
                    task = dag.get_task(ti_key[1])
                    ti = TI(task, ti_key[2])
                    self.executor.queue_task_instance(ti, pickle_id=pickle_id)

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
