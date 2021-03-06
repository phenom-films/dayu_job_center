#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'andyguo'

import datetime
import json
import threading

import zmq

from config import *
from util import tprint, recursive_update
from worker import LocalAsyncWorker


class ServerBase(object):
    worker_count = None
    workers = {}
    available_workers = []
    waiting_jobs = []
    running_jobs = []
    finished_jobs = []
    is_running = False

    def setup_connection(self):
        raise NotImplementedError

    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def pause(self, running_job_index):
        raise NotImplementedError

    def resume(self, running_job_index):
        raise NotImplementedError

    def abort(self, running_job_index):
        raise NotImplementedError

    def add_job(self, job):
        raise NotImplementedError

    def add_job_group(self, job_group):
        raise NotImplementedError


class LocalAsyncServer(ServerBase):
    def __init__(self, worker=4):
        self.worker_count = worker
        self.waiting_jobs = []
        # self.waiting_jobs = [{'name'                 : u'哈哈',
        #                       'label'                : None,
        #                       'job_id'               : b'job_{}'.format(x),
        #                       'worker_id'            : None,
        #                       'submission_type'      : 'RandomSleepSubmission',
        #                       'status'               : JOB_READY,
        #                       'job_data'             : x,
        #                       'job_total'            : 3,
        #                       'message'              : None,
        #                       'start_time'           : None,
        #                       'elapse_time'          : '0:00:00',
        #                       'remaining_time'       : '0:00:00',
        #                       'progress'             : 0.0,
        #                       'before_start_callback': {'func': None, 'args': None, 'kwargs': None},
        #                       'after_finish_callback': {'func': None, 'args': None, 'kwargs': None}}
        #                      for x in range(12)]
        self.running_jobs = []
        self.finished_jobs = []
        self.is_running = False
        self.context = zmq.Context.instance()

    def add_job(self, job):
        self.waiting_jobs.append(job)

    def add_job_group(self, job_group):
        self.waiting_jobs.append(job_group)

    def resume(self, running_job_index):
        if self.is_running is False:
            return
        worker_id = self.running_jobs[running_job_index]['worker_id'].encode('ascii')
        self.control_socket.send_multipart([worker_id, JOB_RESUME])

    def pause(self, running_job_index):
        if self.is_running is False:
            return
        worker_id = self.running_jobs[running_job_index]['worker_id'].encode('ascii')
        self.control_socket.send_multipart([worker_id, JOB_PAUSE])

    def abort(self, running_job_index):
        worker_id = self.running_jobs[running_job_index]['worker_id'].encode('ascii')
        self.control_socket.send_multipart([worker_id, JOB_STOP])

    def stop(self):
        for w in self.workers:
            self.control_socket.send_multipart([w, WORKER_EXIT])

        exit_worker_count = 0
        while exit_worker_count < self.worker_count:
            message = self.control_socket.recv_multipart()
            if message[-1] == WORKER_EXIT:
                exit_worker_count += 1

        self.is_running = False
        self.job_socket.close()
        self.control_socket.close()
        self.context.term()
        print 'server stop!'
        # for t in self.worker_threads:
        #     print self.worker_threads[t].is_alive()

    def setup_connection(self):
        self.context = zmq.Context.instance()
        self.job_socket = self.context.socket(zmq.ROUTER)
        self.job_socket.bind('inproc://backend')
        self.control_socket = self.context.socket(zmq.ROUTER)
        self.control_socket.bind('inproc://worker_control')

    def spawn_workers(self):
        for x in range(self.worker_count):
            w = LocalAsyncWorker(x)
            t = threading.Thread(target=w.start)
            self.workers[w.identity] = {'thread'               : t,
                                        'before_start_callback': {'func': None, 'args': None, 'kwargs': None},
                                        'after_finish_callback': {'func': None, 'args': None, 'kwargs': None}}
            t.start()
        tprint('==== start worker thread ====')

        while len(self.available_workers) < self.worker_count:
            message = self.control_socket.recv_multipart()
            if message[-1] == WORKER_IDLE:
                self.available_workers.append(message[0])
        tprint('==== all worker thread ready ====')

    def send_job(self):
        while self.available_workers and self.waiting_jobs:
            job = self.waiting_jobs.pop(0)
            worker_id = self.available_workers.pop(0)
            self.before_start(job, worker_id)

            job['worker_id'] = worker_id
            self.running_jobs.append(job)
            self.job_socket.send_multipart([worker_id, json.dumps(job)])

    def before_start(self, job, worker_id):
        self.workers[worker_id]['before_start_callback'].update(job.pop('before_start_callback'))
        self.workers[worker_id]['after_finish_callback'].update(job.pop('after_finish_callback'))
        func = self.workers[worker_id]['before_start_callback']['func']
        if func:
            func(*self.workers[worker_id]['before_start_callback']['args'],
                 **self.workers[worker_id]['before_start_callback']['kwargs'])

    def update_time(self, job):
        _progress = job['progress']
        if _progress != 0:
            _delta = datetime.datetime.now() - datetime.datetime.strptime(job['start_time'], DATETIME_FORMATTER)
            job['elapse_time'] = str(_delta).split('.')[0]
            job['remaining_time'] = \
                str(datetime.timedelta(seconds=_delta.total_seconds() * (1.0 - _progress) / _progress)).split('.')[0]

    def update_jobs(self):
        message = self.job_socket.recv_multipart()
        job = json.loads(message[-1])
        job_id_list = [x['job_id'] for x in self.running_jobs]
        job_index = job_id_list.index(job['job_id'])
        recursive_update(self.running_jobs[job_index], (job))

        for j in self.running_jobs:
            self.update_time(j)
            if j.has_key('children_jobs'):
                for single_job in j['children_jobs']:
                    self.update_time(single_job)

        if job['status'] == JOB_FINISHED:
            worker_id = job['worker_id']
            self.after_finish(job, worker_id)

            self.available_workers.append(message[0])
            self.finished_jobs.append(self.running_jobs.pop(job_index))

        if job['status'] in (JOB_ERROR, JOB_UNKNOWN, JOB_STOP):
            self.available_workers.append(message[0])
            self.finished_jobs.append(self.running_jobs.pop(job_index))

    def after_finish(self, job, worker_id):
        func = self.workers[worker_id]['after_finish_callback']['func']
        if func:
            func(*self.workers[worker_id]['after_finish_callback']['args'],
                 **self.workers[worker_id]['after_finish_callback']['kwargs'])

    def start(self):
        from pprint import pprint
        self.is_running = True
        self.setup_connection()
        self.spawn_workers()

        while True:
            if len(self.waiting_jobs) == 0 and len(self.running_jobs) == 0:
                break

            self.send_job()
            self.update_jobs()

            print '---------------'
            pprint(self.running_jobs)
            # print [(x['job_id'], x['progress']) for x in self.running_jobs]

        self.stop()


if __name__ == '__main__':
    from job import *

    a = Job(job_total=3, submission_type='RandomSleepSubmission')
    b = Job(job_total=3, submission_type='RandomSleepSubmission')
    c = Job(job_total=3, submission_type='RandomSleepSubmission')

    jg = JobGroup()
    jg.add_job(a)
    jg.add_job(b)
    jg.add_job(c)

    server = LocalAsyncServer()
    server.add_job_group(jg)
    server.start()
    print server.finished_jobs
