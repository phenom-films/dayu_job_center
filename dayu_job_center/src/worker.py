#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'andyguo'

import json

import zmq

from config import *
from error import WorkerExitError
from util import tprint


class WorkerBase(object):
    worker_type = None
    is_running = False
    current_job = None
    current_submission = None

    def __init__(self, identity=None):
        if identity is None:
            import uuid
            identity = uuid.uuid4().hex
        self.identity = '{}_{}'.format(self.worker_type, identity)
        self.context = zmq.Context.instance()

    def setup_connection(self):
        raise NotImplementedError

    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def reset(self):
        self.current_job = None
        self.current_submission = None

    def update_job(self):
        self.current_job['worker_id'] = self.identity
        self.current_job['status'] = self.current_submission.status
        self.current_job['start_time'] = self.current_submission.start_time.strftime(DATETIME_FORMATTER)
        self.current_job['progress'] = self.current_submission.progress


    def job_to_submission(self, job):
        raise NotImplementedError

    def check_control_signal(self):
        raise NotImplementedError


class LocalAsyncWorker(WorkerBase):
    worker_type = 'LocalAsyncWorker'
    job_socket = None
    control_socket = None
    poller = None

    def setup_connection(self):
        self.job_socket = self.context.socket(zmq.DEALER)
        self.job_socket.identity = self.identity
        self.job_socket.connect('inproc://backend')

        self.control_socket = self.context.socket(zmq.DEALER)
        self.control_socket.identity = self.identity
        self.control_socket.connect('inproc://worker_control')

        self.control_socket.send(WORKER_IDLE)

        self.poller = zmq.Poller()
        self.poller.register(self.job_socket, zmq.POLLIN)
        self.poller.register(self.control_socket, zmq.POLLIN)

    def check_control_signal(self):
        if self.control_socket.poll(0):
            signal = self.control_socket.recv()
            tprint('============================ {}: {}'.format(self.identity, signal))
            if signal == WORKER_EXIT:
                raise WorkerExitError()
            if signal == JOB_PAUSE:
                while self.control_socket.recv() != JOB_RESUME:
                    pass

    def job_to_submission(self, job, submission_class=None):
        if submission_class is None:
            import submission
            submission_class = getattr(submission, job['submission_type'])
        instance = submission_class(job['job_data'], job['job_total'])
        return instance

    def start(self):
        self.is_running = True
        self.setup_connection()

        while True:
            try:
                sockets = dict(self.poller.poll(500))
                if self.control_socket in sockets:
                    self.check_control_signal()

                if self.job_socket in sockets:
                    self.reset()
                    message = self.job_socket.recv_multipart()
                    self.current_job = json.loads(message[-1])
                    self.current_submission = self.job_to_submission(self.current_job)

                    for _ in self.current_submission.start():
                        self.update_job()
                        self.check_control_signal()
                        self.job_socket.send(json.dumps(self.current_job))

                    self.update_job()
                    self.job_socket.send(json.dumps(self.current_job))


            except WorkerExitError as e:
                break

        self.is_running = False
