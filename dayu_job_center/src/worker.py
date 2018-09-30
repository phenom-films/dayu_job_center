#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'andyguo'

import time
import random
import zmq
from util import tprint
import json

def simulate_running(worker, socket, message):
    for x in range(5):
        data = json.loads(message[-1])
        data['to'] = worker
        data['progress'] = x / 4.0 * 100.0
        socket.send_multipart([message[0], message[1], json.dumps(data)])
        time.sleep(random.random() / 2.0)


class WorkerBase(object):
    def __init__(self, identity=None):
        self.identity = 'LocalAsyncWorker[{}]'.format(identity)

    def run(self, context=None):
        raise NotImplementedError()


class LocalAsyncWorker(WorkerBase):
    def run(self, context=None):
        context = context or zmq.Context.instance()
        socket = context.socket(zmq.ROUTER)
        socket.connect('inproc://dayu_local_async_server')

        while True:
            message = socket.recv_multipart()
            tprint('{} get {}'.format(self.identity, str(message)))

            if message[-1] == 'DAYU_KILL':
                socket.send_multipart([message[0], message[1], 'DAYU_WORKER_EXIT'])
                break

            simulate_running(self.identity, socket, message)

