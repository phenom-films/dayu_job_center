#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'andyguo'

import time
import random
import zmq
from util import tprint
import json


class WorkerBase(object):
    def __init__(self, identity=None):
        self.identity = 'LocalAsyncWorker[{}]'.format(identity)

    def run(self):
        raise NotImplementedError()


class LocalAsyncWorker(WorkerBase):
    def check_control_signal(self):
        signal = None
        try:
            signal = self.control_socket.recv(zmq.NOBLOCK)
        except:
            return

        try:
            tprint('============================ {}: {}'.format(self.identity, signal))
            if signal == '{}_KILL'.format(self.identity):
                raise zmq.ZMQError()
            if signal == '{}_PAUSE'.format(self.identity):
                while self.control_socket.recv() != '{}_RESUME'.format(self.identity):
                    return
        except Exception as e:
            raise

    def run(self):
        self.context = zmq.Context.instance()
        self.data_socket = self.context.socket(zmq.DEALER)
        self.data_socket.setsockopt(zmq.IDENTITY, self.identity)
        self.data_socket.connect('inproc://backend')

        self.control_socket = self.context.socket(zmq.SUB)
        self.control_socket.setsockopt(zmq.SUBSCRIBE, self.identity)
        self.control_socket.connect('inproc://worker_control')

        self.data_socket.send('WORKER_READY')

        poller = zmq.Poller()
        poller.register(self.data_socket, zmq.POLLIN)
        poller.register(self.control_socket, zmq.POLLIN)

        try:
            while True:
                sockets = dict(poller.poll(500))
                if self.control_socket in sockets:
                    self.check_control_signal()

                if self.data_socket in sockets:
                    message = self.data_socket.recv_multipart()
                    obj = json.loads(message[-1])
                    obj['worker_id'] = self.identity

                    count = 5
                    for x in range(count):
                        self.check_control_signal()

                        obj['status'] = float(x) / count
                        self.data_socket.send(json.dumps(obj))
                        time.sleep(random.random())

                    obj['status'] = 'DAYU_FINISH'
                    self.data_socket.send(json.dumps(obj))


        except Exception as e:
            pass
