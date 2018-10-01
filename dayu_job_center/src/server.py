#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'andyguo'

import zmq
import sys
import time
import random
import threading
from util import tprint
from worker import LocalAsyncWorker
import json


class LocalAsyncServer(object):
    def __init__(self, worker=4):
        self.worker_count = worker
        self.worker_threads = {}
        self.job_list = [{'worker_id'     : None,
                          'op'            : 'data',
                          'job_id'        : 'job_{}'.format(x),
                          'status'        : None,
                          'data'          : x,
                          'message'       : None,
                          'start_time'    : None,
                          'elapse_time'   : None,
                          'remaining_time': None,
                          'progress'      : 0.0} for x in range(12)]
        self.running_list = []
        self.done_list = []
        self.is_running = False

    def resume(self, index):
        if self.is_running is False:
            return
        # print self.running_list[index]
        worker_id = self.running_list[index]['worker_id']
        self.control_socket.send('{}_RESUME'.format(worker_id))

    def pause(self, index):
        if self.is_running is False:
            return
        # print self.running_list[index]
        worker_id = self.running_list[index]['worker_id']
        self.control_socket.send('{}_PAUSE'.format(worker_id))

    def _update_job_info(self, obj):
        job_id_list = [x['job_id'] for x in self.running_list]
        job_index = job_id_list.index(obj['job_id'])
        self.running_list[job_index].update(obj)
        return job_index

    def run(self):
        self.context = zmq.Context.instance()
        self.data_socket = self.context.socket(zmq.ROUTER)
        self.data_socket.bind('inproc://backend')
        self.control_socket = self.context.socket(zmq.PUB)
        self.control_socket.bind('inproc://worker_control')

        self.is_running = True
        for x in range(self.worker_count):
            w = LocalAsyncWorker(x)
            t = threading.Thread(target=w.run)
            self.worker_threads[w.identity] = t
            t.start()
        tprint('==== start worker thread ====')

        available_worker = []
        while len(available_worker) < self.worker_count:
            message = self.data_socket.recv_multipart()
            tprint(message)
            if message[-1] == 'WORKER_READY':
                available_worker.append(message[0])
        tprint('==== all worker thread ready ====')

        while self.job_list or self.running_list:
            print '---------------'
            while available_worker and self.job_list:
                assign_job = self.job_list.pop(0)
                assign_worker = available_worker.pop(0)
                assign_job['worker_id'] = assign_worker
                self.running_list.append(assign_job)
                self.data_socket.send_multipart([assign_worker, json.dumps(assign_job)])

            message = self.data_socket.recv_multipart()
            obj = json.loads(message[-1])
            job_index = self._update_job_info(obj)

            if obj['status'] == 'DAYU_FINISH':
                available_worker.append(message[0])
                self.done_list.append(self.running_list.pop(job_index))

            print [(x['job_id'], x['status']) for x in self.running_list]

        for w in self.worker_threads:
            self.control_socket.send('{}_KILL'.format(w))
        self.is_running = False
        print 'exit'
        for t in self.worker_threads:
            print self.worker_threads[t].is_alive()


if __name__ == '__main__':
    server = LocalAsyncServer()
    server.run()
