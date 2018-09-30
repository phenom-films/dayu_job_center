#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'andyguo'

import zmq
import time
import random
import json
from collections import OrderedDict
import uuid


class LocalAsyncClient(object):
    def __init__(self, name=None, port=18765):
        if name is None:
            name = uuid.uuid4().hex
        self.name = name
        self.port = port
        self.job_list = []
        self.done_list = []
        self.exit_flag = False

    def add_job(self, job):
        job.update({'job_id': uuid.uuid4().hex})
        self.job_list.append(job)

    def add_job_group(self, job_group):
        self.job_list.append(job_group)

    def run(self):
        context = zmq.Context.instance()
        socket = context.socket(zmq.DEALER)
        socket.setsockopt(zmq.IDENTITY, '{}'.format(self.name))
        socket.connect('tcp://localhost:{}'.format(self.port))

        for job in self.job_list:
            job.update({'from': self.name, 'to': None})
            socket.send(json.dumps(job))

        while True:
            message = socket.recv_multipart()

            # print message
            data = json.loads(message[-1])
            found_index = [x['job_id'] for x in self.job_list].index(data['job_id'])
            self.job_list[found_index] = data

            if data['progress'] == 100.0:
                self.done_list.append(self.job_list.pop(found_index))

            print '========================='
            print self.job_list
            print self.done_list

            if not self.job_list:
                socket.send('DAYU_KILL')
                reply_message = socket.recv_multipart()
                while self.exit_flag:
                    if reply_message[-1] == 'DAYU_SERVER_EXIT':
                        self.exit_flag = True
                break

        print 'exit!'


if __name__ == '__main__':
    c = LocalAsyncClient()
    c.add_job({'op': 'copy'})
    c.add_job({'op': 'delete'})
    c.add_job({'op': 'mov'})

    c.run()
