#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'andyguo'

import uuid
from config import *


class Job(dict):
    _important_keys = ('name', 'label', 'job_id', 'worker_id', 'submission_type', 'status',
                       'job_data', 'job_total', 'message', 'start_time', 'elapse_time', 'remaining_time',
                       'progress', 'before_start_callback', 'after_finish_callback')

    def __init__(self,
                 name=None,
                 submission_type=None,
                 job_data=None,
                 job_total=None,
                 **kwargs):
        super(Job, self).__init__(**kwargs)
        self['name'] = name or kwargs.get('name', None)
        self['label'] = kwargs.get('label', self['name'])
        self['job_id'] = kwargs.get('job_id', 'job_{}'.format(uuid.uuid4().hex))
        self['worker_id'] = kwargs.get('worker_id', None)
        self['submission_type'] = submission_type or kwargs.get('submission_type', None)
        self['status'] = kwargs.get('status', JOB_READY)
        self['job_data'] = job_data or kwargs.get('job_data', None)
        self['job_total'] = job_total or kwargs.get('job_total', 1.0)
        self['message'] = kwargs.get('message', None)
        self['start_time'] = kwargs.get('start_time', None)
        self['elapse_time'] = kwargs.get('elapse_time', '0:00:00')
        self['remaining_time'] = kwargs.get('remaining_time', '0:00:00')
        self['progress'] = kwargs.get('progress', 0.0)
        self['before_start_callback'] = {'func': None, 'args': None, 'kwargs': None}
        self['after_finish_callback'] = {'func': None, 'args': None, 'kwargs': None}

    def add_before_start_callback(self, func, args, kwargs):
        self['before_start_callback']['func'] = func
        self['before_start_callback']['args'] = args
        self['before_start_callback']['kwargs'] = kwargs

    def add_after_finish_callback(self, func, args, kwargs):
        self['after_finish_callback']['func'] = func
        self['after_finish_callback']['args'] = args
        self['after_finish_callback']['kwargs'] = kwargs

    def __getattr__(self, item):
        if item in self.__class__._important_keys:
            return self[item]
        else:
            raise KeyError(item)


class JobGroup(dict):
    def __init__(self,
                 name=None,
                 **kwargs):
        super(JobGroup, self).__init__(**kwargs)
        self['name'] = name or kwargs.get('name', None)
        self['label'] = kwargs.get('label', self['name'])
        self['job_id'] = kwargs.get('job_id', 'job_{}'.format(uuid.uuid4().hex))
        self['worker_id'] = kwargs.get('worker_id', None)
        self['submission_type'] = kwargs.get('submission_type', None)
        self['status'] = kwargs.get('status', JOB_READY)
        self['message'] = kwargs.get('message', None)
        self['start_time'] = kwargs.get('start_time', None)
        self['elapse_time'] = kwargs.get('elapse_time', '0:00:00')
        self['remaining_time'] = kwargs.get('remaining_time', '0:00:00')
        self['progress'] = kwargs.get('progress', 0.0)
        self['before_start_callback'] = {'func': None, 'args': None, 'kwargs': None}
        self['after_finish_callback'] = {'func': None, 'args': None, 'kwargs': None}
        self['children_jobs'] = kwargs.get('children_jobs', [])

    def add_job(self, job):
        job = dict(job)
        job.pop('before_start_callback', None)
        job.pop('after_finish_callback', None)
        self['children_jobs'].append(job)

    def add_before_start_callback(self, func, args, kwargs):
        self['before_start_callback']['func'] = func
        self['before_start_callback']['args'] = args
        self['before_start_callback']['kwargs'] = kwargs

    def add_after_finish_callback(self, func, args, kwargs):
        self['after_finish_callback']['func'] = func
        self['after_finish_callback']['args'] = args
        self['after_finish_callback']['kwargs'] = kwargs

    def __getattr__(self, item):
        if item in self.__class__._important_keys:
            return self[item]
        else:
            raise KeyError(item)


if __name__ == '__main__':
    jg = JobGroup()
    print jg
