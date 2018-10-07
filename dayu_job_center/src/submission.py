#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'andyguo'

import datetime

from config import *


class SubmissionBase(object):
    total = 1.0
    start_time = None
    progress = 0.0
    status = None

    def __init__(self, cmd, total):
        self.cmd = cmd
        self.total = total

    def setup(self):
        self.start_time = datetime.datetime.now()
        self.progress = 0.0

    def update_status(self, current):
        self.progress = current / self.total

    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError


class CopySubmission(SubmissionBase):
    def start(self):
        from unipath import Path
        self.setup()
        self.status = JOB_RUNNING

        current = 0.0
        from_files = self.cmd.get('from_files', [])
        to_files = self.cmd.get('to_files', [])
        for file_pair in zip(from_files, to_files):
            self.update_status(current)
            yield

            from_file = Path(file_pair[0])
            to_file = Path(file_pair[1])
            to_files.parent.mkdir(parents=True)
            from_file.copy(to_file)
            current += 1
        else:
            self.status = JOB_FINISHED
            yield


class TestSubmission(SubmissionBase):
    pass


class RandomSleepSubmission(TestSubmission):
    def start(self):
        import random
        import time
        self.setup()

        self.status = JOB_RUNNING
        current = 0.0
        while current <= self.total:
            self.update_status(current)
            yield
            time.sleep(random.random())
            current += 1

        self.status = JOB_FINISHED
