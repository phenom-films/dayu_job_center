#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'andyguo'

import datetime

from config import *


class SubmissionBase(object):
    total = 1
    start_time = None
    progress = 0.0
    status = None

    def __init__(self, cmd, total):
        self.cmd = cmd
        self.total = total

    def setup(self):
        pass

    def update_status(self, current):
        raise NotImplementedError

    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError


class TestSubmission(SubmissionBase):
    pass


class RandomSleepSubmission(TestSubmission):
    def setup(self):
        super(RandomSleepSubmission, self).setup()
        self.start_time = datetime.datetime.now()
        self.progress = 0.0

    def update_status(self, current):
        self.progress = current / self.total

    def start(self):
        import random
        import time
        self.setup()

        self.status = JOB_RUNNING
        current = 0.0
        while current <= self.total:
            time.sleep(random.random())
            yield
            current += 1
            self.update_status(current)

        self.status = JOB_FINISHED
