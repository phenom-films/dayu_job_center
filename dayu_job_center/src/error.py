#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'andyguo'


class WorkerExitError(KeyboardInterrupt):
    pass


class JobStopError(KeyboardInterrupt):
    pass
