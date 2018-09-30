#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'andyguo'

import sys

def tprint(msg):
    """like print, but won't get newlines confused with multiple threads"""
    sys.stdout.write(msg + '\n')
    sys.stdout.flush()

