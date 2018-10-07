#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'andyguo'

import sys


def tprint(msg):
    """like print, but won't get newlines confused with multiple threads"""
    sys.stdout.write(str(msg) + '\n')
    sys.stdout.flush()


def recursive_update(origin_dict, new_dict):
    for k, v in new_dict.items():
        if isinstance(v, dict) and isinstance(origin_dict.get(k, None), dict):
            recursive_update(origin_dict[k], v)
        origin_dict[k] = v
