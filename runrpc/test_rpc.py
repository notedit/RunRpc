# -*- encoding: utf-8 -*-
# File: test_rpc.py
# Date: 20111210
# Author: notedit

import time
import rpc

@rpc.remote()
def now():
    return time.strftime('%Y-%m-%d %H:%M:%S')

@rpc.remote()
def add(x,y):
    return x+y

@rpc.remote()
def inc2(x):
    return x+2

@rpc.remote()
def raiseerror():
    raise rpc.BackendError('dfdfdf','dfdfdfdfdfdfdf')


if __name__ == '__main__':
    rpc.start()
