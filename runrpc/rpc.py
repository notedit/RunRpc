# -*- coding: utf-8 -*-
# file: rpc.py
# author: notedit
# date: 20111207

import re
import pymongo

from bson import BSON
from pymongo import Connection
from tornado.netutil import bind_sockets
from tornado.netutil import bind_unix_socket
from tornado.process import fork_processes
from tornado.ioloop  import IOLoop

from rpcserver import BackendError
from rpcserver import RpcServer


remote_funcmapping = {}
def remote():
    global remote_funcmapping
    def inter1(func):
        funcname = func.__name__
        if not remote_funcmapping.has_key(funcname):
            remote_funcmapping[funcname] = func
        else:
            raise KeyError('%s:funcname declare more than once'%repr(funcname))
        def inter2(*args,**kwargs):
            return func(*args,**kwargs)
        return inter2
    return lambda func:inter1(func)

class RpcClient(object):

    def __init__(self,host='localhost',port=27018):
        self.__connection = Connection(host,port,max_pool_size=10)
        return

    def __getattr__(self,funcname):
        if funcname.startwith('_'):
            return "you are trying to call %s from our backend"%funcname
        else:
            func = lambda *args,**kwargs:self.__call__(funcname,*args,**kwargs)
            return func

    def __call__(self,funcname,*args,**kwargs):
        argstr = BSON.encode((args,kwargs))
        querydict = {'funcname':funcname,'argstr':argstr}
        try:
            collection = self.__connection['backend']['rpc']
            ret = collection.find_one(querydict)
            
            return ret['result']
        finally:
            if self.__connection:
                self.__connection.end_request()

def start(port=27018,funcmapping={}):
    sockets = bind_sockets(port)
    fork_processes(2)
    s = RpcServer(funcmapping=remote_funcmapping)
    s.add_sockets(sockets)
    IOLoop.instance().start()

def start_with_unix_socket(file='/tmp/backend.rpc'):
    sock = bind_unix_socket(file)
    fork_processes(2)
    s = RpcServer(funcmapping=remote_funcmapping)
    s.add_socket(sock)
    IOLoop.instance().start()



    
