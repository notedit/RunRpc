# -*- coding: utf-8 -*-
# file: rpcserver.py
# author: notedit
# date: 20111207

import struct
import bson
from bson import BSON
from tornado.netutil import TCPServer
from tornado.netutil import bind_sockets

'''mongo protocol'''

QUERY = '$query'
RESULT = 'result'
ADMIN_CMD = 'admin.$cmd'
COLLECTION = 'backend.rpc'
CMD_MAPPING = {
        'ismaster':{'ismaster':1,'msg':'not paired','ok':1.0},
         }


class BackendError(Exception):

    def __init__(self,message,detail):
        self.message = message
        self.detail = detail

    def __str__(self):
        return 'BackendError(%s,%s)'%(self.message,self.detail)

    def __repr__(self):
        return 'BackendError(%s,%s)'%(self.message,self.detail)

class MongoProtocol(object):

    def __init__(self):
        self.__id = 0
        self.__data_length = None
        self.__request_id = 0

    def dataRecieved(self,stream,address):
        self.stream = stream
        stream.read_bytes(16,self.handleheader)


    def handleheader(self,msgheader):
        self.__data_length,self.__request_id,_,op_code = struct.unpack('<iiii',msgheader)
        if op_code != 2004:
            #todo write something and then close the iostream
            retdict = {'$error':{'message':'InternalError',
                                 'detail':'rpc can not support this operation'}}
            self.sendMessage(bson.BSON.encode(retdict))
            return
        try:
            self.stream.read_bytes(self.__data_length-16,self.handlequery)
        except:
            pass

    def handlequery(self,query):
        #flags,packet = query[:4],query[4:]
        #skip the skip and limit
        try:
            self.__collection,pos = bson._get_c_string(query,4)
            query = query[pos+8:]
            spec = bson.BSON(query).decode()
        except:
            retdict = {'$error':{'message':'InternalError',
                                 'detail':'unvalid bson'}}
            self.sendMessage(bson.BSON.encode(retdict))
            self.stream.close()

        if self.__collection == ADMIN_CMD:
            if spec.get('ismaster',''):
                self.sendMessage(bson.BSON.encode(CMD_MAPPING['ismaster']))
            else:
                self.sendMessage('',0)
        elif self.__collection == COLLECTION:
            self.query(spec[QUERY])
        else:
            retdict = {'$error':{'message':'InternalError',
                                 'detail':'collection does not exist'}}
            self.sendMessage(bson.BSON.encode(retdict))
        self.stream.read_bytes(16,self.handleheader)

    def sendMessage(self,message,limit=1):
        try:
            message = struct.pack('<iqii',0,0,0,limit) + message
            header = struct.pack('<iiii',16+len(message),0,self.__request_id,1)
            self.stream.write(header+message)
        except:
            pass

    def query(self,qdict):
        try:
            funcname = qdict.get('funcname')
            func = self.funcmapping.get(funcname)
            if func:
                args,kwargs = qdict['argstr']
                ret = func(*args,**kwargs)
                retdict = {'$result':ret}
                self.sendMessage(bson.BSON.encode(retdict))
                del retdict
            else:
                raise BackendError('FunctionNameError',repr(funcname))
        except BackendError,err:
            retdict = {'$error':{'message':err.message,
                                 'detail':err.detail}}
            self.sendMessage(bson.BSON.encode(retdict))

        except Exception,err:
            retdict = {'$error':{'message':'InternalError',
                                 'detail':repr(err)}}
            self.sendMessage(bson.BSON.encode(retdict))


class RpcServer(TCPServer):

    protocol = MongoProtocol

    def __init__(self,host='localhost',port=27019,backlog=1024,funcmapping={}):
        TCPServer.__init__(self)
        self.funcmapping = funcmapping
        
    def buildProtocol(self):
        p = self.protocol()
        p.server = self
        p.funcmapping = self.funcmapping
        return p

    def handle_stream(self,stream,address):
        print 'i got a connection from :',address
        p = self.buildProtocol()
        p.dataRecieved(stream,address)
