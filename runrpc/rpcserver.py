# -*- coding: utf-8 -*-
# file: rpcserver.py
# author: notedit
# date: 20111207

import struct
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
        self.__waiting_header = true
        self.__data_length = None
        self.__request_id = 0

    def dataRecieved(self,stream,address):
        self.stream = stream
        stream.read_bytes(16,self.handleheader)


    def handleheader(self,msgheader):
        self.__data_length,self.__request_id,_,op_code = struct.unpack('<iiii',msgheader)
        if op_code != 2004:
            #todo write something and then close the iostream
            e = BackendError('OperationError','rpc can not support this operation')
            self.sendMessage(bson.BSON.encode({'$error':repr(e)}))
            self.stream.close()
            return
        try:
            self.stream.read_bytes(self.__data_length-16,self.handlequery)
        except:
            self.stream.close()

    def handlequery(self,query):
        #flags,packet = query[:4],query[4:]
        self.__collection,query = bson._get_c_string(query[4:])
        #skip the skip and limit
        try:
            spec = bson.BSON(query[8:]).decode()
        except:
            e = BackendError('InternalError','unvalid bson')
            retdict = {'$error':repr(e)}
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
            e = BackendError('InternalError','collection does not exist')
            retdict = {'$error':repr(e)}
            self.sendMessage(bson.BSON.encode(retdict))
            self.stream.close()

    def sendMessage(self,message,limit=1):
        try:
            message = struct.pack('<iqii',0,0,0,limit) + message
            header = struct.pack('<iiii',16+len(message),0,self.__request_id,1)
            self.stream.write(header)
            self.stream.write(message)
        except:
            pass

    def query(self,qdict):
        try:
            funcname = qdict[QUERY].get('funcname')
            func = self.funcmapping.get(funcname)
            if func:
                args,kwargs = qdict[QUERY]['argstr']
                ret = func(*args,**kwargs)
                retdict = {'$result':ret}
                self.sendMessage(bson.BSON.encode(retdict))
            else:
                raise BackendError('FunctionNameError',repr(funcname))
        except BackendError,err:
            e = BackendError('InternalError',err.detail)
            retdict = {'$error':repr(e)}
            self.sendMessage(bson.BSON.encode(retdict))
            self.stream.close()
        except Exception,err:
            e = BackendError('InternalError',repr(err))
            retdict = {'$error':repr(e)}
            self.sendMessage(bson.BSON.encode(retdict))
            self.stream.close()


class RpcServer(TCPServer):

    protocol = MongoProtocol

    def __init__(self,host='localhost',port=27017,backlog=1024,funcmapping={}):
        TCPServer.__init__(self)
        self.funcmapping = funcmapping
        
    def buildProtocol(self):
        p = self.protocol()
        p.server = self
        return p

    def handle_stream(self,stream,address):
        p = self.buildProtocol(self)
        p.dataRecieved(stream,address)
