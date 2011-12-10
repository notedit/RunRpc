# -*- coding: utf-8 -*-
# Filename: test_client.py
# Author: notedit
# Date: 20111210

import time
import traceback
import rpc

def main():
    client = rpc.RpcClient(port=27019)
    now = client.now()
    print 'now:',now
    add = client.add(3,4)
    print add
    inc2 = client.inc2(5)
    print inc2

    try:
        error = client.raiseerror()
    except rpc.BackendError,e:
        print repr(e)

    try:
        doesnotexist = client.whatthefuck()
    except rpc.BackendError,e:
        print repr(e)

    s = time.time()
    for i in xrange(10000):
        now = client.now()
    end = time.time() - s
    print '10000 times in:',end

if __name__ == '__main__':
    main()
