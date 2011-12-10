# -*- coding: utf-8 -*-
# Filename: test_client.py
# Author: notedit
# Date: 20111210

import traceback
import rpc

def main():
    client = rpc.RpcClient(port=27018)
    now = client.now()
    print 'now:',now
    add = client.add(3,4)
    print add
    inc2 = client.inc2(5)
    print inc2

    try:
        error = client.raiseerror()
    except rpc.BackendError,e:
        traceback.print_exc()
        print repr(e)

    try:
        doesnotexist = client.whatthefuck()
    except rpc.BackendError,e:
        traceback.print_exc()


if __name__ == '__main__':
    main()
