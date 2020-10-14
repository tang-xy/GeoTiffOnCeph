# coding:utf-8
from thrift.transport import TSocket,TTransport
from thrift.protocol import TBinaryProtocol
from hbase.ttypes import ColumnDescriptor
from hbase import Hbase

import sys
import os
from time import time

def createTable():
    transport = TSocket.TSocket('instance-2', 9090)

    transport = TTransport.TBufferedTransport(transport)

    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = Hbase.Client(protocol)
    transport.open()


    contents = ColumnDescriptor(name='gf1', maxVersions=1)
    client.createTable('image', [contents])

    print(client.getTableNames())

    transport.close()

if __name__ == "__main__":
    model = sys.argv[1]
    if model == 'createTable':
        createTable()