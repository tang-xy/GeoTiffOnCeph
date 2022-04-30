# coding:utf-8

from thrift.transport.TTransport import TFramedTransport
from thrift.protocol import TCompactProtocol
# from hbase.ttypes import ColumnDescriptor, Mutation, TScan
from hbase import Hbase
from thrift.transport import TSocket,TTransport
from thrift.protocol import TBinaryProtocol
from TestHDFS import do_foreach_file
from hbase.ttypes import *

import sys
import os
from time import time

# transport = TFramedTransport(TSocket.TSocket('ceph', 9090))  
# protocol = TCompactProtocol.TCompactProtocol(transport)  
# client = Hbase.Client(protocol)  
socket = TSocket.TSocket('ceph01',9090)
socket.setTimeout(5000)

transport = TTransport.TBufferedTransport(socket)
protocol = TBinaryProtocol.TBinaryProtocol(transport)

client = Hbase.Client(protocol)
socket.open()
fp = open("res.txt","w")

def create_table():
    global client

    contents = ColumnDescriptor(name='gf1:', maxVersions=1)
    client.createTable('image', [contents])

    print(client.getTableNames())


def update_data():
    start = time()
    print("Start: " + str(start))
    global client
    if client.isTableEnabled('image') == False:
        raise Exception("表不可用")
    do_foreach_file('/data/datatrans/unrar/75GData', upload_hbase, end_name='.tif')
    # do_foreach_file('32652(copy)/5104', upload_hbase_att, '.tif')
    

    stop = time()
    print("Stop: " + str(stop))
    print("总耗时" + str(stop-start) + "秒")

def upload_hbase(path):
    global client
    global fp
    start = time()
    basename =  os.path.basename(path)
    filename, fileend = os.path.splitext(basename)
    if fileend != '.tif' or filename[0] == "M":
        print("error:" + path)
        return
    else:
        mutations = []
        with open(path, 'rb') as image:
            mutations.append(Mutation(column="gf1:data", value=image.read().decode('utf-8')))
        with open(os.path.splitext(path)[0] + '.tfw', 'rb') as tfw:
            mutations.append(Mutation(column="gf1:tfw", value=tfw.read()))
        with open(path + '.xml', 'rb') as meta_data:
            mutations.append(Mutation(column="gf1:xml", value=meta_data.read()))
    client.mutateRow('image', filename, mutations)
    end = time()
    file_size = os.stat(path).st_size
    fp.write(filename + ",time," + str(end - start) + ",size," + str(file_size)) 

if __name__ == "__main__":
    model = sys.argv[1]
    #model = "test"
    start = time()
    if model == 'create_table':
        create_table()
    if model == 'update_data':
        update_data()
    if model == 'test':
        # upload_hbase('/data/datatrans/unrar/75GData/5104/00/2013/510400201310120000006256250160001.tif')
        id = client.scannerOpenWithPrefix("image","5104",["data:tif", "meta:tfw"])
    stop = time()
    print("Stop: " + str(stop))
    print("总耗时" + str(stop-start) + "秒")