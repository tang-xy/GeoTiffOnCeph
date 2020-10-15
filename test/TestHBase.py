# coding:utf-8
from thrift.transport import TSocket
from thrift.transport.TTransport import TFramedTransport
from thrift.protocol import TCompactProtocol
from hbase.ttypes import ColumnDescriptor, Mutation
from hbase import Hbase
from TestHDFS import do_foreach_file
#from osgeo import gdal

import sys
import os
from time import time

# def open_image(path):
#     image = gdal.Open(path)
#     meta = image.GetMetadata()
#     return image, meta

transport = TFramedTransport(TSocket.TSocket('instance-2', 9090))  
protocol = TCompactProtocol.TCompactProtocol(transport)  
client = Hbase.Client(protocol)  


def transport_decorator(function):
    def decorator(url = 'instance-2', port = 9090):
        transport = TFramedTransport(TSocket.TSocket(url, port))  
        protocol = TCompactProtocol.TCompactProtocol(transport)  
        client = Hbase.Client(protocol)  
        transport.open()
        t = function(client)
        transport.close()
        return t

    return decorator

def create_table():
    transport = TFramedTransport(TSocket.TSocket('instance-2', 9090))  
    protocol = TCompactProtocol.TCompactProtocol(transport)  
    client = Hbase.Client(protocol)  
    transport.open()

    contents = ColumnDescriptor(name='gf1', maxVersions=1)
    client.createTable('image', [contents])

    print(client.getTableNames())

    transport.close()

def update_data():
    start = time()
    print("Start: " + str(start))

    transport.open()
    global client
    if client.isTableEnabled('image') == False:
        raise Exception("表不可用")
    do_foreach_file('32652(copy)/5104', upload_hbase, '.tif')
    transport.close()

    stop = time()
    print("Stop: " + str(stop))
    print("总耗时" + str(stop-start) + "秒")

def upload_hbase(path):
    global client

    basename =  os.path.basename(path)
    meta_dict = {}
    with open(os.path.splitext(path)[0] + '.tfw', 'rb') as tfw:
        meta_dict['tfw'] =  repr(tfw.read())
    with open(path + '.xml', 'rb') as meta_data:
        meta_dict['tfw'] =  repr(meta_data.read())
        #ceph_editor.upload_file(path, 'new_' + basename, meta_dict = meta_dict)
    mutations = [Mutation(column = basename + "_" + k, value=meta_dict[k]) for k in meta_dict]
    with open(path, 'rb') as image:
        mutations.append(Mutation(column="gf1_data", value=image))
    client.mutateRow('image', basename, mutations)

if __name__ == "__main__":
    model = sys.argv[1]
    if model == 'create_table':
        create_table()
    if model == 'update_data':
        update_data()