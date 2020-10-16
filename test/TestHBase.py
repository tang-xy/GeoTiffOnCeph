# coding:utf-8
from thrift.transport import TSocket
from thrift.transport.TTransport import TFramedTransport
from thrift.protocol import TCompactProtocol
from hbase.ttypes import ColumnDescriptor, Mutation
from hbase import Hbase
#from TestHDFS import do_foreach_file
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

def do_foreach_file(url, func, end_name = ''):
    for f in os.listdir(url):
        real_path = os.path.join(url,f)
        if os.path.isfile(real_path):
            filename, fileend = os.path.splitext(real_path)
            if end_name == '' or fileend == end_name:
                func(real_path)
        elif os.path.isdir(real_path):
            do_foreach_file(real_path, func, end_name)
        else:
            print("其他情况:" + real_path)

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
    global transport
    transport.open()
    global client
    if client.isTableEnabled('image') == False:
        raise Exception("表不可用")
    transport.close()
    do_foreach_file('32652(copy)/5104', upload_hbase, '.tif')
    

    stop = time()
    print("Stop: " + str(stop))
    print("总耗时" + str(stop-start) + "秒")

def upload_hbase(path):
    global client
    global transport
    transport.open()
    basename =  os.path.basename(path)
    meta_dict = {}
    with open(os.path.splitext(path)[0] + '.tfw', 'rb') as tfw:
        meta_dict['tfw'] =  repr(tfw.read())
    with open(path + '.xml', 'rb') as meta_data:
        meta_dict['tfw'] =  repr(meta_data.read())
        #ceph_editor.upload_file(path, 'new_' + basename, meta_dict = meta_dict)
    mutations = [Mutation(column = 'gf1' + "_" + k, value=meta_dict[k]) for k in meta_dict]
    mutations = []
    with open(path, 'rb') as image:
        mutations.append(Mutation(column="gf1:data", value=str([i for i in range(80000)])))
        #mutations.append(Mutation(column="gf1_data", value=image.read()))
    client.mutateRow('image', basename, mutations)
    transport.close()

if __name__ == "__main__":
    model = sys.argv[1]
    #model = "test"
    if model == 'create_table':
        create_table()
    if model == 'update_data':
        update_data()
    if model == 'test':
        upload_hbase('32652(copy)/5104\\70\\2013\\510470201305240000006256250169001.tif')