# coding:utf-8
from thrift.transport import TSocket
from thrift.transport.TTransport import TFramedTransport
from thrift.protocol import TCompactProtocol
from hbase.ttypes import ColumnDescriptor, Mutation
from hbase import Hbase
from osgeo import gdal

import sys
import os
from time import time

def open_image(path):
    image = gdal.Open(path)
    meta = image.GetMetadata()
    return image, meta

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
    transport = TFramedTransport(TSocket.TSocket('instance-2', 9090))  
    protocol = TCompactProtocol.TCompactProtocol(transport)  
    client = Hbase.Client(protocol)  
    transport.open()
    if client.isTableEnabled('image') == False:
        raise Exception("表不可用")
    #mutations = [Mutation(column="gf1:")
    image_data, meta_data = open_image("32652(copy)/5104/70/2013/510470201305240000006256250169001.tif")
    transport.close()

if __name__ == "__main__":
    model = sys.argv[1]
    if model == 'create_table':
        create_table()
    if model == 'update_data':
        update_data()