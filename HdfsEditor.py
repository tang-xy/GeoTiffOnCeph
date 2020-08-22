# coding:utf-8
from hdfs import InsecureClient

class HdfsEditor(InsecureClient):
    def __init__(self):
        super().__init__('http://instance-2:9870',user='hdfs')
        