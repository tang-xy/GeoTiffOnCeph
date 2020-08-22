# coding:utf-8
import sys, os
from Ceph3BoTo3 import CephS3BOTO3
from HdfsEditor import HdfsEditor
from time import time

def createtif(filepath):
    filename, fileend = os.path.splitext(filepath)
    if fileend == '.tfw':
        tiffile = open(filename + ".tif","w+")
        tiffile.write(str([i for i in range(800000)]))

def do_foreach_file(url, func):
    for f in os.listdir(url):
        real_path=os.path.join(url,f)
        if os.path.isfile(real_path):
            func(real_path)
        elif os.path.isdir(real_path):
            do_foreach_file(real_path, createtif)
        else:
            print("其他情况:" + real_path)

def upload_tif():
    client_hdfs = HdfsEditor()
    client_hdfs.upload('/gf1', '32652')


if __name__ == "__main__":
    model = "upload"
    #model = sys.argv[2]
    if model == 'upload':
        start = time()
        print("Start: " + str(start))
        if False:
            do_foreach_file('32652(copy)/5104', createtif)
        upload_tif()
        stop = time()
        print("Stop: " + str(stop))
        print(str(stop-start) + "秒")
        