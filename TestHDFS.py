# coding:utf-8
import sys, os
from Ceph3BoTo3 import CephS3BOTO3
from HdfsEditor import HdfsEditor
from hdfs import InsecureClient
from time import time
from MyTimeit import timeit_wrapper

def createtif(filepath):
    filename, fileend = os.path.splitext(filepath)
    if fileend == '.tfw':
        tiffile = open(filename + ".tif","w+")
        tiffile.write(str([i for i in range(80000)]))


def do_foreach_file(url, func):
    for f in os.listdir(url):
        real_path=os.path.join(url,f)
        if os.path.isfile(real_path):
            func(real_path)
        elif os.path.isdir(real_path):
            do_foreach_file(real_path, func)
        else:
            print("其他情况:" + real_path)

# @timeit_wrapper
def upload_tif(client_hdfs):
    for i in range(1):
        start = time()
        client_hdfs.upload('/gf1', '32652(copy)')
        client_hdfs.delete('/gf1', recursive  = True)
        stop = time()
        print('第{0}次，{1}秒'.format(i, str(stop-start)))

def download_tif(client_hdfs):
    for i in range(600):
        start = time()
        client_hdfs.download('/gf1', '32652(copy)', overwrite = True)
        stop = time()
        print('第{0}次，{1}秒'.format(i, str(stop-start)))

if __name__ == "__main__":
    #model = "upload&delete"
    model = sys.argv[1]
    client_hdfs = InsecureClient('http://instance-2:9870',user='hdfs')
    if model == 'create_upload_delete':
        if True:
            start = time()
            do_foreach_file('32652(copy)/5104', createtif)
            stop = time()
            print("写入耗时" + str(stop-start) + "秒")
        start = time()
        print("Start: " + str(start))
        upload_tif(client_hdfs)
        stop = time()
        print("Stop: " + str(stop))
        print("总耗时" + str(stop-start) + "秒")
    elif model == 'upload_delete':
        start = time()
        print("Start: " + str(start))
        if False:
            do_foreach_file('32652(copy)/5104', createtif)
        upload_tif(client_hdfs)
        stop = time()
        print("Stop: " + str(stop))
        print("总耗时" + str(stop-start) + "秒")
    elif model == 'upload_download':
        do_foreach_file('32652(copy)/5104', createtif)
        client_hdfs.delete('/gf1', recursive  = True)
        if client_hdfs.content('/gf1',False) == None:
            client_hdfs.upload('/gf1', '32652(copy)')
        start = time()
        print("Start: " + str(start))
        #download_tif(client_hdfs)
        for i in client_hdfs.walk('32652(copy)/5104'):
            print(i)
        stop = time()
        print("Stop: " + str(stop))
        print("总耗时" + str(stop-start) + "秒")
        
        