# coding:utf-8
import sys, os
from Ceph3BoTo3 import CephS3BOTO3
from HdfsEditor import HdfsEditor
from hdfs import InsecureClient
from time import time
from MyTimeit import timeit_wrapper
from GetInforFromGridSystem.GridCalculate import GridCalculate
import random

def createtif(filepath):
    filename, fileend = os.path.splitext(filepath)
    if fileend == '.tfw':
        with open(filename + ".tif","w+") as tiffile:
            tiffile.write(str([80000 - i for i in range(80000)]))


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

# @timeit_wrapper
def upload_tif(client_hdfs):
    for i in range(1):
        start = time()
        client_hdfs.upload('/gf1', '32652(copy)')
        client_hdfs.delete('/gf1', recursive  = True)
        stop = time()
        print('第{0}次，{1}秒'.format(i, str(stop-start)))

def download_tif(client_hdfs):
    for i in range(10):
        start = time()
        for root, dir, filenames in client_hdfs.walk('/gf1/5104'):
            if filenames != []:
                for filename in filenames:
                    real_path = os.path.join(root, filename)
                    client_hdfs.download(real_path, '32652_new/' + filename, overwrite = True)
        stop = time()
        print('第{0}次，{1}秒'.format(i, str(stop-start)))

def rows_download_tif(client_hdfs):
    for i in range(10):
        start = time()
        gridcode_lt_rb = random.sample(range(510470, 510479), 2)
        gridcode_lt_rb.sort()
        gridcodes = GridCalculate.GridCodeToGridlist(str(gridcode_lt_rb[0]), str(gridcode_lt_rb[1]))
        for gridcode in gridcodes:
            tmp = gridcode[4 : 6]
            path = '/gf1/5104/' + tmp + '/2013'
            for root, dir, filenames in client_hdfs.walk(path):
                if filenames != []:
                    for filename in filenames:
                        real_path = os.path.join(root, filename)
                        client_hdfs.download(real_path, '32652_new/' + filename, overwrite = True)
        stop = time()
        print('第{0}次, {2}个格网, {1}秒'.format(i, str(stop-start), gridcode_lt_rb[1] - gridcode_lt_rb[0] + 1))


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
        #do_foreach_file('32652(copy)/5104', createtif)
        #client_hdfs.delete('/gf1', recursive  = True)
        if client_hdfs.content('/gf1',False) == None:
            client_hdfs.upload('/gf1', '32652(copy)')
        start = time()
        print("Start: " + str(start))
        download_tif(client_hdfs)
        stop = time()
        print("Stop: " + str(stop))
        print("总耗时" + str(stop-start) + "秒")
    elif model == 'rows_download':
        if client_hdfs.content('/gf1',False) == None:
            client_hdfs.upload('/gf1', '32652(copy)')
        start = time()
        print("Start: " + str(start))
        rows_download_tif(client_hdfs)
        stop = time()
        print("Stop: " + str(stop))
        print("总耗时" + str(stop-start) + "秒")
        