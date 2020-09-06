# coding:utf-8
from Ceph3BoTo3 import CephS3BOTO3
from TestHDFS import do_foreach_file, createtif
from GetInforFromGridSystem.GridCalculate import GridCalculate
import random
import sys
import os
from time import time

ceph_editor = CephS3BOTO3('gf1')

def upload_ceph(path):
    global ceph_editor
    basename =  os.path.basename(path)
    ceph_editor.upload_file(path, basename)

def download_tif():
    for i in range(5):
        start = time()
        ceph_editor.download_all_file('32652_new')
        stop = time()
        print('第{0}次，{1}秒'.format(i, str(stop-start)))

def upload_delete_tif():
    global ceph_editor
    for i in range(60):
        start = time()
        do_foreach_file('32652(copy)/5104', upload_ceph)
        ceph_editor.delete_all_by_client()
        stop = time()
        print('第{0}次，{1}秒'.format(i, str(stop-start)))

def rows_download_tif():
    global ceph_editor
    for i in range(60):
        start = time()
        gridcode_lt_rb = random.sample(range(510470, 510479), 2)
        gridcode_lt_rb.sort()
        gridcodes = GridCalculate.GridCodeToGridlist(str(gridcode_lt_rb[0]), str(gridcode_lt_rb[1]))
        for gridcode in gridcodes:
            ceph_editor.download_dir(gridcode, '32652_new')
        stop = time()
        print('第{0}次, {2}个格网, {1}秒'.format(i, str(stop-start), gridcode_lt_rb[1] - gridcode_lt_rb[0] + 1))

if __name__ == "__main__":
    if 'gf1' not in ceph_editor.get_bucket():
        ceph_editor.create_bucket('gf1')
    model = sys.argv[1]
    start = time()
    print("Start: " + str(start))
    if model == 'create':
        do_foreach_file('32652(copy)/5104', createtif)
        do_foreach_file('32652(copy)/5104', upload_ceph)
    elif model == 'upload_delete':
        upload_delete_tif()
    elif model == 'download':
        download_tif()
    elif model == 'rows_download':
        rows_download_tif()
    stop = time()
    print("Stop: " + str(stop))
    print("总耗时" + str(stop-start) + "秒")