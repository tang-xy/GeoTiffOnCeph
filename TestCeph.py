# coding:utf-8
from Ceph3BoTo3 import CephS3BOTO3
from TestHDFS import do_foreach_file, createtif
from GetInforFromGridSystem.GridCalculate import GridCalculate
import random
import sys
import os
from time import time

ceph_editor = CephS3BOTO3('gf1')
fp = open("res.txt","w")
file_num = 0


def upload_ceph(path):
    global ceph_editor
    basename =  os.path.basename(path)
    ceph_editor.upload_file(path, basename)

def upload_ceph_with_att(path):
    global ceph_editor
    global fp
    global file_num
    if file_num > 10000:
        return
    start_att = time()
    basename =  os.path.basename(path)
    filename, fileend = os.path.splitext(basename)
    if fileend != '.tif' or filename[0] == "M":
        print("error:" + path)
        return
    else:
        meta_dict = {}
        with open(os.path.splitext(path)[0] + '.tfw', 'rb') as tfw:
            meta_dict['tfw'] =  repr(tfw.read())
        with open(path + '.xml', 'rb') as meta_data:
            meta_dict['tfw'] =  repr(meta_data.read())
        ceph_editor.upload_file(path, 'new_' + basename, meta_dict = meta_dict)
    end = time()
    file_size = os.stat(path).st_size
    fp.write(filename + ",time," + str(end - start_att) + ",size," + str(file_size) + "\n") 
    file_num += 1

def download_tif():
    start = time()
    i = ceph_editor.download_all_file('/data/datatrans/download')
    fp.write(i) 
    fp.close()
    stop = time()
    print('共{0}次，{1}秒'.format(1, str(stop-start)))

def upload_delete_tif():
    global ceph_editor
    for i in range(10):
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
        filter_time = 0
        row_time = 0
        for gridcode in gridcodes:
            tmp1, tmp2 = ceph_editor.download_dir(gridcode + '2013', '32652_new')
            filter_time += tmp1
            row_time += tmp2
        stop = time()
        print('第{0}次, {2}个格网, {1}秒, filter耗时{3},下载耗时{4}'.format(i, str(stop - start), gridcode_lt_rb[1] - gridcode_lt_rb[0] + 1, filter_time, row_time))

def meta_data():
    global ceph_editor
    for i in range(600):
        start = time()
        gridcode_lt_rb = random.sample(range(510400, 510499), 2)
        gridcode_lt_rb.sort()
        gridcodes = GridCalculate.GridCodeToGridlist(str(gridcode_lt_rb[0]), str(gridcode_lt_rb[1]))
        filter_time = 0
        row_time = 0
        for gridcode in gridcodes:
            tmp1, tmp2 = ceph_editor.get_metadata(gridcode + '2013')
            filter_time += tmp1
            row_time += tmp2
        stop = time()
        print('第{0}次, {2}个格网, {1}秒, filter耗时{3},查询元数据耗时{4}'.format(i, str(stop - start), gridcode_lt_rb[1] - gridcode_lt_rb[0] + 1, filter_time, row_time))

def get_list():
    global ceph_editor
    global fp
    grids = open("data\\grid.txt")
    line = grids.readline()[:-1]
    while line:
        start = time()
        gridcode_lt_rb = random.sample(range(510400, 510499), 1)[0]
        timecode = random.sample(range(2013, 2018), 1)[0]
        if ceph_editor.bucket == None:
            ceph_editor.bucket = ceph_editor.s3_resource.Bucket(ceph_editor.bucket_name)
        # objs = ceph_editor.bucket.objects.filter(Prefix = 'new_' + str(gridcode_lt_rb) + str(timecode))
        objs = ceph_editor.bucket.objects.filter(Prefix = 'new_' + line)
        num = 0
        for obj in objs:
            num += 1
        end = time()
        line = grids.readline()[:-1]
        fp.write("time," + str(end - start) + ",grid_num," + str(num)) 
    fp.close()
        

if __name__ == "__main__":
    if 'GeoHugeTest' not in ceph_editor.get_bucket():
        ceph_editor.create_bucket('GeoHugeTest')
    model = sys.argv[1]
    # model = 'create'
    start = time()
    print("Start: " + str(start))
    if model == 'create':
        # start_att = time()
        # do_foreach_file('/data/datatrans/unrar/75GData', upload_ceph_with_att, end_name='.tif')
        # end_att = time()
        # print("属性上传耗时{0}".format(end_att - start_att))

        start_all = time()
        do_foreach_file('/zhaolong/data/Landsat8/clip', upload_ceph)
        end_all = time()
        print("全部上传耗时{0}".format(end_all - start_all))
        fp.close()
    elif model == 'upload_delete':
        upload_delete_tif()
    elif model == 'download':
        download_tif()
    elif model == 'rows_download':
        rows_download_tif()
    elif model == 'meta_data':
        meta_data()
    elif model == "getlist":
        get_list()
    elif model == 'delete':
        ceph_editor.delete_all_by_bucket("")
    stop = time()
    print("Stop: " + str(stop))
    print("总耗时" + str(stop-start) + "秒")