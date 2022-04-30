from Ceph3BoTo3 import CephS3BOTO3
from TestHDFS import do_foreach_file
import Polysize
import Postgre 

import random
import sys
import os
from time import time
# ceph_editor = CephS3BOTO3('gf1')
fp = open("meta_res.txt","w")
file_num = 0
 
def upload_psql(path):
    basename =  os.path.basename(path)
    filename, fileend = os.path.splitext(basename)
    start = time()
    print(filename + " Start: " + str(start))
    x1, y1, x2, y2 = Polysize.get_poly(path)
    Postgre.insert(filename, x1, y1, x2, y2)
    stop = time()
    print(filename + " Stop: " + str(stop))
    global fp
    fp.write(filename + ',' + str(stop-start) + '\n')
 
def create_psql():
    Postgre.create()
    do_foreach_file('/data/datatrans/unrar/75GData', upload_psql, end_name='.tif')
       
def get_poly():
    xmin = random.randint(100000, 9999999)
    xmax = random.randint(xmin, 9999999)
    
    ymin = random.randint(100000, 9999999)
    ymax = random.randint(ymin, 9999999)
    
    return xmin, ymin, xmax, ymax

def select_meta(num):
    i = 0
    while i < num:
        x1, y1, x2, y2 = get_poly()
        start_psql = time()
        res = Postgre.select(x1, y1, x2, y2)
        stop_psql = time()
        if res > 0:
            i += 1
            fp.write('res' + ',' + str(stop_psql - start_psql) + '\n')
            
if __name__ == "__main__":
    # if 'gf1' not in ceph_editor.get_bucket():
    #     ceph_editor.create_bucket('gf1')
    model = sys.argv[1]
    # model = 'create'
    start = time()
    print("Start: " + str(start))
    if model == 'create_psql':
        create_psql()
    if model == 'select':
        select_meta(1000)
    stop = time()
    print("Stop: " + str(stop))
    print("总耗时" + str(stop-start) + "秒")