#encoding=utf-8
import rados, sys, os
from time import time
import random
from RadosConn import RadosConn

if __name__ == "__main__":
    cluster = RadosConn(pool_name='mosic2003')
    keylist = cluster.read_all_image_key()
    random.shuffle(keylist)
    start = time()
    print("Start: " + str(start))
    for key in keylist:
        temp = cluster.read_image_rados(key)
    #cluster.read_all_image_rados()
    stop = time()
    print("Stop: " + str(stop))
    print(str(stop-start) + "秒")