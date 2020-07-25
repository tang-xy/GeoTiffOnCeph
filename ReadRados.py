#encoding=utf-8
import rados, sys, os
from time import time
from RadosConn import RadosConn

if __name__ == "__main__":
    cluster = RadosConn(pool_name='mosic2003')
    start = time()
    print("Start: " + str(start))
    cluster.read_all_image_rados()
    stop = time()
    print("Stop: " + str(stop))
    print(str(stop-start) + "秒")