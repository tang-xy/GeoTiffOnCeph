#encoding=utf-8
import rados, sys, os
from time import time
from RadosConn import RadosConn

if __name__ == "__main__":
    cluster = RadosConn(pool_name='mosic2003')
    start = time()
    print("Start: " + str(start))
    for root, dirs, files in os.walk('mosic2003'):
        for di in dirs:
            path = os.path.join(root, di)
            cluster.write_image_file(path + '/day/result.tif', di + 'day')
            cluster.write_image_file(path + '/night/result.tif', di + 'night')
        break
    stop = time()
    print("Stop: " + str(stop))
    print(str(stop-start) + "秒")