# coding:utf-8
from Ceph3BoTo3 import CephS3BOTO3
from osgeo import gdal
import os
from time import time
import json
# 新版本boto3


if __name__ == "__main__":
    # boto3
    cephs3_boto3 = CephS3BOTO3()
    #cephs3_boto3.get_bucket()
    #cephs3_boto3.create_bucket('mosic2003')
    start = time()
    print("Start: " + str(start))
    all_objects = cephs3_boto3.s3_client.list_objects(Bucket = 'mosic2003')
    obj_names = [obj['Key'] for obj in all_objects['Contents']]
    for obj_key in obj_names:
        cephs3_boto3.download(obj_key)
    stop = time()
    print("Stop: " + str(stop))
    print(str(stop-start) + "秒")

    start = time()
    print("Start: " + str(start))
    for root, dirs, files in os.walk('mosic2003'):
        for di in dirs:
            path = os.path.join(root, di)
            day, day_image = RsImage(path + '/day/result.tif')
            night, night_image = RsImage(path + '/night/result.tif')
        break
    stop = time()
    print("Stop: " + str(stop))
    print(str(stop-start) + "秒")