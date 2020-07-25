# coding:utf-8
from Ceph3BoTo3 import CephS3BOTO3
from osgeo import gdal
import os
from time import time
import json
# 新版本boto3

def RsImage(filepath):
    dic = {}
    dataset = gdal.Open(filepath)
    dic['im_width'] = dataset.RasterXSize
    dic['im_height'] = dataset.RasterYSize
    bandda = dataset.GetRasterBand(1)
    #dic['dayData'] = bandda.ReadRaster(0, 0, dic['im_width'], dic['im_height'])#.tobytes().decode('utf-8')
    img = bandda.ReadRaster(0, 0, dic['im_width'], dic['im_height'])
    dic['im_geotrans'] = dataset.GetGeoTransform()
    dic['im_proj'] = dataset.GetProjection()
    del dataset
    return dic, img

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
        # print(cephs3_boto3.download(obj_key))
    stop = time()
    print("Stop: " + str(stop))
    print(str(stop-start) + "秒")