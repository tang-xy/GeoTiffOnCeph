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
    for root, dirs, files in os.walk('mosic2003'):
        for di in dirs:
            path = os.path.join(root, di)
            day, day_image = RsImage(path + '/day/result.tif')
            night, night_image = RsImage(path + '/night/result.tif')
            strtemp = json.dumps(day)
            #cephs3_boto3.upload(di + 'day_img', day_image)
            cephs3_boto3.upload(di + 'day', strtemp)
            strtemp = json.dumps(night)
            #cephs3_boto3.upload(di + 'night_img', night_image)
            #cephs3_boto3.upload(di + 'night', strtemp)
            #dictemp = json.loads(strtemp)
            #cephs3_boto3.upload('ts', night_image.dayData.tobytes())
            
        break
    stop = time()
    print("Stop: " + str(stop))
    print(str(stop-start) + "秒")