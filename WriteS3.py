# coding:utf-8
from boto3.session import Session
from osgeo import gdal
import os
from time import time
import json
# 新版本boto3

class CephS3BOTO3():

    def __init__(self):
        access_key = '8ETYBTBV4Z91JESIA0AV'
        secret_key = 'OQVA7pzM8wapjWccKKxTB13ju0CVMN9W0PYe7maB'
        self.session = Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        self.url = 'http://instance-1:7480'
        self.s3_client = self.session.client('s3', endpoint_url=self.url)
        self.bucket_name = 'mosic2003'

    def get_bucket(self):
        buckets = [bucket['Name'] for bucket in self.s3_client.list_buckets()['Buckets']]
        print(buckets)
        return buckets

    def create_bucket(self, bucket_name, acl = "public-read-write"):
        # 默认是私有的桶
        # self.s3_client.create_bucket(Bucket=bucket_name)
        # 创建公开可读的桶
        # ACL有如下几种"private","public-read","public-read-write","authenticated-read"
        self.s3_client.create_bucket(Bucket = bucket_name, ACL = acl)

    def upload(self, obj_name, obj):
        resp = self.s3_client.put_object(
            Bucket = self.bucket_name,# 存储桶名称
            Key = obj_name, # 上传到
            Body = obj
        )
        print(resp)
        return resp

    def download(self, obj_name):
        resp = self.s3_client.get_object(
            Bucket = self.bucket_name,
            Key = obj_name
        )
        return resp['Body'].read()

def RsImage(filepath):
    dic = {}
    dataset = gdal.Open(filepath)
    dic['im_width'] = dataset.RasterXSize
    dic['im_height'] = dataset.RasterYSize
    bandda = dataset.GetRasterBand(1)
    dic['dayData'] = bandda.ReadRaster(0, 0, dic['im_width'], dic['im_height'])#.tobytes().decode('utf-8')
    dic['im_geotrans'] = dataset.GetGeoTransform()
    dic['im_proj'] = dataset.GetProjection()
    del dataset
    return dic

if __name__ == "__main__":
    # boto3
    #ephs3_boto3 = CephS3BOTO3()
    #cephs3_boto3.get_bucket()
    #cephs3_boto3.create_bucket('mosic2003')
    start = time()
    print("Start: " + str(start))
    for root, dirs, files in os.walk('mosic2003'):
        for di in dirs:
            path = os.path.join(root, di)
            day_image = RsImage(path + '\\day\\result.tif')
            night_image = RsImage(path + '\\night\\result.tif')
            strtemp = json.dumps(day_image)
            dictemp = json.loads(strtemp)
            #cephs3_boto3.upload(di + 'day', day_image)
            #cephs3_boto3.upload(di + 'night', night_image)
            #cephs3_boto3.upload('ts', night_image.dayData.tobytes())
            break
        break
    stop = time()
    print("Stop: " + str(stop))
    print(str(stop-start) + "秒")