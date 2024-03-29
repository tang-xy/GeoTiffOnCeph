# coding:utf-8
from boto3.session import Session
from time import time
# from osgeo import gdal
class CephS3BOTO3():

    def __init__(self, bucket_name = ''):
        access_key = 'MCNBMBAERC2UA0E2EA4P'
        secret_key = 'm0I03C0oWxnFrRFVq2KNRcwZPSh0ffiaxpFmexnA'
        self.session = Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        self.url = 'http://ceph1:7480'
        self.s3_client = self.session.client('s3', endpoint_url = self.url)
        self.bucket_name = bucket_name
        self.s3_resource = self.session.resource('s3', endpoint_url = self.url)
        self.bucket = None
        self.filename_valid = ['tif', 'hdf']

    def get_bucket(self):
        buckets = [bucket['Name'] for bucket in self.s3_client.list_buckets()['Buckets']]
        print(buckets)
        return buckets

    def create_bucket(self, bucket_name, acl = "public-read-write"):
        # 默认是私有的桶
        # self.s3_client.create_bucket(Bucket=bucket_name)
        # 创建公开可读的桶
        # ACL有如下几种"private","public-read","public-read-write","authenticated-read"
        self.bucket_name = bucket_name
        self.s3_client.create_bucket(Bucket = bucket_name, ACL = acl)

    def upload(self, obj_name, obj, meta_dict = {}):
        '''meta_dict未实现'''
        resp = self.s3_client.put_object(
            Bucket = self.bucket_name,# 存储桶名称
            Key = obj_name, # 上传到
            Body = obj
        )
        print(resp)
        return resp

    def delete_all_by_client(self):
        #最多删除1000条数据
        resp = self.s3_client.list_objects(Bucket = self.bucket_name)
        keylist = [{ 'Key' : obj["Key"] } for obj in resp['Contents']]
        self.s3_client.delete_objects(
            Bucket = self.bucket_name,
            Delete = {
                'Objects': keylist
            }
        )
    
    def delete_all_by_bucket(self, pre):
        if self.bucket == None:
            self.bucket = self.s3_resource.Bucket(self.bucket_name)
        self.bucket.objects.filter(Prefix=pre).delete()

    def download_all_file(self, path):
        if self.bucket == None:
            self.bucket = self.s3_resource.Bucket(self.bucket_name)
        lis = ""
        for obj in self.bucket.objects.all():
            start = time()
            obj.Object().download_file(path + '/' + obj.key)
            end = time()
            lis += ("size,{0},time,{1}\n".format(obj.size, str(end-start)))
        return lis
        # resp = self.s3_client.list_objects(Bucket = self.bucket_name)
        # keylist = [obj["Key"] for obj in resp['Contents']]
        # for key in keylist:
        #     self.s3_client.download_file(
        #         Bucket = self.bucket_name,
        #         Key = key,
        #         Filename = path + '/' + key
        #     )


    def download_dir(self, bucket_prefix, path):
        # if self.bucket == None:
        #     self.bucket = self.s3_resource.Bucket(self.bucket_name)
        # objs = self.bucket.objects.filter(Prefix = bucket_prefix)
        start = time()
        resp = self.s3_client.list_objects(Bucket = self.bucket_name, Prefix = bucket_prefix)
        keylist = [obj["Key"] for obj in resp['Contents']]
        now = time()
        for key in keylist:
            self.s3_client.download_file(
                Bucket = self.bucket_name,
                Key = key,
                Filename = path + '/' + key
            )
        end = time()
        return now - start, end - now

    def get_metadata(self, bucket_prefix):
        start = time()
        if self.bucket == None:
            self.bucket = self.s3_resource.Bucket(self.bucket_name)
        objs = self.bucket.objects.filter(Prefix = bucket_prefix)
        now = time()
        for obj in objs:
            neveruse = obj.Object().metadata
        end = time()
        return now - start, end - now

    def upload_file(self, file_path, obj_name, meta_dict = {}):
        return self.s3_client.upload_file(
            file_path, self.bucket_name, obj_name,
            ExtraArgs={'ACL': 'public-read-write' ,  'Metadata': meta_dict}
        )


    def download(self, obj_name):
        resp = self.s3_client.get_object(
            Bucket = self.bucket_name,
            Key = obj_name
        )
        return resp['Body'].read()

    # def get_gdal_dataset_inmemory(self, obj_name):
    #     if obj_name.split('.')[-1] not in self.filename_valid:
    #         raise Exception("不合法的文件名")
    #     imageBuffer = self.download(obj_name)
    #     memFilename = "/vsimem/" + obj_name
    #     gdal.FileFromMemBuffer(memFilename, imageBuffer)
    #     return gdal.Open(memFilename)