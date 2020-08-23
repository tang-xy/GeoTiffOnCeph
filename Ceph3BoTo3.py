# coding:utf-8
from boto3.session import Session
class CephS3BOTO3():

    def __init__(self, bucket_name = ''):
        access_key = '6S75J41Q2S86GT8FD1ND'
        secret_key = 'GP7W6AOKTHrP3iDFzadtGBYTiuLsTXbMaC2XceB8'
        self.session = Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        self.url = 'http://instance-2:7480'
        self.s3_client = self.session.client('s3', endpoint_url=self.url)
        self.bucket_name = bucket_name

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

    def upload(self, obj_name, obj):
        resp = self.s3_client.put_object(
            Bucket = self.bucket_name,# 存储桶名称
            Key = obj_name, # 上传到
            Body = obj
        )
        print(resp)
        return resp

    def delete_all(self):
        resp = self.s3_client.list_objects(Bucket = self.bucket_name)
        keylist = [obj["Key"] for obj in resp['Contents']]
        self.s3_client.delete_objects(
            Bucket = self.bucket_name,
            Delete = {
                'Objects': keylist
            }
        )

    def upload_file(self, file_path, obj_name):
        return self.s3_client.upload_file(
            file_path, self.bucket_name, obj_name,
            ExtraArgs={'ACL': 'public-read-write'}
        )


    def download(self, obj_name):
        resp = self.s3_client.get_object(
            Bucket = self.bucket_name,
            Key = obj_name
        )
        return resp['Body'].read()
