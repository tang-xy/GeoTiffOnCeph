import boto.s3.connection

access_key = '7P9UC5AGGUI9U950MD9Y'
secret_key = 'SBLVm20ghDErP9wmCZR6a1uHKyjTx8Zz6vgjZ22e'

conn = boto.connect_s3(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        host='35.236.5.7', port=7480,
        is_secure=False, calling_format=boto.s3.connection.OrdinaryCallingFormat(),
       )

bucket = conn.create_bucket('my-new-bucket')
for bucket in conn.get_all_buckets():
    print("{name} {created}".format(
        name=bucket.name,
        created=bucket.creation_date,
    ))