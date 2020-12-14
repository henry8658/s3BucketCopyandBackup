import os
import boto3
import sys
import hashlib
from botocore.exceptions import ClientError

def download_file(bucket, file_name, object_name=None):

    if object_name is None:
        object_name = file_name

    s3_client = boto3.client('s3')
    try:
        response = s3_client.download_file(bucket, object_name, file_name)
    except Exception as e:
        print(e)
        return False
    return True

def make_directories(path, bucket):
    file_name = []
    object_name = []
    file_list = []
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    for obj in bucket.objects.all():
        if obj.key.endswith('/'):
            os.makedirs(os.path.join(path, obj.key), exist_ok=True)
        else:
            file_name.append(os.path.join(path, obj.key))
            object_name.append(obj.key)
    file_list.append(file_name)
    file_list.append(object_name)
    return file_list

def checkModifiedFile(bucket, obj):
    if getETag(bucket, obj) == compute_s3_etag(obj):
        return True

def getETag(bucket, obj):
    s3_client = boto3.client('s3')
    response = s3_client.head_object(Bucket=bucket, Key=obj)
    return response['ETag']

def compute_s3_etag(file_path, chunk_size=8 * 1024 * 1024):
    "compute aws s3 etag"

    md5s = []

    try:
        with open(file_path, 'rb') as fp:
            while True:
                data = fp.read(chunk_size)
                if not data:
                    break
                md5s.append(hashlib.md5(data))
    except FileNotFoundError:
        return "not found"

    if len(md5s) == 1:
        return '"{}"'.format(md5s[0].hexdigest())

    digests = b"".join(m.digest() for m in md5s)
    digests_md5 = hashlib.md5(digests)
    return '"{}-{}"'.format(digests_md5.hexdigest(), len(md5s))

def checkBucket(bucket):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.head_bucket(Bucket=bucket)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
    except ClientError as e:
        print("No such bucket!")
        return False

def restore_files(path, bucket):
    file_list = make_directories(path, bucket)

    print("--Following files were downloaded from " + bucket)

    for file_name, object_name in zip(file_list[0], file_list[1]):
        if getETag(bucket, object_name) != compute_s3_etag(file_name):
            download_file(bucket, file_name, object_name)
            print(object_name)

def main():
    if len(sys.argv) == 3:
        bucket = sys.argv[1]
        if checkBucket(bucket):
            restore_files(sys.argv[2], bucket)
            print("--Restore completed--")
    else:
        print("Usage: bucket-name directory-name")

if __name__ == "__main__":
    main()
