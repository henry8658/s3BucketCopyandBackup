import boto3
import os
import sys
import hashlib
from botocore.exceptions import ClientError

def make_dir_inBucket(bucket, dir_list):
    s3 = boto3.client('s3')
    bucket_name = bucket
    print("----Following directories are created in " + bucket)
    for folder_name in dir_list:
        s3.put_object(Bucket=bucket_name, Key=(folder_name+'/'))
        print(folder_name)

def upload_file(file_name, bucket, object_name=None):

    if object_name is None:
        object_name = file_name

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except Exception as e:
        print(e)
        return False
    return True

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

def create_bucket(bucket):
    s3 = boto3.resource('s3')
    client = boto3.client('s3')
    print("creating bucket.. " + bucket)
    session = boto3.Session(profile_name='default')
    region_name = session.region_name
    if region_name is None:
        region_name = 'us-west-2'
    try:
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={'LocationConstraint': region_name})
        waiter = client.get_waiter('bucket_exists')
    except ClientError as e:
        print(e)
        print("Bucket creation fail")
        return False

    print("bucket created")
    return True

def get_bucket_list(bucket):
    bucket_dir_set = set()
    bucket_file_dict = {}

    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket)

    for obj in response.get('Contents', []):
        if obj['Key'].endswith('/'):
            bucket_dir_set.add(obj['Key'].rstrip('/'))
        else:
            bucket_file_dict[obj['Key']] = obj['ETag']
    return (bucket_dir_set, bucket_file_dict)

def get_local_list(dirPath, bucket_dir_set):
    local_dir_list = []
    local_file_dict = {}

    for (root, dirs, files) in os.walk(dirPath, topdown=True):
        path = os.path.relpath(root, '.')
        if path not in bucket_dir_set:
            local_dir_list.append(path)

        for file in files:
            if file is not None:
                local_file_dict[path + '/' + file] = compute_s3_etag(path + '/' + file)
    return (local_dir_list, local_file_dict)

def backup_files(bucket, bucket_file_dict, local_file_dict):
    print("----Following files are uploaded to " + bucket)
    for local_file in local_file_dict:
        if local_file not in bucket_file_dict:
            upload_file(local_file, bucket)
            print(local_file)
        elif bucket_file_dict[local_file] != local_file_dict[local_file]:
            upload_file(local_file, bucket)
            print(local_file)

def main():
    if len(sys.argv) == 3:
        bucket=sys.argv[2]
        dir=sys.argv[1]
        if os.path.isdir(dir):
            isValidBucket = checkBucket(bucket)

            if not isValidBucket:
                isValidBucket = create_bucket(bucket)

            if isValidBucket:
                bucket_tuple = get_bucket_list(bucket)
                local_tuple = get_local_list(dir, bucket_tuple[0])
                make_dir_inBucket(bucket, local_tuple[0])
                backup_files(bucket, bucket_tuple[1], local_tuple[1])
            else:
                print("Error in connecting or creating the bucket")
        else:
            print("Directory not found or is not a directory")
    else:
        print("Usage: directory-name bucket-name")

if __name__ == "__main__":
    main()
