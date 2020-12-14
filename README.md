# s3BucketCopyandBackup
Python program that copy all directories and its files from the given AWS S3 Bucket. It uploads a directory and its files to the given AWS S3 Bucket as well.

## Requirement Dependency

```bash
pip install boto3
pip install hashlib
```

## Usage
```bash
python3 backup.py local-directory-name bucket-name
python3 restore.py bucket-name local-directory-name
```

## License
[MIT](https://choosealicense.com/licenses/mit/)

