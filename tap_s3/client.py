import boto3

class S3Client:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self._session = boto3.Session(
            aws_access_key_id=aws_access_key_id, 
            aws_secret_access_key=aws_secret_access_key)
        self._s3 = self._session.resource('s3')