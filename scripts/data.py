import sys
import os
import zipfile
import io

print("Before Append:", sys.path)
sys.path.append(os.path.join(os.getcwd(), 'utils'))
print("After Append:", sys.path)

from utils.awsUtils import AWSConnector

aws_access_key = "AKIAURNGZACMWT44T5OM"
aws_secret_key = "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"

aws_obj = AWSConnector(aws_access_key, aws_secret_key, client='s3', region='us-east-1')

aws_obj.create_session()

s3 = aws_obj.create_aws_client()

def unzip_and_upload_to_s3(source_bucket, source_key, destination_bucket, destination_key_prefix):

    # Download the zip file from S3
    obj = s3.get_object(Bucket=source_bucket, Key=source_key)

    # Unzip the file in memory
    with zipfile.ZipFile(io.BytesIO(obj['Body'].read())) as zip_ref:
        for file_name in zip_ref.namelist():
            file_content = zip_ref.read(file_name)
            # Upload each file from the zip archive to S3
            s3_key = f"{destination_key_prefix}/{file_name}"
            s3.put_object(Bucket=destination_bucket, Key=s3_key, Body=file_content)

# Example usage
source_bucket = 'pavan-aws-bw-requirements'
source_key = 'shweta/zip_files/Airline Loyalty Program.zip'
destination_bucket = 'pavan-aws-bw-requirements'
destination_key_prefix = 'shweta/unzip_files'

unzip_and_upload_to_s3(source_bucket, source_key, destination_bucket, destination_key_prefix)