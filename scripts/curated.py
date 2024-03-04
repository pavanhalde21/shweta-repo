import sys
import os

print("Before Append:", sys.path)
sys.path.append(os.path.join(os.getcwd(), 'utils'))
print("After Append:", sys.path)

from utils.awsUtils import AWSConnector

aws_access_key = "AKIAURNGZACMWT44T5OM"
aws_secret_key = "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"

aws_obj = AWSConnector(aws_access_key, aws_secret_key, client='s3', region='us-east-1')

aws_obj.create_session()

s3 = aws_obj.create_aws_client()

def filter_and_upload_csv(source_bucket, source_key_prefix, destination_bucket, destination_key_prefix, min_size=128 * 1024):

    # List objects in the source directory
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_key_prefix)

    # Check if any objects are found
    if 'Contents' in response:
        for obj in response['Contents']:
            # Extract file name and size
            file_name = obj['Key'].split('/')[-1]
            file_size = obj['Size']

            # Filter only CSV files with size greater than min_size
            if file_name.lower().endswith('.csv') and file_size > min_size:
                # Download the CSV file
                response = s3.get_object(Bucket=source_bucket, Key=obj['Key'])
                file_content = response['Body'].read()

                # Upload CSV file to the destination bucket
                destination_key = f"{destination_key_prefix}/{file_name}"
                s3.put_object(Bucket=destination_bucket, Key=destination_key, Body=file_content)

# Example usage
source_bucket = 'pavan-aws-bw-requirements'
source_key_prefix = 'shweta/unzip_files'
destination_bucket = 'pavan-aws-bw-requirements'
destination_key_prefix = 'shweta/curate'
filter_and_upload_csv(source_bucket, source_key_prefix, destination_bucket, destination_key_prefix)

