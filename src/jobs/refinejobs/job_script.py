import boto3
import os
from io import BytesIO
from zipfile import ZipFile

# Initialize S3 client
s3 = boto3.client('s3')

def unzip_and_upload(bucket_name, key, extract_to_bucket, extract_to_prefix):
    # Download the zip file from S3
    zip_obj = s3.get_object(Bucket=bucket_name, Key=key)
    zip_data = BytesIO(zip_obj['Body'].read())

    # Unzip the file
    with ZipFile(zip_data, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            file_name = file_info.filename
            file_size = file_info.file_size
            if file_name.endswith('.csv') and file_size > 128 * 1024:

                csv_data = zip_ref.read(file_name)

                s3.put_object(
                    Bucket=extract_to_bucket,
                    Key=os.path.join(extract_to_prefix, file_name),
                    Body=csv_data
                )

def main():
    # Specify S3 locations
    source_bucket = 'pavan-aws-bw-requirements'
    source_key = 'shweta/zip_files/Airline Loyalty Program.zip'
    extract_to_bucket = 'pavan-aws-bw-requirements'
    extract_to_prefix = 'shweta/unzip_files/'

    # Unzip and upload CSV files
    unzip_and_upload(source_bucket, source_key, extract_to_bucket, extract_to_prefix)

if __name__ == "__main__":
    main()
