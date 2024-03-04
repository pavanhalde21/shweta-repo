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

aws_client = aws_obj.create_aws_client()

aws_client.upload_file(r"D:\_02_CS\10_Feb_BW_Project\01_Final\shweta\scripts\saving_job.py", 'pavan-aws-bw-requirements', 'shweta/scripts/saving_job.py')
