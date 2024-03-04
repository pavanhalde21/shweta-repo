
import sys
import os

print("Before Append:", sys.path)
sys.path.append(os.path.join(os.getcwd(), 'utils'))
print("After Append:", sys.path)

from utils.awsUtils import AWSConnector

aws_access_key = "AKIAURNGZACMWT44T5OM"
aws_secret_key = "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"

aws_obj = AWSConnector(aws_access_key, aws_secret_key, client='glue', region='us-east-1')

aws_obj.create_session()

aws_client = aws_obj.create_aws_client()

response = aws_client.create_job(
    Name="seprating_dataset_Job",
    Role="arn:aws:iam::312271765657:role/pavan-glue-can-access-s3",
    Command={
        'Name': 'glueetl',
        'ScriptLocation': "s3://pavan-aws-bw-requirements/shweta/scripts/saving_job.py"
    },
    GlueVersion='4.0',
    WorkerType='G.1X',
    NumberOfWorkers=2,
    ExecutionProperty={
        'MaxConcurrentRuns': 1
    }
)

response = aws_client.start_job_run(JobName='seprating_dataset_Job')
job_run_id = response['JobRunId']
print(f"Job run started with ID: {job_run_id}")


