import boto3

class AWSConnector:
    def __init__(self, aws_access_key, aws_secret_key, client='s3', region='us-east-1'):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.region = region
        self.aws_client = client
        self.session = self.create_session()
        self.aws_client_conn = self.create_aws_client()

    def create_session(self):
        """
        Create an AWS session using the provided credentials and region.
        """
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.region
        )
        return session

    def create_aws_client(self):
        """
        Create an AWS client using the AWS session.
        """
        aws_client_conn = self.session.client(self.aws_client)
        return aws_client_conn

