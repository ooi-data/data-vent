import os 
import sys
from prefect_aws import AwsCredentials, ECSTask, S3Bucket
from loguru import logger

AWS_CLUSTER_NAME = 'flapjack-octopus'

def set_aws_block() -> None:
    aws_key = os.environ.get('AWS_KEY')
    aws_secret = os.environ.get('AWS_SECRET')


    AwsCredentials(
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name="us-west-2"
    ).save("my-aws-creds", overwrite=True)
    logger.info("Saved prefect AWS credential block")


def set_ecs_block() -> None:
    aws_credentials = AwsCredentials.load('my-aws-creds')

    ecs_task = ECSTask(
        #TODO need docker image for data-vent
        image="jdduprey/data-vent:latest",
        aws_credentials=aws_credentials,
        cluster=AWS_CLUSTER_NAME,
        #stream_output=True,
        #execution_role_arn='arn:aws:iam::779369213159:role/execution-role-ecs'

    )
    ecs_task.save("my-ecs-task", overwrite=True)
    logger.info("Saved prefect ECS task block")

    # S3 bucket
    bucket_name = 'ooi-prefect-code-bucket'
    # s3_client = aws_credentials.get_s3_client()
    # s3_client.create_bucket(ss
    #     Bucket=bucket_name,
    #     CreateBucketConfiguration={"LocationConstraint": aws_credentials.region_name}
    # )
    s3_bucket = S3Bucket(
        bucket_name=bucket_name,
        credentials=aws_credentials,
    )
    s3_bucket.save("my-s3-bucket", overwrite=True)
    logger.info("Saved prefect S3 code storage block")


if __name__ == "__main__":
    set_aws_block()
    set_ecs_block()