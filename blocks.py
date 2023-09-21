import os 
from prefect_aws import AwsCredentials, ECSTask, S3Bucket
from loguru import logger

AWS_CLUSTER_NAME = 'flapjack-octopus'
CPU = 2048 #2 vCPU
MEM = 16384 #16384 MiB

def set_aws_block() -> None:
    aws_key = os.environ.get('AWS_KEY')
    aws_secret = os.environ.get('AWS_SECRET')

    if aws_key is None or aws_secret is None:
        raise ValueError('environmental secret variables cannot be None')

    AwsCredentials(
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name="us-west-2"
    ).save("my-aws-creds", overwrite=True)
    logger.info("Saved prefect AWS credential block")


def set_ecs_block() -> None:
    aws_credentials = AwsCredentials.load('my-aws-creds')

    ecs_task = ECSTask(
        #task_definition_arn='arn:aws:ecs:us-west-2:688122822031:task-definition/prefect__stream-ingest__stream-ingest-deployment:30', #TODO
        image="jdduprey/data-vent:latest",
        aws_credentials=aws_credentials,
        cluster=AWS_CLUSTER_NAME,
        auto_deregister_task_definition=False,
        cpu=CPU,
        memory=MEM,
        #launch_type='FARGATE',
        #stream_output=True,
        #execution_role_arn='arn:aws:iam::779369213159:role/execution-role-ecs'
        env={
            'AWS_KEY': os.environ.get('AWS_KEY'),
            'AWS_SECRET': os.environ.get('AWS_SECRET'),
            'OOI_USERNAME': os.environ.get('OOI_USERNAME'),
            'OOI_TOKEN': os.environ.get('OOI_TOKEN'),
        }

    )
    ecs_task.save("my-ecs-task", overwrite=True)
    logger.info(f"Saved prefect ECS task block with | {CPU} cpu | {MEM} MiB memory |")

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
