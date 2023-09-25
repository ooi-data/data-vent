import os 
from prefect_aws import AwsCredentials
from loguru import logger

AWS_CLUSTER_NAME = 'flapjack-octopus'

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

# *** DEPRECATED ***
def set_ecs_block() -> None:
    logger.warning("Use of ECS blocks in the data-harvest is deprecated, infrastructure is defined by prefect work pool")

if __name__ == "__main__":
    set_aws_block()
    set_ecs_block()
