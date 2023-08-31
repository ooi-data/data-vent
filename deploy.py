from data_vent.flow import stream_ingest, run_stream_ingest
from prefect.deployments import Deployment
from prefect_aws import ECSTask, S3Bucket

from loguru import logger
# from prefect.filesystems import GitHub #TODO maybe use github as storage?

def deploy_parent_and_child_flows() -> None:
    stream_ingest_deployment = Deployment.build_from_flow(
        flow=stream_ingest,
        name="stream-ingest-deployment",
        #output="./deployments/stream-ingest-deployment.yaml",
        infrastructure=ECSTask.load("my-ecs-task"),
        storage=S3Bucket.load("my-s3-bucket"),
    )

    stream_ingest_deployment.apply()
    logger.info("Deployed child flow: see 'data_vent/deployments/stream-ingest-deployment.yaml'")

    run_stream_ingest_deployment = Deployment.build_from_flow(
        flow=run_stream_ingest,
        name="run-stream-ingest-deployment",
        #output="./deployments/run-stream-ingest-deployment.yaml",
        infrastructure=ECSTask.load("my-ecs-task"),
        storage=S3Bucket.load("my-s3-bucket"),
    )

    run_stream_ingest_deployment.apply()
    logger.info("Deployed parent flow: see 'data_vent/deployments/run-stream-ingest-deployment.yaml'")


if __name__ == "__main__":
    deploy_parent_and_child_flows()