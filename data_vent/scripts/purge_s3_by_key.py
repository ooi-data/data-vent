import boto3

s3 = boto3.client('s3')

BUCKET = 'ooi-rca-qaqc-test'
DELETION_TAG = 'foo'
DRY_RUN = True

def purge_s3_by_key(BUCKET, DELETION_TAG, DRY_RUN):
    # List objects in the bucket
    response = s3.list_objects_v2(Bucket=BUCKET)
    # Check if the bucket has objects
    for obj in response['Contents']:
        # Check if the object key contains the string 'deploy'
        if DELETION_TAG in obj['Key']:
            if DRY_RUN:
                print(f"Could delete s3://{BUCKET}/{obj['Key']}")
            else:
                print(f"Deleting s3://{BUCKET}/{obj['Key']}")
                s3.delete_object(Bucket=BUCKET, Key=obj['Key'])

if __name__ == "__main__":
    purge_s3_by_key(BUCKET, DELETION_TAG, DRY_RUN)
