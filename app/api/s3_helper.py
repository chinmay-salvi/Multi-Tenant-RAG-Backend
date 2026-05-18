from aiobotocore.session import get_session
from botocore.exceptions import ClientError

from app.core.config import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, S3_BUCKET_NAME


# Define the async function to upload a file to S3
async def upload_file_to_s3(
    uploaded_file_path: str,
    s3_key: str,
) -> str:
    # Create a new session for aiobotocore
    session = get_session()

    async with session.create_client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    ) as s3_client:
        # Upload the file to S3
        with open(uploaded_file_path, "rb") as file_data:
            response = await s3_client.put_object(
                Body=file_data, Bucket=S3_BUCKET_NAME, Key=s3_key
            )

        # Construct the S3 object URL
        return f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"


async def create_presigned_url(s3_key, expiration=43200):
    """Generate a presigned URL to share an S3 object

    :param s3_key: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Create a new session for aiobotocore
    session = get_session()

    async with session.create_client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    ) as s3_client:
        try:
            response = await s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": S3_BUCKET_NAME, "Key": s3_key},
                ExpiresIn=expiration,
            )
        except ClientError as e:
            return None

    # The response contains the presigned URL
    return response
