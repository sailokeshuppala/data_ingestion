
import boto3
import logging
import os
from botocore.exceptions import ClientError, EndpointConnectionError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import Optional
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

class R2Client:
    def __init__(self):
        self.client = boto3.client(
            's3',
            endpoint_url=os.getenv("R2_ENDPOINT"),
            aws_access_key_id=os.getenv("R2_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("R2_SECRET_KEY"),
            config=boto3.session.Config(retries={'max_attempts': 3})
        )
        self.bucket = os.getenv("R2_BUCKET")
        if not self.bucket:
            raise ValueError("R2_BUCKET not set in environment variables")

    def list_audio_files(self) -> list:
        paginator = self.client.get_paginator('list_objects_v2')
        files = []
        try:
            for page in paginator.paginate(Bucket=self.bucket):
                for obj in page.get('Contents', []):
                    files.append(obj['Key'])
            return files
        except (ClientError, EndpointConnectionError) as e:
            logger.error(f"Error listing audio files: {e}")
            raise e

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        retry=retry_if_exception_type((ClientError, EndpointConnectionError))
    )
    def stream_file(self, key: str, chunk_size: Optional[int] = 1024 * 1024) -> bytes:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            body = response['Body']
            chunks = []
            while chunk := body.read(chunk_size):
                chunks.append(chunk)
            return b''.join(chunks)
        except (ClientError, EndpointConnectionError) as e:
            logger.error(f"Error streaming file {key}: {e}")
            raise e
  