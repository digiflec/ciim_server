import boto3
from botocore.exceptions import ClientError

from kinesis_stream import KinesisStream

kinesis_client = boto3.client("kinesis")

stream = KinesisStream('office3-outsight-objects', create_if_not_found=False)


records = stream.get_records(1000)

for record in records:
    print(record)

