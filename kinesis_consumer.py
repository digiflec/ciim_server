import boto3
from botocore.exceptions import ClientError

from kinesis_stream import KinesisStream

kinesis_client = boto3.client("kinesis")

stream = KinesisStream('office3-cron-objects', create_if_not_found=False)


records_gen = stream.get_records_iter()

for record in records_gen:
    print(record)

