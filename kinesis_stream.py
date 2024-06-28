import sys
import json
import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


# snippet-start:[python.example_code.kinesis.KinesisStream.class]
class KinesisStream:
    """Encapsulates a Kinesis stream."""

    def __init__(self, name, region_name='eu-west-2', create_if_not_found=True):
        """
        :param kinesis_client: A Boto3 Kinesis client.
        """
        self.kinesis_client = self.kinesis_client = boto3.client('kinesis', region_name=region_name)
        self.name = name
        
        self.stream_exists_waiter = self.kinesis_client.get_waiter("stream_exists")

        self._load(create_if_not_found)

    # snippet-end:[python.example_code.kinesis.KinesisStream.class]

    def _clear(self):
        """
        Clears property data of the stream object.
        """
        self.name = None
        self.details = None

    def arn(self):
        """
        Gets the Amazon Resource Name (ARN) of the stream.
        """
        return self.details["StreamARN"]

    # snippet-start:[python.example_code.kinesis.CreateStream]
    def _create(self, wait_until_exists=True):
        """
        Creates a stream.

        :param wait_until_exists: When True, waits until the service reports that
                                  the stream exists, then queries for its metadata.
        """
        try:
            self.kinesis_client.create_stream(StreamName=self.name, ShardCount=1)
    
            logger.info("Created stream %s.", self.name)
            if wait_until_exists:
                logger.info("Waiting until exists.")
                self.stream_exists_waiter.wait(StreamName=self.name)
                self.describe()
        except ClientError:
            logger.exception("Couldn't create stream %s.", self.name)
            raise
        
    # snippet-end:[python.example_code.kinesis.CreateStream]

    # snippet-start:[python.example_code.kinesis.DescribeStream]
    def describe(self):
        """
        Gets metadata about a stream.

        :param name: The name of the stream.
        :return: Metadata about the stream.
        """
        try:
            response = self.kinesis_client.describe_stream(StreamName=self.name)
            self.details = response["StreamDescription"]
            logger.info("Got stream %s.", self.name)
        except ClientError:
            logger.exception("Couldn't get %s.", self.name)
            raise
        else:
            return self.details

    # snippet-end:[python.example_code.kinesis.DescribeStream]

    def _load(self, create_if_not_found):
        try:
            self.describe()
            
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                if create_if_not_found==True:
                    logger.warn("Stream not found, Creating a new stream: {self.name}")
                    self._create(wait_until_exists=True)
                
                else:
                    logger.error('Stream not found, Exiting.')
                    sys.exit()
            
            else: 
                raise err

    # snippet-start:[python.example_code.kinesis.DeleteStream]
    def delete(self):
        """
        Deletes a stream.
        """
        try:
            self.kinesis_client.delete_stream(StreamName=self.name)
            self._clear()
            logger.info("Deleted stream %s.", self.name)
        except ClientError:
            logger.exception("Couldn't delete stream %s.", self.name)
            raise

    # snippet-end:[python.example_code.kinesis.DeleteStream]

    # snippet-start:[python.example_code.kinesis.PutRecord]
    def put_record(self, data, partition_key="No"):
        """
        Puts data into the stream. The data is formatted as JSON before it is passed
        to the stream.

        :param data: The data to put in the stream.
        :param partition_key: The partition key to use for the data.
        :return: Metadata about the record, including its shard ID and sequence number.
        """
        try:
            response = self.kinesis_client.put_record(
                StreamName=self.name, Data=data, PartitionKey=partition_key
            )
            logger.info("Put record in stream %s.", self.name)
        except ClientError:
            logger.exception("Couldn't put record in stream %s.", self.name)
            raise
        else:
            return response

    # snippet-end:[python.example_code.kinesis.PutRecord]

    # snippet-start:[python.example_code.kinesis.GetRecords]
    def get_records(self, limit):
        """
        Gets records from the stream. This function is a generator that first gets
        a shard iterator for the stream, then uses the shard iterator to get records
        in batches from the stream. Each batch of records is yielded back to the
        caller until the specified maximum number of records has been retrieved.

        :param max_records: The maximum number of records to retrieve.
        :return: Yields the current batch of retrieved records.
        """
        try:
            records_found = 0
            while not records_found:
                response = self.kinesis_client.get_shard_iterator(
                    StreamName=self.name,
                    ShardId=self.details["Shards"][0]["ShardId"],
                    ShardIteratorType="LATEST",
                )
                shard_iter = response["ShardIterator"]
                
                response = self.kinesis_client.get_records(
                    ShardIterator=shard_iter, Limit=limit
                )
                # print(response)
                records = response["Records"]
                if len(records) > 0: records_found = 1
                shard_iter = response["NextShardIterator"]
                
                logger.info("Got %s records.", len(records))
                
            return records
        except ClientError:
            logger.exception("Couldn't get records from stream %s.", self.name)
            raise
    # snippet-end:[python.example_code.kinesis.GetRecords]

    def get_records_iter(self, shard_iter_type = "LATEST"):
        """
        Gets records from the stream. This function is a generator that first gets
        a shard iterator for the stream, then uses the shard iterator to get records
        in batches from the stream. Each batch of records is yielded back to the
        caller until the specified maximum number of records has been retrieved.

        :param max_records: The maximum number of records to retrieve.
        :return: Yields the current batch of retrieved records.
        """
        try:
            response = self.kinesis_client.get_shard_iterator(
                StreamName=self.name,
                ShardId=self.details["Shards"][2]["ShardId"],
                ShardIteratorType=shard_iter_type,
            )
            shard_iter = response["ShardIterator"]

            while response:
                response = self.kinesis_client.get_records(
                    ShardIterator=shard_iter, Limit=1
                )
                shard_iter = response["NextShardIterator"]
                records = response["Records"]
                # logger.info("Got %s records.", len(records))
                yield records
            logger.exception("Shard closed. Couldn't get records from stream %s.", self.name)
            raise 
            # TODO: Now the consumer closes when the shard closes. It should keep looking for new data in the stream and auto start if there is data
        except ClientError:
            logger.exception("Couldn't get records from stream %s.", self.name)
            raise

    