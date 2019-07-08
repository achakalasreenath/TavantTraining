import boto3

db = boto3.client("dynamodbstreams")
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client("s3")

streams = db.list_streams(TableName="table_1")

previous_records = s3.get_object(Bucket="sree-first-bucket",Key = "previous_records")["Body"].read().decode("utf-8")
stream_arn = streams['Streams'][0]['StreamArn']
stream = db.describe_stream(StreamArn = stream_arn)
print(stream)
#shard_id = stream["StreamDescription"]["Shards"][1]["ShardId"]
for shard in stream["StreamDescription"]["Shards"]:
    count = 0
    sequence_number_range = shard["SequenceNumberRange"]
    current_shard_iterator = db.get_shard_iterator(StreamArn = stream_arn, ShardId = shard["ShardId"], ShardIteratorType='AT_SEQUENCE_NUMBER',SequenceNumber = sequence_number_range["StartingSequenceNumber"])["ShardIterator"]
    while(current_shard_iterator != None and count <100):
        records = db.get_records(ShardIterator = current_shard_iterator)
        if "NextShardIterator" in records:
            current_shard_iterator = records["NextShardIterator"]
            if records["Records"] != []:
                    print(records["Records"])
        count += 1







