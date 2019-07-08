import boto3

s3 = boto3.resource("s3")
dynamodb = boto3.client("dynamodb")
sns = boto3.client("sns")
import uuid


def save_to_dynamo_db(key):
    path = "{}{}{}".format('/tmp/', key, uuid.uuid4())
    s3.Bucket("sree-second-bucket").download_file(key, path)
    # object = s3.Object("sree-second-bucket",key)
    # object_etag = object.e_tag
    # object_body = eval(object.get(IfMatch=object_etag)["Body"].read())
    with open(path, 'rt') as f:
        file = f.read()
    for line in file:
        words = line.split(',')
        if words[5] == "C" or words[5] == 'U':
            dynamodb.put_item(TableName="table_1", Item={"name": {"S": words[0]}, "first_name": {'S': words[1]},
                                                         "second_name": {"S", words[2]},
                                                         "mobile_no": {"S", words[3]}, "email_id": {"S", words[4]},
                                                         "flag": {"S", words[5]}})
            sns.publish(TopicArn="arn:aws:sns:ap-south-1:150761330509:topic_1",
                        Message="username \'{}\' has been created".format(words[1]))
        if words[5] == "D":
            dynamodb.update_item(TableName="table_1", Key={"name": {"S": words[1]}},
                                 AttributeUpdates={"flag": {"Value": {"S": words[5]}}})
            sns.publish(TopicArn="arn:aws:sns:ap-south-1:150761330509:topic_1",
                        Message="username \'{}\' has been deleted".format(words[1]))

    # dynamodb.put_item(TableName="table_1",Item = {"name":{"S":key},"username":{"S":object_body["username"]}})


def delete_from_dynamo_db(key):
    items_to_be_deleted = dynamodb.query(TableName="table_1",
                                         IndexName="file_name-index",
                                         Select="SPECIFIC_ATTRIBUTES",
                                         ProjectionExpression="#f",
                                         KeyConditionExpression="#f = :filename",
                                         ExpressionAttributeNames={"#f": "file_name"},
                                         ExpressionAttributeValues={":filename": {"S": key}})["Items"]
    print(items_to_be_deleted)
    for item in items_to_be_deleted:
        dynamodb.delete_item(TableName="table_1", Key={"name": item["name"]})

    # dynamodb.delete_item(TableName ="table_1",Key = {"name":{"S":key}})
    # sns.publish(TopicArn="arn:aws:sns:ap-south-1:150761330509:topic_1", Message="{} has been deleted".format(key))


def handler(event, context):
    if "Records" in event:
        for record in event["Records"]:
            if record["eventName"] == "ObjectCreated:Put":
                save_to_dynamo_db(record["s3"]["object"]["key"])
            if record["eventName"] == "ObjectRemoved:Delete":
                delete_from_dynamo_db(record["s3"]["object"]["key"])
