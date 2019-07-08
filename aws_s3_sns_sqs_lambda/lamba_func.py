import boto3
sqs = boto3.client("sqs")
s3 = boto3.resource("s3")
dynamodb = boto3.client("dynamodb")


def check_event_messages(queue_url):
    messages =  sqs.receive_message(QueueUrl = queue_url, MessageAttributeNames = ['Body',"ReceiptHandle"],MaxNumberOfMessages=10,WaitTimeSeconds=20)
    if "Messages" in messages:
        for message in messages["Messages"]:
            message_body = eval(message["Body"])
            if "Records" in message_body:
                for record in message_body["Records"]:
                    if record["eventName"] == "ObjectCreated:Put":
                        save_to_dynamo_db(record["s3"]["object"]["key"])
                        sqs.delete_message(QueueUrl = queue_url,ReceiptHandle= message["ReceiptHandle"])
                    if record["eventName"] == "ObjectRemoved:Delete":
                        delete_from_dynamo_db(record["s3"]["object"]["key"])
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])


def save_to_dynamo_db(key):
    s3.Bucket("sree-second-bucket-1").download_file(key, 'download_file')
    with open("download_file", 'rb') as f:
        file = f.read()
    dynamodb.put_item(TableName = "table_1", Item = {"name":{"S":key},"file" :{'B':file}})


def delete_from_dynamo_db(key):
    dynamodb.delete_item(TableName ="table_1",Key = {"name":{"S":key}})

while(True):
    check_event_messages("https://sqs.ap-south-1.amazonaws.com/150761330509/save_or_delete_object")