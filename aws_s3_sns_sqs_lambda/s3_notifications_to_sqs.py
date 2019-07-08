import boto3

sqs = boto3.client("sqs")
s3 = boto3.resource("s3")
dynamodb = boto3.client("dynamodb")

def create_notification(queue_name,type_of_notifiation):
    #queue = sqs.get_queue_attributes(QueueUrl="https://sqs.ap-south-1.amazonaws.com/150761330509/object_created",
    #                                 AttributeNames=['QueueArn'])
    queue = sqs.create_queue(QueueName=queue_name)
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue["QueueUrl"], AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"]
    object_created_notification = s3.BucketNotification("sree-first-bucket")
    object_created_notification.put(NotificationConfiguration={
        'QueueConfigurations': [{'QueueArn': queue_arn, "Events": type_of_notifiation}]})
    return queue["QueueUrl"]


def delete_object_notification(queue_name):
    delete_notification = s3.BucketNotification("sree-first-bucket")
    queue = sqs.create_queue(QueueName = queue_name)
    queue_arn = sqs.get_queue_attributes(QueueUrl = queue["QueueUrl"], AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    delete_notification.put(NotificationConfiguration={
        'QueueConfigurations': [{'QueueArn': queue_arn, "Events": ['s3:ObjectRemoved:Delete']}]})
    return queue["QueueUrl"]


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
            else:
                print(message)
    else:
        print("No Messages Available")
def save_to_dynamo_db(key):
    print("saving...{}".format(key))
    s3.Bucket("sree-first-bucket").download_file(key, 'download_file')
    with open("download_file", 'rb') as f:
        file = f.read()
    dynamodb.put_item(TableName = "table_1", Item = {"name":{"S":key},"file" :{'B':file}})

def delete_from_dynamo_db(key):
    print("Deleting...{}".format(key))
    #s3.Bucket("sree-first-bucket").delete_objects(Delete = {'Objects':[{"Key":key}]})
    dynamodb.delete_item(TableName ="table_1",Key = {"name":{"S":key}})

#print_messages()
#save_to_dynamo_db(object_key)