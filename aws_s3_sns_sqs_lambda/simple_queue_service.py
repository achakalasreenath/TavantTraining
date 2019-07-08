import boto3

ACCESS_KEY = "AKIASGGQTK5GTY55WI2G"
SECRET_ACCESS_KEY = "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"

sqs = boto3.resource('sqs')

queue = sqs.create_queue(QueueName = "queue_1", Attributes ={'delay': 5})

print(queue.get_queue_by_name)