import boto3

s3 = boto3.resource('s3')
dynamodb = boto3.resource("dynamodb")
ACCESS_KEY = "AKIASGGQTK5GTY55WI2G"
SECRET_ACCESS_KEY = "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"

for bucket in s3.buckets.all():
    print(bucket.name)
data = open(file = r'C:\Users\achakala.sreenath\Downloads\test.csv', mode='rb' )
#s3.Bucket("sree-first-bucket").put_object(Key="test.csv",Body = data)

