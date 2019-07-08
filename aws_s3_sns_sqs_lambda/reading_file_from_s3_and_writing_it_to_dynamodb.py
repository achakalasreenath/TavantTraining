import boto3

s3 = boto3.resource('s3')
dynamodb = boto3.resource("dynamodb")


bucket = s3.Bucket('sree-first-bucket')
s3.Bucket("sree-first-bucket").download_file('test.csv','test_download.csv')

with open(file = 'test_download.csv', mode ='rt') as f:
    column_names = []
    for line in f:
        for word in line.split(','):
            column_names.append(word)
            print(column_names)



table1 = dynamodb.create_table(TableName = 'table_1', KeySchema=[
    {
        'AttributeName': column_names[0],
        'KeyType':"HASH"
    },
    {
        'AttributeName': column_names[1],
        'KeyType': "RANGE"
    }

],
    AttributeDefinitions=[
    {
        'AttributeName': column_names[0],
        'AttributeType': "S"
    },
    {
        'AttributeName': column_names[1],
        'AttributeType': "S"
    }
],
    ProvisionedThroughput={
                                   'ReadCapacityUnits': 5,
                                   'WriteCapacityUnits': 5
    },
    StreamSpecification ={
        "StreamEnabled":True,
        "StreamViewType":'NEW_AND_OLD_IMAGES'
    }
                               )
