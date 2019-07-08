import boto3

dynamodb = boto3.resource("dynamodb")

table = dynamodb.create_table(TableName = "test_table", KeySchema=[
    {
        'AttributeName':"column1",
        'KeyType':"HASH"
    },
    {
        'AttributeName':"column2",
        'KeyType':"RANGE"
    },
    {
        'AttributeName': "column3",
        'KeyType': "RANGE"
    }
],
                              AttributeDefinitions=[
                                  {
                                      'AttributeName': 'column1',
                                      'AttributeType': "S"
                                  },
                                  {
                                      'AttributeName': 'column2',
                                      'AttributeType': "S"
                                  },
                                  {
                                      'AttributeName': "column3",
                                      'AttributeType': "S"
                                  }
                              ],
                              ProvisionedThroughput={
                                  'ReadCapacityUnits': 5,
                                  'WriteCapacityUnits': 5
                              }
                              )