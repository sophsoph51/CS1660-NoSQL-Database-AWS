import boto3
     
s3 =boto3.resource('s3',
    aws_access_key_id='',
    aws_secret_access_key=''
    )
    
try:
    s3.create_bucket(Bucket='proj2-name', CreateBucketConfiguration={'LocationConstraint': 'us-east-2'})
except:
    print("this may already exist")
    
bucket =s3.Bucket("proj2-name")

bucket.Acl().put(ACL = 'public-read')

dyndb =boto3.resource(
    'dynamodb',region_name='us-east-2',
    aws_access_key_id='',
    aws_secret_access_key='')
    
try:
    table = dyndb.create_table(
        TableName = 'DataTable',
        KeySchema = [
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    table= dyndb.Table("DataTable")
    
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)

import csv
urlbase = "https://s3-us-east-2.amazonaws.com/proj2-name/"
with open('experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print(item)
        body = open("exp1.txt", 'rb')
        s3.Object('proj2-name', item[3]).put(Body=body)
        md=s3.Object('proj2-name', item[3]).Acl().put(ACL='public-read')
        
        url = urlbase+item[3]
        metadata_item= {'PartitionKey': item[0], 'RowKey': item[1], 'description': item[4], 'date': item[2], 'url':url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")
    

response = table.get_item(
    Key={
        'PartitionKey': 'experiment3',
        'RowKey': '4'
    }
)
item = response['Item']
print(item)
print(response)

        





















        
        