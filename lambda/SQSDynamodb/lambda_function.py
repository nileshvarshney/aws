import json
import boto3
import boto3
from datetime import datetime

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    no_of_message = str(len(event['Records']))
    print(f'No of message found :{no_of_message}')
    
    # write all the SQS message as it in Dynamodb.
    for message in event['Records']:
        print(message)
        table = dynamodb.Table('Message')

        resonse = table.put_item(
           Item={
               "MessageId": message['messageId'],
                "Body": message['body'],
                "loading_time": datetime.now().isoformat()
                }
            )
    
        print(f"Wrote message to DynamoDB :{json.dumps(resonse)}")

# SQL sample data
# {
#   "Records": [
#     {
#       "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
#       "receiptHandle": "MessageReceiptHandle",
#       "body": "Hello from SQS!",
#       "attributes": {
#         "ApproximateReceiveCount": "1",
#         "SentTimestamp": "1523232000000",
#         "SenderId": "123456789012",
#         "ApproximateFirstReceiveTimestamp": "1523232000001"
#       },
#       "messageAttributes": {},
#       "md5OfBody": "{{{md5_of_body}}}",
#       "eventSource": "aws:sqs",
#       "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:MyQueue",
#       "awsRegion": "us-east-1"
#     }
#   ]
# }