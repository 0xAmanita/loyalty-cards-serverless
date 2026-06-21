import os
import json
import time
import uuid
import boto3
import urllib
import codecs
import csv
import string
import random
from decimal import Decimal

IS_OFFLINE = os.environ.get("IS_OFFLINE")

if IS_OFFLINE:
    dynamodb = boto3.resource(
        "dynamodb",
        endpoint_url="http://localhost:8000",
        region_name="ap-southeast-1",
        aws_access_key_id="local",
        aws_secret_access_key="local"
    )
else:
    dynamodb = boto3.resource("dynamodb")

table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])

ALLOWED_UPDATE_FIELDS = {"customer_name", "card_number", "points"}

# Initialize SQS
sqs = boto3.resource('sqs', region_name='ap-southeast-1')
queue = sqs.get_queue_by_name(QueueName='yldevier-loyalty-cards-sqs')

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body, cls=DecimalEncoder),
    }


# POST /loyalty-cards
def create(event, context):
    try:
        data = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return response(400, {"error": "Invalid JSON body"})

    if not data.get("customer_name") or not data.get("card_number"):
        return response(
            400, {"error": "customer_name and card_number are required"}
        )

    item = {
        "id": str(uuid.uuid4()),
        "customer_name": data["customer_name"],
        "card_number": data["card_number"],
        "points": data.get("points", 0),
        "created_at": int(time.time()),
    }

    table.put_item(Item=item)

    # SQS
    try:
        queue.send_message(MessageBody=json.dumps(item))
        print(f"Sent Loyalty Card {item['id']} to SQS.")
    except Exeption as e:
        print(f'Error sending message to SQS: {str(e)}')

    # CloudWatch Logs
    logs_client = boto3.client('logs', region_name='ap-southeast-1')
    LOG_GROUP = "LoyaltyCardsEventLogGroup-yldevier"
    LOG_STREAM = "LoyaltyCardsEventStream-yldevier"

    try:
        logs_client.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass

    # Push Event Log to CloudWatch
    log_event = {
        "event": "loyalty_card_created",
        "pid": item.get("id"),
        "data": item
    }

    logs_client.put_log_events(
        logGroupName=LOG_GROUP,
        logStreamName=LOG_STREAM,
        logEvents = [
            {
                'timestamp': int(time.time() * 1000),
                'message': json.dumps(log_event, cls=DecimalEncoder)
            }
        ]
    )
    # Output CloudWatch Log
    print("Loyalty Card creation event logged in CloudWatch.")

    # Debug
    print(f'Loyalty Card Created: {item}')
    return response(201, item)


# GET /loyalty-cards
def get_all(event, context):
    items = []
    kwargs = {}

    while True:
        result = table.scan(**kwargs)
        items.extend(result.get("Items", []))
        last_key = result.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key

    # Debug
    print(f'Loyalty Cards: {items}')
    return response(200, items)


# GET /loyalty-cards/{id}
def get_one(event, context):
    card_id = event["pathParameters"]["id"]
    result = table.get_item(Key={"id": card_id})
    item = result.get("Item")

    if not item:
        return response(404, {"error": "Loyalty card not found"})

    # Debug
    print(f'Individual Loyalty Card: {item}')
    return response(200, item)


# PUT /loyalty-cards/{id}
def update(event, context):
    card_id = event["pathParameters"]["id"]

    try:
        data = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return response(400, {"error": "Invalid JSON body"})

    existing = table.get_item(Key={"id": card_id}).get("Item")
    if not existing:
        return response(404, {"error": "Loyalty card not found"})

    update_fields = {k: v for k, v in data.items() if k in ALLOWED_UPDATE_FIELDS}
    if not update_fields:
        return response(400, {"error": "No valid fields to update"})

    expr = "SET " + ", ".join(f"#{k} = :{k}" for k in update_fields)
    names = {f"#{k}": k for k in update_fields}
    values = {f":{k}": v for k, v in update_fields.items()}

    table.update_item(
        Key={"id": card_id},
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )

    updated = table.get_item(Key={"id": card_id})["Item"]

    # Debug
    print(f'Updated: {updated}')
    return response(200, updated)


# DELETE /loyalty-cards/{id}
def delete(event, context):
    card_id = event["pathParameters"]["id"]

    existing = table.get_item(Key={"id": card_id}).get("Item")
    if not existing:
        return response(404, {"error": "Loyalty card not found"})
    debug_var = table.delete_item(Key={"id": card_id})

    # Debug
    print(f'Deleted: {debug_var}')
    table.delete_item(Key={"id": card_id})

    return response(200, {"message": f"Loyalty card {card_id} deleted"})

def batch_create_loyalty_cards(event, context):
    print("file uploaded trigger")
    print(event)
    
    print("Extract file location from event payload")
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    localFilename = f'/tmp/{key}'
    s3_client = boto3.client('s3', region_name='ap-southeast-1')
    
    print("downloaded file to /tmp folder")
    s3_client.download_file(bucket, key, localFilename)
    
    print("reading CSV file and looping it over...")
    
    with open(localFilename, 'r') as f:
        csv_reader = csv.DictReader(f, skipinitialspace=True)
        required_keys = ["customer_name", "card_number"]

        for row in csv_reader:
            item = {
                "id": str(uuid.uuid4()),
                "card_number": row.get("card_number", "").strip(),
                "customer_name": row.get("customer_name", "").strip(),
            }
            table.put_item(Item=item)
    
    print("All done!")
    return {}

def generate_code(prefix, string_length):
    letters = string.ascii_uppercase
    return prefix + ''.join(random.choice(letters) for i in range (string_length))

# Receive Messages from SQS
def sqs_trigger(event, context):
    print(f'Event: {event}')
    print(f'Context: {context}')

    fieldnames = ["customer_name", "card_number", "points", "id", "created_at"]

    file_randomized_prefix = generate_code("yldevier", 8)
    file_name = f'/tmp/product_created{file_randomized_prefix}.csv'
    bucket = "yldevier-loyalty-card-csv-pipeline"
    object_name = f'product_created_{file_randomized_prefix}.csv'

    with open(file_name, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        for payload in event["Records"]:
            json_payload = json.loads(payload["body"])
            writer.writerow(json_payload)

    s3_client = boto3.client('s3')
    response = s3_client.upload_file(file_name, bucket, object_name)
        
    print("All done!")
    return {}
