#Write a Python script to send a message to a specific SQS queue and print the message ID.

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def send_message_to_sqs(queue_url, message_body):
    # Create an SQS client
    sqs = boto3.client('sqs')

    try:
        # Send a message to the specified SQS queue
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body
        )
        
        # Print the message ID
        print("Message sent! Message ID:", response['MessageId'])
    except NoCredentialsError:
        print("Error: No AWS credentials found.")
    except PartialCredentialsError:
        print("Error: Incomplete AWS credentials.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Replace with your SQS queue URL
    queue_url = 'https://sqs.ap-south-1.amazonaws.com/257394475874/sai-21'
    message_body = 'Hello, SQS! This is a test message.'

    send_message_to_sqs(queue_url, message_body)
