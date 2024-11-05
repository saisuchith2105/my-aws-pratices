#Write a Python program using Boto3 to list all SQS queues in your AWS account and print their URLs.

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def list_sqs_queues():
    try:
        # Create an SQS client
        sqs = boto3.client('sqs')

        # Retrieve a list of queues
        response = sqs.list_queues()

        # Check if any queues exist
        if 'QueueUrls' in response:
            print("List of SQS Queues:")
            for queue_url in response['QueueUrls']:
                print(queue_url)
        else:
            print("No SQS queues found.")

    except NoCredentialsError:
        print("Credentials not available.")
    except PartialCredentialsError:
        print("Incomplete credentials provided.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    list_sqs_queues()
