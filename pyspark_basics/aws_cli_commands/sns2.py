#Write a Python script to publish a message to an SNS topic by specifying the topic ARN and a custom message.

import boto3

def publish_to_sns(topic_arn, message):
    # Create an SNS client
    sns_client = boto3.client('sns')

    try:
        # Publish a message to the specified SNS topic
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message
        )
        print("Message published successfully!")
        print("Message ID:", response['MessageId'])
    except Exception as e:
        print("Error publishing message:", e)

if __name__ == "__main__":
    # Specify your SNS topic ARN and message here
    topic_arn = 'arn:aws:sns:ap-south-1:257394475874:sai-21'
    message = 'Hello, this is a test message from Python!'

    # Call the function to publish the message
    publish_to_sns(topic_arn, message)
