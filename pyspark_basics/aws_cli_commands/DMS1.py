
#Write a Python program using Boto3 to list all DMS replication instances in your AWS account.

import boto3

def list_dms_replication_instances():
    # Create a DMS client
    dms_client = boto3.client('dms')
    
    try:
        # Call the describe_replication_instances method
        response = dms_client.describe_replication_instances()
        
        # Extract the list of replication instances
        replication_instances = response['ReplicationInstances']
        
        if not replication_instances:
            print("No replication instances found.")
            return

        # Print details of each replication instance
        for instance in replication_instances:
            print(f"Replication Instance ID: {instance['ReplicationInstanceIdentifier']}")
            print(f"Status: {instance['ReplicationInstanceStatus']}")
            print(f"Instance Class: {instance['ReplicationInstanceClass']}")
            print(f"Engine Version: {instance['EngineVersion']}")
            print(f"Allocated Storage: {instance['AllocatedStorage']} GB")
            print("------")
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    list_dms_replication_instances()
